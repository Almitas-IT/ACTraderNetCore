using aACTrader.Services.Admin;
using aCommons;
using aCommons.Cef;
using Ciloci.Flee;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;

namespace aACTrader.Operations.Impl
{
    public class ExpressionEvaluator
    {
        private readonly ILogger<ExpressionEvaluator> _logger;
        private readonly ExpressionContext _context;
        private readonly LogDataService _logDataService;

        public ExpressionEvaluator(ILogger<ExpressionEvaluator> logger, LogDataService logDataService)
        {
            _logger = logger;
            _logDataService = logDataService;
            _context = new ExpressionContext();
            _context.Imports.AddType(typeof(Math));
        }

        /// <summary>
        /// Calculates fund returns using ETF regression coefficients
        /// Calculates cumulative ETF returns since last nav date plus ETF intra-day returns to calculate total ETF returns
        /// Fund ETF returns are estimated as R-Squared Weighted Average of 6m, 12m and 24m returns
        /// </summary>
        /// <param name="ticker"></param>
        /// <param name="fundMaster"></param>
        /// <param name="fundForecast"></param>
        /// <param name="etfHistReturnsDict"></param>
        /// <param name="fundETFTickerDict"></param>
        /// <param name="priceTickerMap"></param>
        /// <param name="fxRatesDict"></param>
        /// <param name="securityPriceDict"></param>
        public void CalculateETFNav(
            string ticker
            , FundMaster fundMaster
            , FundForecast fundForecast
            , IDictionary<string, FundETFReturn> etfHistReturnsDict
            , IDictionary<string, HashSet<string>> fundETFTickerDict
            , IDictionary<string, string> priceTickerMap
            , IDictionary<string, FXRate> fxRatesDict
            , IDictionary<string, SecurityPrice> securityPriceDict)
        {
            try
            {
                double totalReturn = 0.0;
                double totalRSquared = 0.0;

                //clear variables from execution context
                _context.Variables.Clear();

                //get historical ETF returns (cumulative returns since reported nav date to previous trading day)
                FundETFReturn fundETFReturn;
                IDictionary<string, Nullable<double>> histETFReturns = null;
                if (etfHistReturnsDict.TryGetValue(ticker, out fundETFReturn))
                    histETFReturns = fundETFReturn.HistoricalETFReturn;

                //populate ETF returns in expression evaluator execution context
                HashSet<string> etfTickers = fundETFTickerDict[ticker];
                foreach (string etfTicker in etfTickers)
                {
                    double etfPriceRtn;

                    //if currency, then use FX rate return, else use security return
                    if ("CAD".Equals(etfTicker, StringComparison.CurrentCultureIgnoreCase) ||
                        "GBP".Equals(etfTicker, StringComparison.CurrentCultureIgnoreCase) ||
                        "EUR".Equals(etfTicker, StringComparison.CurrentCultureIgnoreCase))
                    {
                        if (fxRatesDict.TryGetValue(etfTicker, out FXRate fxRate))
                        {
                            etfPriceRtn = fxRate.FXReturn.GetValueOrDefault();
                        }
                        else
                        {
                            etfPriceRtn = 0;
                            _logger.LogDebug("Defaulting FX return for ticker: " + etfTicker + " to 0, FundTicker: " + ticker);
                        }
                    }
                    else
                    {
                        //lookup security price
                        SecurityPrice securityPrice = SecurityPriceLookupOperations.GetSecurityPrice(etfTicker, priceTickerMap, securityPriceDict);
                        if (securityPrice != null)
                        {
                            etfPriceRtn = securityPrice.PrcRtn.GetValueOrDefault();
                        }
                        else
                        {
                            etfPriceRtn = 0;
                            _logger.LogDebug("Defaulting return for ticker: " + etfTicker + " to 0, FundTicker: " + ticker);
                        }
                    }

                    //link historical ETF returns and current's day return to calculate ETF return since last reported nav date
                    if (histETFReturns != null && histETFReturns.TryGetValue(etfTicker, out double? etfHistRtn))
                        etfPriceRtn = (1.0 + etfPriceRtn) * (1.0 + etfHistRtn.GetValueOrDefault()) - 1.0;

                    string newTicker = etfTicker.Replace(" ", "_");
                    _context.Variables[newTicker] = etfPriceRtn;
                }

                //6M (regression return * r-squared)
                if (!string.IsNullOrEmpty(fundMaster.RegExp6M))
                {
                    Nullable<double> regReturn = EvaluateExpression(fundMaster.RegExp6M, fundForecast.Ticker);
                    if (regReturn.HasValue)
                    {
                        totalReturn += (regReturn.GetValueOrDefault() * fundMaster.RSqrd6M.GetValueOrDefault());
                        totalRSquared += fundMaster.RSqrd6M.GetValueOrDefault();
                    }
                }

                //12M (regression return * r-squared)
                if (!string.IsNullOrEmpty(fundMaster.RegExp12M))
                {
                    Nullable<double> regReturn = EvaluateExpression(fundMaster.RegExp12M, fundForecast.Ticker);
                    if (regReturn.HasValue)
                    {
                        totalReturn += (regReturn.GetValueOrDefault() * fundMaster.RSqrd12M.GetValueOrDefault());
                        totalRSquared += fundMaster.RSqrd12M.GetValueOrDefault();
                    }
                }

                //24M (regression return * r-squared)
                if (!string.IsNullOrEmpty(fundMaster.RegExp24M))
                {
                    Nullable<double> regReturn = EvaluateExpression(fundMaster.RegExp24M, fundForecast.Ticker);
                    if (regReturn.HasValue)
                    {
                        totalReturn += (regReturn.GetValueOrDefault() * fundMaster.RSqrd24M.GetValueOrDefault());
                        totalRSquared += fundMaster.RSqrd24M.GetValueOrDefault();
                    }
                }

                double etfReturn = totalReturn / totalRSquared;
                if (!Double.IsNaN(etfReturn) && !Double.IsInfinity(etfReturn))
                {
                    fundForecast.ETFRtn = etfReturn;
                    fundForecast.ETFNav = fundForecast.LastDvdAdjNav.GetValueOrDefault() * (1.0 + etfReturn);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error evaluating ETF regression expression for ticker: " + ticker);
                _logDataService.SaveLog("ExpressionEvaluator", "CalculateETFNav", ticker, ex.Message, "ERROR");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="expression"></param>
        /// <param name="ticker"></param>
        /// <returns></returns>
        private Nullable<double> EvaluateExpression(string expression, string ticker)
        {
            Nullable<double> result = null;
            try
            {
                IGenericExpression<double> iGenericExpression = _context.CompileGeneric<double>(expression.Replace(" ", "_"));
                result = iGenericExpression.Evaluate();
            }
            catch (Exception)
            {
                //_logger.LogError(ex, "Error evaluating ETF regression expression for ticker: " + ticker);
            }
            return result;
        }
    }
}