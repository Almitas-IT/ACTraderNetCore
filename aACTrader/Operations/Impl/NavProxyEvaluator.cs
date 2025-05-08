using aCommons;
using aCommons.Cef;
using aCommons.Utils;
using Ciloci.Flee;
using LazyCache;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;

namespace aACTrader.Operations.Impl
{
    public class NavProxyEvaluator
    {
        private readonly ILogger<NavProxyEvaluator> _logger;
        private readonly ExpressionContext _context;
        private static readonly DateTime TodaysDate = DateTime.Now.Date;

        public NavProxyEvaluator(ILogger<NavProxyEvaluator> logger)
        {
            _logger = logger;
            _context = new ExpressionContext();
            _context.Imports.AddType(typeof(Math));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ticker"></param>
        /// <param name="fundMaster"></param>
        /// <param name="fundForecast"></param>
        /// <param name="fundProxyFormula"></param>
        /// <param name="etfHistReturnsDict"></param>
        /// <param name="fxRatesDict"></param>
        /// <param name="securityPriceDict"></param>
        /// <param name="priceTickerMap"></param>
        public void Calculate(
            string ticker
            , FundMaster fundMaster
            , FundForecast fundForecast
            , FundProxyFormula fundProxyFormula
            , IDictionary<string, FundETFReturn> etfHistReturnsDict
            , IDictionary<string, FXRate> fxRatesDict
            , IDictionary<string, SecurityPrice> securityPriceDict
            , IDictionary<string, string> priceTickerMap)
        {
            try
            {
                //if (ticker.Equals("HTCF LN", StringComparison.CurrentCultureIgnoreCase))
                //    _logger.LogDebug("Ticker: " + ticker);

                _context.Variables.Clear();

                //populate ETF returns
                //get historical ETF returns (cumulative returns since fund's reported nav date to previous trading day)
                IDictionary<string, Nullable<double>> histETFReturns = null;
                if (etfHistReturnsDict.TryGetValue(ticker, out FundETFReturn fundETFReturn))
                    histETFReturns = fundETFReturn.HistoricalETFReturn;

                //populate ETF returns in expression evaluator execution context
                //link the historical ETF returns to current's day return to calculate ETF return since fund's reported nav date
                foreach (FundProxy fundProxy in fundProxyFormula.ProxyTickersWithCoefficients)
                {
                    string etfTicker = fundProxy.ETFTicker;

                    //if currency, then use FX rate return, else use security return
                    double etfRtn = 0.0;
                    if (etfTicker.Contains("Curncy", StringComparison.CurrentCultureIgnoreCase))
                    {
                        etfRtn = GetFxRate(etfTicker, fxRatesDict);
                    }
                    else if (etfTicker.Contains("PORT RTN", StringComparison.CurrentCultureIgnoreCase) || etfTicker.Contains("PORT_RTN", StringComparison.CurrentCultureIgnoreCase))
                    {
                        etfRtn = fundForecast.PortRtn.GetValueOrDefault();
                        //_logger.LogInformation("Populating Port Rtn for Fund Ticker/ETF Ticker: " + ticker + "/" + etfTicker);
                    }
                    else
                    {
                        //get security price
                        SecurityPrice securityPrice = SecurityPriceLookupOperations.GetSecurityPrice(etfTicker, priceTickerMap, securityPriceDict);
                        if (securityPrice != null)
                        {
                            etfRtn = securityPrice.PrcRtn.GetValueOrDefault();
                            if (histETFReturns != null && histETFReturns.TryGetValue(etfTicker, out double? etfHistRtn))
                                etfRtn = (1.0 + securityPrice.PrcRtn.GetValueOrDefault()) * (1.0 + etfHistRtn.GetValueOrDefault()) - 1.0;
                        }
                    }

                    string newTicker = etfTicker.Replace(" ", "_");
                    _context.Variables[newTicker] = etfRtn;

                    //if (ticker.Equals("MPV US"))
                    //    _logger.LogInformation("MPV US ETF Rtns: " + etfTicker + "/" + etfRtn);
                }

                //calculate proxy return
                Nullable<double> proxyReturn = EvaluateExpression(_context, fundProxyFormula.ProxyFormula, ticker);
                if (fundForecast.LastDvdAdjNav.HasValue && proxyReturn.HasValue)
                {
                    double leverageRatio = fundForecast.LevRatio.GetValueOrDefault();
                    double proxyAdjReturn = proxyReturn.GetValueOrDefault() * (1.0 + leverageRatio);
                    double proxyNav = fundForecast.LastDvdAdjNav.GetValueOrDefault();
                    proxyNav *= (1.0 + proxyAdjReturn);

                    if (fundForecast.AI.HasValue)
                        proxyNav += fundForecast.AI.GetValueOrDefault();

                    if (!Double.IsNaN(proxyNav) && !Double.IsInfinity(proxyNav))
                        fundForecast.ProxyNav = proxyNav;
                    if (!Double.IsNaN(proxyReturn.GetValueOrDefault()) && !Double.IsInfinity(proxyReturn.GetValueOrDefault()))
                        fundForecast.ProxyRtn = proxyReturn;
                    if (!Double.IsNaN(proxyAdjReturn) && !Double.IsInfinity(proxyAdjReturn))
                        fundForecast.AdjProxyRtn = proxyAdjReturn;

                    fundForecast.ProxyForm = fundProxyFormula.ProxyFormula;

                    //if (ticker.Equals("MPV US"))
                    //    _logger.LogInformation("MPV US: " + proxyReturn + "/" + proxyAdjReturn + "/" + proxyNav + "/" + leverageRatio);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error evaluating Proxy Formula for ticker: " + ticker);
                // LogDataService.Instance.SaveLog("NavProxyEvaluator", "Calculate", ticker, ex.Message, "ERROR");
            }
        }

        private Nullable<double> EvaluateExpression(ExpressionContext context, string expression, string ticker)
        {
            Nullable<double> result = null;
            try
            {
                IGenericExpression<double> eGeneric = context.CompileGeneric<double>(expression.Replace(" ", "_"));
                result = eGeneric.Evaluate();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error evaluating the expression for ticker: " + ticker);
            }
            return result;
        }

        /// <summary>
        /// Get Proxy Formula along with all inputs and outputs to display in UI
        /// </summary>
        /// <param name="cache"></param>
        /// <param name="ticker"></param>
        /// <returns></returns>
        public IList<FundProxy> GetProxyFormula(CachingService cache, string ticker)
        {
            IList<FundProxy> fundProxyList = new List<FundProxy>();

            IDictionary<string, FundProxyFormula> fundProxyFormulaDict = cache.Get<IDictionary<string, FundProxyFormula>>(CacheKeys.PROXY_FORMULAS);
            IDictionary<string, FundForecast> fundForecastDict = cache.Get<IDictionary<string, FundForecast>>(CacheKeys.FUND_FORECASTS);
            IDictionary<string, string> priceTickerMap = cache.Get<IDictionary<string, string>>(CacheKeys.PRICE_TICKER_MAP);
            IDictionary<string, FXRate> fxRateDict = cache.Get<IDictionary<string, FXRate>>(CacheKeys.FX_RATES);

            if (fundProxyFormulaDict.TryGetValue(ticker, out FundProxyFormula fundProxyFormula))
            {
                string proxyFormula = fundProxyFormula.ProxyFormula;
                if (!string.IsNullOrEmpty(proxyFormula)
                    && fundProxyFormula.ProxyTickersWithCoefficients.Count > 0
                    && fundForecastDict.TryGetValue(ticker, out FundForecast fundForecast))
                {
                    IDictionary<string, SecurityPrice> securityPriceDict = cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);
                    IDictionary<string, FundETFReturn> proxyHistReturnsDict = cache.Get<IDictionary<string, FundETFReturn>>(CacheKeys.PROXY_ETF_RETURNS);

                    //get historical Proxy returns (cumulative returns since fund's reported nav date to previous trading day)
                    IDictionary<string, Nullable<double>> histETFReturns = null;
                    if (proxyHistReturnsDict.TryGetValue(ticker, out FundETFReturn fundETFReturn))
                        histETFReturns = fundETFReturn.HistoricalETFReturn;

                    foreach (FundProxy fundProxyWithCoefficients in fundProxyFormula.ProxyTickersWithCoefficients)
                    {
                        string etfTicker = fundProxyWithCoefficients.ETFTicker;
                        FundProxy fundProxy = new FundProxy
                        {
                            FundTicker = ticker,
                            ETFTicker = etfTicker,
                            NavDateAsString = DateUtils.ConvertToDate(fundForecast.LastNavDt),
                            Beta = fundProxyWithCoefficients.Beta
                        };

                        //if currency, then use FX rate return, else use security return
                        if (etfTicker.Contains("Curncy", StringComparison.CurrentCultureIgnoreCase))
                        {
                            double etfRtn = GetFxRate(etfTicker, fxRateDict);
                            fundProxy.DailyRtn = etfRtn;
                            fundProxy.RtnSinceNavDate = 0.0;
                            fundProxy.TotalRtn = (1.0 + etfRtn) - 1.0;
                        }
                        else if (etfTicker.Contains("PORT RTN", StringComparison.CurrentCultureIgnoreCase) || etfTicker.Contains("PORT_RTN", StringComparison.CurrentCultureIgnoreCase))
                        {
                            double etfRtn = fundForecast.PortRtn.GetValueOrDefault();
                            fundProxy.DailyRtn = etfRtn;
                            fundProxy.RtnSinceNavDate = 0.0;
                            fundProxy.TotalRtn = (1.0 + etfRtn) - 1.0;
                            _logger.LogInformation("Populating Port Rtn for Fund Ticker/ETF Ticker: " + ticker + "/" + etfTicker);
                        }
                        else
                        {
                            //get security price
                            SecurityPrice securityPrice = SecurityPriceLookupOperations.GetSecurityPrice(etfTicker, priceTickerMap, securityPriceDict);
                            if (securityPrice != null && securityPrice.PrcRtn.HasValue)
                            {
                                double etfRtn = securityPrice.PrcRtn.GetValueOrDefault();
                                double? etfHistRtn = 0;
                                if (histETFReturns != null && histETFReturns.ContainsKey(etfTicker))
                                    etfHistRtn = histETFReturns[etfTicker];

                                fundProxy.DailyRtn = etfRtn;
                                fundProxy.RtnSinceNavDate = etfHistRtn;
                                fundProxy.TotalRtn = (1.0 + etfRtn) * (1.0 + etfHistRtn.GetValueOrDefault()) - 1.0;
                            }
                        }
                        fundProxyList.Add(fundProxy);
                    }
                }
            }
            return fundProxyList;
        }

        /// <summary>
        /// Gets live fx return (converts from given currency to target currency)
        /// </summary>
        /// <param name="currency"></param>
        /// <param name="fxRatesDict"></param>
        /// <returns></returns>
        private double GetFxRate(string currency, IDictionary<string, FXRate> fxRatesDict)
        {
            double fromFxRateRtn = 0.0;
            double toFxRateRtn = 0.0;
            double fxRateRtn = 0.0;

            try
            {
                string fromCurrency = currency.Substring(0, 3);
                string toCurrency = currency.Substring(3, 3);

                if (!fromCurrency.Equals("USD", StringComparison.CurrentCultureIgnoreCase))
                {
                    if (fxRatesDict.TryGetValue(fromCurrency, out FXRate fxRate))
                        fromFxRateRtn = fxRate.FXReturn.GetValueOrDefault();
                }

                if (!toCurrency.Equals("USD", StringComparison.CurrentCultureIgnoreCase))
                {
                    if (fxRatesDict.TryGetValue(toCurrency, out FXRate fxRate))
                        toFxRateRtn = fxRate.FXReturn.GetValueOrDefault();
                }

                fxRateRtn = (1.0 + fromFxRateRtn) * (1.0 + toFxRateRtn) - 1.0;
            }
            catch (Exception)
            {
            }

            return fxRateRtn;
        }
    }
}