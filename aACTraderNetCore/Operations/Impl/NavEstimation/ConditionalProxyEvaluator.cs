using aCommons;
using aCommons.Cef;
using aCommons.Utils;
using Ciloci.Flee;
using LazyCache;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;

namespace aACTrader.Operations.Impl.NavEstimation
{
    public class ConditionalProxyEvaluator
    {
        private readonly ILogger<ConditionalProxyEvaluator> _logger;
        private readonly ExpressionContext _context;
        private readonly CachingService _cache;

        public ConditionalProxyEvaluator(ILogger<ConditionalProxyEvaluator> logger, CachingService cachingService)
        {
            _logger = logger;
            _cache = cachingService;
            _context = new ExpressionContext();
            _context.Imports.AddType(typeof(Math));
        }

        public void Process(string ticker
            , FundForecast fundForecast
            , FundCondProxy fundCondProxy
            , IDictionary<string, FXRate> fxRatesDict
            , IDictionary<string, SecurityPrice> securityPriceDict
            , IDictionary<string, string> priceTickerMap)
        {
            try
            {
                //General Proxy
                if (!string.IsNullOrEmpty(fundCondProxy.ProxyFormula))
                    CalculateProxyReturn(ticker, fundForecast,
                        fundCondProxy.ProxyFormulaDetails, fxRatesDict, securityPriceDict, priceTickerMap, "General");

                //Conditional Proxy 1
                if (!string.IsNullOrEmpty(fundCondProxy.Cond1ProxyFormula))
                    CalculateProxyReturn(ticker, fundForecast,
                        fundCondProxy.Cond1ProxyFormulaDetails, fxRatesDict, securityPriceDict, priceTickerMap, "Condition1");

                //Conditional Proxy 2
                if (!string.IsNullOrEmpty(fundCondProxy.Cond2ProxyFormula))
                    CalculateProxyReturn(ticker, fundForecast,
                        fundCondProxy.Cond2ProxyFormulaDetails, fxRatesDict, securityPriceDict, priceTickerMap, "Condition2");

                //Conditional Formula 1
                if (!string.IsNullOrEmpty(fundCondProxy.Cond1Formula))
                    fundForecast.Cond1ProxyFlag = CalculateConditionalProxy(ticker,
                        fundCondProxy.Cond1FormulaDetails, fxRatesDict, securityPriceDict, priceTickerMap);

                //Conditional Formula 2
                if (!string.IsNullOrEmpty(fundCondProxy.Cond2Formula))
                    fundForecast.Cond2ProxyFlag = CalculateConditionalProxy(ticker,
                        fundCondProxy.Cond2FormulaDetails, fxRatesDict, securityPriceDict, priceTickerMap);

                //_logger.LogInformation("Conditional Proxy for ticker: " +
                //    ticker + "/" +
                //    fundForecast.GenProxyRtn + "/" +
                //    fundForecast.GenProxyNav + "/" +
                //    fundForecast.Cond1ProxyFlag + "/" +
                //    fundForecast.Cond1ProxyRtn + "/" +
                //    fundForecast.Cond1ProxyNav + "/" +
                //    fundForecast.Cond2ProxyFlag + "/" +
                //    fundForecast.Cond2ProxyRtn + "/" +
                //    fundForecast.Cond2ProxyNav);
            }
            catch (Exception ex)
            {
                _logger.LogError("Error processing conditional proxy for ticker: " + ticker, ex);
            }
        }

        private void CalculateProxyReturn(
            string ticker
            , FundForecast fundForecast
            , FundProxyFormula fundProxyFormula
            , IDictionary<string, FXRate> fxRatesDict
            , IDictionary<string, SecurityPrice> securityPriceDict
            , IDictionary<string, string> priceTickerMap
            , string proxyType)

        {
            try
            {
                _context.Variables.Clear();
                foreach (FundProxy fundProxy in fundProxyFormula.ProxyTickersWithCoefficients)
                {
                    string etfTicker = fundProxy.ETFTicker;
                    double etfRtn = 0.0;
                    if (etfTicker.Contains("Curncy", StringComparison.CurrentCultureIgnoreCase))
                        etfRtn = GetFXRate(etfTicker, fxRatesDict);
                    else
                    {
                        SecurityPrice securityPrice = SecurityPriceLookupOperations.GetSecurityPrice(etfTicker, priceTickerMap, securityPriceDict);
                        if (securityPrice != null)
                            etfRtn = securityPrice.PrcRtn.GetValueOrDefault();
                    }

                    string newTicker = etfTicker.Replace(" ", "_");
                    _context.Variables[newTicker] = etfRtn;
                }
                double? proxyReturn = EvaluateExpression(_context, fundProxyFormula.ProxyFormula, ticker);
                if (fundForecast.LastDvdAdjNav.HasValue && proxyReturn.HasValue)
                {
                    double leverageRatio = fundForecast.LevRatio.GetValueOrDefault();
                    double proxyAdjReturn = proxyReturn.GetValueOrDefault() * (1.0 + leverageRatio);
                    double proxyNav = fundForecast.LastDvdAdjNav.GetValueOrDefault();
                    proxyNav *= (1.0 + proxyAdjReturn);

                    if (fundForecast.AI.HasValue)
                        proxyNav += fundForecast.AI.GetValueOrDefault();

                    if (!Double.IsNaN(proxyNav) && !Double.IsInfinity(proxyNav))
                    {
                        if (proxyType.Equals("General"))
                            fundForecast.GenProxyNav = proxyNav;
                        else if (proxyType.Equals("Condition1"))
                            fundForecast.Cond1ProxyNav = proxyNav;
                        else if (proxyType.Equals("Condition2"))
                            fundForecast.Cond2ProxyNav = proxyNav;
                    }
                    if (!Double.IsNaN(proxyAdjReturn) && !Double.IsInfinity(proxyAdjReturn))
                    {
                        if (proxyType.Equals("General"))
                            fundForecast.GenProxyRtn = proxyAdjReturn;
                        else if (proxyType.Equals("Condition1"))
                            fundForecast.Cond1ProxyRtn = proxyAdjReturn;
                        else if (proxyType.Equals("Condition2"))
                            fundForecast.Cond2ProxyRtn = proxyAdjReturn;
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("Error calculating proxy return for ticker: " + ticker + "/" + fundProxyFormula.ProxyFormula, ex);
            }
        }

        private string? CalculateConditionalProxy(string ticker
            , FundProxyFormula fundProxyFormula
            , IDictionary<string, FXRate> fxRatesDict
            , IDictionary<string, SecurityPrice> securityPriceDict
            , IDictionary<string, string> priceTickerMap)

        {
            try
            {
                _context.Variables.Clear();
                foreach (FundProxy fundProxy in fundProxyFormula.ProxyTickersWithCoefficients)
                {
                    string etfTicker = fundProxy.ETFTicker;
                    double etfRtn = 0.0;
                    if (etfTicker.Contains("Curncy", StringComparison.CurrentCultureIgnoreCase))
                        etfRtn = GetFXRate(etfTicker, fxRatesDict);
                    else
                    {
                        SecurityPrice securityPrice = SecurityPriceLookupOperations.GetSecurityPrice(etfTicker, priceTickerMap, securityPriceDict);
                        if (securityPrice != null)
                            etfRtn = securityPrice.PrcRtn.GetValueOrDefault();
                    }

                    string newTicker = etfTicker.Replace(" ", "_");
                    _context.Variables[newTicker] = etfRtn;
                }
                string? result = EvaluateBooleanExpression(_context, fundProxyFormula.ProxyFormula, ticker);
                return result;
            }
            catch (Exception ex)
            {
                _logger.LogError("Error calculating conditional proxy for ticker: " + ticker + "/" + fundProxyFormula.ProxyFormula, ex);
            }
            return null;
        }

        private double? EvaluateExpression(ExpressionContext context, string expression, string ticker)
        {
            double? result = null;
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

        private string? EvaluateBooleanExpression(ExpressionContext context, string expression, string ticker)
        {
            string? result = "N";
            try
            {
                IGenericExpression<bool> eGeneric = context.CompileGeneric<bool>(expression.Replace(" ", "_"));
                bool flag = eGeneric.Evaluate();
                if (flag)
                    result = "Y";
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error evaluating the boolean expression for ticker: " + ticker);
            }
            return result;
        }

        /// <summary>
        /// Get Proxy Formula along with all inputs and outputs to display in UI
        /// </summary>
        /// <param name="cache"></param>
        /// <param name="ticker"></param>
        /// <returns></returns>
        public IList<FundProxy> GetProxyFormula(string ticker)
        {
            IList<FundProxy> fundProxyList = new List<FundProxy>();

            IDictionary<string, FundCondProxy> fundCondProxyDict = _cache.Get<IDictionary<string, FundCondProxy>>(CacheKeys.CONDITIONAL_PROXY);
            IDictionary<string, FundForecast> fundForecastDict = _cache.Get<IDictionary<string, FundForecast>>(CacheKeys.FUND_FORECASTS);
            IDictionary<string, string> priceTickerMap = _cache.Get<IDictionary<string, string>>(CacheKeys.PRICE_TICKER_MAP);
            IDictionary<string, FXRate> fxRateDict = _cache.Get<IDictionary<string, FXRate>>(CacheKeys.FX_RATES);
            IDictionary<string, SecurityPrice> securityPriceDict = _cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);

            if (fundCondProxyDict.TryGetValue(ticker, out FundCondProxy fundProxy))
            {
                if (fundForecastDict.TryGetValue(ticker, out FundForecast fundForecast))
                {
                    if (!string.IsNullOrEmpty(fundProxy.ProxyFormula))
                        PopulateProxy(fundProxy.Ticker, "General Proxy", fundForecast, fundProxy.ProxyFormulaDetails, fxRateDict, securityPriceDict, priceTickerMap, fundProxyList);
                    if (!string.IsNullOrEmpty(fundProxy.Cond1ProxyFormula))
                        PopulateProxy(fundProxy.Ticker, "Condition1 Proxy", fundForecast, fundProxy.Cond1ProxyFormulaDetails, fxRateDict, securityPriceDict, priceTickerMap, fundProxyList);
                    if (!string.IsNullOrEmpty(fundProxy.Cond2ProxyFormula))
                        PopulateProxy(fundProxy.Ticker, "Condition2 Proxy", fundForecast, fundProxy.Cond2ProxyFormulaDetails, fxRateDict, securityPriceDict, priceTickerMap, fundProxyList);
                    if (!string.IsNullOrEmpty(fundProxy.Cond1Formula))
                        PopulateProxy(fundProxy.Ticker, "Condition1", fundForecast, fundProxy.Cond1FormulaDetails, fxRateDict, securityPriceDict, priceTickerMap, fundProxyList);
                    if (!string.IsNullOrEmpty(fundProxy.Cond2Formula))
                        PopulateProxy(fundProxy.Ticker, "Condition2", fundForecast, fundProxy.Cond2FormulaDetails, fxRateDict, securityPriceDict, priceTickerMap, fundProxyList);
                }
            }
            return fundProxyList;
        }

        private void PopulateProxy(
            string ticker
            , string proxyType
            , FundForecast fundForecast
            , FundProxyFormula fundProxyFormula
            , IDictionary<string, FXRate> fxRateDict
            , IDictionary<string, SecurityPrice> securityPriceDict
            , IDictionary<string, string> priceTickerMap
            , IList<FundProxy> fundProxyList)
        {
            foreach (FundProxy fundProxyWithCoefficients in fundProxyFormula.ProxyTickersWithCoefficients)
            {
                string etfTicker = fundProxyWithCoefficients.ETFTicker;
                FundProxy fundProxy = new FundProxy
                {
                    FundTicker = ticker,
                    ETFTicker = etfTicker,
                    NavDateAsString = DateUtils.ConvertToDate(fundForecast.LastNavDt),
                    Beta = fundProxyWithCoefficients.Beta,
                    ProxyType = proxyType,
                };

                //if currency, then use FX rate return, else use security return
                if (etfTicker.Contains("Curncy", StringComparison.CurrentCultureIgnoreCase))
                {
                    double etfRtn = GetFXRate(etfTicker, fxRateDict);
                    fundProxy.DailyRtn = etfRtn;
                    fundProxy.RtnSinceNavDate = 0.0;
                    fundProxy.TotalRtn = 1.0 + etfRtn - 1.0;
                }
                else if (etfTicker.Contains("PORT RTN", StringComparison.CurrentCultureIgnoreCase)
                    || etfTicker.Contains("PORT_RTN", StringComparison.CurrentCultureIgnoreCase))
                {
                    double etfRtn = fundForecast.PortRtn.GetValueOrDefault();
                    fundProxy.DailyRtn = etfRtn;
                    fundProxy.RtnSinceNavDate = 0.0;
                    fundProxy.TotalRtn = 1.0 + etfRtn - 1.0;
                    _logger.LogInformation("Populating Port Rtn for Fund Ticker/ETF Ticker: " + ticker + "/" + etfTicker);
                }
                else
                {
                    SecurityPrice securityPrice = SecurityPriceLookupOperations.GetSecurityPrice(etfTicker, priceTickerMap, securityPriceDict);
                    if (securityPrice != null && securityPrice.PrcRtn.HasValue)
                    {
                        double etfRtn = securityPrice.PrcRtn.GetValueOrDefault();
                        double? etfHistRtn = 0;
                        //if (histETFReturns != null && histETFReturns.ContainsKey(etfTicker))
                        //    etfHistRtn = histETFReturns[etfTicker];
                        fundProxy.DailyRtn = etfRtn;
                        fundProxy.RtnSinceNavDate = etfHistRtn;
                        fundProxy.TotalRtn = (1.0 + etfRtn) * (1.0 + etfHistRtn.GetValueOrDefault()) - 1.0;
                    }
                }
                fundProxyList.Add(fundProxy);
            }
        }

        /// <summary>
        /// Gets live fx return (converts from given currency to target currency)
        /// </summary>
        /// <param name="currency"></param>
        /// <param name="fxRatesDict"></param>
        /// <returns></returns>
        private double GetFXRate(string currency, IDictionary<string, FXRate> fxRatesDict)
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