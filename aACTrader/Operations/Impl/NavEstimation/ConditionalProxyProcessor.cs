using aACTrader.DAO.Repository;
using aACTrader.Utils;
using aCommons;
using aCommons.Cef;
using LazyCache;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;

namespace aACTrader.Operations.Impl.NavEstimation
{
    public class ConditionalProxyProcessor
    {
        private readonly ILogger<ConditionalProxyProcessor> _logger;
        private readonly FundForecastDao _fundForecastDao;
        private readonly CachingService _cache;

        public ConditionalProxyProcessor(ILogger<ConditionalProxyProcessor> logger, FundForecastDao fundForecastDao, CachingService cache)
        {
            _logger = logger;
            _fundForecastDao = fundForecastDao;
            _cache = cache;
        }

        public void Initialize()
        {
            _logger.LogInformation("Initializing ConditionalProxyProcessor...");
            IDictionary<string, FundMaster> fundMasterDict = _cache.Get<IDictionary<string, FundMaster>>(CacheKeys.FUND_MASTER);
            IDictionary<string, FundCondProxy> dict = _fundForecastDao.GetFundConditionalProxies();
            foreach (FundCondProxy fundProxy in dict.Values)
            {
                _logger.LogInformation("Processing ConditionalProxyProcessor for ticker: " + fundProxy.Ticker);
                if (!string.IsNullOrEmpty(fundProxy.ProxyFormula))
                    fundProxy.ProxyFormulaDetails = ProcessProxyFormula(fundProxy.Ticker, fundProxy.ProxyFormula);
                if (!string.IsNullOrEmpty(fundProxy.Cond1ProxyFormula))
                    fundProxy.Cond1ProxyFormulaDetails = ProcessProxyFormula(fundProxy.Ticker, fundProxy.Cond1ProxyFormula);
                if (!string.IsNullOrEmpty(fundProxy.Cond2ProxyFormula))
                    fundProxy.Cond2ProxyFormulaDetails = ProcessProxyFormula(fundProxy.Ticker, fundProxy.Cond2ProxyFormula);
                if (!string.IsNullOrEmpty(fundProxy.Cond1Formula))
                    fundProxy.Cond1FormulaDetails = ProcessProxyFormula(fundProxy.Ticker, fundProxy.Cond1Formula);
                if (!string.IsNullOrEmpty(fundProxy.Cond2Formula))
                    fundProxy.Cond2FormulaDetails = ProcessProxyFormula(fundProxy.Ticker, fundProxy.Cond2Formula);

                if (fundMasterDict.TryGetValue(fundProxy.Ticker, out FundMaster fundMaster))
                    PopulateFundProxy(fundProxy, fundMaster);
            }
            _cache.Remove(CacheKeys.CONDITIONAL_PROXY);
            _cache.Add(CacheKeys.CONDITIONAL_PROXY, dict, DateTimeOffset.MaxValue);
        }

        private FundProxyFormula ProcessProxyFormula(string ticker, string proxyFormula)
        {
            try
            {
                IList<FundProxy> proxyTickersWithCoefficients = FundUtils.GetProxyTickers(ticker, proxyFormula);
                FundProxyFormula fundProxyFormula = new FundProxyFormula
                {
                    Ticker = ticker,
                    ProxyFormula = proxyFormula,
                    ProxyTickersWithCoefficients = proxyTickersWithCoefficients
                };
                return fundProxyFormula;
            }
            catch (Exception ex)
            {
                _logger.LogError("Error parsing proxy formula for ticker: " + ticker + "/" + proxyFormula, ex);
                return null;
            }
        }

        private void PopulateFundProxy(FundCondProxy fundProxy, FundMaster fundMaster)
        {
            FundCondProxyTO data = new FundCondProxyTO();
            data.Ticker = fundProxy.Ticker;
            data.ProxyFormula = fundProxy.ProxyFormula;
            data.Cond1Formula = fundProxy.Cond1Formula;
            data.Cond1ProxyFormula = fundProxy.Cond1ProxyFormula;
            data.Cond2Formula = fundProxy.Cond2Formula;
            data.Cond2ProxyFormula = fundProxy.Cond2ProxyFormula;
            fundMaster.FundCondProxyTO = data;
        }
    }
}