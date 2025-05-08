using aACTrader.DAO.Repository;
using aCommons;
using aCommons.Cef;
using LazyCache;
using Microsoft.Extensions.Logging;
using Quartz;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace aACTrader.Jobs
{
    [DisallowConcurrentExecution]
    public class SaveFundForecastIntraDayJob : IJob
    {
        private readonly ILogger<SaveFundForecastIntraDayJob> _logger;
        private CachingService _cache { get; set; }
        private FundForecastDao _fundForecastDao { get; set; }

        public SaveFundForecastIntraDayJob(ILogger<SaveFundForecastIntraDayJob> logger, CachingService cache, FundForecastDao fundForecastDao)
        {
            _logger = logger;
            _cache = cache;
            _fundForecastDao = fundForecastDao;
        }

        public Task Execute(IJobExecutionContext context)
        {
            try
            {
                _logger.LogInformation("SaveFundForecastIntraDayJob - STARTED");
                SaveForecasts();
                _logger.LogInformation("SaveFundForecastIntraDayJob - DONE");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving forecasts");
            }
            return Task.CompletedTask;
        }

        private void SaveForecasts()
        {
            try
            {
                _logger.LogInformation("Saving Fund Forecasts - STARTED");
                IDictionary<string, FundForecast> fundForecastDict = _cache.Get<IDictionary<string, FundForecast>>(CacheKeys.FUND_FORECASTS);
                IDictionary<string, FundMaster> fundMasterDict = _cache.Get<IDictionary<string, FundMaster>>(CacheKeys.FUND_MASTER);
                IDictionary<string, FundAlphaModelScores> fundAlphaModelScoresDict = _cache.Get<IDictionary<string, FundAlphaModelScores>>(CacheKeys.ALPHA_MODEL_SCORES);
                IDictionary<string, SecurityPrice> securityPriceDict = _cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);
                IDictionary<string, string> priceTickerMap = _cache.Get<IDictionary<string, string>>(CacheKeys.PRICE_TICKER_MAP);
                _fundForecastDao.SaveFundForecasts(fundForecastDict, fundMasterDict, fundAlphaModelScoresDict, securityPriceDict, priceTickerMap);
                _logger.LogInformation("Saved Fund Forecasts - DONE");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving Fund Forecasts");
            }
        }
    }
}