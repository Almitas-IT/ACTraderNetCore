using aACTrader.DAO.Repository;
using aACTrader.Operations.Reports;
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
    public class SaveFundForecastJob : IJob
    {
        private readonly ILogger<SaveFundForecastJob> _logger;
        private readonly CachingService _cache;
        private readonly FundForecastDao _fundForecastDao;
        private readonly SecurityPriceDao _securityPriceDao;
        private readonly CommonDao _commonDao;
        private TradeSummaryReport _tradeSummaryReport { get; set; }

        public SaveFundForecastJob(ILogger<SaveFundForecastJob> logger
            , CachingService cache
            , FundForecastDao fundForecastDao
            , SecurityPriceDao securityPriceDao
            , CommonDao commonDao
            , TradeSummaryReport tradeSummaryReport)
        {
            _logger = logger;
            _cache = cache;
            _fundForecastDao = fundForecastDao;
            _securityPriceDao = securityPriceDao;
            _commonDao = commonDao;
            _tradeSummaryReport = tradeSummaryReport;
        }

        public Task Execute(IJobExecutionContext context)
        {
            try
            {
                _logger.LogInformation("SaveFundForecastJob - STARTED");
                SaveSecurityPrices();
                SavePortHoldingReturns();
                SaveForecasts();
                SaveFundPDStats();
                SaveTradeSummary();
                _logger.LogInformation("SaveFundForecastJob - DONE");
            }
            catch (Exception ex)
            {
                _logger.LogInformation(ex, "Error running SaveFundForecastJob");
            }
            return Task.CompletedTask;
        }

        /// <summary>
        /// Save Fund Forecasts
        /// </summary>
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
                _logger.LogInformation(ex, "Error saving Fund Forecasts");
            }
        }


        /// <summary>
        /// Save Security Prcies
        /// </summary>
        private void SaveSecurityPrices()
        {
            try
            {
                _logger.LogInformation("Saving Security Prices - STARTED");
                IDictionary<string, SecurityPrice> securityPriceDict = _cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);
                _securityPriceDao.SaveDailyPrices(securityPriceDict);
                _logger.LogInformation("Saved Security Prices - DONE");
            }
            catch (Exception ex)
            {
                _logger.LogInformation(ex, "Error saving Security Prices");
            }
        }

        /// <summary>
        /// Save Port Holding Returns
        /// </summary>
        private void SavePortHoldingReturns()
        {
            try
            {
                _logger.LogInformation("Saving Port Holding Returns - STARTED");
                IList<FundHoldingsReturn> fundHoldingsReturnList = _commonDao.GetFundHoldingsReturn();
                _fundForecastDao.SavePortHoldingReturns(fundHoldingsReturnList);
                _logger.LogInformation("Saved Port Holding Returns - DONE");
            }
            catch (Exception ex)
            {
                _logger.LogInformation(ex, "Error saving Port Holding Returns");
            }
        }

        /// <summary>
        /// Save Fund PD Stats
        /// </summary>
        private void SaveFundPDStats()
        {
            try
            {
                _logger.LogInformation("Saving Fund PD Stats - STARTED");
                IDictionary<string, FundPDStats> fundPDStatsDict = _cache.Get<IDictionary<string, FundPDStats>>(CacheKeys.LIVE_FUND_PD_STATS);
                if (fundPDStatsDict != null && fundPDStatsDict.Count > 0)
                    _fundForecastDao.SaveFundPDStats(fundPDStatsDict);
                _logger.LogInformation("Saved Fund PD Stats - DONE");
            }
            catch (Exception ex)
            {
                _logger.LogInformation(ex, "Error saving Fund PD Stats");
            }
        }

        /// <summary>
        /// Save Trade Summary
        /// </summary>
        private void SaveTradeSummary()
        {
            try
            {
                _logger.LogInformation("Saving Trade Summary - STARTED");
                _tradeSummaryReport.SaveTradeSummary();
                _logger.LogInformation("Saved Trade Summary - DONE");
            }
            catch (Exception ex)
            {
                _logger.LogInformation(ex, "Error saving Trade Summary");
            }
        }
    }
}