using aACTrader.Common;
using aACTrader.DAO.Repository;
using aACTrader.Operations.Impl;
using aCommons;
using aCommons.Pfd;
using LazyCache;
using Microsoft.Extensions.Logging;
using Quartz;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace aACTrader.Jobs
{
    [DisallowConcurrentExecution]
    public class UpdateBBGDataCacheJob : IJob
    {
        private readonly ILogger<UpdateBBGDataCacheJob> _logger;
        private readonly CachingService _cache;
        private readonly CommonDao _commonDao;
        private readonly PfdBaseDao _pfdBaseDao;
        private readonly CommonOperations _commonOperations;
        private readonly PfdCommonOperations _pfdCommonOperations;
        private readonly BBGOperations _bbgOperations;

        public UpdateBBGDataCacheJob(ILogger<UpdateBBGDataCacheJob> logger
            , CachingService cache
            , CommonDao commonDao
            , PfdBaseDao pfdBaseDao
            , CommonOperations commonOperations
            , PfdCommonOperations pfdCommonOperations
            , BBGOperations bbgOperations)
        {
            _logger = logger;
            _cache = cache;
            _commonDao = commonDao;
            _pfdBaseDao = pfdBaseDao;
            _commonOperations = commonOperations;
            _pfdCommonOperations = pfdCommonOperations;
            _bbgOperations = bbgOperations;
        }

        Task IJob.Execute(IJobExecutionContext context)
        {
            _bbgOperations.UpdateBBGDataCache();
            return Task.CompletedTask;
        }

        /// <summary>
        /// Re-generates security cash flows based on updated interest rate curves (preferred floating rate securities)
        /// Spot rates, spot and forwared rate curves are updated every 'x' minutes minutes from BBG
        /// </summary>
        private void UpdatePfdCashFlows()
        {
            _pfdCommonOperations.GenerateCashFlows();
        }

        /// <summary>
        /// Updates spot rates
        /// Spot rates are updated every 'x' minutes minutes from BBG
        /// </summary>
        private void UpdatePfdSpotRates()
        {
            try
            {
                IDictionary<string, IDictionary<int, SpotRate>> spotRatesDict = _pfdBaseDao.GetSpotRates();
                _cache.Remove(CacheKeys.PFD_SPOT_RATES);
                _cache.Add(CacheKeys.PFD_SPOT_RATES, spotRatesDict, DateTimeOffset.MaxValue);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error updating Spot Rates");
            }
        }

        /// <summary>
		/// Updates interest rate curves (spot and forward) for calculating expected cash flows for preferreds
        /// Spot rates are updated every 'x' minutes from BBG
        /// Spot & Forward rate curves are re-generated based on updated spot rates
        /// 
        /// Rates are moved up and down by 25 bps for effective duration calculation
        /// </summary>
        private void UpdatePfdInterestRateCurves()
        {
            _logger.LogInformation("Updating Pfd Interest Rate Curves - STARTED");

            try
            {
                IDictionary<string, IRCurve> baseForwardRateCurveDict = _pfdBaseDao.GetInterestRateCurvesByScenario(GlobalConstants.RATE_SCENARIO_BASE);
                if (baseForwardRateCurveDict != null && baseForwardRateCurveDict.Count > 0)
                {
                    _pfdCommonOperations.ExtendRateCurves(baseForwardRateCurveDict);
                    _cache.Remove(CacheKeys.PFD_FWD_RATE_CURVES);
                    _cache.Add(CacheKeys.PFD_FWD_RATE_CURVES, baseForwardRateCurveDict, DateTimeOffset.MaxValue);
                }

                IDictionary<string, IRCurve> curveUpForwardRateCurveDict = _pfdBaseDao.GetInterestRateCurvesByScenario(GlobalConstants.RATE_SCENARIO_UP);
                if (curveUpForwardRateCurveDict != null && curveUpForwardRateCurveDict.Count > 0)
                {
                    _pfdCommonOperations.ExtendRateCurves(curveUpForwardRateCurveDict);
                    _cache.Remove(CacheKeys.PFD_FWD_RATE_CURVES_UP);
                    _cache.Add(CacheKeys.PFD_FWD_RATE_CURVES_UP, curveUpForwardRateCurveDict, DateTimeOffset.MaxValue);
                }

                IDictionary<string, IRCurve> curveDownForwardRateCurveDict = _pfdBaseDao.GetInterestRateCurvesByScenario(GlobalConstants.RATE_SCENARIO_DOWN);
                if (curveDownForwardRateCurveDict != null && curveDownForwardRateCurveDict.Count > 0)
                {
                    _pfdCommonOperations.ExtendRateCurves(curveDownForwardRateCurveDict);
                    _cache.Remove(CacheKeys.PFD_FWD_RATE_CURVES_DOWN);
                    _cache.Add(CacheKeys.PFD_FWD_RATE_CURVES_DOWN, curveDownForwardRateCurveDict, DateTimeOffset.MaxValue);
                }

                _logger.LogInformation("Updating Pfd Interest Rate Curves - DONE");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error updating Interest Rate Curves");
            }
        }

        /// <summary>
        /// Updates ETF returns since last nav date
        /// Since navs are updated from BBG every 'x' minutes, this call is to re-calc the cumulative returns since last nav date
        /// </summary>
        private void UpdateETFReturns()
        {
            try
            {
                _commonOperations.PopulateETFReturns();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error updating Fund ETF Returns");
            }
        }

        /// <summary>
        /// Updates Proxy ETF returns since last nav date 
        /// </summary>
        private void UpdateProxyETFReturns()
        {
            try
            {
                _commonOperations.PopulateProxyETFReturns();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error updating Proxy ETF Returns");
            }
        }

        /// <summary>
        /// Updates latest fund navs
        /// Navs are updated every 'x' minutes from BBG during trading hours
        /// </summary>
        private void UpdateFundNavs()
        {
            try
            {
                _commonOperations.UpdateFundNavs();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error updating Fund Navs");
            }
        }

        /// <summary>
        /// Update Fund Nav dates
        /// </summary>
        private void UpdateHoldingsNavDates()
        {
            try
            {
                _commonDao.UpdateFundNavOverrides();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error updating Fund Nav Overrides");
            }
        }
    }
}