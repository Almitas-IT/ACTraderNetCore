using aACTrader.Common;
using aACTrader.DAO.Repository;
using aACTrader.Services.Messaging;
using aCommons;
using aCommons.Pfd;
using LazyCache;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace aACTrader.Operations.Impl
{
    public class BBGOperations
    {
        private readonly ILogger<BBGOperations> _logger;
        private readonly BBGSecurityUpdatePublisher _securityUpdatePublisher;
        private readonly BBGJobUpdatePublisher _jobUpdatePublisher;
        private readonly CachingService _cache;
        private readonly CommonDao _commonDao;
        private readonly PfdBaseDao _pfdBaseDao;
        private readonly CommonOperations _commonOperations;
        private readonly PfdCommonOperations _pfdCommonOperations;

        public BBGOperations(ILogger<BBGOperations> logger
            , BBGSecurityUpdatePublisher securityUpdatePublisher
            , BBGJobUpdatePublisher jobUpdatePublisher
            , CachingService cache
            , CommonDao commonDao
            , PfdBaseDao pfdBaseDao
            , CommonOperations commonOperations
            , PfdCommonOperations pfdCommonOperations)
        {
            _logger = logger;
            _securityUpdatePublisher = securityUpdatePublisher;
            _jobUpdatePublisher = jobUpdatePublisher;
            _cache = cache;
            _commonDao = commonDao;
            _pfdBaseDao = pfdBaseDao;
            _commonOperations = commonOperations;
            _pfdCommonOperations = pfdCommonOperations;
            _logger.LogInformation("Initializing BBGOperations...");
        }

        public void PublishSecurities(IList<BBGSecurity> securities)
        {
            foreach (BBGSecurity sec in securities)
                _securityUpdatePublisher.PublishMessage(sec);
        }

        public void SubmitJobRequest(BBGJob jobName)
        {
            _jobUpdatePublisher.PublishMessage(jobName);
        }

        public void UpdateBBGDataCache()
        {
            _logger.LogInformation("Updating BBG Data Cache - STARTED");

            //_logger.LogInformation("Updating Published Fund Navs...");
            UpdateFundNavs();

            //_logger.LogInformation("Updating ETF Returns...");
            UpdateETFReturns();

            //_logger.LogInformation("Updating Proxy ETF Returns...");
            UpdateProxyETFReturns();

            //_logger.LogInformation("Updating Port Proxy ETF Returns...");
            UpdatePortProxyETFReturns();

            //_logger.LogInformation("Updating Holdings Nav Dates and Returns...");
            UpdateHoldingsNavDates();

            //_logger.LogInformation("Populating Interest Rate Curves...");
            UpdatePfdInterestRateCurves();

            //_logger.LogInformation("Populating Spot Rates...");
            UpdatePfdSpotRates();

            //_logger.LogInformation("Generating Cashflows...");
            UpdatePfdCashFlows();

            //_logger.LogInformation("Updating Data Validation Checks...");
            _commonOperations.RunDataValidationChecks();

            _commonOperations.PopulateLatestFundNavEstErrDetails();

            //_logger.LogInformation("Updating Pfd Curves Update Time...");
            ApplicationData applicationData = _cache.Get<ApplicationData>(CacheKeys.APPLICATION_DATA_FLAG);
            applicationData.PfdDataLastUpdateTime = DateTime.Now;

            _logger.LogInformation("Updating BBG Data Cache - DONE");
        }

        public void UpdateBBGDataCacheEurope()
        {
            _logger.LogInformation("Updating BBG Data Cache (Europe) - STARTED");

            //_logger.LogInformation("Updating Published Fund Navs...");
            UpdateFundNavs();

            //_logger.LogInformation("Updating ETF Returns...");
            UpdateETFReturns();

            //_logger.LogInformation("Updating Proxy ETF Returns...");
            UpdateProxyETFReturns();

            //_logger.LogInformation("Updating Port Proxy ETF Returns...");
            UpdatePortProxyETFReturns();

            //_logger.LogInformation("Updating Holdings Nav Dates and Returns...");
            UpdateHoldingsNavDates();

            //_logger.LogInformation("Updating Data Validation Checks...");
            _commonOperations.RunDataValidationChecks();

            //_logger.LogInformation("Updating Pfd Curves Update Time...");
            ApplicationData applicationData = _cache.Get<ApplicationData>(CacheKeys.APPLICATION_DATA_FLAG);
            applicationData.PfdDataLastUpdateTime = DateTime.Now;

            _logger.LogInformation("Updating BBG Data Cache (Europe) - DONE");
        }

        public async Task UpdateCache()
        {
            await Task.Run(() => UpdateBBGDataCache());
        }

        public async Task UpdateCacheEurope()
        {
            await Task.Run(() => UpdateBBGDataCacheEurope());
        }

        /// <summary>
        /// Re-generates security cash flows based on updated interest rate curves (preferred floating rate securities)
        /// Spot rates, spot and forwared rate curves are updated every 'x' minutes minutes from BBG
        /// </summary>
        private void UpdatePfdCashFlows()
        {
            _logger.LogInformation("Updating Pfd Cash Flows - STARTED");
            _pfdCommonOperations.GenerateCashFlows();
            _logger.LogInformation("Updating Pfd Cash Flows - DONE");
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
                //_cache.Remove(CacheKeys.PFD_SPOT_RATES);
                _cache.Add(CacheKeys.PFD_SPOT_RATES, spotRatesDict, DateTimeOffset.MaxValue);

                IDictionary<string, IDictionary<int, SpotRate>> spotRatesDictNew = _pfdBaseDao.GetSpotRatesNew();
                //_cache.Remove(CacheKeys.PFD_SPOT_RATES_NEW);
                _cache.Add(CacheKeys.PFD_SPOT_RATES_NEW, spotRatesDictNew, DateTimeOffset.MaxValue);
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
                    //_cache.Remove(CacheKeys.PFD_FWD_RATE_CURVES);
                    _cache.Add(CacheKeys.PFD_FWD_RATE_CURVES, baseForwardRateCurveDict, DateTimeOffset.MaxValue);
                }

                IDictionary<string, IRCurve> curveUpForwardRateCurveDict = _pfdBaseDao.GetInterestRateCurvesByScenario(GlobalConstants.RATE_SCENARIO_UP);
                if (curveUpForwardRateCurveDict != null && curveUpForwardRateCurveDict.Count > 0)
                {
                    _pfdCommonOperations.ExtendRateCurves(curveUpForwardRateCurveDict);
                    //_cache.Remove(CacheKeys.PFD_FWD_RATE_CURVES_UP);
                    _cache.Add(CacheKeys.PFD_FWD_RATE_CURVES_UP, curveUpForwardRateCurveDict, DateTimeOffset.MaxValue);
                }

                IDictionary<string, IRCurve> curveDownForwardRateCurveDict = _pfdBaseDao.GetInterestRateCurvesByScenario(GlobalConstants.RATE_SCENARIO_DOWN);
                if (curveDownForwardRateCurveDict != null && curveDownForwardRateCurveDict.Count > 0)
                {
                    _pfdCommonOperations.ExtendRateCurves(curveDownForwardRateCurveDict);
                    //_cache.Remove(CacheKeys.PFD_FWD_RATE_CURVES_DOWN);
                    _cache.Add(CacheKeys.PFD_FWD_RATE_CURVES_DOWN, curveDownForwardRateCurveDict, DateTimeOffset.MaxValue);
                }

                IDictionary<string, IRCurve> interestRateCurveDict = _pfdBaseDao.GetInterestRateCurvesNew();
                if (interestRateCurveDict != null && interestRateCurveDict.Count > 0)
                {
                    _pfdCommonOperations.ExtendRateCurves(interestRateCurveDict);
                    //_cache.Remove(CacheKeys.PFD_FWD_RATE_CURVES_NEW);
                    _cache.Add(CacheKeys.PFD_FWD_RATE_CURVES_NEW, interestRateCurveDict, DateTimeOffset.MaxValue);
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
                _commonOperations.PopulateAltProxyETFReturns();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error updating Proxy ETF Returns");
            }
        }

        /// <summary>
        /// Updates Port Proxy ETF returns since last nav date 
        /// </summary>
        private void UpdatePortProxyETFReturns()
        {
            try
            {
                _commonOperations.PopulatePortProxyETFReturns();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error updating Port Proxy ETF Returns");
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
        /// Update fund nav dates
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