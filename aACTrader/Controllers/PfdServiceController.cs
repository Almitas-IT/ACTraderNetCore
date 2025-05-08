using aACTrader.DAO.Repository;
using aACTrader.Model;
using aACTrader.Operations.Impl;
using aCommons;
using aCommons.Cef;
using aCommons.Pfd;
using LazyCache;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;

namespace aACTrader.Controllers
{
    [ApiController]
    public class PfdServiceController : Controller
    {
        private readonly ILogger<PfdServiceController> _logger;
        private readonly CachingService _cache;
        private readonly PfdCommonOperations _pfdCommonOperations;
        private readonly PfdBaseDao _pfdBaseDao;

        public PfdServiceController(ILogger<PfdServiceController> logger
            , CachingService cache
            , PfdCommonOperations pfdCommonOperations
            , PfdBaseDao pfdBaseDao)
        {
            _logger = logger;
            _cache = cache;
            _pfdCommonOperations = pfdCommonOperations;
            _pfdBaseDao = pfdBaseDao;
            //_logger.LogInformation("Initializing PfdServiceController...");
        }

        [Route("/PfdService/CalculatePrice")]
        [HttpPost]
        public double? CalculatePrice(InputParameters parameters)
        {
            return _pfdCommonOperations.Solve("PRICE", parameters.Ticker, 0d, parameters.InputValue, 0d, parameters.Exchangeable, null);
        }

        [Route("/PfdService/CalculatePriceByScenario")]
        [HttpPost]
        public double? CalculatePriceByScenario(InputParameters parameters)
        {
            return _pfdCommonOperations.SolveByScenario("PRICE_BY_SCENARIO", parameters.Ticker, 0d, parameters.InputValue, 0d, parameters.Exchangeable, parameters.ScenarioName);
        }

        [Route("/PfdService/CalculateSpread")]
        [HttpPost]
        public double? CalculateSpread(InputParameters parameters)
        {
            return _pfdCommonOperations.Solve("SPREAD", parameters.Ticker, parameters.InputValue, 0d, 0d, parameters.Exchangeable, null);
        }

        [Route("/PfdService/CalculateYield")]
        [HttpPost]
        public double? CalculateYield(InputParameters parameters)
        {
            return _pfdCommonOperations.Solve("YIELD", parameters.Ticker, parameters.InputValue, 0d, 0d, parameters.Exchangeable, null);
        }

        [Route("/PfdService/CalculateYieldToCall")]
        [HttpPost]
        public double? CalculateYieldToCall(InputParameters parameters)
        {
            return _pfdCommonOperations.Solve("YIELD_TO_CALL", parameters.Ticker, parameters.InputValue, 0d, 0d, parameters.Exchangeable, null);
        }

        [Route("/PfdService/CalculateYieldToPrice")]
        [HttpPost]
        public double? CalculateYieldToPrice(InputParameters parameters)
        {
            return _pfdCommonOperations.Solve("YIELD_TO_PRICE", parameters.Ticker, 0d, 0d, parameters.InputValue, parameters.Exchangeable, null);
        }

        [Route("/PfdService/GetPfdRateCurve")]
        [HttpPost]
        public IList<RateCurveTO> GetPfdRateCurve(InputParameters parameters)
        {
            return _pfdCommonOperations.GetPfdRateCurve(parameters.CurveName);
        }

        [Route("/PfdService/GetPfdRateCurveNew")]
        [HttpPost]
        public IList<RateCurveTO> GetPfdRateCurveNew(InputParameters parameters)
        {
            return _pfdCommonOperations.GetPfdRateCurveNew(parameters.CurveName);
        }

        [Route("/PfdService/GetPfdRateResetDetails")]
        [HttpPost]
        public RateResetTO GetPfdRateResetDetails(InputParameters parameters)
        {
            return _pfdCommonOperations.GetPfdRateResetDetails(parameters.Ticker);
        }

        [Route("/PfdService/GetPfdSecurityMasterData")]
        [HttpGet]
        public IList<PfdSecurity> GetPfdSecurityMasterData()
        {
            IDictionary<string, PfdSecurity> dict = _cache.Get<IDictionary<string, PfdSecurity>>(CacheKeys.PFD_SECURITY_MASTER);
            IList<PfdSecurity> list = dict.Values.ToList<PfdSecurity>();
            return list;
        }

        [Route("/PfdService/GetPfdSecurityMaster")]
        [HttpPost]
        public IList<PfdSecurity> GetPfdSecurityMaster(InputParameters parameters)
        {
            IDictionary<string, PfdSecurity> dict = _cache.Get<IDictionary<string, PfdSecurity>>(CacheKeys.PFD_SECURITY_MASTER);
            if (parameters.AssetType.Equals("All", StringComparison.CurrentCultureIgnoreCase))
            {
                IList<PfdSecurity> list = dict.Values.ToList<PfdSecurity>();
                return list;
            }
            else if (parameters.AssetType.Equals("Fixed", StringComparison.CurrentCultureIgnoreCase))
            {
                IList<PfdSecurity> list = dict.Values.ToList<PfdSecurity>().Where(p => p.FixedFloat.Equals("fixed", StringComparison.CurrentCultureIgnoreCase)).ToList<PfdSecurity>();
                return list;
            }
            else if (parameters.AssetType.Equals("Float", StringComparison.CurrentCultureIgnoreCase))
            {
                IList<PfdSecurity> list = dict.Values.ToList<PfdSecurity>().Where(p => p.FixedFloat.Equals("float", StringComparison.CurrentCultureIgnoreCase)).ToList<PfdSecurity>();
                return list;
            }
            return null;
        }

        [Route("/PfdService/SavePfdSecurityOverrides")]
        [HttpPost]
        public void SavePfdSecurityOverrides(IList<PfdSecurity> overrides)
        {
            _pfdCommonOperations.SaveSecurityDetails(overrides);
        }

        [Route("/PfdService/SaveInterestRateCurveMembers")]
        [HttpPost]
        public void SaveInterestRateCurveMembers(IList<IRCurveMember> curveMembers)
        {
            _pfdCommonOperations.SaveInterestRateCurveMembers(curveMembers);
        }

        [Route("/PfdService/SavePfdSecurityAnalytics")]
        [HttpPost]
        public void SavePfdSecurityAnalytics(IList<PfdSecurityAnalytic> analytics)
        {
            _pfdBaseDao.SavePfdSecurityAnalytics(analytics);
        }

        [Route("/PfdService/GetPfdSecurityAnalytics")]
        [HttpPost]
        public IList<PfdSecurityAnalytic> GetPfdSecurityAnalytics(InputParameters parameters)
        {
            return _pfdBaseDao.GetPfdSecurityAnalytics(parameters.Ticker, parameters.StartDate, parameters.EndDate);
        }

        [Route("/PfdService/GetPfdSecurityMasterNew")]
        [HttpPost]
        public IList<FundGroup> GetPfdSecurityMasterNew(InputParameters parameters)
        {
            return _pfdCommonOperations.GetFundTickers(parameters);
        }

        [Route("/PfdService/CalculateEffectiveDuration")]
        [HttpPost]
        public double? CalculateEffectiveDuration(DurationParamters parameters)
        {
            double? modifiedDuration = null;
            try
            {
                if (parameters.PriceBase > 0)
                    modifiedDuration = (parameters.PriceDown - parameters.PriceUp) / (2.0 * parameters.PriceBase * parameters.YieldChange);
            }
            catch (Exception)
            {
            }
            return modifiedDuration;
        }

        [Route("/PfdService/GetPfdSecurityCashFlowsSummary")]
        [HttpPost]
        public PeriodicCashFlowSummary GetPfdSecurityCashFlowsSummary(InputParameters parameters)
        {
            return _pfdCommonOperations.GetPfdSecurityCashFlowsSummary(parameters.Ticker);
        }

        [Route("/PfdService/CalculateIRRToPrice")]
        [HttpPost]
        public double? CalculateIRRToPrice(InputParameters parameters)
        {
            return _pfdCommonOperations.CalculatePriceGivenIRR(parameters.Ticker, parameters.InputValue);
        }
    }
}
