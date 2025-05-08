using aACTrader.DAO.Repository;
using aACTrader.Model;
using aACTrader.Operations.Impl;
using aACTrader.SignalR.Services;
using aCommons;
using aCommons.Cef;
using aCommons.DTO;
using aCommons.Fund;
using aCommons.MarketMonitor;
using aCommons.Utils;
using aCommons.Web;
using LazyCache;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;

namespace aACTrader.Controllers
{
    [ApiController]
    //[Produces(MediaTypeNames.Application.Json)]
    public class DataMaintenanceController : ControllerBase
    {
        private readonly ILogger<DataMaintenanceController> _logger;
        private readonly CachingService _cache;
        private readonly CommonDao _commonDao;
        private readonly BaseDao _baseDao;
        private readonly HoldingsDao _holdingsDao;
        private readonly StatsDao _statsDao;
        private readonly FundDao _fundDao;
        private readonly SecurityRiskDao _securityRiskDao;
        private readonly FundForecastDao _fundForecastDao;
        private readonly FundSupplementalDataDao _fundSupplementalDataDao;
        private readonly CommonOperations _commonOperations;
        private readonly FundAlertManager _fundAlertManager;
        private readonly FundAlertManagerNew _fundAlertManagerNew;
        private readonly NavProxyEvaluator _navProxyEvaluator;
        private readonly AltNavProxyEvaluator _altNavProxyEvaluator;
        private readonly FundHoldingsOperations _fundHoldingsOperations;
        private readonly FundHistoryOperations _fundHistoryOperations;
        private readonly CompanyHoldingsReportGenerator _companyHoldingsReportGenerator;
        private readonly ActivistHoldingsOperations _activistHoldingsOperations;
        private readonly BrokerReportOperations _brokerReportOperations;
        private readonly PositionReconOperations _positionReconOperations;
        private readonly SecurityReturnOperations _securityReturnOperations;
        private readonly TradeOrderOperations _tradeOrderOperations;
        private readonly NotificationService _notificationService;

        public DataMaintenanceController(ILogger<DataMaintenanceController> logger
            , CachingService cache
            , CommonDao commonDao
            , BaseDao baseDao
            , HoldingsDao holdingsDao
            , StatsDao statsDao
            , FundDao fundDao
            , SecurityRiskDao securityRiskDao
            , FundForecastDao fundForecastDao
            , FundSupplementalDataDao fundSupplementalDataDao
            , CommonOperations commonOperations
            , FundAlertManager fundAlertManager
            , FundAlertManagerNew fundAlertManagerNew
            , NavProxyEvaluator navProxyEvaluator
            , AltNavProxyEvaluator altNavProxyEvaluator
            , FundHoldingsOperations fundHoldingsOperations
            , FundHistoryOperations fundHistoryOperations
            , CompanyHoldingsReportGenerator companyHoldingsReportGenerator
            , ActivistHoldingsOperations activistHoldingsOperations
            , BrokerReportOperations brokerReportOperations
            , PositionReconOperations positionReconOperations
            , SecurityReturnOperations securityReturnOperations
            , TradeOrderOperations tradeOrderOperations
            , NotificationService notificationService
            )
        {
            _logger = logger;
            _cache = cache;
            _commonDao = commonDao;
            _baseDao = baseDao;
            _holdingsDao = holdingsDao;
            _statsDao = statsDao;
            _fundDao = fundDao;
            _securityRiskDao = securityRiskDao;
            _fundForecastDao = fundForecastDao;
            _fundSupplementalDataDao = fundSupplementalDataDao;
            _commonOperations = commonOperations;
            _fundAlertManager = fundAlertManager;
            _fundAlertManagerNew = fundAlertManagerNew;
            _navProxyEvaluator = navProxyEvaluator;
            _altNavProxyEvaluator = altNavProxyEvaluator;
            _fundHoldingsOperations = fundHoldingsOperations;
            _fundHistoryOperations = fundHistoryOperations;
            _companyHoldingsReportGenerator = companyHoldingsReportGenerator;
            _activistHoldingsOperations = activistHoldingsOperations;
            _brokerReportOperations = brokerReportOperations;
            _positionReconOperations = positionReconOperations;
            _securityReturnOperations = securityReturnOperations;
            _tradeOrderOperations = tradeOrderOperations;
            _notificationService = notificationService;
            //_logger.LogInformation("Initializing DataMaintenanceController...");
        }

        [Route("/DataMaintenanceService/GetAlmHoldings")]
        [HttpPost]
        public IList<AlmHolding> GetAlmHoldings(InputParameters reqParams)
        {
            DateTime? asofDate = DateUtils.ConvertToDate(reqParams.AsofDate);
            return _holdingsDao.GetAlmHoldings(asofDate.GetValueOrDefault());
        }

        [Route("/DataMaintenanceService/GetSecurityMaster")]
        [HttpPost]
        public IList<SecurityMaster> GetSecurityMaster(InputParameters reqParams)
        {
            return _baseDao.GetSecurityMaster(reqParams.Ticker, reqParams.Status);
        }

        [Route("/DataMaintenanceService/GetCEFFundTickers")]
        [HttpPost]
        public IList<FundGroup> GetCEFFundTickers(CEFParameters reqParams)
        {
            return _commonOperations.GetCEFFundTickers(reqParams);
        }

        [Route("/DataMaintenanceService/GetFundHoldingReturns")]
        [HttpPost]
        public IList<FundHoldingReturn> GetFundHoldingReturns(InputParameters reqParams)
        {
            return _commonOperations.GetFundHoldingReturn(reqParams);
        }

        [Route("/DataMaintenanceService/GetFundHoldingReturnsNew")]
        [HttpPost]
        public IList<FundHoldingReturn> GetFundHoldingReturnsNew(InputParameters reqParams)
        {
            return _commonOperations.GetFundHoldingReturnsNew(reqParams);
        }

        [Route("/DataMaintenanceService/GetFundHoldings")]
        [HttpPost]
        public IList<FundHolding> GetFundHoldings(InputParameters reqParams)
        {
            return _baseDao.GetFundHoldings(reqParams.Ticker, reqParams.Source);
        }

        [Route("/DataMaintenanceService/GetPortHoldings")]
        [HttpPost]
        public IList<FundHolding> GetPortHoldings(InputParameters reqParams)
        {
            return _baseDao.GetFundHoldings(reqParams.Country);
        }

        [Route("/DataMaintenanceService/GetFundRedemptionDetails")]
        [HttpPost]
        public IList<FundRedemption> GetFundRedemptionDetails(InputParameters reqParams)
        {
            IDictionary<string, FundRedemption> dict = _cache.Get<IDictionary<string, FundRedemption>>(CacheKeys.FUND_REDEMPTIONS);
            IList<FundRedemption> list = dict.Values.ToList<FundRedemption>();

            if (!string.IsNullOrEmpty(reqParams.Ticker))
            {
                IList<FundRedemption> filteredList = list.Where(f => f.Ticker.Equals(reqParams.Ticker, StringComparison.CurrentCultureIgnoreCase)).ToList<FundRedemption>();
                return filteredList;
            }
            else
            {
                return list;
            }
        }

        [Route("/DataMaintenanceService/GetUserDataOverrides")]
        [HttpGet]
        public IList<UserDataOverride> GetUserDataOverrides()
        {
            IDictionary<string, UserDataOverride> dict = _cache.Get<IDictionary<string, UserDataOverride>>(CacheKeys.USER_OVERRIDES);
            IList<UserDataOverride> list = new List<UserDataOverride>(dict.Values);
            return list;
        }

        [Route("/DataMaintenanceService/SaveUserDataOverrides")]
        [HttpPost]
        public void SaveUserDataOverrides(IList<UserDataOverride> userDataOverrides)
        {
            _commonOperations.SaveUserOverrides(userDataOverrides);
        }

        [Route("/DataMaintenanceService/GetFundCurrencyHedges")]
        [HttpPost]
        public IList<FundCurrencyHedge> GetFundCurrencyHedges(InputParameters reqParams)
        {
            return _baseDao.GetFundCurrencyHedges(reqParams.Ticker);
        }

        [Route("/DataMaintenanceService/GetETFRegressions")]
        [HttpPost]
        public IList<RegressionCoefficient> GetETFRegressions(InputParameters reqParams)
        {
            IDictionary<string, FundNav> fundNavDict = _cache.Get<IDictionary<string, FundNav>>(CacheKeys.FUND_NAVS);
            IDictionary<string, FundETFReturn> etfReturnsDict = _cache.Get<IDictionary<string, FundETFReturn>>(CacheKeys.ETF_RETURNS);
            IDictionary<string, SecurityPrice> securityPriceDict = _cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);
            IDictionary<string, string> priceTickerMap = _cache.Get<IDictionary<string, string>>(CacheKeys.PRICE_TICKER_MAP);

            //get fund nav date
            FundNav fundNav;
            fundNavDict.TryGetValue(reqParams.Ticker, out fundNav);

            //get historical etf returns
            FundETFReturn fundETFReturn;
            etfReturnsDict.TryGetValue(reqParams.Ticker, out fundETFReturn);

            //populate nav date, etf return since last nav date and latest etf return
            IList<RegressionCoefficient> list = _commonDao.GetETFRegressionCoefficients(reqParams.Ticker);
            foreach (RegressionCoefficient reg in list)
            {
                if (fundNav != null)
                {
                    reg.NavDate = fundNav.LastNavDate;
                    reg.NavDateAsString = DateUtils.ConvertDate(reg.NavDate, "yyyy-MM-dd");
                }

                if (fundETFReturn != null && fundETFReturn.HistoricalETFReturn != null)
                {
                    if (fundETFReturn.HistoricalETFReturn.ContainsKey(reg.ETFTicker))
                        reg.RtnSinceNavDate = fundETFReturn.HistoricalETFReturn[reg.ETFTicker];
                }

                //get security price
                SecurityPrice securityPrice = SecurityPriceLookupOperations.GetSecurityPrice(reg.ETFTicker, priceTickerMap, securityPriceDict);
                if (securityPrice != null)
                {
                    reg.DailyRtn = securityPrice.PrcRtn;
                    double totalRtn = (1 + reg.DailyRtn.GetValueOrDefault()) * (1 + reg.RtnSinceNavDate.GetValueOrDefault()) - 1;
                    reg.TotalRtn = totalRtn;
                }
            }

            return list;
        }

        [Route("/DataMaintenanceService/SaveCustomView")]
        [HttpPost]
        public void SaveCustomView(IList<CustomView> customViewList)
        {
            _baseDao.SaveCustomView(customViewList);
        }

        [Route("/DataMaintenanceService/GetCustomViewNames")]
        [HttpGet]
        public IList<CustomView> GetCustomViewNames()
        {
            return _baseDao.GetCustomViews();
        }

        [Route("/DataMaintenanceService/GetCustomView")]
        [HttpPost]
        public IList<CustomView> GetCustomView(CustomView customView)
        {
            return _baseDao.GetCustomViewDetails(customView.ViewName);
        }

        [Route("/DataMaintenanceService/GetBDCNavHistory")]
        [HttpPost]
        public IList<BDCNavHistory> GetBDCNavHistory(InputParameters reqParams)
        {
            return _baseDao.GetBDCNavHistory(reqParams.Ticker);
        }

        [Route("/DataMaintenanceService/GetReitBookValueHistory")]
        [HttpPost]
        public IList<ReitBookValueHistory> GetReitBookValueHistory(InputParameters reqParams)
        {
            return _baseDao.GetReitBookValueHistory(reqParams.Ticker);
        }

        [Route("/DataMaintenanceService/SaveBDCNavHistory")]
        [HttpPost]
        public void SaveBDCNavHistory(IList<BDCNavHistory> bdcNavHistory)
        {
            _baseDao.SaveBDCNavHistory(bdcNavHistory);
            _commonOperations.UpdateFundNavs();
        }

        [Route("/DataMaintenanceService/SaveReitBookValueHistory")]
        [HttpPost]
        public void SaveReitBookValueHistory(IList<ReitBookValueHistory> reitBookValueHistory)
        {
            _baseDao.SaveReitBookValueHistory(reitBookValueHistory);
            _commonOperations.UpdateFundNavs();
        }

        [Route("/DataMaintenanceService/GetFundAlerts")]
        [HttpPost]
        public IList<FundAlert> GetFundAlerts(FundAlertParameters reqParams)
        {
            if (string.IsNullOrEmpty(reqParams.IncludeIRRFunds))
                reqParams.IncludeIRRFunds = "Y";

            if (string.IsNullOrEmpty(reqParams.GroupByAssetClass))
                reqParams.GroupByAssetClass = "Y";

            if (string.IsNullOrEmpty(reqParams.AssetClassLevel))
                reqParams.AssetClassLevel = "Level 3";

            if (string.IsNullOrEmpty(reqParams.GroupByCurrency))
                reqParams.GroupByCurrency = "N";

            if (string.IsNullOrEmpty(reqParams.GroupByCountry))
                reqParams.GroupByCountry = "N";

            if (string.IsNullOrEmpty(reqParams.GroupByState))
                reqParams.GroupByState = "N";

            return _fundAlertManager.GetFundAlerts(reqParams);
        }

        [Route("/DataMaintenanceService/GetFundAlertTargets")]
        [HttpGet]
        public IList<FundAlertTarget> GetFundAlertTargets()
        {
            return _cache.Get<IList<FundAlertTarget>>(CacheKeys.FUND_ALERT_TARGETS);
        }

        [Route("/DataMaintenanceService/SaveFundAlertTargets")]
        [HttpPost]
        public void SaveFundAlertTargets(IList<FundAlertTarget> fundAlertTargets)
        {
            _fundAlertManager.SaveFundAlertTargets(fundAlertTargets);
        }

        [Route("/DataMaintenanceService/GetTradeTaxLots")]
        [HttpPost]
        public IList<TradeTaxLot> GetTradeTaxLots(TaxLotParameters reqParams)
        {
            string account = (reqParams.Account.Equals("All", StringComparison.CurrentCultureIgnoreCase)) ? null : reqParams.Account;
            string broker = (reqParams.Broker.Equals("All", StringComparison.CurrentCultureIgnoreCase)) ? null : reqParams.Broker;
            string AgeRangeFrom = (reqParams.AgeRangeFrom.Equals("", StringComparison.CurrentCultureIgnoreCase)) ? null : reqParams.AgeRangeFrom;
            string AgeRangeTo = (reqParams.AgeRangeTo.Equals("", StringComparison.CurrentCultureIgnoreCase)) ? null : reqParams.AgeRangeTo;
            return _tradeOrderOperations.GetTradeTaxLots(reqParams.Ticker, account, broker, reqParams.MultiBrokerFlag, AgeRangeFrom, AgeRangeTo);
        }

        [Route("/DataMaintenanceService/GetTradeTaxLotSummary")]
        [HttpPost]
        public IList<TradeTaxLotSummary> GetTradeTaxLotSummary(TaxLotParameters reqParams)
        {
            string account = (reqParams.Account.Equals("All", StringComparison.CurrentCultureIgnoreCase)) ? null : reqParams.Account;
            string broker = (reqParams.Broker.Equals("All", StringComparison.CurrentCultureIgnoreCase)) ? null : reqParams.Broker;
            return _baseDao.GetTaxLotSummary(account, broker);
        }

        [Route("/DataMaintenanceService/SaveFundCurrencyHedges")]
        [HttpPost]
        public void SaveFundCurrencyHedges(IList<FundCurrencyHedge> fundCurrencyHedgeList)
        {
            _baseDao.SaveFundCurrencyHedges(fundCurrencyHedgeList);
        }

        [Route("/DataMaintenanceService/SaveFundRedemptionRules")]
        [HttpPost]
        public async void SaveFundRedemptionRules(IList<FundRedemption> fundRedemptionList)
        {
            _commonOperations.SaveFundRedemptionRules(fundRedemptionList);
            //await _notificationService.FundSupplementalDataUpdate();
        }

        [Route("/DataMaintenanceService/GetFundNavHistory")]
        [HttpPost]
        public IList<FundNavHistory> GetFundNavHistory(ServiceDataContract reqParams)
        {
            DateTime? startDate = DateUtils.ConvertToDate(reqParams.StartDate, "yyyy-MM-dd");
            DateTime? endDate = DateUtils.ConvertToDate(reqParams.EndDate, "yyyy-MM-dd");
            return _baseDao.GetFundNavHistory(reqParams.Ticker, startDate.GetValueOrDefault(), endDate.GetValueOrDefault());
        }

        [Route("/DataMaintenanceService/GetFundEarningsHistory")]
        [HttpPost]
        public IList<FundEarningHist> GetFundEarningsHistory(ServiceDataContract reqParams)
        {
            return _baseDao.GetFundEarningsHistory(reqParams.Ticker, reqParams.ProfilePeriod);
        }

        [Route("/DataMaintenanceService/SaveFundHoldings")]
        [HttpPost]
        public void SaveFundHoldings(IList<FundHolding> fundHoldings)
        {
            _baseDao.SaveFundHoldings(fundHoldings, "Y");
        }

        [Route("/DataMaintenanceService/UpdateFundHoldings")]
        [HttpPost]
        public void UpdateFundHoldings(IList<FundHolding> fundHoldings)
        {
            _baseDao.SaveFundHoldings(fundHoldings, "N");
        }

        [Route("/DataMaintenanceService/UpdateUserFundHoldings")]
        [HttpPost]
        public void UpdateUserFundHoldings(IList<FundHolding> fundHoldings)
        {
            _baseDao.SaveUserFundHoldings(fundHoldings, "N");
        }

        [Route("/DataMaintenanceService/GetFundHistory")]
        [HttpPost]
        public IList<FundHistory> GetFundHistory(ServiceDataContract reqParams)
        {
            return _baseDao.GetFundHistory(reqParams.Ticker, reqParams.StartDate, reqParams.EndDate);
        }

        [Route("/DataMaintenanceService/GetSectorHistory")]
        [HttpPost]
        public IList<FundNavHistory> GetSectorHistory(ServiceDataContract reqParams)
        {
            DateTime? startDate = DateUtils.ConvertToDate(reqParams.StartDate, "yyyy-MM-dd");
            DateTime? endDate = DateUtils.ConvertToDate(reqParams.EndDate, "yyyy-MM-dd");
            return _baseDao.GetSectorHistory(reqParams.Country, reqParams.SecurityType
                , reqParams.CEFInstrumentType, reqParams.Sector, reqParams.FundCategory, startDate.GetValueOrDefault(), endDate.GetValueOrDefault());
        }

        [Route("/DataMaintenanceService/GetProxyFormula")]
        [HttpPost]
        public IList<FundProxy> GetProxyFormula(ServiceDataContract reqParams)
        {
            return _navProxyEvaluator.GetProxyFormula(_cache, reqParams.Ticker);
        }

        [Route("/DataMaintenanceService/GetAltProxyFormula")]
        [HttpPost]
        public IList<FundProxy> GetAltProxyFormula(ServiceDataContract reqParams)
        {
            return _altNavProxyEvaluator.GetProxyFormula(_cache, reqParams.Ticker);
        }

        [Route("/DataMaintenanceService/GetHoldingsExposureReport")]
        [HttpPost]
        public IList<HoldingExposureDetail> GetHoldingsExposureReport(ExposureReportParameters reqParams)
        {
            IList<string> groupList = new List<string>();

            if (!string.IsNullOrEmpty(reqParams.GroupBy1))
                groupList.Add(reqParams.GroupBy1);

            if (!string.IsNullOrEmpty(reqParams.GroupBy2))
                groupList.Add(reqParams.GroupBy2);

            if (!string.IsNullOrEmpty(reqParams.GroupBy3))
                groupList.Add(reqParams.GroupBy3);

            if (!string.IsNullOrEmpty(reqParams.GroupBy4))
                groupList.Add(reqParams.GroupBy4);

            if (!string.IsNullOrEmpty(reqParams.GroupBy5))
                groupList.Add(reqParams.GroupBy5);

            if (!string.IsNullOrEmpty(reqParams.GroupBy6))
                groupList.Add(reqParams.GroupBy6);

            string[] groups = new string[groupList.Count];
            int i = 0;

            if (!string.IsNullOrEmpty(reqParams.GroupBy1))
            {
                groups[i] = reqParams.GroupBy1;
                i++;
            }

            if (!string.IsNullOrEmpty(reqParams.GroupBy2))
            {
                groups[i] = reqParams.GroupBy2;
                i++;
            }

            if (!string.IsNullOrEmpty(reqParams.GroupBy3))
            {
                groups[i] = reqParams.GroupBy3;
                i++;
            }

            if (!string.IsNullOrEmpty(reqParams.GroupBy4))
            {
                groups[i] = reqParams.GroupBy4;
                i++;
            }

            if (!string.IsNullOrEmpty(reqParams.GroupBy5))
            {
                groups[i] = reqParams.GroupBy5;
                i++;
            }

            if (!string.IsNullOrEmpty(reqParams.GroupBy6))
            {
                groups[i] = reqParams.GroupBy6;
                i++;
            }

            IList<HoldingExposureDetail> exposureDetails = _fundHoldingsOperations.GenerateExposureReport(_cache, reqParams.Portfolio, reqParams.Broker, groups, reqParams.ShowDetails);
            return exposureDetails;
        }

        [Route("/DataMaintenanceService/GetActivistHoldingsChangeReport")]
        [HttpPost]
        public IList<CompanyHoldingsChangeReport> GetActivistHoldingsChangeReport(ActivistReportParameters reqParams)
        {
            DateTime? startDate = DateUtils.ConvertToDate(reqParams.StartDate, "yyyy-MM-dd");
            DateTime? endDate = DateUtils.ConvertToDate(reqParams.EndDate, "yyyy-MM-dd");
            IList<CompanyHoldingsChangeReport> holdings = _companyHoldingsReportGenerator.GenerateCompanyHoldingsChangeReport(_cache, reqParams.ActivistName, startDate, endDate);
            return holdings;
        }

        [Route("/DataMaintenanceService/GetFundHistoryList")]
        [HttpPost]
        public IList<FundHistoryMaster> GetFundHistory(IList<ServiceDataContract> reqParams)
        {
            return _fundHistoryOperations.GetFundHistory(reqParams);
        }

        [Route("/DataMaintenanceService/GetFundHistoricalData")]
        [HttpPost]
        public IList<FundHistoryMasterNew> GetFundHistoricalData(IList<ServiceDataContract> reqParams)
        {
            return _fundHistoryOperations.GetFundHistoryNew(reqParams);
        }

        [Route("/DataMaintenanceService/GetBDCHistoricalData")]
        [HttpPost]
        public IList<BDCFundHistory> GetBDCHistoricalData(IList<ServiceDataContract> reqParams)
        {
            return _fundHistoryOperations.GetBDCFundHistory(reqParams);
        }

        [Route("/DataMaintenanceService/GetFundHistoricalSummary")]
        [HttpPost]
        public FundHistoryMasterSummary GetFundHistoricalSummary(IList<ServiceDataContract> reqParams)
        {
            return _fundHistoryOperations.GetFundHistorySummary(reqParams);
        }

        [Route("/DataMaintenanceService/GetFundHolders")]
        [HttpPost]
        public IList<FundHolder> GetFundHolders(ActivistReportParameters reqParams)
        {
            return _holdingsDao.GetFundHolders(reqParams.ReportType);
        }

        [Route("/DataMaintenanceService/GetFundHolderSummary")]
        [HttpPost]
        public IList<FundHolder> GetFundHolderSummary(ActivistReportParameters reqParams)
        {
            return _holdingsDao.GetFundHoldersSummary(reqParams.ReportType);
        }

        [Route("/DataMaintenanceService/GetSectorForecasts")]
        [HttpGet]
        public IList<SectorForecast> GetSectorForecasts()
        {
            IDictionary<string, SectorForecast> dict = _cache.Get<IDictionary<string, SectorForecast>>(CacheKeys.SECTOR_FORECASTS);
            IList<SectorForecast> list = new List<SectorForecast>(dict.Values);
            return list;
        }

        [Route("/DataMaintenanceService/GetAlmHoldingsSharesRecall")]
        [HttpPost]
        public IList<AlmHolding> GetAlmHoldingsSharesRecall(ExposureReportParameters reqParams)
        {
            return _fundHoldingsOperations.GenerateSharesRecallReport(reqParams.Portfolio, reqParams.Broker);
        }

        [Route("/DataMaintenanceService/GetGlobalDataOverrides")]
        [HttpGet]
        public IList<GlobalDataOverride> GetGlobalDataOverrides()
        {
            IDictionary<string, GlobalDataOverride> dict = _cache.Get<IDictionary<string, GlobalDataOverride>>(CacheKeys.GLOBAL_OVERRIDES);
            IList<GlobalDataOverride> list = new List<GlobalDataOverride>(dict.Values);
            return list;
        }

        [Route("/DataMaintenanceService/SaveGlobalDataOverrides")]
        [HttpPost]
        public void SaveGlobalDataOverrides(IList<GlobalDataOverride> globalDataOverrides)
        {
            _commonOperations.SaveGlobalOverrides(globalDataOverrides);
        }

        [Route("/DataMaintenanceService/GetSecurityIdentifierMap")]
        [HttpGet]
        public IList<SecurityIdentifier> GetSecurityIdentifierMap()
        {
            IDictionary<string, SecurityIdentifier> dict = _cache.Get<IDictionary<string, SecurityIdentifier>>(CacheKeys.POSITION_SECURITY_MAP);
            IList<SecurityIdentifier> list = new List<SecurityIdentifier>(dict.Values);
            return list;
        }

        [Route("/DataMaintenanceService/GetPositionMasterList")]
        [HttpGet]
        public IList<PositionMaster> GetPositionMasterList()
        {
            IDictionary<string, PositionMaster> dict = _cache.Get<IDictionary<string, PositionMaster>>(CacheKeys.POSITION_MASTER);
            return dict.Values.ToList<PositionMaster>();
        }

        [Route("/DataMaintenanceService/SaveSharesRecallOverrides")]
        [HttpPost]
        public void SaveSharesRecallOverrides(IList<AlmHolding> holdings)
        {
            _fundHoldingsOperations.SaveSharesRecallOverrides(holdings);
        }

        [Route("/DataMaintenanceService/SaveAlmHoldings")]
        [HttpPost]
        public void SaveAlmHoldings(IList<AlmHolding> holdings)
        {
            _fundHoldingsOperations.SaveAlmHoldings(holdings);
        }

        [Route("/DataMaintenanceService/SaveSecurityRiskFactors")]
        [HttpPost]
        public void SaveSecurityRiskFactors(IList<SecurityRiskFactor> riskFactors)
        {
            _commonOperations.SaveSecurityRiskFactors(riskFactors);
        }

        [Route("/DataMaintenanceService/GetActivistHoldings")]
        [HttpPost]
        public IList<ActivistHolding> GetActivistHoldings(ActivistReportParameters parameters)
        {
            return _activistHoldingsOperations.GetActivistHoldings(parameters.ActivistName, parameters.Ticker);
        }

        [Route("/DataMaintenanceService/GetActivistHoldingsHistory")]
        [HttpPost]
        public IList<ActivistHolding> GetActivistHoldingsHistory(ActivistReportParameters parameters)
        {
            return _activistHoldingsOperations.GetActivistHoldingsHistory(parameters.ActivistName, parameters.Ticker);
        }

        [Route("/DataMaintenanceService/GetActivistScores")]
        [HttpPost]
        public IList<ActivistScore> GetActivistScores(ActivistReportParameters parameters)
        {
            return _activistHoldingsOperations.GetActivistScores(parameters.Country);
        }

        [Route("/DataMaintenanceService/SaveActivistScores")]
        [HttpPost]
        public void SaveActivistScores(IList<ActivistScore> activistScores)
        {
            _activistHoldingsOperations.SaveActivistScores(_cache, activistScores);
        }

        [Route("/DataMaintenanceService/GetExpectedAlphaModelScores")]
        [HttpGet]
        public IList<FundAlphaModelScores> GetExpectedAlphaModelScores()
        {
            IDictionary<string, FundAlphaModelScores> dict = _commonDao.GetFundAlphaModelScores();
            IList<FundAlphaModelScores> list = new List<FundAlphaModelScores>(dict.Values);
            return list;
        }

        [Route("/DataMaintenanceService/GetHistoricalDiscountStats")]
        [HttpPost]
        public IList<FundGroupDiscountStats> GetHistoricalDiscountStats(HistoricalDiscountStatsParameters parameters)
        {
            return _statsDao.GetFundDiscountStats(parameters.SecurityType, parameters.Country);
        }

        [Route("/DataMaintenanceService/GetHistoricalDiscountStatsForDate")]
        [HttpPost]
        public FundGroupDiscountStats GetHistoricalDiscountStatsForDate(HistoricalDiscountStatsParameters parameters)
        {
            Nullable<DateTime> startDate = DateUtils.ConvertToDate(parameters.StartDate, "MM/dd/yyyy");
            if (startDate == null)
                startDate = DateUtils.ConvertToDate(parameters.StartDate, "yyyy-MM-dd");
            return _statsDao.GetFundDiscountStats(parameters.SecurityType, parameters.Country, parameters.GroupName, startDate.GetValueOrDefault());
        }

        [Route("/DataMaintenanceService/GetHistoricalDiscountStatsHistory")]
        [HttpPost]
        public IList<FundGroupDiscountStatsHist> GetHistoricalDiscountStatsHistory(HistoricalDiscountStatsParameters parameters)
        {
            return _statsDao.GetFundDiscountStatsHistory(parameters.GroupName, parameters.SecurityType, parameters.Country);
        }

        [Route("/DataMaintenanceService/GetFundHoldersPosition")]
        [HttpPost]
        public Nullable<double> GetFundHoldersPosition(ActivistReportParameters parameters)
        {
            IDictionary<string, ActivistHolding> dict = _cache.Get<IDictionary<string, ActivistHolding>>(CacheKeys.HOLDERS_MAP);
            if (dict.TryGetValue(parameters.Ticker + "|" + parameters.ActivistName, out ActivistHolding holding))
                return holding.Position;
            return null;
        }

        [Route("/DataMaintenanceService/GetFundHoldersOwnershipPct")]
        [HttpPost]
        public double? GetFundHoldersOwnershipPct(ActivistReportParameters parameters)
        {
            IDictionary<string, ActivistHolding> dict = _cache.Get<IDictionary<string, ActivistHolding>>(CacheKeys.HOLDERS_MAP);
            if (dict.TryGetValue(parameters.Ticker + "|" + parameters.ActivistName, out ActivistHolding holding))
                return holding.PctOutstanding;
            return null;
        }

        [Route("/DataMaintenanceService/GetFundHoldersOwnershipChangePct")]
        [HttpPost]
        public double? GetFundHoldersOwnershipChangePct(ActivistReportParameters parameters)
        {
            IDictionary<string, ActivistHolding> dict = _cache.Get<IDictionary<string, ActivistHolding>>(CacheKeys.HOLDERS_MAP);
            if (dict.TryGetValue(parameters.Ticker + "|" + parameters.ActivistName, out ActivistHolding holding))
                return holding.PositionChange;
            return null;
        }

        [Route("/DataMaintenanceService/GetFundHoldersOwnershipFilingSource")]
        [HttpPost]
        public string GetFundHoldersOwnershipFilingSource(ActivistReportParameters parameters)
        {
            IDictionary<string, ActivistHolding> dict = _cache.Get<IDictionary<string, ActivistHolding>>(CacheKeys.HOLDERS_MAP);
            if (dict.TryGetValue(parameters.Ticker + "|" + parameters.ActivistName, out ActivistHolding holding))
                return holding.FilingSource;
            return null;
        }

        [Route("/DataMaintenanceService/GetFundHoldersOwnershipFilingDate")]
        [HttpPost]
        public DateTime? GetFundHoldersOwnershipFilingDate(ActivistReportParameters parameters)
        {
            IDictionary<string, ActivistHolding> dict = _cache.Get<IDictionary<string, ActivistHolding>>(CacheKeys.HOLDERS_MAP);
            if (dict.TryGetValue(parameters.Ticker + "|" + parameters.ActivistName, out ActivistHolding holding))
                return holding.FilingDate;
            return null;
        }

        [Route("/DataMaintenanceService/GetAllActivistHoldings")]
        [HttpGet]
        public IList<ActivistHolding> GetAllActivistHoldings()
        {
            IDictionary<string, ActivistHolding> dict = _cache.Get<IDictionary<string, ActivistHolding>>(CacheKeys.HOLDERS_MAP);
            return dict.Values.ToList<ActivistHolding>();
        }

        [Route("/DataMaintenanceService/GetTradeHistory")]
        [HttpPost]
        public IList<Trade> GetTradeHistory(TradeHistoryReportParameters parameters)
        {
            return _holdingsDao.GetTradeHistory(parameters.StartDate, parameters.EndDate,
                parameters.Portfolio, parameters.Broker, parameters.Ticker, parameters.Currency, parameters.Country,
                parameters.GeoLevel1, parameters.GeoLevel2, parameters.GeoLevel3,
                parameters.AssetClassLevel1, parameters.AssetClassLevel2, parameters.AssetClassLevel3, parameters.FundCategory, parameters.SecurityType
                );
        }

        [Route("/DataMaintenanceService/GetFundDetails")]
        [HttpPost]
        public FundDetail GetFundDetails(InputParameters parameters)
        {
            return _brokerReportOperations.GetFundDetails(parameters.Broker, parameters.FundName);
        }

        [Route("/DataMaintenanceService/GetFundCashDetails")]
        [HttpPost]
        public FundCashDetail GetFundCashDetails(InputParameters parameters)
        {
            return _brokerReportOperations.GetFundCashDetails(parameters.Broker
                , parameters.FundName, parameters.Currency, parameters.AssetType);
        }

        [Route("/DataMaintenanceService/GetSecurityRebateRates")]
        [HttpGet]
        public IList<SecurityRebateRate> GetSecurityRebateRates()
        {
            IDictionary<string, SecurityRebateRate> dict = _cache.Get<IDictionary<string, SecurityRebateRate>>(CacheKeys.SECURITY_REBATE_RATES);
            IList<SecurityRebateRate> list = new List<SecurityRebateRate>(dict.Values);
            return list;
        }

        [Route("/DataMaintenanceService/GetSecurityActualRebateRates")]
        [HttpGet]
        public IList<SecurityActualRebateRate> GetSecurityActualRebateRates()
        {
            return _cache.Get<IList<SecurityActualRebateRate>>(CacheKeys.SECURITY_ACTUAL_REBATE_RATES);
        }

        [Route("/DataMaintenanceService/GetFundMarginAttributionDetails")]
        [HttpPost]
        public FundMarginAttributionDetail GetFundMarginAttributionDetails(InputParameters parameters)
        {
            return _brokerReportOperations.GetFundMarginAttributionDetails(parameters.Broker, parameters.FundName);
        }

        [Route("/DataMaintenanceService/GetFundAlertsLive")]
        [HttpGet]
        public IList<FundAlert> GetFundAlertsLive()
        {
            return _cache.Get<IList<FundAlert>>(CacheKeys.FUND_ALERTS);
        }

        [Route("/DataMaintenanceService/GetFundRightsOffers")]
        [HttpGet]
        public IList<FundRightsOffer> GetFundRightsOffers()
        {
            IDictionary<string, FundRightsOffer> dict = _cache.Get<IDictionary<string, FundRightsOffer>>(CacheKeys.FUND_RIGHTS_OFFERS);
            IList<FundRightsOffer> list = new List<FundRightsOffer>(dict.Values);
            return list;
        }

        [Route("/DataMaintenanceService/SaveFundRightsOffers")]
        [HttpPost]
        public void SaveFundRightsOffers(IList<FundRightsOffer> fundRightsOffers)
        {
            _commonOperations.SaveFundRightsOfferDetails(fundRightsOffers);
        }

        [Route("/DataMaintenanceService/GetPositionRecon")]
        [HttpPost]
        public IList<PositionRecon> GetPositionRecon(InputParameters parameters)
        {
            IDictionary<string, PositionRecon> dict = _positionReconOperations.GenerateReconReport(parameters.AsofDate);
            IList<PositionRecon> list = dict.Values.ToList<PositionRecon>()
                .OrderByDescending(p => p.PositionBreakFlag)
                .ThenByDescending(p => p.MarketValueBreakFlag)
                .ThenByDescending(p => Math.Abs(p.DerivedMarketValue.GetValueOrDefault()))
                .ToList<PositionRecon>();
            return list;
        }

        [Route("/DataMaintenanceService/GetFundSupplementalData")]
        [HttpGet]
        public IList<FundSupplementalData> GetFundSupplementalData()
        {
            IDictionary<string, FundSupplementalData> dict = _cache.Get<IDictionary<string, FundSupplementalData>>(CacheKeys.FUND_SUPPLEMENTAL_DETAILS);
            IList<FundSupplementalData> list = new List<FundSupplementalData>();
            if (dict != null && dict.Count > 0)
            {
                foreach (FundSupplementalData data in dict.Values)
                    list.Add(data);
            }
            return list;
        }

        [Route("/DataMaintenanceService/GetFundFinanceDetails")]
        [HttpPost]
        public FundFinanceDetail GetFundFinanceDetails(InputParameters parameters)
        {
            return _brokerReportOperations.GetFundFinanceDetails(parameters.Broker, parameters.FundName, parameters.Currency);
        }

        [Route("/DataMaintenanceService/GetFundInterestEarningDetails")]
        [HttpPost]
        public FundInterestEarningDetail GetFundInterestEarningDetails(InputParameters parameters)
        {
            return _brokerReportOperations.GetFundInterestEarningDetails(parameters.Broker, parameters.FundName, parameters.Currency);
        }

        [Route("/DataMaintenanceService/GetFundTenderOffers")]
        [HttpGet]
        public IList<FundTenderOffer> GetFundTenderOffers()
        {
            IDictionary<string, FundTenderOffer> dict = _cache.Get<IDictionary<string, FundTenderOffer>>(CacheKeys.FUND_TENDER_OFFERS);
            IList<FundTenderOffer> list = new List<FundTenderOffer>(dict.Values);
            return list;
        }

        [Route("/DataMaintenanceService/SaveFundTenderOffers")]
        [HttpPost]
        public void SaveFundTenderOffers(IList<FundTenderOffer> fundTenderOffers)
        {
            _commonOperations.SaveFundTenderOfferDetails(fundTenderOffers);
        }

        [Route("/DataMaintenanceService/GetPositionExposureReport")]
        [HttpGet]
        public IList<PositionExposure> GetPositionExposureReport()
        {
            IDictionary<string, PositionExposure> dict = _fundHoldingsOperations.GenerateExposureReportWithCurrency(_cache);
            return dict.Values.ToList<PositionExposure>().OrderBy(p => p.Ticker).ToList<PositionExposure>();
        }

        [Route("/DataMaintenanceService/GetPositionExposureCurrencyList")]
        [HttpGet]
        public IList<string> GetPositionExposureCurrencyList()
        {
            return _fundHoldingsOperations.GenerateCurrencyExposureList(_cache);
        }

        [Route("/DataMaintenanceService/GetPositionSecurityDetails")]
        [HttpGet]
        public IList<SecurityMaster> GetPositionSecurityDetails()
        {
            IDictionary<string, SecurityMaster> dict = _cache.Get<IDictionary<string, SecurityMaster>>(CacheKeys.POSITION_SECURITY_MASTER_DETAILS);
            return dict.Values.ToList<SecurityMaster>();
        }

        [Route("/DataMaintenanceService/SaveSecurityMasterDetails")]
        [HttpPost]
        public void SaveSecurityMasterDetails(IList<SecurityMaster> securityMasterList)
        {
            _baseDao.SaveSecurityMasterDetails(securityMasterList);
        }

        [Route("/DataMaintenanceService/GetSecurityPerformanceDetails")]
        [HttpGet]
        public IList<SecurityPerformance> GetSecurityPerformanceDetails()
        {
            IDictionary<string, SecurityPerformance> dict = _baseDao.GetSecurityPerformance();
            IList<SecurityPerformance> list = new List<SecurityPerformance>(dict.Values);
            return list;
        }

        [Route("/DataMaintenanceService/GetFundCurrencyDetails")]
        [HttpPost]
        public FundCurrencyDetail GetFundCurrencyDetails(InputParameters parameters)
        {
            return _brokerReportOperations.GetFundCurrencyDetails(parameters.Broker
                , parameters.FundName, parameters.Currency, parameters.ContractType);
        }

        [Route("/DataMaintenanceService/GetSecurityPerformanceDetailsHistory")]
        [HttpPost]
        public IList<SecurityPerformance> GetSecurityPerformanceDetailsHistory(InputParameters parameters)
        {
            DateTime? startDate = DateUtils.ConvertToDate(parameters.StartDate, "yyyy-MM-dd");
            DateTime? endDate = DateUtils.ConvertToDate(parameters.EndDate, "yyyy-MM-dd");
            return _baseDao.GetSecurityPerformance(startDate.GetValueOrDefault(), endDate.GetValueOrDefault()).Values.ToList<SecurityPerformance>();
        }

        [Route("/DataMaintenanceService/GetBDCFundReturn")]
        [HttpPost]
        public double? GetBDCFundReturn(FundReturnParameters parameters)
        {
            IDictionary<string, FundReturn> dict = _cache.Get<IDictionary<string, FundReturn>>(CacheKeys.BDC_FUND_RETURNS);
            string id = parameters.Ticker + "|" + parameters.ReturnType + "|" + parameters.ReturnPeriod;
            if (dict.TryGetValue(id, out FundReturn fundReturn))
                return fundReturn.Return;
            return null;
        }

        [Route("/DataMaintenanceService/GetBDCFundReturns")]
        [HttpGet]
        public IList<FundReturn> GetBDCFundReturns()
        {
            IDictionary<string, FundReturn> dict = _cache.Get<IDictionary<string, FundReturn>>(CacheKeys.BDC_FUND_RETURNS);
            return dict.Values.ToList<FundReturn>();
        }

        [Route("/DataMaintenanceService/GetWatchlistSecurities")]
        [HttpGet]
        public IList<SecurityMaster> GetWatchlistSecurities()
        {
            IDictionary<string, SecurityMaster> dict = _cache.Get<IDictionary<string, SecurityMaster>>(CacheKeys.WATCHLIST_SECURITIES);
            IList<SecurityMaster> list = dict.Values.ToList<SecurityMaster>();
            return list.OrderBy(f => f.FundCategory).ThenBy(f => f.Ticker).ToList<SecurityMaster>();
        }

        [Route("/DataMaintenanceService/GetWatchlistSecurityAlerts")]
        [HttpGet]
        public IList<FundAlertDetail> GetWatchlistSecurityAlerts()
        {
            return _cache.Get<IList<FundAlertDetail>>(CacheKeys.TRADING_TARGET_ALERTS);
        }

        [Route("/DataMaintenanceService/GetTradingTargets")]
        [HttpGet]
        public IList<TradingTarget> GetTradingTargets()
        {
            IDictionary<string, TradingTarget> dict = _cache.Get<IDictionary<string, TradingTarget>>(CacheKeys.TRADING_TARGETS);
            IList<TradingTarget> list = new List<TradingTarget>();
            if (dict != null && dict.Count > 0)
            {
                foreach (KeyValuePair<string, TradingTarget> kvp in dict.OrderBy(t => t.Key))
                    list.Add(kvp.Value);
            }
            return list;
        }

        [Route("/DataMaintenanceService/SaveTradingTargets")]
        [HttpPost]
        public void SaveTradingTargets(IList<TradingTarget> list)
        {
            _fundAlertManagerNew.SaveTradingTargets(list);
        }

        [Route("/DataMaintenanceService/GetExpectedAlphaModelParams")]
        [HttpGet]
        public IList<ExpectedAlphaModelParams> GetExpectedAlphaModelParams()
        {
            IDictionary<string, ExpectedAlphaModelParams> dict = _cache.Get<IDictionary<string, ExpectedAlphaModelParams>>(CacheKeys.EXPECTED_ALPHA_MODEL_PARAMS);
            IList<ExpectedAlphaModelParams> list = new List<ExpectedAlphaModelParams>(dict.Values);
            return list;
        }

        [Route("/DataMaintenanceService/GetSecurityTotalReturns")]
        [HttpGet]
        public IList<FundSecurityTotalReturn> GetSecurityTotalReturns()
        {
            return _securityRiskDao.GetSecurityTotalReturns();
        }

        [Route("/DataMaintenanceService/GetFundSwapDetails")]
        [HttpPost]
        public FundSwapDetail GetFundSwapDetails(InputParameters parameters)
        {
            return _brokerReportOperations.GetFundSwapDetails(parameters.Broker
                , parameters.FundName, parameters.Ticker, parameters.Side);
        }

        [Route("/DataMaintenanceService/GetHistSecurityReturn")]
        [HttpPost]
        public double? GetHistSecurityReturn(SecurityReturnParameters parameters)
        {
            DateTime? fromDate = DateUtils.ConvertToDate(parameters.FromDate, "yyyyMMdd");
            DateTime? toDate = DateUtils.ConvertToDate(parameters.ToDate, "yyyyMMdd");
            Nullable<double> result = _securityReturnOperations.GetSecurityReturn(parameters.Ticker
                , fromDate.GetValueOrDefault(), toDate.GetValueOrDefault());
            return result;
        }

        [Route("/DataMaintenanceService/GetHistFXReturn")]
        [HttpPost]
        public double? GetHistFXReturn(FXReturnParameters parameters)
        {
            DateTime? fromDate = DateUtils.ConvertToDate(parameters.FromDate, "yyyy-MM-dd");
            DateTime? toDate = DateUtils.ConvertToDate(parameters.ToDate, "yyyy-MM-dd");
            Nullable<double> result = _securityReturnOperations.GetFXReturn(parameters.FromCurrency
                , parameters.ToCurrency, fromDate.GetValueOrDefault(), toDate.GetValueOrDefault());
            return result;
        }

        [Route("/DataMaintenanceService/GetFundPositionMarketValue")]
        [HttpPost]
        public double? GetFundPositionMarketValue(InputParameters parameters)
        {
            return _brokerReportOperations.GetIBPositionMarketValue(parameters.Broker
                , parameters.FundName, parameters.AssetType, parameters.Side, parameters.AssetGroup);
        }

        [Route("/DataMaintenanceService/GetGlobalMarketStats")]
        [HttpGet]
        public IList<GlobalMarketSummary> GetGlobalMarketStats()
        {
            return _statsDao.GetGlobalMarketStats();
        }

        [Route("/DataMaintenanceService/SaveCAFundForecasts")]
        [HttpPost]
        public void SaveCAFundForecasts(IList<CAFundForecast> fundForecasts)
        {
            _fundForecastDao.SaveCAFundForecasts(fundForecasts);
        }

        [Route("/DataMaintenanceService/GetGlobalMarketMonthEndLevel")]
        [HttpPost]
        public double? GetGlobalMarketMonthEndLevel(SecurityReturnParameters parameters)
        {
            Nullable<DateTime> startDate = DateUtils.ConvertToOADate(parameters.FromDate);
            if (startDate == null)
                startDate = DateUtils.ConvertToDate(parameters.FromDate, "yyyy-MM-dd");
            if (startDate == null)
                startDate = DateUtils.ConvertToDate(parameters.FromDate, "MM/dd/yyyy");

            double? result = null;
            IDictionary<string, IDictionary<DateTime, Nullable<double>>> dict = _cache.Get<IDictionary<string, IDictionary<DateTime, Nullable<double>>>>(CacheKeys.GMM_MONTH_END_HIST);
            IDictionary<DateTime, Nullable<double>> histValuesDict;
            if (dict.TryGetValue(parameters.Ticker, out histValuesDict))
                histValuesDict.TryGetValue(startDate.GetValueOrDefault(), out result);

            return result;
        }

        [Route("/DataMaintenanceService/GetGlobalMarketHistory")]
        [HttpPost]
        public IList<GlobalMarketHistory> GetGlobalMarketHistory(SecurityReturnParameters parameters)
        {
            Nullable<DateTime> startDate = DateUtils.ConvertToOADate(parameters.FromDate);
            if (startDate == null)
                startDate = DateUtils.ConvertToDate(parameters.FromDate, "yyyy-MM-dd");
            if (startDate == null)
                startDate = DateUtils.ConvertToDate(parameters.FromDate, "MM/dd/yyyy");

            Nullable<DateTime> endDate = DateUtils.ConvertToOADate(parameters.ToDate);
            if (endDate == null)
                endDate = DateUtils.ConvertToDate(parameters.ToDate, "yyyy-MM-dd");
            if (endDate == null)
                endDate = DateUtils.ConvertToDate(parameters.ToDate, "MM/dd/yyyy");

            IList<GlobalMarketHistory> list = _statsDao.GetGlobalMarketHistory(parameters.Ticker, startDate.GetValueOrDefault(), endDate.GetValueOrDefault());
            return list.OrderByDescending(x => x.EffectiveDate).ToList<GlobalMarketHistory>();
        }

        [Route("/DataMaintenanceService/GetFundEstimatedLeverage")]
        [HttpGet]
        public IList<FundLeverage> GetFundEstimatedLeverage()
        {
            IDictionary<string, FundLeverage> dict = _fundSupplementalDataDao.GetFundLeverageRatios();
            return dict.Values.ToList<FundLeverage>();
        }

        [Route("/DataMaintenanceService/GetFundNavs")]
        [HttpGet]
        public IList<FundNav> GetFundNavs()
        {
            IDictionary<string, FundNav> dict = _cache.Get<IDictionary<string, FundNav>>(CacheKeys.FUND_NAVS);
            return dict.Values.ToList<FundNav>();
        }

        [Route("/DataMaintenanceService/GetSectorDiscountStats")]
        [HttpGet]
        public IList<SectorDiscountStats> GetSectorDiscountStats()
        {
            IDictionary<string, SectorDiscountStats> dict = _cache.Get<IDictionary<string, SectorDiscountStats>>(CacheKeys.SECTOR_HIST_DISCOUNT_STATS);
            IList<SectorDiscountStats> list = new List<SectorDiscountStats>(dict.Values);
            return list;
        }

        [Route("/DataMaintenanceService/GetHistoricalDiscountStatsSecurityList")]
        [HttpPost]
        public IList<SecurityMaster> GetHistoricalDiscountStatsSecurityList(HistoricalDiscountStatsParameters parameters)
        {
            return _statsDao.GetFundDiscountStatsSecurityList(parameters.SecurityType
                , parameters.Country, parameters.GroupName, parameters.Period);
        }

        [Route("/DataMaintenanceService/GetPortHoldingsForTicker")]
        [HttpPost]
        public IList<FundHolding> GetPortHoldingsForTicker(ServiceDataContract reqParams)
        {
            return _baseDao.GetPortHoldingsForTicker(reqParams.Ticker);
        }

        [Route("/DataMaintenanceService/SaveCurrencyExposures")]
        [HttpPost]
        public void SaveCurrencyExposures(IList<FundCurrExp> list)
        {
            _fundDao.SaveCurrencyExposures(list);
            _commonOperations.PopulateFundCurrencyExposures();
        }

        [Route("/DataMaintenanceService/GetPortHoldingsExposures")]
        [HttpPost]
        public IList<PortHoldingsExposuresTO> GetPortHoldingsExposures(ServiceDataContract reqParams)
        {
            return _baseDao.GetPortHoldingsExposures(reqParams.Ticker);
        }

        [Route("/DataMaintenanceService/GetActivistSummary")]
        [HttpGet]
        public IList<ActivistSummaryTO> GetActivistSummary()
        {
            IList<ActivistSummaryTO> list = new List<ActivistSummaryTO>();
            list = _holdingsDao.GetActivistSummary();
            return list;
        }

        [Route("/DataMaintenanceService/GetActivistMaster")]
        [HttpGet]
        public IList<ActivistMasterTO> GetActivistMaster()
        {
            IList<ActivistMasterTO> list = new List<ActivistMasterTO>();
            list = _holdingsDao.GetActivistMaster();
            return list;
        }
    }
}