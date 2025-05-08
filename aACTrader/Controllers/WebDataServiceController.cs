using aACTrader.DAO.Repository;
using aACTrader.Model;
using aACTrader.Operations.Impl;
using aACTrader.Operations.Reports;
using aACTrader.SignalR.Services;
using aCommons;
using aCommons.Admin;
using aCommons.Agile;
using aCommons.Alerts;
using aCommons.Cef;
using aCommons.Compliance;
using aCommons.Crypto;
using aCommons.DTO;
using aCommons.DTO.CEFA;
using aCommons.DTO.Fidelity;
using aCommons.DTO.JPM;
using aCommons.Fund;
using aCommons.MarketMonitor;
using aCommons.Security;
using aCommons.Trading;
using aCommons.Utils;
using aCommons.Web;
using LazyCache;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace aACTrader.Controllers
{
    [ApiController]
    public class WebDataServiceController : Controller
    {
        private readonly ILogger<WebDataServiceController> _logger;
        private readonly CachingService _cache;
        private readonly CommonDao _commonDao;
        private readonly SPACDao _spacDao;
        private readonly StatsDao _statsDao;
        private readonly WebDao _webDao;
        private readonly FundHistoryDao _fundHistoryDao;
        private readonly FundCashDao _fundCashDao;
        private readonly SecurityAlertDao _securityAlertDao;
        private readonly HoldingsDao _holdingsDao;
        private readonly CryptoDao _cryptoDao;
        private readonly AgileDao _agileDao;
        private readonly TradingDao _tradingDao;
        private readonly FundDao _fundDao;
        private readonly BrokerDataDao _brokerDataDao;
        private readonly ComplianceDao _complianceDao;
        private readonly CommonOperations _commonOperations;
        private readonly FundHistoryOperations _fundHistoryOperations;
        private readonly FundExposureReport _fundExposureReport;
        private readonly GlobalMarketMonitorOperations _globalMarketMonitorOperations;
        private readonly JPMMarginOperations _jpmMarginOperations;
        private readonly WebDataOperations _webDataOperations;
        private readonly SecurityPriceOperation _securityPriceOperation;
        private readonly TradeOrderOperations _tradeOrderOperations;
        private readonly FundHoldingsOperations _fundHoldingsOperations;
        private readonly HoldingExposureReport _holdingExposureReport;
        private readonly NotificationService _notificationService;
        private readonly DailyPnLReport _dailyPnLReport;
        private readonly SecurityRiskDao _securityRiskDao;

        public WebDataServiceController(ILogger<WebDataServiceController> logger
            , CachingService cache
            , CommonDao commonDao
            , SPACDao spacDao
            , StatsDao statsDao
            , WebDao webDao
            , FundHistoryDao fundHistoryDao
            , FundCashDao fundCashDao
            , SecurityAlertDao securityAlertDao
            , HoldingsDao holdingsDao
            , CryptoDao cryptoDao
            , AgileDao agileDao
            , TradingDao tradingDao
            , FundDao fundDao
            , BrokerDataDao brokerDataDao
            , ComplianceDao complianceDao
            , CommonOperations commonOperations
            , FundHistoryOperations fundHistoryOperations
            , FundExposureReport fundExposureReport
            , GlobalMarketMonitorOperations globalMarketMonitorOperations
            , JPMMarginOperations jpmMarginOperations
            , WebDataOperations webDataOperations
            , SecurityPriceOperation securityPriceOperation
            , TradeOrderOperations tradeOrderOperations
            , FundHoldingsOperations fundHoldingsOperations
            , HoldingExposureReport holdingExposureReport
            , NotificationService notificationService
            , DailyPnLReport dailyPnLReport
            , SecurityRiskDao securityRiskDao
            )
        {
            _logger = logger;
            _cache = cache;
            _commonDao = commonDao;
            _spacDao = spacDao;
            _statsDao = statsDao;
            _webDao = webDao;
            _fundHistoryDao = fundHistoryDao;
            _fundCashDao = fundCashDao;
            _securityAlertDao = securityAlertDao;
            _holdingsDao = holdingsDao;
            _cryptoDao = cryptoDao;
            _agileDao = agileDao;
            _tradingDao = tradingDao;
            _fundDao = fundDao;
            _brokerDataDao = brokerDataDao;
            _complianceDao = complianceDao;
            _commonOperations = commonOperations;
            _fundHistoryOperations = fundHistoryOperations;
            _fundExposureReport = fundExposureReport;
            _globalMarketMonitorOperations = globalMarketMonitorOperations;
            _jpmMarginOperations = jpmMarginOperations;
            _webDataOperations = webDataOperations;
            _securityPriceOperation = securityPriceOperation;
            _tradeOrderOperations = tradeOrderOperations;
            _fundHoldingsOperations = fundHoldingsOperations;
            _holdingExposureReport = holdingExposureReport;
            _notificationService = notificationService;
            _dailyPnLReport = dailyPnLReport;
            _securityRiskDao = securityRiskDao;
            //_logger.LogInformation("Initializing WebDataServiceController...");
        }

        [Route("/WebDataService/GetFundGroupDiscountSummary")]
        [HttpPost]
        public IList<FundGroupDiscountStatsTO> GetFundGroupDiscountSummary(HistoricalDiscountStatsParameters parameters)
        {
            IDictionary<string, FundGroupDiscountStatsTO> dict = _statsDao.GetFundDiscountStatsSummary(parameters.SecurityType, parameters.Country);
            IList<FundGroupDiscountStatsTO> list = new List<FundGroupDiscountStatsTO>();
            foreach (KeyValuePair<string, FundGroupDiscountStatsTO> kvp in dict)
                list.Add(kvp.Value);
            return list;
        }

        [Route("/WebDataService/GetHistoricalDiscountDetails")]
        [HttpPost]
        public IList<FundGroupDiscountStatsHist> GetHistoricalDiscountDetails(HistoricalDiscountStatsParameters parameters)
        {
            return _statsDao.GetFundDiscountStatsDetails(
                parameters.GroupName, parameters.SecurityType, parameters.Country, parameters.StartDate, parameters.EndDate);
        }

        [Route("/WebDataService/GetFundHistoryDetails")]
        [HttpPost]
        public IList<FundHistoryTO> GetFundHistoryDetails(InputParameters reqParams)
        {
            DateTime? startDate = DateUtils.ConvertToDate(reqParams.StartDate, "yyyy-MM-dd");
            DateTime? endDate = DateUtils.ConvertToDate(reqParams.EndDate, "yyyy-MM-dd");
            return _fundHistoryDao.GetFundHistoryDetails(reqParams.Ticker, startDate.GetValueOrDefault(), endDate.GetValueOrDefault(), reqParams.NavFreq);
        }

        [Route("/WebDataService/GetREITHistoryDetails")]
        [HttpPost]
        public IList<FundHistoryTO> GetREITHistoryDetails(InputParameters reqParams)
        {
            DateTime? startDate = DateUtils.ConvertToDate(reqParams.StartDate, "yyyy-MM-dd");
            DateTime? endDate = DateUtils.ConvertToDate(reqParams.EndDate, "yyyy-MM-dd");
            return _fundHistoryDao.GetREITHistoryDetails(reqParams.Ticker, startDate.GetValueOrDefault(), endDate.GetValueOrDefault());
        }

        [Route("/WebDataService/GetDividendExSchedule")]
        [HttpGet]
        public IList<DividendScheduleTO> GetDividendExSchedule()
        {
            return _webDao.GetDividendSchedule();
        }

        [Route("/WebDataService/GetCorporateActions")]
        [HttpGet]
        public IList<CorporateAction> GetCorporateActions()
        {
            return _webDao.GetCorporateActions();
        }

        [Route("/WebDataService/GetSecurityAlerts")]
        [HttpGet]
        public IList<SecurityAlert> GetSecurityAlerts()
        {
            return _securityAlertDao.GetSecurityAlerts();
        }

        [Route("/WebDataService/GetSecurityAlertsForUser")]
        [HttpPost]
        public IList<SecurityAlert> GetSecurityAlertsForUser(InputParameters reqParams)
        {
            return _securityAlertDao.GetSecurityAlerts(reqParams.UserName);
        }

        [Route("/WebDataService/GetSecurityAlertDetails")]
        [HttpPost]
        public IList<SecurityAlertDetail> GetSecurityAlertDetails(InputParameters reqParams)
        {
            IList<SecurityAlertDetail> securityAlertDetails = _cache.Get<IList<SecurityAlertDetail>>(CacheKeys.SECURITY_ALERTS);
            IList<SecurityAlertDetail> list = new List<SecurityAlertDetail>();

            bool includeSecurity = true;
            foreach (SecurityAlertDetail data in securityAlertDetails)
            {
                includeSecurity = true;

                //Alerts by User
                if (!reqParams.UserName.Equals("All"))
                    if (!reqParams.UserName.Equals(data.SecurityAlert.UserName, StringComparison.CurrentCultureIgnoreCase))
                        includeSecurity = false;

                //Alerts by Country
                if (includeSecurity && !reqParams.Country.Equals("All")
                    && !reqParams.Country.Equals(data.SecurityAlert.CountryCode, StringComparison.CurrentCultureIgnoreCase))
                    includeSecurity = false;

                //Alerts by Asset Type
                if (includeSecurity && !reqParams.AssetType.Equals("All")
                    && !reqParams.AssetType.Equals(data.SecurityAlert.AssetType, StringComparison.CurrentCultureIgnoreCase))
                    includeSecurity = false;

                if (includeSecurity)
                    list.Add(data);
            }
            return list.OrderBy(s => s.SecurityAlert.AlertType)
                .ThenBy(s => s.SecurityAlert.Ticker1)
                .ToList<SecurityAlertDetail>();
        }

        [Route("/WebDataService/GetFundSummaryExposures")]
        [HttpGet]
        public IList<FundSummaryExpTO> GetFundSummaryExposures()
        {
            return _fundCashDao.GetFundSummaryExposures();
        }

        [Route("/WebDataService/GetFundDetailExposures")]
        [HttpGet]
        public IList<FundDetailExpTO> GetFundDetailExposures()
        {
            return _fundCashDao.GetFundDetailExposures();
        }

        [Route("/WebDataService/GetPositionReconDetails")]
        [HttpPost]
        public IList<PositionReconTO> GetPositionReconDetails(InputParameters reqParams)
        {
            DateTime? endDate = DateUtils.ConvertToDate(reqParams.EndDate, "yyyy-MM-dd");
            return _holdingsDao.GetPositionRecon(endDate.GetValueOrDefault());
        }

        [Route("/WebDataService/SaveFundSummaryExposures")]
        [HttpPost]
        public void SaveFundSummaryExposures(IList<FundSummaryExp> list)
        {
            _fundCashDao.SaveFundSummaryExposures(list);
        }

        [Route("/WebDataService/SaveFundDetailExposures")]
        [HttpPost]
        public void SaveFundDetailExposures(IList<FundDetailExp> list)
        {
            _fundCashDao.SaveFundDetailExposures(list);
        }

        [Route("/WebDataService/GetFundDetailSummary")]
        [HttpGet]
        public IList<FundDetailTO> GetFundDetailSummary()
        {
            IDictionary<string, FundDetailTO> dict = new Dictionary<string, FundDetailTO>();

            //Populate Measures
            dict.Add("MARGIN", new FundDetailTO("Margin"));
            dict.Add("CASH", new FundDetailTO("Cash"));
            dict.Add("LONG_MV", new FundDetailTO("Long MV"));
            dict.Add("SHORT_MV", new FundDetailTO("Short MV"));
            dict.Add("FX_MTM", new FundDetailTO("Fx MTM"));
            dict.Add("NAV", new FundDetailTO("Nav"));

            //Fidelity
            IList<FundDetail> fidelityFundDetails = _cache.Get<IList<FundDetail>>(CacheKeys.FIDELITY_FUND_DETAILS);
            foreach (FundDetail fdData in fidelityFundDetails)
            {
                if (fdData.FundName.Equals("OPP", StringComparison.CurrentCultureIgnoreCase))
                {
                    dict["MARGIN"].FundOppFidelityValue = fdData.Margin;
                    dict["CASH"].FundOppFidelityValue = fdData.Cash;
                    dict["LONG_MV"].FundOppFidelityValue = fdData.LongMV;
                    dict["SHORT_MV"].FundOppFidelityValue = fdData.ShortMV;
                    dict["FX_MTM"].FundOppFidelityValue = fdData.FxMTM;
                    dict["NAV"].FundOppFidelityValue = fdData.Nav;

                    dict["MARGIN"].FidelityReportDate = fdData.EffectiveDate;
                    dict["CASH"].FidelityReportDate = fdData.EffectiveDate;
                    dict["LONG_MV"].FidelityReportDate = fdData.EffectiveDate;
                    dict["SHORT_MV"].FidelityReportDate = fdData.EffectiveDate;
                    dict["FX_MTM"].FidelityReportDate = fdData.EffectiveDate;
                    dict["NAV"].FidelityReportDate = fdData.EffectiveDate;
                }
                else if (fdData.FundName.Equals("TAC", StringComparison.CurrentCultureIgnoreCase))
                {
                    dict["MARGIN"].FundTacFidelityValue = fdData.Margin;
                    dict["CASH"].FundTacFidelityValue = fdData.Cash;
                    dict["LONG_MV"].FundTacFidelityValue = fdData.LongMV;
                    dict["SHORT_MV"].FundTacFidelityValue = fdData.ShortMV;
                    dict["FX_MTM"].FundTacFidelityValue = fdData.FxMTM;
                    dict["NAV"].FundTacFidelityValue = fdData.Nav;
                }
            }

            //JPM
            IList<FundDetail> jpmFundDetails = _cache.Get<IList<FundDetail>>(CacheKeys.JPM_FUND_DETAILS);
            foreach (FundDetail jpmData in jpmFundDetails)
            {
                if (jpmData.FundName.Equals("OPP", StringComparison.CurrentCultureIgnoreCase))
                {
                    dict["MARGIN"].FundOppJPMValue = jpmData.Margin;
                    dict["CASH"].FundOppJPMValue = jpmData.Cash;
                    dict["LONG_MV"].FundOppJPMValue = jpmData.LongMV;
                    dict["SHORT_MV"].FundOppJPMValue = jpmData.ShortMV;
                    dict["FX_MTM"].FundOppJPMValue = jpmData.FxMTM;
                    dict["NAV"].FundOppJPMValue = jpmData.EquityValue;

                    dict["MARGIN"].JPMReportDate = jpmData.EffectiveDate;
                    dict["CASH"].JPMReportDate = jpmData.EffectiveDate;
                    dict["LONG_MV"].JPMReportDate = jpmData.EffectiveDate;
                    dict["SHORT_MV"].JPMReportDate = jpmData.EffectiveDate;
                    dict["FX_MTM"].JPMReportDate = jpmData.EffectiveDate;
                    dict["NAV"].JPMReportDate = jpmData.EffectiveDate;
                }
                else if (jpmData.FundName.Equals("TAC", StringComparison.CurrentCultureIgnoreCase))
                {
                    dict["MARGIN"].FundTacJPMValue = jpmData.Margin;
                    dict["CASH"].FundTacJPMValue = jpmData.Cash;
                    dict["LONG_MV"].FundTacJPMValue = jpmData.LongMV;
                    dict["SHORT_MV"].FundTacJPMValue = jpmData.ShortMV;
                    dict["FX_MTM"].FundTacJPMValue = jpmData.FxMTM;
                    dict["NAV"].FundTacJPMValue = jpmData.EquityValue;
                }
            }

            //IB
            IList<FundDetail> ibFundDetails = _cache.Get<IList<FundDetail>>(CacheKeys.IB_FUND_DETAILS);
            foreach (FundDetail ibData in ibFundDetails)
            {
                if (ibData.FundName.Equals("OPP", StringComparison.CurrentCultureIgnoreCase))
                {
                    dict["MARGIN"].FundOppIBValue = ibData.Margin;
                    dict["CASH"].FundOppIBValue = ibData.Cash;
                    dict["LONG_MV"].FundOppIBValue = ibData.LongMV;
                    dict["SHORT_MV"].FundOppIBValue = ibData.ShortMV;
                    dict["FX_MTM"].FundOppIBValue = ibData.FxMTM;
                    dict["NAV"].FundOppIBValue = ibData.Nav;

                    dict["MARGIN"].IBReportDate = ibData.EffectiveDate;
                    dict["CASH"].IBReportDate = ibData.EffectiveDate;
                    dict["LONG_MV"].IBReportDate = ibData.EffectiveDate;
                    dict["SHORT_MV"].IBReportDate = ibData.EffectiveDate;
                    dict["FX_MTM"].IBReportDate = ibData.EffectiveDate;
                    dict["NAV"].IBReportDate = ibData.EffectiveDate;
                }
                else if (ibData.FundName.Equals("TAC", StringComparison.CurrentCultureIgnoreCase))
                {
                    dict["MARGIN"].FundTacIBValue = ibData.Margin;
                    dict["CASH"].FundTacIBValue = ibData.Cash;
                    dict["LONG_MV"].FundTacIBValue = ibData.LongMV;
                    dict["SHORT_MV"].FundTacIBValue = ibData.ShortMV;
                    dict["FX_MTM"].FundTacIBValue = ibData.FxMTM;
                    dict["NAV"].FundTacIBValue = ibData.Nav;
                }
            }

            //Jefferies
            IList<FundDetail> jeffFundDetails = _cache.Get<IList<FundDetail>>(CacheKeys.JEFFERIES_FUND_DETAILS);
            foreach (FundDetail jeffData in jeffFundDetails)
            {
                if (jeffData.FundName.Equals("OPP", StringComparison.CurrentCultureIgnoreCase))
                {
                    dict["MARGIN"].FundOppJefferiesValue = jeffData.Margin;
                    dict["CASH"].FundOppJefferiesValue = jeffData.Cash;
                    dict["LONG_MV"].FundOppJefferiesValue = jeffData.LongMV;
                    dict["SHORT_MV"].FundOppJefferiesValue = jeffData.ShortMV;
                    dict["FX_MTM"].FundOppJefferiesValue = jeffData.FxMTM;
                    dict["NAV"].FundOppJefferiesValue = jeffData.Nav;

                    dict["MARGIN"].JefferiesReportDate = jeffData.EffectiveDate;
                    dict["CASH"].JefferiesReportDate = jeffData.EffectiveDate;
                    dict["LONG_MV"].JefferiesReportDate = jeffData.EffectiveDate;
                    dict["SHORT_MV"].JefferiesReportDate = jeffData.EffectiveDate;
                    dict["FX_MTM"].JefferiesReportDate = jeffData.EffectiveDate;
                    dict["NAV"].JefferiesReportDate = jeffData.EffectiveDate;
                }
                else if (jeffData.FundName.Equals("TAC", StringComparison.CurrentCultureIgnoreCase))
                {
                    dict["MARGIN"].FundTacJefferiesValue = jeffData.Margin;
                    dict["CASH"].FundTacJefferiesValue = jeffData.Cash;
                    dict["LONG_MV"].FundTacJefferiesValue = jeffData.LongMV;
                    dict["SHORT_MV"].FundTacJefferiesValue = jeffData.ShortMV;
                    dict["FX_MTM"].FundTacJefferiesValue = jeffData.FxMTM;
                    dict["NAV"].FundTacJefferiesValue = jeffData.Nav;
                }
            }
            //EDF
            IList<FundDetail> edfFundDetails = _cache.Get<IList<FundDetail>>(CacheKeys.EDF_FUND_DETAILS);
            foreach (FundDetail edfData in edfFundDetails)
            {
                if (edfData.FundName.Equals("OPP", StringComparison.CurrentCultureIgnoreCase))
                {
                    dict["MARGIN"].FundOppEDFValue = edfData.Margin;
                    dict["CASH"].FundOppEDFValue = edfData.Cash;
                    dict["LONG_MV"].FundOppEDFValue = edfData.LongMV;
                    dict["SHORT_MV"].FundOppEDFValue = edfData.ShortMV;
                    dict["FX_MTM"].FundOppEDFValue = edfData.FxMTM;
                    dict["NAV"].FundOppEDFValue = edfData.Nav;

                    dict["MARGIN"].EDFReportDate = edfData.EffectiveDate;
                    dict["CASH"].EDFReportDate = edfData.EffectiveDate;
                    dict["LONG_MV"].EDFReportDate = edfData.EffectiveDate;
                    dict["SHORT_MV"].EDFReportDate = edfData.EffectiveDate;
                    dict["FX_MTM"].EDFReportDate = edfData.EffectiveDate;
                    dict["NAV"].EDFReportDate = edfData.EffectiveDate;
                }
                else if (edfData.FundName.Equals("TAC", StringComparison.CurrentCultureIgnoreCase))
                {
                    dict["MARGIN"].FundTacEDFValue = edfData.Margin;
                    dict["CASH"].FundTacEDFValue = edfData.Cash;
                    dict["LONG_MV"].FundTacEDFValue = edfData.LongMV;
                    dict["SHORT_MV"].FundTacEDFValue = edfData.ShortMV;
                    dict["FX_MTM"].FundTacEDFValue = edfData.FxMTM;
                    dict["NAV"].FundTacEDFValue = edfData.Nav;
                }
            }

            return dict.Values.ToList<FundDetailTO>();
        }

        [Route("/WebDataService/GetSecurityExtensionDetails")]
        [HttpGet]
        public IList<SecurityMasterExt> GetSecurityExtensionDetails()
        {
            IDictionary<string, SecurityMasterExt> dict = _commonOperations.GetSecurityExtDetails();
            IList<SecurityMasterExt> list = new List<SecurityMasterExt>(dict.Values);
            return list.OrderBy(s => s.Ticker).ToList<SecurityMasterExt>();
        }

        [Route("/WebDataService/GetSecurityExtensionInfo")]
        [HttpGet]
        public IList<SecurityMasterExt> GetSecurityExtensionInfo()
        {
            IDictionary<string, SecurityMasterExt> dict = _commonOperations.GetSecurityExtDetails();
            IList<TradeTarget> tradeTargets = _tradingDao.GetTradeTargets();
            foreach (TradeTarget tradeTarget in tradeTargets)
            {
                string id = tradeTarget.Ticker + "_" + tradeTarget.Side;
                if (dict.TryGetValue(id, out SecurityMasterExt securityMasterExt))
                {
                    securityMasterExt.TgtDscnt = tradeTarget.TgtDscnt;
                    securityMasterExt.LastModifyDate = tradeTarget.LastUpdated;
                }
                else
                {
                    securityMasterExt = new SecurityMasterExt();
                    securityMasterExt.Ticker = id;
                    securityMasterExt.TgtDscnt = tradeTarget.TgtDscnt;
                    securityMasterExt.LastModifyDate = tradeTarget.LastUpdated;
                    dict.Add(securityMasterExt.Ticker, securityMasterExt);
                }
            }
            IList<SecurityMasterExt> list = new List<SecurityMasterExt>(dict.Values);
            return list.OrderBy(s => s.Ticker).ToList<SecurityMasterExt>();
        }

        [Route("/WebDataService/SaveSecurityExtensionDetails")]
        [HttpPost]
        public async void SaveSecurityExtensionDetails(IList<SecurityMasterExt> list)
        {
            _commonOperations.SaveSecurityExtDetails(list);
            await _notificationService.SecurityMstExtUpdate();
        }

        [Route("/WebDataService/GetFundNavEstDetails")]
        [HttpPost]
        public IList<FundNavReportTO> GetFundNavEstDetails(InputParameters reqParams)
        {
            DateTime? startDate = DateUtils.ConvertToDate(reqParams.StartDate, "yyyy-MM-dd");
            DateTime? endDate = DateUtils.ConvertToDate(reqParams.EndDate, "yyyy-MM-dd");
            return _fundHistoryOperations.GetFundNavEstDetails(reqParams.Ticker, startDate.GetValueOrDefault(), endDate.GetValueOrDefault());
        }

        [Route("/WebDataService/GetLatestFundNavEstDetails")]
        [HttpPost]
        public IList<FundNavReportTO> GetLatestFundNavEstDetails(InputParameters reqParams)
        {
            double? errorThreshold = null;
            if (!string.IsNullOrEmpty(reqParams.ErrorThreshold))
                errorThreshold = DataConversionUtils.ConvertToDouble(reqParams.ErrorThreshold);
            DateTime? asofDate = DateUtils.ConvertToDate(reqParams.AsofDate, "yyyy-MM-dd");
            return _fundHistoryOperations.GetLatestFundNavEstDetails(reqParams.Country, errorThreshold, asofDate.GetValueOrDefault());
        }

        [Route("/WebDataService/GetFundExposureReport")]
        [HttpPost]
        public IList<FundExposureSummaryTO> GetFundExposureReport(InputParameters reqParams)
        {
            DateTime? startDate = DateUtils.ConvertToDate(reqParams.StartDate, "yyyy-MM-dd");
            return _fundExposureReport.GetFundExposures(reqParams.StartDate);
        }

        [Route("/WebDataService/GetFundSectorExposureReport")]
        [HttpPost]
        public IList<FundExposureSummaryTO> GetFundSectorExposureReport(InputParameters reqParams)
        {
            DateTime? startDate = DateUtils.ConvertToDate(reqParams.StartDate, "yyyy-MM-dd");
            if (reqParams.ReportType.Equals("Default"))
                return _fundExposureReport.GetFundSectorExposureReport(reqParams.StartDate);
            else
                return _fundExposureReport.GetFundSectorExposureReportNew(reqParams.StartDate);
        }

        [Route("/WebDataService/GetGMMMonthEndLevels")]
        [HttpPost]
        public IList<GlobalMarketHistory> GetGMMMonthEndLevels(InputParameters reqParams)
        {
            DateTime? startDate = DateUtils.ConvertToDate(reqParams.StartDate, "yyyy-MM-dd");
            DateTime? endDate = DateUtils.ConvertToDate(reqParams.EndDate, "yyyy-MM-dd");
            return _globalMarketMonitorOperations.GetMonthEndLevels(reqParams.Ticker);
        }

        [Route("/WebDataService/GetGlobalMarketIndicators")]
        [HttpGet]
        public IList<GlobalMarketIndicator> GetGlobalMarketIndicators()
        {
            IDictionary<string, GlobalMarketIndicator> dict = _cache.Get<IDictionary<string, GlobalMarketIndicator>>(CacheKeys.GMM_INDICATORS);
            return dict.Values.OrderBy(x => x.value).ToList<GlobalMarketIndicator>();
        }

        [Route("/WebDataService/GetJPMMarginReport")]
        [HttpPost]
        public IList<SecurityMarginDetailTO> GetJPMMarginReport(InputParameters reqParams)
        {
            return _jpmMarginOperations.GenerateMarginReport(reqParams.FundName);
        }

        [Route("/WebDataService/GetCryptoSecurityDetails")]
        [HttpGet]
        public IList<CryptoSecMst> GetCryptoSecurityDetails()
        {
            return _cryptoDao.GetCryptoSecurityList();
        }

        [Route("/WebDataService/SaveCryptoSecurityDetails")]
        [HttpPost]
        public void SaveCryptoSecurityDetails(IList<CryptoSecMst> list)
        {
            _cryptoDao.SaveCryptoSecurityDetails(list);
            _commonOperations.PopulateCryptoSecurityDetails();
        }

        [Route("/WebDataService/GetUserOverrideRpt")]
        [HttpGet]
        public IList<UserDataOverrideRpt> GetUserOverrideRpt()
        {
            return _commonDao.GetUserDataOverrideRpt();
        }

        [Route("/WebDataService/GetSecurityDataCheckReport")]
        [HttpPost]
        public IList<SecurityDataErrorTO> GetSecurityDataCheckReport(InputParameters reqParams)
        {
            return _webDataOperations.GetSecurityDataCheckReport(reqParams.Country, reqParams.Ticker);
        }

        [Route("/WebDataService/GetAgilePositions")]
        [HttpPost]
        public IList<AgilePosition> GetAgilePositions(InputParameters parameters)
        {
            DateTime? startDate = DateUtils.ConvertToDate(parameters.StartDate, "yyyy-MM-dd");
            DateTime? endDate = DateUtils.ConvertToDate(parameters.EndDate, "yyyy-MM-dd");
            return _agileDao.GetPositions(parameters.Ticker, startDate.GetValueOrDefault(), endDate.GetValueOrDefault());
        }

        [Route("/WebDataService/GetAgileTrades")]
        [HttpPost]
        public IList<AgileTrade> GetAgileTrades(InputParameters parameters)
        {
            DateTime? startDate = DateUtils.ConvertToDate(parameters.StartDate, "yyyy-MM-dd");
            DateTime? endDate = DateUtils.ConvertToDate(parameters.EndDate, "yyyy-MM-dd");
            return _agileDao.GetTrades(parameters.Ticker, startDate.GetValueOrDefault(), endDate.GetValueOrDefault());
        }

        [Route("/WebDataService/GetAgileDailyPerf")]
        [HttpPost]
        public IList<AgileDailyPerf> GetAgileDailyPerf(InputParameters parameters)
        {
            DateTime? startDate = DateUtils.ConvertToDate(parameters.StartDate, "yyyy-MM-dd");
            DateTime? endDate = DateUtils.ConvertToDate(parameters.EndDate, "yyyy-MM-dd");
            return _agileDao.GetDailyPerf(parameters.Ticker, startDate.GetValueOrDefault(), endDate.GetValueOrDefault());
        }

        [Route("/WebDataService/GetSharesImbalanceList")]
        [HttpGet]
        public IList<SharesImbalanceTO> GetSharesImbalanceList()
        {
            return _securityPriceOperation.GetSharesImbalanceDetails();
        }

        [Route("/WebDataService/GetJPMCurrenctExposures")]
        [HttpGet]
        public IList<FundCurrencyExposureTO> GetJPMCurrenctExposures()
        {
            IDictionary<string, FundCurrencyExposureTO> dict = _fundCashDao.GetFundCurrencyExposures("JPM");
            IList<FundCurrencyExposureTO> list = new List<FundCurrencyExposureTO>(dict.Values);
            return list;
        }

        [Route("/WebDataService/GetTradeTargetDiscounts")]
        [HttpGet]
        public IList<TradeTargetTO> GetTradeTargetDiscounts()
        {
            return _tradeOrderOperations.GetTradeTargetDiscounts();
        }

        [Route("/WebDataService/GetTradeTargets")]
        [HttpGet]
        public IList<TradeTarget> GetTradeTargets()
        {
            return _tradeOrderOperations.GetTradeTargets();
        }

        [Route("/WebDataService/GetALMFunctions")]
        [HttpPost]
        public IList<ALMFunction> GetALMFunctions(InputParameters parameters)
        {
            return _webDao.GetALMFunctions(parameters.FuncType, parameters.FuncCategory, parameters.FuncName, parameters.DataSrc);
        }

        [Route("/WebDataService/GetALMFunctionCategories")]
        [HttpPost]
        public IList<string> GetALMFunctionCategories(InputParameters parameters)
        {
            return _webDao.GetALMFunctionCategories(parameters.FuncType);
        }

        [Route("/WebDataService/GetFundCurrencyExposures")]
        [HttpGet]
        public IList<FundCurrExpTO> GetFundCurrencyExposures()
        {
            IList<FundCurrExpTO> _list = _webDataOperations.GetFundCurrencyExposures();
            return _list;
        }

        [Route("/WebDataService/GetAlmHoldingsByDate")]
        [HttpPost]
        public IList<PositionTO> GetAlmHoldingsByDate(InputParameters parameters)
        {
            DateTime? asOfDate = DateUtils.ConvertToDate(parameters.AsofDate, "yyyy-MM-dd");
            string fundName = parameters.FundName;
            string longShortPosInd = parameters.Side;
            string topHoldings = parameters.ErrorThreshold;
            if ("All".Equals(fundName)) fundName = null;
            if ("All".Equals(longShortPosInd)) longShortPosInd = null;
            if ("All".Equals(topHoldings)) topHoldings = null;
            return _fundHoldingsOperations.GetAlmHoldingsByDate(asOfDate.GetValueOrDefault(), fundName, longShortPosInd, topHoldings);
        }

        [Route("/WebDataService/GetHoldingsByDate")]
        [HttpPost]
        public IList<HoldingExposureTO> GetHoldingsByDate(InputParameters parameters)
        {
            DateTime? asOfDate = DateUtils.ConvertToDate(parameters.AsofDate, "yyyy-MM-dd");
            string longShortPosInd = parameters.Side;
            string topHoldings = parameters.ErrorThreshold;
            if ("All".Equals(longShortPosInd)) longShortPosInd = null;
            if ("All".Equals(topHoldings)) topHoldings = null;
            return _holdingExposureReport.GetExposureReport(asOfDate.GetValueOrDefault(), longShortPosInd, topHoldings, parameters.GroupBy);
        }

        [Route("/WebDataService/GetJPMPnL")]
        [HttpPost]
        public IList<JPMSecurityPerfTO> GetJPMPnL(InputParameters parameters)
        {
            Nullable<DateTime> startDate = DateUtils.ConvertToOADate(parameters.StartDate);
            if (startDate == null)
                startDate = DateUtils.ConvertToDate(parameters.StartDate, "yyyy-MM-dd");
            if (startDate == null)
                startDate = DateUtils.ConvertToDate(parameters.StartDate, "MM/dd/yyyy");

            Nullable<DateTime> endDate = DateUtils.ConvertToOADate(parameters.EndDate);
            if (endDate == null)
                endDate = DateUtils.ConvertToDate(parameters.EndDate, "yyyy-MM-dd");
            if (endDate == null)
                endDate = DateUtils.ConvertToDate(parameters.EndDate, "MM/dd/yyyy");

            string fund = parameters.FundName;
            if ("All".Equals(parameters.FundName))
                fund = null;
            string ticker = parameters.Ticker;
            if (string.IsNullOrEmpty(parameters.Ticker))
                ticker = null;

            IList<JPMSecurityPerfTO> list = _fundDao.GetJPMPnL(startDate.GetValueOrDefault(), endDate.GetValueOrDefault(), fund, ticker);
            return list.OrderBy(x => x.SecDesc).OrderByDescending(x => x.AsOfDate).ToList<JPMSecurityPerfTO>();
        }

        [Route("/WebDataService/GetFidelityPnL")]
        [HttpPost]
        public IList<PositionTypeDetailsTO> GetFidelityPnL(InputParameters parameters)
        {
            Nullable<DateTime> startDate = DateUtils.ConvertToOADate(parameters.StartDate);
            if (startDate == null)
                startDate = DateUtils.ConvertToDate(parameters.StartDate, "yyyy-MM-dd");
            if (startDate == null)
                startDate = DateUtils.ConvertToDate(parameters.StartDate, "MM/dd/yyyy");

            Nullable<DateTime> endDate = DateUtils.ConvertToOADate(parameters.EndDate);
            if (endDate == null)
                endDate = DateUtils.ConvertToDate(parameters.EndDate, "yyyy-MM-dd");
            if (endDate == null)
                endDate = DateUtils.ConvertToDate(parameters.EndDate, "MM/dd/yyyy");

            string fund = parameters.FundName;
            if ("All".Equals(parameters.FundName))
                fund = null;
            string ticker = parameters.Ticker;
            if (string.IsNullOrEmpty(parameters.Ticker))
                ticker = null;

            IList<PositionTypeDetailsTO> list = _fundDao.GetFidelityPnL(startDate.GetValueOrDefault(), endDate.GetValueOrDefault(), fund, ticker);
            return list.OrderBy(x => x.SecDesc).OrderByDescending(x => x.RptDate).ToList<PositionTypeDetailsTO>();
        }

        [Route("/WebDataService/UpdateReconData")]
        [HttpPost]
        public async void UpdateReconData(IList<PositionReconTO> objlist)
        {
            IList<PositionReconTO> list = new List<PositionReconTO>();
            _holdingsDao.UpdateReconData(objlist);
            _commonOperations.UpdateALMHoldings();
            await _notificationService.PositionUpdate();
        }

        [Route("/WebDataService/GetEODHoldingsByDate")]
        [HttpPost]
        public IList<EODPositionTO> GetEODHoldingsByDate(InputParameters parameters)
        {
            DateTime? asOfDate = DateUtils.ConvertToDate(parameters.AsofDate, "yyyy-MM-dd");
            return _holdingsDao.GetEODHoldingsByDate(asOfDate.GetValueOrDefault());
        }

        [Route("/WebDataService/GetBrokerFundSummary")]
        [HttpPost]
        public IList<FundSummaryTO> GetBrokerFundSummary(InputParameters reqParams)
        {
            DateTime? asofDate = DateUtils.ConvertToDate(reqParams.AsofDate, "yyyy-MM-dd");
            return _brokerDataDao.GetFundSummary(reqParams.FundName, asofDate.GetValueOrDefault());
        }

        [Route("/WebDataService/GetFundFuturesExposureReport")]
        [HttpPost]
        public IList<FundFuturesExposureTO> GetFundFuturesExposureReport(InputParameters reqParams)
        {
            DateTime? asofDate = DateUtils.ConvertToDate(reqParams.AsofDate, "yyyy-MM-dd");
            return _fundExposureReport.GetFundFuturesExposureReport(asofDate.GetValueOrDefault(), reqParams.FundName);
        }

        [Route("/WebDataService/GetRules")]
        [HttpGet]
        public IList<RuleMst> GetRules()
        {
            return _complianceDao.GetRules();
        }

        [Route("/WebDataService/GetRuleRunList")]
        [HttpPost]
        public IList<RuleRunMst> GetRuleRunList(InputParameters reqParams)
        {
            DateTime? startdate = DateUtils.ConvertToDate(reqParams.StartDate, "yyyy-MM-dd");
            DateTime? enddate = DateUtils.ConvertToDate(reqParams.EndDate, "yyyy-MM-dd");
            return _complianceDao.GetRuleRunList(startdate.GetValueOrDefault(), enddate.GetValueOrDefault());
        }

        [Route("/WebDataService/GetRuleRunSummary")]
        [HttpPost]
        public IList<RuleRunSummary> GetRuleRunSummary(InputParameters reqParams)
        {
            return _complianceDao.GetRuleRunSummary(reqParams.RuleMstId.GetValueOrDefault());
        }

        [Route("/WebDataService/GetRuleRunDetails")]
        [HttpPost]
        public IList<RuleRunDetail> GetRuleRunDetails(InputParameters reqParams)
        {
            return _complianceDao.GetRuleRunDetails(reqParams.RuleMstId.GetValueOrDefault(), reqParams.Ticker);
        }

        [Route("/WebDataService/GetSecOwnershipDetails")]
        [HttpPost]
        public IList<SecOwnership> GetSecOwnershipDetails(InputParameters reqParams)
        {
            return _securityAlertDao.GetSecOwnershipDetails(reqParams.Ticker, reqParams.StartDate, reqParams.EndDate);
        }

        [Route("/WebDataService/GetSecFilingDetails")]
        [HttpPost]
        public IList<SecFilingDetail> GetSecFilingDetails(InputParameters reqParams)
        {
            return _securityAlertDao.GetSecFilingDetails(reqParams.Ticker, reqParams.StartDate, reqParams.EndDate);
        }

        [Route("/WebDataService/SaveSecurityFilingDetails")]
        [HttpPost]
        public void SaveSecurityFilingDetails(IList<SecFilingDetail> list)
        {
            _securityAlertDao.SaveSecurityFilingDetails(list);
        }

        [Route("/WebDataService/SaveComplianceFileContent")]
        [HttpPost]
        public void SaveComplianceFileContent(InputParameters reqParams)
        {
            if (!string.IsNullOrEmpty(reqParams.FileName) && !string.IsNullOrEmpty(reqParams.FileContent))
            {
                try
                {
                    //TODO: externalize the file path
                    byte[] fileBytes = Encoding.ASCII.GetBytes(reqParams.FileContent);
                    string filePath = Path.Combine("C:\\Dropbox\\Dropbox (Almitas)\\Almitas Team Folder\\IT\\Compliance\\TradeList\\", reqParams.FileName);
                    System.IO.File.WriteAllBytes(filePath, fileBytes);
                }
                catch (Exception ex)
                {
                    _logger.LogError($"Error saving file: {ex.Message}", ex);
                }
            }
        }

        [Route("/WebDataService/GetPortCurrencyExposureOverrides")]
        [HttpGet]
        public IList<FundCurrExpTO> GetPortCurrencyExposureOverrides()
        {
            IDictionary<string, FundCurrExpTO> dict = _cache.Get<IDictionary<string, FundCurrExpTO>>(CacheKeys.FUND_CURRENCY_EXPOSURES);
            IList<FundCurrExpTO> _list = dict.Values.ToList<FundCurrExpTO>();
            return _list;
        }

        [Route("/WebDataService/SavePortCurrencyExposureOverrides")]
        [HttpPost]
        public void SavePortCurrencyExposureOverrides(IList<FundCurrExpTO> list)
        {
            _fundDao.SavePortCurrencyExposureOverrides(list);
            _commonOperations.PopulateFundCurrencyExposures();
        }

        [Route("/WebDataService/GetFundHistoricalStats")]
        [HttpPost]
        public IList<FundHistStatsTO> GetFundHistoricalStats(InputParameters reqParams)
        {
            return _commonOperations.GetFundHistoricalStats(reqParams.Country, reqParams.SecurityType, reqParams.Ticker);
        }

        [Route("/WebDataService/GetBatchMonitorDetails")]
        [HttpGet]
        public IList<BatchMonitorTO> GetBatchMonitorDetails()
        {
            IDictionary<string, BatchMonitorTO> dict = _cache.Get<Dictionary<string, BatchMonitorTO>>(CacheKeys.BATCH_MONITOR);
            return dict.Values.ToList<BatchMonitorTO>();
        }

        [Route("/WebDataService/GetFundNavEstErrStatsDetails")]
        [HttpPost]
        public IList<FundNavEstErrorStatsTO> GetFundNavEstErrStatsDetails(InputParameters reqParams)
        {
            return _fundDao.GetFundNavEstErrStatsDetails(reqParams.StartDate, reqParams.EndDate, reqParams.Country);
        }

        [Route("/WebDataService/GetFundNavEstErrByTicker")]
        [HttpPost]
        public IList<FundNavEstErrorStatsTO> GetFundNavEstErrByTicker(InputParameters reqParams)
        {
            return _fundDao.GetFundNavEstErrByTicker(reqParams.Ticker);
        }

        [Route("/WebDataService/GetAlmPositionsByDate")]
        [HttpPost]
        public IList<AlmHoldingPerfTO> GetAlmPositionsByDate(InputParameters parameters)
        {
            if (parameters.GroupBy.Equals("Sector"))
            {
                DailyPnLTO dailyPnLTO = _cache.Get<DailyPnLTO>(CacheKeys.DAILY_PNL_SUMMARY);
                if (dailyPnLTO != null)
                    return dailyPnLTO.Positions.OrderBy(p => p.SortId).ThenBy(p => p.GrpName).ThenByDescending(p => p.IsGroup).ToList<AlmHoldingPerfTO>();
                return null;
            }
            else
            {
                DateTime? asOfDate = DateUtils.ConvertToDate(parameters.AsofDate, "yyyy-MM-dd");
                IDictionary<string, AlmHoldingPerfTO> dict = _dailyPnLReport.GetDailyPnL(asOfDate.GetValueOrDefault(), parameters.GroupBy);
                IList<AlmHoldingPerfTO> list = dict.Values.ToList<AlmHoldingPerfTO>();
                return list.OrderBy(p => p.SortId).ThenBy(p => p.GrpName).ThenByDescending(p => p.IsGroup).ToList<AlmHoldingPerfTO>();
            }
        }

        [Route("/WebDataService/GetAlmLongPositions")]
        [HttpGet]
        public IList<PositionRankTO> GetAlmLongPositions()
        {
            DailyPnLTO dailyPnLTO = _cache.Get<DailyPnLTO>(CacheKeys.DAILY_PNL_SUMMARY);
            if (dailyPnLTO != null)
                return dailyPnLTO.LongPositions;
            return null;
        }

        [Route("/WebDataService/GetAlmShortPositions")]
        [HttpGet]
        public IList<PositionRankTO> GetAlmShortPositions()
        {
            DailyPnLTO dailyPnLTO = _cache.Get<DailyPnLTO>(CacheKeys.DAILY_PNL_SUMMARY);
            if (dailyPnLTO != null)
                return dailyPnLTO.ShortPositions;
            return null;
        }

        [Route("/WebDataService/GetAlmPriceChanges")]
        [HttpGet]
        public IList<PositionPerfTO> GetAlmPriceChanges()
        {
            DailyPnLTO dailyPnLTO = _cache.Get<DailyPnLTO>(CacheKeys.DAILY_PNL_SUMMARY);
            if (dailyPnLTO != null)
                return dailyPnLTO.PricePerf;
            return null;
        }

        [Route("/WebDataService/GetAlmMVChanges")]
        [HttpGet]
        public IList<PositionPerfTO> GetAlmMVChanges()
        {
            DailyPnLTO dailyPnLTO = _cache.Get<DailyPnLTO>(CacheKeys.DAILY_PNL_SUMMARY);
            if (dailyPnLTO != null)
                return dailyPnLTO.MVPerf;
            return null;
        }

        [Route("/WebDataService/GetAlmFundChanges")]
        [HttpGet]
        public IList<SectorSummaryTO> GetAlmFundChanges()
        {
            DailyPnLTO dailyPnLTO = _cache.Get<DailyPnLTO>(CacheKeys.DAILY_PNL_SUMMARY);
            if (dailyPnLTO != null)
                return dailyPnLTO.SectorSummary;
            return null;
        }

        [Route("/WebDataService/GetPortHoldingDataChecks")]
        [HttpGet]
        public IList<FundHoldingReturn> GetPortHoldingDataChecks()
        {
            return _webDao.GetPortHoldingDataChecks();
        }

        [Route("/WebDataService/GetCEFAFieldMaster")]
        [HttpPost]
        public IList<CEFAFieldMstTO> GetCEFAFieldMaster(InputParameters reqParams)
        {
            return _fundHistoryDao.GetCEFAFieldMaster(reqParams.AssetGroup);
        }

        [Route("/WebDataService/GetCEFAFieldHistory")]
        [HttpPost]
        public IList<CEFAFieldDetTO> GetCEFAFieldHistory(InputParameters reqParams)
        {
            return _fundHistoryDao.GetCEFAFieldHistory(reqParams.Ticker, reqParams.AssetGroup);
        }

        [Route("/WebDataService/GetManualDataOverrides")]
        [HttpGet]
        public IList<ManualDataOverrideTO> GetManualDataOverrides()
        {
            return _webDao.GetManualDataOverrides();
        }


        [Route("/WebDataService/GetNavDataOverrides")]
        [HttpPost]
        public IList<NavOverridesTO> GetNavDataOverrides(InputParameters reqParams)
        {
            return _webDao.GetNavDataOverrides(reqParams.StartDate, reqParams.EndDate, reqParams.Ticker);
        }

        [Route("/WebDataService/GetSwapMarginDetails")]
        [HttpPost]
        public IList<SwapMarginDetTO> GetSwapMarginDetails(InputParameters reqParams)
        {
            return _webDao.GetSwapMarginDetails(reqParams.Broker, reqParams.Ticker);
        }

        [Route("/WebDataService/GetCryptoMarginDetails")]
        [HttpPost]
        public IList<SwapMarginDetTO> GetCryptoMarginDetails(InputParameters reqParams)
        {
            return _webDao.GetCryptoMarginDetails(reqParams.Broker, reqParams.Ticker);
        }

        [Route("/WebDataService/GetDailyDataLoadSummary")]
        [HttpPost]
        public IList<DailyDataLoadSummaryTO> GetDailyDataLoadSummary(InputParameters reqParams)
        {
            return _webDao.GetDailyDataLoadSummary(reqParams.StartDate, reqParams.EndDate);
        }

        [Route("/WebDataService/GetBBGFundHistoryDetails")]
        [HttpPost]
        public IList<FundHistoryTO> GetBBGFundHistoryDetails(InputParameters reqParams)
        {
            DateTime? startDate = DateUtils.ConvertToDate(reqParams.StartDate, "yyyy-MM-dd");
            DateTime? endDate = DateUtils.ConvertToDate(reqParams.EndDate, "yyyy-MM-dd");
            return _fundHistoryDao.GetBBGFundHistory(reqParams.Ticker, startDate.GetValueOrDefault(), endDate.GetValueOrDefault());
        }

        [Route("/WebDataService/SaveSecurityRiskFactorsWithDates")]
        [HttpPost]
        public void SaveSecurityRiskFactorsWithDates(IList<SecurityRiskFactor> list)
        {
            _commonOperations.SaveSecurityRiskFactorsWithDates(list);
        }

        [Route("/WebDataService/GetSecurityRiskFactorsWithDates")]
        [HttpGet]
        public IList<SecurityRiskFactor> GetSecurityRiskFactorsWithDates()
        {
            IDictionary<string, SecurityRiskFactor> dict = _cache.Get<Dictionary<string, SecurityRiskFactor>>(CacheKeys.SECURITY_RISK_FACTORS_WITH_DATES);
            return dict.Values.ToList<SecurityRiskFactor>();
        }

        [Route("/WebDataService/GetALMFxFwdPosDetails")]
        [HttpPost]
        public IList<ALMFxFwdPosDetailsTO> GetALMFxFwdPosDetails(InputParameters reqParams)
        {
            return _brokerDataDao.GetALMFxFwdPosDetails();
        }

        [Route("/WebDataService/GetTenderOfferHist")]
        [HttpGet]
        public IList<ALMTenderOfferHistTO> GetTenderOfferHist()
        {
            return _commonDao.GetALMTenderOfferHistDetails();
        }

        [Route("/WebDataService/SaveALMTenderOfferHistDetails")]
        [HttpPost]
        public void SaveALMTenderOfferHistDetails(IList<ALMTenderOfferHistTO> list)
        {
            _commonDao.SaveALMTenderOfferHistDetails(list);
        }

        [Route("/WebDataService/GetPFICPositionDetails")]
        [HttpPost]
        public IList<PFICPositionDetailsTO> GetPFICPositionDetails(InputParameters reqParams)
        {
            return _webDao.GetPFICPositionDetails(reqParams.StartDate, reqParams.EndDate, reqParams.FundName);
        }
    }
}