using aACTrader.Compliance;
using aACTrader.DAO.Repository;
using aACTrader.Model;
using aACTrader.Operations.Impl;
using aACTrader.Operations.Impl.BatchOrders;
using aACTrader.Operations.Impl.NavEstimation;
using aACTrader.Services.Admin;
using aACTrader.SignalR.Services;
using aCommons;
using aCommons.Admin;
using aCommons.Derivatives;
using aCommons.DTO;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;

namespace aACTrader.Controllers
{
    [ApiController]
    public class AdminServiceController : Controller
    {
        private readonly ILogger<AdminServiceController> _logger;
        private readonly CommonOperations _commonOperations;
        private readonly EmailOperations _emailOperations;
        private readonly FundRedemptionTriggerOperations _fundRedemptionTriggerOperations;
        private readonly FundAlertManagerNew _fundAlertManagerNew;
        private readonly BBGOperations _bbgOperations;
        private readonly SecurityPriceDao _securityPriceDao;
        private readonly PfdCommonOperations _pfdCommonOperations;
        private readonly SecurityPriceOperation _securityPriceOperations;
        private readonly TradeOrderOperations _tradeOrderOperations;
        private readonly BatchOrderOperations _batchOrderOperations;
        private readonly LogDataService _logDataService;
        private readonly DataValidationChecks _dataValidationChecks;
        private readonly AdminDao _adminDao;
        private readonly NotificationService _notificationService;
        private readonly RulesManager _rulesManager;
        private readonly SecurityFilingThresholdOperations _secFilingOperations;
        private readonly ConditionalProxyProcessor _conditionalProxyProcessor;

        public AdminServiceController(ILogger<AdminServiceController> logger
            , CommonOperations commonOperations
            , FundRedemptionTriggerOperations fundRedemptionTriggerOperations
            , FundAlertManagerNew fundAlertManagerNew
            , BBGOperations bbgOperations
            , SecurityPriceDao securityPriceDao
            , PfdCommonOperations pfdCommonOperations
            , SecurityPriceOperation securityPriceOperation
            , TradeOrderOperations tradeOrderOperations
            , BatchOrderOperations batchOrderOperations
            , LogDataService logDataService
            , DataValidationChecks dataValidationChecks
            , AdminDao adminDao
            , EmailOperations emailOperations
            , NotificationService notificationService
            , RulesManager rulesManager
            , SecurityFilingThresholdOperations secFilingOperations
            , ConditionalProxyProcessor conditionalProxyProcessor)
        {
            _logger = logger;
            _commonOperations = commonOperations;
            _fundRedemptionTriggerOperations = fundRedemptionTriggerOperations;
            _fundAlertManagerNew = fundAlertManagerNew;
            _bbgOperations = bbgOperations;
            _securityPriceDao = securityPriceDao;
            _pfdCommonOperations = pfdCommonOperations;
            _securityPriceOperations = securityPriceOperation;
            _tradeOrderOperations = tradeOrderOperations;
            _batchOrderOperations = batchOrderOperations;
            _logDataService = logDataService;
            _dataValidationChecks = dataValidationChecks;
            _adminDao = adminDao;
            _emailOperations = emailOperations;
            _notificationService = notificationService;
            _rulesManager = rulesManager;
            _secFilingOperations = secFilingOperations;
            _conditionalProxyProcessor = conditionalProxyProcessor;
            //_logger.LogInformation("Initializing AdminServiceController...");
        }

        [Route("/AdminService/UpdateBrokerReportsDataCache")]
        [HttpGet]
        public void UpdateBrokerReportsDataCache()
        {
            _commonOperations.PopulateBrokerReportDetails();
        }

        [Route("/AdminService/UpdateALMPositionsDataCache")]
        [HttpGet]
        public async void UpdateALMPositionsDataCache()
        {
            _commonOperations.UpdateALMHoldings();
            await _notificationService.PositionUpdate();
        }

        [Route("/AdminService/UpdateSecurityPricingFlags")]
        [HttpGet]
        public void UpdateSecurityPricingFlags()
        {
            _commonOperations.UpdateSecurityPriceFlags();
        }

        [Route("/AdminService/UpdateFundNavs")]
        [HttpGet]
        public void UpdateFundNavs()
        {
            _commonOperations.UpdateFundNavs();
        }

        [Route("/AdminService/UpdateFXRates")]
        [HttpGet]
        public void UpdateFXRates()
        {
            _commonOperations.PopulateFXRates();
        }

        [Route("/AdminService/UpdateUserOverrides")]
        [HttpGet]
        public void UpdateUserOverrides()
        {
            _commonOperations.PopulateUserOverrides();
        }

        [Route("/AdminService/UpdateETFRegReturns")]
        [HttpGet]
        public void UpdateETFRegReturns()
        {
            _commonOperations.PopulateETFReturns();
        }

        [Route("/AdminService/UpdateProxyReturns")]
        [HttpGet]
        public void UpdateProxyReturns()
        {
            _commonOperations.PopulateProxyETFReturns();
            _commonOperations.PopulateAltProxyETFReturns();
            _commonOperations.PopulatePortProxyETFReturns();
        }

        [Route("/AdminService/UpdateFundRightsOfferDetails")]
        [HttpGet]
        public void UpdateFundRightsOfferDetails()
        {
            _commonOperations.PopulateFundRightsOfferDetails();
        }

        [Route("/AdminService/UpdateFundTenderOfferDetails")]
        [HttpGet]
        public void UpdateFundTenderOfferDetails()
        {
            _commonOperations.PopulateFundTenderOfferDetails();
        }

        [Route("/AdminService/UpdateFundRedemptionDetails")]
        [HttpGet]
        public async void UpdateFundRedemptionDetails()
        {
            _commonOperations.PopulateFundRedemptionDetails();
            await _notificationService.FundSupplementalDataUpdate();
        }

        [Route("/AdminService/UpdateFundPortDates")]
        [HttpGet]
        public async void UpdateFundPortDates()
        {
            _commonOperations.PopulateFundPortDates();
            await _notificationService.FundSupplementalDataUpdate();
        }

        [Route("/AdminService/UpdateFundRedemptionTriggers")]
        [HttpGet]
        public void UpdateFundRedemptionTriggers()
        {
            _fundRedemptionTriggerOperations.ProcessFundRedemptionTriggers();
        }

        [Route("/AdminService/UpdateFundSupplementalDetails")]
        [HttpGet]
        public async void UpdateFundSupplementalDetails()
        {
            _commonOperations.PopulateFundNavUpdateFrequency();
            _commonOperations.PopulateFundSupplementalDetails();
            _commonOperations.PopulatePositionTickerMap();
            await _notificationService.FundSupplementalDataUpdate();
        }

        [Route("/AdminService/UpdateFundSupplementalData")]
        [HttpGet]
        public async void UpdateFundSupplementalData()
        {
            _commonOperations.PopulateFundSupplementalData();
            _commonOperations.PopulateFundSupplementalDetails();
            _commonOperations.PopulatePositionTickerMap();
            await _notificationService.FundSupplementalDataUpdate();
        }

        [Route("/AdminService/UpdateTradeTargets")]
        [HttpGet]
        public void UpdateTradeTargets()
        {
            _fundAlertManagerNew.PopulateWatchlistSecurities();
        }

        [Route("/AdminService/UpdateSecurityAlertTargets")]
        [HttpGet]
        public void UpdateSecurityAlertTargets()
        {
            _commonOperations.PopulateSecurityAlertTargets();
        }

        [Route("/AdminService/UpdateFundMaster")]
        [HttpGet]
        public void UpdateFundMaster()
        {
            _commonOperations.RefreshFundMaster();
        }

        [Route("/AdminService/UpdateDataValidationChecks")]
        [HttpGet]
        public void UpdateDataValidationChecks()
        {
            _dataValidationChecks.RunValidationChecks();
        }

        [Route("/AdminService/UpdateSecurityUpdateBBG")]
        [HttpPost]
        public void UpdateSecurityUpdateBBG(IList<BBGSecurity> securities)
        {
            _bbgOperations.PublishSecurities(securities);
        }

        [Route("/AdminService/UpdateFundNavsBBG")]
        [HttpGet]
        public void UpdateFundNavsBBG()
        {
            BBGJob jobName = new BBGJob("FundNavs");
            _bbgOperations.SubmitJobRequest(jobName);
        }

        [Route("/AdminService/UpdateAllFundNavsBBG")]
        [HttpGet]
        public void UpdateAllFundNavsBBG()
        {
            BBGJob jobName = new BBGJob("AllFundNavs");
            _bbgOperations.SubmitJobRequest(jobName);
        }

        [Route("/AdminService/UpdatePDPricesBBG")]
        [HttpGet]
        public void UpdatePDPricesBBG()
        {
            BBGJob jobName = new BBGJob("PDPrices");
            _bbgOperations.SubmitJobRequest(jobName);
        }

        [Route("/AdminService/UpdateLatestPricesBBG")]
        [HttpGet]
        public void UpdateLatestPricesBBG()
        {
            BBGJob jobName = new BBGJob("LatestPrices");
            _bbgOperations.SubmitJobRequest(jobName);
        }

        [Route("/AdminService/UpdatePDFXRatesBBG")]
        [HttpGet]
        public void UpdatePDFXRatesBBG()
        {
            BBGJob jobName = new BBGJob("PDFxRates");
            _bbgOperations.SubmitJobRequest(jobName);
        }

        [Route("/AdminService/UpdateLatestFXRatesBBG")]
        [HttpGet]
        public void UpdateLatestFXRatesBBG()
        {
            BBGJob jobName = new BBGJob("LatestFxRates");
            _bbgOperations.SubmitJobRequest(jobName);
        }

        [Route("/AdminService/UpdatePfdSecurityMasterBBG")]
        [HttpGet]
        public void UpdatePfdSecurityMasterBBG()
        {
            BBGJob jobName = new BBGJob("PfdSecurityMaster");
            _bbgOperations.SubmitJobRequest(jobName);
        }

        [Route("/AdminService/UpdateSecurityMasterBBG")]
        [HttpGet]
        public void UpdateSecurityMasterBBG()
        {
            BBGJob jobName = new BBGJob("SecurityMaster");
            _bbgOperations.SubmitJobRequest(jobName);
        }

        [Route("/AdminService/UpdateHistoricalSecurityReturnsBBG")]
        [HttpGet]
        public void UpdateHistoricalSecurityReturnsBBG()
        {
            BBGJob jobName = new BBGJob("SecurityReturns");
            _bbgOperations.SubmitJobRequest(jobName);
        }

        [Route("/AdminService/UpdateForwardCurvesBBG")]
        [HttpGet]
        public void UpdateForwardCurvesBBG()
        {
            BBGJob jobName = new BBGJob("UpdateForwardCurves");
            _bbgOperations.SubmitJobRequest(jobName);
        }

        [Route("/AdminService/RunMarketDataService")]
        [HttpGet]
        public void RunMarketDataService()
        {
            BBGJob jobName = new BBGJob("MarketDataService");
            _bbgOperations.SubmitJobRequest(jobName);
        }

        [Route("/AdminService/RunBatchDataService")]
        [HttpGet]
        public void RunBatchDataService()
        {
            BBGJob jobName = new BBGJob("BatchDataService");
            _bbgOperations.SubmitJobRequest(jobName);
        }

        [Route("/AdminService/RunUKDataService")]
        [HttpGet]
        public void RunUKDataService()
        {
            BBGJob jobName = new BBGJob("UKDataService");
            _bbgOperations.SubmitJobRequest(jobName);
        }

        [Route("/AdminService/RunBODDataService")]
        [HttpGet]
        public void RunBODDataService()
        {
            BBGJob jobName = new BBGJob("BODDataService");
            _bbgOperations.SubmitJobRequest(jobName);
        }

        [Route("/AdminService/UpdateGlobalMarketMonthEndLevels")]
        [HttpGet]
        public void UpdateGlobalMarketMonthEndLevels()
        {
            _commonOperations.PopulateGlobalMarketMonthEndLevels();
        }

        [Route("/AdminService/UpdateSecurityPriceMapping")]
        [HttpGet]
        public void UpdateSecurityPriceMapping()
        {
            _commonOperations.PopulatePriceTickerMap();
        }

        [Route("/AdminService/UpdateNeovestPricingSymbols")]
        [HttpGet]
        public void UpdateNeovestPricingSymbols()
        {
            IList<Option> list = _securityPriceDao.GetOptionSymbols();
            foreach (Option data in list)
                data.NeovestSymbol = NeovestOperations.GetNeovestSymbol(data.BBGSymbol);
            _securityPriceDao.PopulateNeovestOptionSymbols(list);
        }

        [Route("/AdminService/UpdatePfdDataCache")]
        [HttpGet]
        public void UpdatePfdDataCache()
        {
            _pfdCommonOperations.UpdatePfdDataCache();
        }

        [Route("/AdminService/StartApplicationUpdateData")]
        [HttpGet]
        public void StartApplicationUpdateData()
        {
            _commonOperations.UpdateApplicationDataUpdateFlag("Y");
        }

        [Route("/AdminService/EndApplicationUpdateData")]
        [HttpGet]
        public void EndApplicationUpdateData()
        {
            _commonOperations.UpdateApplicationDataUpdateFlag("N");
        }

        [Route("/AdminService/UpdateMarketClosedSecuritiesFlag")]
        [HttpGet]
        public void UpdateMarketClosedSecuritiesFlag()
        {
            _securityPriceOperations.ResetClosedMarketsFlag();
        }

        [Route("/AdminService/CancelAPIOrders")]
        [HttpGet]
        public void CancelAPIOrders()
        {
            _batchOrderOperations.CancelAPIOrders();
        }

        [Route("/AdminService/EnableLogService")]
        [HttpPost]
        public void EnableLogService(InputParameters reqParams)
        {
            if (reqParams != null && reqParams.Status.Equals("Y", StringComparison.CurrentCultureIgnoreCase))
                _logDataService._sendLogs = true;
            else
                _logDataService._sendLogs = false;
        }

        [Route("/AdminService/SavePrices")]
        [HttpGet]
        public void SavePrices()
        {
            _securityPriceOperations.SavePrices();
        }

        [Route("/AdminService/UpdateCryptoSecurityDetails")]
        [HttpGet]
        public void UpdateCryptoSecurityDetails()
        {
            _commonOperations.PopulateCryptoSecurityDetails();
        }

        [Route("/AdminService/UpdateManualTrades")]
        [HttpGet]
        public void UpdateManualTrades()
        {
            _commonOperations.PopulateManualTrades();
        }

        [Route("/AdminService/UpdateFundForecastFlags")]
        [HttpGet]
        public void UpdateFundForecastFlags()
        {
            _commonOperations.UpdateFundForecastFlags();
        }

        [Route("/AdminService/UpdateSecurityExtDetails")]
        [HttpGet]
        public async void UpdateSecurityExtDetails()
        {
            _commonOperations.PopulateSecurityExtDetails();
            await _notificationService.SecurityMstExtUpdate();
        }

        [Route("/AdminService/UpdateFundNavEstErrorDetails")]
        [HttpGet]
        public void UpdateFundNavEstErrorDetails()
        {
            _commonOperations.PopulateLatestFundNavEstErrDetails();
        }

        //[Route("/AdminService/SendMorningDataChecksMails")]
        //[HttpGet]
        //public void SendMorningDataChecksMails()
        //{
        //    IList<MorningMailData> maillist = _adminDao.GetMorningMailData();
        //    _emailOperations.GenerateMorningMail(maillist);
        //}

        [Route("/AdminService/GetMorningDataChecks")]
        [HttpPost]
        public IList<MorningMailData> GetMorningDataChecks(InputParameters reqParams)
        {
            IList<MorningMailData> maillist = _adminDao.GetMorningMailData(reqParams.AsofDate, reqParams.AssetGroup);
            return maillist;
        }

        [Route("/AdminService/UpdateFundCurrencyExposures")]
        [HttpGet]
        public void UpdateFundCurrencyExposures()
        {
            _commonOperations.PopulateFundCurrencyExposures();
        }

        [Route("/AdminService/RefreshOverrides")]
        [HttpGet]
        public void RefreshOverrides()
        {
            _commonOperations.RefreshOverrides();
        }

        [Route("/AdminService/UpdateComplianceCache")]
        [HttpGet]
        public void UpdateComplianceCache()
        {
            _rulesManager.Initialize();
        }

        [Route("/AdminService/RunDefaultComplianceRules")]
        [HttpGet]
        public void RunDefaultComplianceRules()
        {
            _rulesManager.RunDefaultRules();
        }

        [Route("/AdminService/RunSecurityOwnershipReport")]
        [HttpGet]
        public void RunSecurityOwnershipReport()
        {
            _secFilingOperations.CalculateSecurityOwnershipDetails();
        }

        [Route("/AdminService/GetPrimeBrokerFiles")]
        [HttpGet]
        public IList<PrimebrokerDetailsTO> GetPrimeBrokerFiles()
        {
            IList<PrimebrokerDetailsTO> list = _adminDao.GetPrimeBrokerFiles();
            return list;
        }

        [Route("/AdminService/GetITRepoMst")]
        [HttpGet]
        public IList<ITRepoMstTO> GetITRepoMst()
        {
            IList<ITRepoMstTO> list = _adminDao.GetITRepoMst();
            return list;
        }

        [Route("/AdminService/GetBrokerFilesStatusDetails")]
        [HttpPost]
        public IList<BrokerFilesStatusDetailsTO> GetBrokerFilesStatusDetails(InputParameters reqParams)
        {
            IList<BrokerFilesStatusDetailsTO> list = _adminDao.GetBrokerFilesStatusDetails(reqParams.StartDate, reqParams.EndDate, reqParams.Broker);
            return list;
        }

        [Route("/AdminService/UpdateConditionalProxies")]
        [HttpGet]
        public void UpdateConditionalProxies()
        {
            _conditionalProxyProcessor.Initialize();
        }

        [Route("/AdminService/UpdateCEFFundHistory")]
        [HttpGet]
        public void UpdateCEFFundHistory()
        {
            _commonOperations.PopulateCEFFullFundHistory();
        }

        [Route("/AdminService/UpdateFullFundHistory")]
        [HttpGet]
        public void UpdateFullFundHistory()
        {
            _commonOperations.PopulateFullFundHistory();
        }

        [Route("/AdminService/UpdateSecurityRiskFactors")]
        [HttpGet]
        public void UpdateSecurityRiskFactors()
        {
            _commonOperations.PopulateSecurityRiskFactors();
        }
    }
}
