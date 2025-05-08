using aACTrader.DAO.Repository;
using aACTrader.Model;
using aACTrader.Operations.Impl;
using aACTrader.Operations.Impl.BatchOrders;
using aACTrader.Operations.Reports;
using aACTrader.SignalR.Services;
using aCommons;
using aCommons.Cef;
using aCommons.Derivatives;
using aCommons.DTO;
using aCommons.DTO.BAML;
using aCommons.DTO.BMO;
using aCommons.DTO.EDF;
using aCommons.DTO.Fidelity;
using aCommons.DTO.IB;
using aCommons.DTO.JPM;
using aCommons.DTO.MorganStanley;
using aCommons.DTO.TD;
using aCommons.DTO.TenderHistory;
using aCommons.Pfd;
using aCommons.SPACs;
using aCommons.Trading;
using aCommons.Utils;
using LazyCache;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using FidelityTaxlotDetailsTO = aCommons.DTO.Fidelity.TaxLotDetailsTO;
using TaxLotDetailsTO = aCommons.DTO.TD.TaxLotDetailsTO;

namespace aACTrader.Controllers
{
    [ApiController]
    public class CommonDataServiceController : ControllerBase
    {
        private readonly ILogger<CommonDataServiceController> _logger;
        private readonly CachingService _cache;
        private readonly FundSupplementalDataDao _fundSupplementalDataDao;
        private readonly FundHistoryDao _fundHistoryDao;
        private readonly SecurityRiskDao _securityRiskDao;
        private readonly HoldingsDao _holdingsDao;
        private readonly OptionDao _optionDao;
        private readonly CommonDao _commonDao;
        private readonly FundDao _fundDao;
        private readonly BaseDao _baseDao;
        private readonly SPACDao _spacDao;
        private readonly BrokerDataDao _brokerDataDao;
        private readonly FundCashDao _fundCashDao;
        private readonly FundHistoryOperations _fundHistoryOperations;
        private readonly CommonOperations _commonOperations;
        private readonly TradeOrderOperations _tradeOrderOperations;
        private readonly BatchOrderOperations _batchOrderOperations;
        private readonly SecurityPerformanceOperations _securityPerformanceOperations;
        private readonly NotificationService _notificationService;

        public CommonDataServiceController(ILogger<CommonDataServiceController> logger
            , CachingService cache
            , FundSupplementalDataDao fundSupplementalDataDao
            , FundHistoryDao fundHistoryDao
            , SecurityRiskDao securityRiskDao
            , HoldingsDao holdingsDao
            , OptionDao optionDao
            , CommonDao commonDao
            , FundDao fundDao
            , BaseDao baseDao
            , SPACDao spacDao
            , BrokerDataDao brokerDataDao
            , FundCashDao fundCashDao
            , FundHistoryOperations fundHistoryOperations
            , CommonOperations commonOperations
            , TradeOrderOperations tradeOrderOperations
            , BatchOrderOperations batchOrderOperations
            , SecurityPerformanceOperations securityPerformanceOperations
            , NotificationService notificationService
            )
        {
            _logger = logger;
            _cache = cache;
            _fundSupplementalDataDao = fundSupplementalDataDao;
            _fundHistoryDao = fundHistoryDao;
            _securityRiskDao = securityRiskDao;
            _holdingsDao = holdingsDao;
            _optionDao = optionDao;
            _commonDao = commonDao;
            _fundDao = fundDao;
            _baseDao = baseDao;
            _spacDao = spacDao;
            _brokerDataDao = brokerDataDao;
            _fundCashDao = fundCashDao;
            _fundHistoryOperations = fundHistoryOperations;
            _commonOperations = commonOperations;
            _tradeOrderOperations = tradeOrderOperations;
            _batchOrderOperations = batchOrderOperations;
            _securityPerformanceOperations = securityPerformanceOperations;
            _notificationService = notificationService;
            //_logger.LogInformation("Initializing CommonDataServiceController...");
        }

        [Route("/CommonDataService/GetFundBuybackCounts")]
        [HttpGet]
        public IList<FundBuyback> GetFundBuybackCounts()
        {
            return _fundSupplementalDataDao.GetFundBuybackCounts();
        }

        [Route("/CommonDataService/GetFundBuybackDetails")]
        [HttpPost]
        public IList<FundBuyback> GetFundBuybackDetails(InputParameters reqParams)
        {
            return _fundSupplementalDataDao.GetFundBuybackDetails(reqParams.Ticker);
        }

        [Route("/CommonDataService/GetFundBuybackHistory")]
        [HttpPost]
        public IList<FundBuybackTO> GetFundBuybackHistory(InputParameters reqParams)
        {
            //return _fundSupplementalDataDao.GetFundBuybackHistory(reqParams.Ticker);
            return _fundSupplementalDataDao.GetBuybackHistory(reqParams.Ticker);
        }

        [Route("/CommonDataService/GetFundBuybackSummary")]
        [HttpGet]
        public IList<BuyBackSummaryTO> GetFundBuybackSummary()
        {
            return _commonDao.GetBuyBackSummary();
        }

        [Route("/CommonDataService/GetSCMarginSummary")]
        [HttpPost]
        public IList<SCMarginSummaryTO> GetSCMarginSummary(InputParameters reqParams)
        {
            return _brokerDataDao.GetSCMarginSummary(reqParams.StartDate, reqParams.EndDate);
        }

        [Route("/CommonDataService/GetSCMarginDetails")]
        [HttpPost]
        public IList<SCMarginDetailsTO> GetSCMarginDetails(InputParameters reqParams)
        {
            return _brokerDataDao.GetSCMarginDetails(reqParams.StartDate, reqParams.EndDate);
        }

        [Route("/CommonDataService/GetSCTradeDetails")]
        [HttpPost]
        public IList<SCTradeDetailsTO> GetSCTradeDetails(InputParameters reqParams)
        {
            return _brokerDataDao.GetSCTradeDetails(reqParams.StartDate, reqParams.EndDate);
        }

        [Route("/CommonDataService/GetSCMTMDetails")]
        [HttpPost]
        public IList<SCMTMDetailsTO> GetSCMTMDetails(InputParameters reqParams)
        {
            return _brokerDataDao.GetSCMTMDetails(reqParams.StartDate, reqParams.EndDate);
        }

        [Route("/CommonDataService/GetSCSecurityReset")]
        [HttpPost]
        public IList<SCSecurityResetTO> GetSCSecurityReset(InputParameters reqParams)
        {
            return _brokerDataDao.GetSCSecurityReset(reqParams.StartDate, reqParams.EndDate);
        }

        [Route("/CommonDataService/GetSCSecurityResetDetails")]
        [HttpPost]
        public IList<SCSecurityResetDetailsTO> GetSCSecurityResetDetails(InputParameters reqParams)
        {
            return _brokerDataDao.GetSCSecurityResetDetails(reqParams.StartDate, reqParams.EndDate);
        }

        [Route("/CommonDataService/GetFundDividends")]
        [HttpPost]
        public IList<FundDividend> GetFundDividends(InputParameters reqParams)
        {
            Nullable<DateTime> startDate = DateUtils.ConvertToOADate(reqParams.StartDate);
            if (startDate == null)
                startDate = DateUtils.ConvertToDate(reqParams.StartDate, "MM/dd/yyyy");
            //if (startDate == null)
            //    startDate = DateUtils.ConvertToDate(reqParams.StartDate, "yyyy-MM-dd");

            Nullable<DateTime> endDate = DateUtils.ConvertToOADate(reqParams.EndDate);
            if (endDate == null)
                endDate = DateUtils.ConvertToDate(reqParams.EndDate, "MM/dd/yyyy");
            //if (endDate == null)
            //    endDate = DateUtils.ConvertToDate(reqParams.EndDate, "yyyy-MM-dd");
            return _fundSupplementalDataDao.GetFundDividends(reqParams.Ticker, startDate.GetValueOrDefault(), endDate.GetValueOrDefault());
        }

        [Route("/CommonDataService/GetFundDividendsFinal")]
        [HttpPost]
        public IList<FundDividend> GetFundDividendsFinal(InputParameters reqParams)
        {
            IList<FundDividend> list = new List<FundDividend>();
            DateTime? startDate = DateUtils.ConvertToDate(reqParams.StartDate, "yyyy-MM-dd");
            if (startDate == null)
                startDate = DateUtils.ConvertToDate(reqParams.StartDate, "MM/dd/yyyy");


            DateTime? endDate = DateUtils.ConvertToDate(reqParams.EndDate, "yyyy-MM-dd");
            if (endDate == null)
                endDate = DateUtils.ConvertToDate(reqParams.EndDate, "MM/dd/yyyy");
            list = _fundSupplementalDataDao.GetFundDividendsFinal(reqParams.Ticker, startDate.GetValueOrDefault(), endDate.GetValueOrDefault());
            return list;
        }

        [Route("/CommonDataService/GetFundHistoryWithForecasts")]
        [HttpPost]
        public IList<FundHistory> GetFundHistoryWithForecasts(InputParameters reqParams)
        {
            return _fundHistoryDao.GetFundHistory(reqParams.Ticker);
        }

        [Route("/CommonDataService/GetFundPerformanceCategories")]
        [HttpPost]
        public IList<FundCategoryPerformanceSummary> GetFundPerformanceCategories(InputParameters reqParams)
        {
            return _fundHistoryOperations.GetFundPerformanceCategories(_cache
                , reqParams.AssetType, reqParams.AssetClassLevel1, reqParams.Country);
        }

        [Route("/CommonDataService/GetFundPerformanceSummary")]
        [HttpPost]
        public IList<FundPerformanceSummary> GetFundPerformanceSummary(InputParameters reqParams)
        {
            IList<FundPerformanceSummary> list = _fundHistoryOperations.GetFundPerformanceSummary(_cache
                , reqParams.AssetType, reqParams.AssetClassLevel1, reqParams.Country);
            return list;
        }

        [Route("/CommonDataService/GetSecurityReturns")]
        [HttpPost]
        public IList<SecurityReturn> GetSecurityReturns(InputParameters reqParams)
        {
            return _securityRiskDao.GetSecurityReturns(reqParams.FundTicker, reqParams.Ticker);
        }

        [Route("/CommonDataService/GetCAFundHistoryWithForecasts")]
        [HttpPost]
        public IList<CAFundHistory> GetCAFundHistoryWithForecasts(InputParameters reqParams)
        {
            return _fundHistoryDao.GetCAFundHistory(reqParams.Ticker);
        }

        [Route("/CommonDataService/GetFundNotes")]
        [HttpGet]
        public IList<FundNotes> GetFundNotes()
        {
            IDictionary<string, FundNotes> dict = _cache.Get<IDictionary<string, FundNotes>>(CacheKeys.FUND_NOTES);
            IList<FundNotes> list = new List<FundNotes>(dict.Values);
            return list;
        }

        [Route("/CommonDataService/SaveFundNotes")]
        [HttpPost]
        public void SaveFundNotes(IList<FundNotes> fundNotesList)
        {
            _commonOperations.SaveFundNotes(fundNotesList);
        }

        [Route("/CommonDataService/GetPositionLongShortDetails")]
        [HttpGet]
        public IList<PositionLongShortDetail> GetPositionLongShortDetails()
        {
            return _holdingsDao.GetPositionLongShortDetails();
        }

        [Route("/CommonDataService/GetFundRedemptionTriggers")]
        [HttpPost]
        public IList<FundRedemptionTrigger> GetFundRedemptionTriggers(InputParameters reqParams)
        {
            IDictionary<string, FundRedemptionTrigger> dict = _cache.Get<IDictionary<string, FundRedemptionTrigger>>(CacheKeys.FUND_REDEMPTION_TRIGGERS);
            IList<FundRedemptionTrigger> list = new List<FundRedemptionTrigger>();
            foreach (FundRedemptionTrigger data in dict.Values)
            {
                if (data.Ticker.Equals(reqParams.Ticker, StringComparison.CurrentCultureIgnoreCase))
                    list.Add(data);
            }
            return list;
        }

        [Route("/CommonDataService/GetAllFundRedemptionTriggers")]
        [HttpGet]
        public IList<FundRedemptionTrigger> GetAllFundRedemptionTriggers()
        {
            IDictionary<string, FundRedemptionTrigger> dict = _cache.Get<IDictionary<string, FundRedemptionTrigger>>(CacheKeys.FUND_REDEMPTION_TRIGGERS);
            IList<FundRedemptionTrigger> list = new List<FundRedemptionTrigger>(dict.Values);
            return list;
        }

        [Route("/CommonDataService/SubmitOrders")]
        [HttpPost]
        public void SubmitOrders(IList<NewOrder> orders)
        {
            _batchOrderOperations.ProcessNewOrders(orders);
        }

        [Route("/CommonDataService/SubmitOrdersSim")]
        [HttpPost]
        public void SubmitOrdersSim(IList<NewOrder> orders)
        {
            _batchOrderOperations.ProcessNewOrdersSim(orders);
        }

        [Route("/CommonDataService/GetFundRedemptionDiscountTriggerDetails")]
        [HttpPost]
        public IList<FundRedemptionTriggerDetail> GetFundRedemptionDiscountTriggerDetails(InputParameters reqParams)
        {
            return _fundSupplementalDataDao.GetFundRedemptionDiscountTriggerDetails(reqParams.Ticker);
        }

        [Route("/CommonDataService/GetFundRedemptionNavReturnTriggerDetails")]
        [HttpPost]
        public IList<FundRedemptionTriggerDetail> GetFundRedemptionNavReturnTriggerDetails(InputParameters reqParams)
        {
            return _fundSupplementalDataDao.GetFundRedemptionNavReturnTriggerDetails(reqParams.Ticker, reqParams.TriggerType, reqParams.AssetType);
        }

        [Route("/CommonDataService/GetOptionChain")]
        [HttpPost]
        public IList<OptionChain> GetOptionChain(InputParameters reqParams)
        {
            return _optionDao.GetOptionChain(reqParams.Ticker, reqParams.AssetGroup);
        }

        [Route("/CommonDataService/GetOptionDetails")]
        [HttpGet]
        public IList<OptionDetail> GetOptionDetails()
        {
            IDictionary<string, OptionDetail> dict = _cache.Get<IDictionary<string, OptionDetail>>(CacheKeys.OPTION_DETAILS);
            IList<OptionDetail> list = new List<OptionDetail>(dict.Values);
            return list;
        }

        [Route("/CommonDataService/GetSecurityValidationChecks")]
        [HttpPost]
        public IList<string> GetSecurityValidationChecks(InputParameters reqParams)
        {
            IDictionary<string, string> dict = _cache.Get<IDictionary<string, string>>(CacheKeys.SECURITY_DATA_VALIDATION_CHECKS);
            IList<string> list = new List<string>();
            if (dict.TryGetValue(reqParams.Ticker, out string validationString))
                list.Add(validationString);
            return list;
        }

        [Route("/CommonDataService/GetSecurityNotes")]
        [HttpPost]
        public FundNotes GetSecurityNotes(InputParameters reqParams)
        {
            IDictionary<string, string> dict = _cache.Get<IDictionary<string, string>>(CacheKeys.SECURITY_DATA_VALIDATION_CHECKS);
            IDictionary<string, FundNotes> fundNotesDict = _cache.Get<IDictionary<string, FundNotes>>(CacheKeys.FUND_NOTES);

            //see if there are fund notes, if not create fund notes object and attach data validation check info
            FundNotes fundNotes;
            fundNotesDict.TryGetValue(reqParams.Ticker, out fundNotes);

            if (fundNotes == null)
                fundNotes = new FundNotes();

            //populate data validation check info
            if (dict.TryGetValue(reqParams.Ticker, out string validationString))
                fundNotes.DataChecks = validationString;
            return fundNotes;
        }

        [Route("/CommonDataService/GetLatestPositions")]
        [HttpGet]
        public IList<Holding> GetLatestPositions()
        {
            IDictionary<string, Holding> dict = _commonDao.GetHoldingDetailsByPort();
            IList<Holding> list = new List<Holding>();
            foreach (KeyValuePair<string, Holding> kvp in dict)
                list.Add(kvp.Value);
            return list.OrderBy(x => x.HoldingTicker)
                        .ThenBy(x => x.FundName)
                        .ThenBy(x => x.Broker)
                        .ToList<Holding>();
        }

        [Route("/CommonDataService/GetPositionsHistory")]
        [HttpPost]
        public IList<Holding> GetPositionsHistory(InputParameters reqParams)
        {
            DateTime? startDate = DateUtils.ConvertToDate(reqParams.StartDate, "yyyy-MM-dd");
            DateTime? endDate = DateUtils.ConvertToDate(reqParams.EndDate, "yyyy-MM-dd");
            return _commonDao.GetHoldingsHistoryByPort(reqParams.FundName, reqParams.Broker, reqParams.Ticker, startDate.GetValueOrDefault(), endDate.GetValueOrDefault());
        }

        [Route("/CommonDataService/GetDailyPnLSummary")]
        [HttpPost]
        public IList<SecurityPerformance> GetDailyPnLSummary(InputParameters reqParams)
        {
            DateTime? startDate = DateUtils.ConvertToDate(reqParams.StartDate, "yyyy-MM-dd");
            return _fundDao.GetDailyPnLSummary(startDate.GetValueOrDefault());
        }

        [Route("/CommonDataService/GetDailyPnLDetails")]
        [HttpPost]
        public IList<SecurityPerformance> GetDailyPnLDetails(InputParameters reqParams)
        {
            return _fundDao.GetDailyPnLDetails(reqParams.StartDate);
        }

        [Route("/CommonDataService/GetDailyPnLDetailsExt")]
        [HttpPost]
        public IList<SecurityPerformance> GetDailyPnLDetailsExt(InputParameters reqParams)
        {
            return _securityPerformanceOperations.GetDailyPnLDetails(reqParams.StartDate, reqParams.EndDate, reqParams.FundName, reqParams.Ticker);
        }

        [Route("/CommonDataService/SaveREITDailyDiscounts")]
        [HttpPost]
        public void SaveREITDailyDiscounts(IList<BDCNavHistory> list)
        {
            _fundDao.SaveREITsDailyDiscounts(list);
        }

        [Route("/CommonDataService/SaveREITDailyBVs")]
        [HttpPost]
        public void SaveREITDailyBVs(IList<BDCNavHistory> list)
        {
            _fundDao.SaveREITsDailyBVs(list);
        }

        [Route("/CommonDataService/GetFundHistoricalData")]
        [HttpPost]
        public IList<FundNavHistoryNew> GetFundHistoricalData(InputParameters reqParams)
        {
            Nullable<DateTime> startDate = DateUtils.ConvertToOADate(reqParams.StartDate);
            if (startDate == null)
                startDate = DateUtils.ConvertToDate(reqParams.StartDate, "yyyy-MM-dd");
            if (startDate == null)
                startDate = DateUtils.ConvertToDate(reqParams.StartDate, "MM/dd/yyyy");

            Nullable<DateTime> endDate = DateUtils.ConvertToOADate(reqParams.EndDate);
            if (endDate == null)
                endDate = DateUtils.ConvertToDate(reqParams.EndDate, "yyyy-MM-dd");
            if (endDate == null)
                endDate = DateUtils.ConvertToDate(reqParams.EndDate, "MM/dd/yyyy");

            return _baseDao.GetFundHistoricalData(reqParams.Ticker, startDate.GetValueOrDefault(), endDate.GetValueOrDefault(), reqParams.Side);
        }

        [Route("/CommonDataService/GetFundHistoricalScalarData")]
        [HttpPost]
        public FundHistoryScalar GetFundHistoricalScalarData(InputParameters reqParams)
        {
            //Nullable<DateTime> effectiveDate = DateUtils.ConvertToOADate(reqParams.StartDate);
            Nullable<DateTime> effectiveDate = DateUtils.ConvertToAODate(reqParams.StartDate);
            if (effectiveDate == null)
                effectiveDate = DateUtils.ConvertToDate(reqParams.StartDate, "MM/dd/yyyy");
            //if (effectiveDate == null)
            //    effectiveDate = DateUtils.ConvertToDate(reqParams.StartDate, "yyyy-MM-dd");

            //Nullable<DateTime> effectiveDate = DateUtils.ConvertToOADate(reqParams.StartDate);
            //if (effectiveDate == null)
            //    effectiveDate = DateUtils.ConvertToDate(reqParams.StartDate, "MM/dd/yyyy");
            //if (effectiveDate == null)
            //    effectiveDate = DateUtils.ConvertToDate(reqParams.StartDate, "yyyy-MM-dd");
            //return _fundHistoryDao.GetFundHistoryScalar(reqParams.Ticker, effectiveDate.GetValueOrDefault(), reqParams.LookbackDays.GetValueOrDefault());
            return _fundHistoryOperations.GetFundHistoricalScalarData(reqParams.Ticker, effectiveDate.GetValueOrDefault(), reqParams.LookbackDays.GetValueOrDefault());
        }

        [Route("/CommonDataService/GetFundDividendScalarData")]
        [HttpPost]
        public FundDividendScalar GetFundDividendScalarData(InputParameters reqParams)
        {
            Nullable<DateTime> startDate = DateUtils.ConvertToOADate(reqParams.StartDate);
            if (startDate == null)
                startDate = DateUtils.ConvertToDate(reqParams.StartDate, "MM/dd/yyyy");

            Nullable<DateTime> endDate = DateUtils.ConvertToOADate(reqParams.EndDate);
            if (endDate == null)
                endDate = DateUtils.ConvertToDate(reqParams.EndDate, "MM/dd/yyyy");
            return _fundHistoryDao.GetFundDividendScalar(reqParams.Ticker, startDate.GetValueOrDefault(), endDate.GetValueOrDefault(), reqParams.DvdDateField);
        }

        [Route("/CommonDataService/SaveREITPfdAnalytics")]
        [HttpPost]
        public void SaveREITPfdAnalytics(IList<REITPfdAnalytics> list)
        {
            _fundDao.SaveREITPfdAnalytics(list);
        }

        [Route("/CommonDataService/GetSPACPositions")]
        [HttpGet]
        public IList<SPACPosition> GetSPACPositions()
        {
            return _spacDao.GetSPACPositions();
        }

        [Route("/CommonDataService/GetSPACSecurities")]
        [HttpGet]
        public IList<SPACSecurity> GetSPACSecurities()
        {
            return _spacDao.GetSPACSecurities();
        }

        [Route("/CommonDataService/GetJPMSecurityMarginHistory")]
        [HttpPost]
        public IList<SecurityMarginDetail> GetJPMSecurityMarginHistory(InputParameters reqParams)
        {
            DateTime? startDate = DateUtils.ConvertToDate(reqParams.StartDate, "yyyy-MM-dd");
            DateTime? endDate = DateUtils.ConvertToDate(reqParams.EndDate, "yyyy-MM-dd");

            IList<SecurityMarginDetail> list = _brokerDataDao.GetJPMSecurityMarginDetails(reqParams.FundName
                , reqParams.Ticker, startDate.GetValueOrDefault(), endDate.GetValueOrDefault());
            return list;
        }

        [Route("/CommonDataService/GetFidelitySecurityMarginHistory")]
        [HttpPost]
        public IList<SecurityMargin> GetFidelitySecurityMarginHistory(InputParameters reqParams)
        {
            DateTime? startDate = DateUtils.ConvertToDate(reqParams.StartDate, "yyyy-MM-dd");
            DateTime? endDate = DateUtils.ConvertToDate(reqParams.EndDate, "yyyy-MM-dd");

            IList<SecurityMargin> list = _brokerDataDao.GetFidelitySecurityMarginDetails(reqParams.FundName
                , reqParams.Ticker, startDate.GetValueOrDefault(), endDate.GetValueOrDefault());
            return list;
        }

        [Route("/CommonDataService/GetJPMSecurityMarginDetails")]
        [HttpGet]
        public IList<SecurityMarginDetail> GetJPMSecurityMarginDetails()
        {
            IDictionary<string, SecurityMarginDetail> dict = _cache.Get<IDictionary<string, SecurityMarginDetail>>(CacheKeys.JPM_SECURITY_MARGIN_RATES);
            IList<SecurityMarginDetail> list = new List<SecurityMarginDetail>();
            foreach (KeyValuePair<string, SecurityMarginDetail> kvp in dict)
                list.Add(kvp.Value);
            return list;
        }

        [Route("/CommonDataService/GetFidelitySecurityMarginDetails")]
        [HttpGet]
        public IList<SecurityMargin> GetFidelitySecurityMarginDetails()
        {
            IDictionary<string, SecurityMargin> dict = _cache.Get<IDictionary<string, SecurityMargin>>(CacheKeys.FIDELITY_SECURITY_MARGIN_RATES);
            return dict.Values.ToList<SecurityMargin>();
        }

        [Route("/CommonDataService/GetJPMSecurityBorrowRates")]
        [HttpPost]
        public IList<SecurityBorrowRate> GetJPMSecurityBorrowRates(InputParameters reqParams)
        {
            IList<SecurityBorrowRate> list = _brokerDataDao.GetJPMSecurityBorrowRates(reqParams.Ticker);
            return list;
        }

        [Route("/CommonDataService/GetSecurityActualRebateRates")]
        [HttpGet]
        public IList<SecurityActualRebateRate> GetSecurityActualRebateRates()
        {
            IList<SecurityActualRebateRate> list = _fundCashDao.GetSecurityActualRebateRates();
            return list;
        }

        [Route("/CommonDataService/GetApplicationDataFlag")]
        [HttpGet]
        public ApplicationData GetApplicationDataFlag()
        {
            return _cache.Get<ApplicationData>(CacheKeys.APPLICATION_DATA_FLAG);
        }

        [Route("/CommonDataService/GetPfdCurvesUpdate")]
        [HttpGet]
        public ApplicationData GetPfdCurvesUpdate()
        {
            return _cache.Get<ApplicationData>(CacheKeys.PFD_CURVES_UPDATE_TIME);
        }

        [Route("/CommonDataService/GetPfdTickerBloombergCodeMap")]
        [HttpGet]
        public IList<PfdSecurityMap> GetPfdTickerBloombergCodeMap()
        {
            IDictionary<string, PfdSecurityMap> dict = _cache.Get<IDictionary<string, PfdSecurityMap>>(CacheKeys.PFD_TICKER_BLOOMBERG_CODE_MAP);
            IList<PfdSecurityMap> list = new List<PfdSecurityMap>();
            foreach (KeyValuePair<string, PfdSecurityMap> kvp in dict)
                list.Add(kvp.Value);
            return list;
        }

        [Route("/CommonDataService/GetJobDetails")]
        [HttpPost]
        public IList<JobDetail> GetJobDetails(InputParameters reqParams)
        {
            IList<JobDetail> list = _baseDao.GetJobDetails(reqParams.JobName);
            return list;
        }

        [Route("/CommonDataService/GetJobDetailsByDate")]
        [HttpPost]
        public IList<JobDetail> GetJobDetailsByDate(InputParameters reqParams)
        {
            IList<JobDetail> list = _baseDao.GetJobDetailsByDate(reqParams.EndDate);
            return list;
        }

        [Route("/CommonDataService/GetFDFundDetails")]
        [HttpPost]
        public IList<FundDetailsTO> GetFDFundDetails(InputParameters reqParams)
        {
            IList<FundDetailsTO> list = _brokerDataDao.GetFDFundDetails(reqParams.StartDate, reqParams.EndDate);
            return list;
        }

        [Route("/CommonDataService/GetFDFundCashBalance")]
        [HttpPost]
        public IList<FundCashBalanceTO> GetFDFundCashBalance(InputParameters reqParams)
        {
            IList<FundCashBalanceTO> list = _brokerDataDao.GetFDFundCashBalance(reqParams.StartDate, reqParams.EndDate);
            return list;
        }

        [Route("/CommonDataService/GetFDCorpActionDetails")]
        [HttpPost]
        public IList<CorpActionDetailsTO> GetFDCorpActionDetails(InputParameters reqParams)
        {
            IList<CorpActionDetailsTO> list = _brokerDataDao.GetFDCorpActionDetails(reqParams.StartDate, reqParams.EndDate);
            return list;
        }

        [Route("/CommonDataService/GetFDIssuerMarginDetails")]
        [HttpPost]
        public IList<IssuerMarginDetailsTO> GetFDIssuerMarginDetails(InputParameters reqParams)
        {
            IList<IssuerMarginDetailsTO> list = _brokerDataDao.GetFDIssuerMarginDetails(reqParams.StartDate, reqParams.EndDate);
            return list;
        }

        [Route("/CommonDataService/GetFDMarginAttributionDetails")]
        [HttpPost]
        public IList<MarginAttributionDetailsTO> GetFDMarginAttributionDetails(InputParameters reqParams)
        {
            IList<MarginAttributionDetailsTO> list = _brokerDataDao.GetFDMarginAttributionDetails(reqParams.StartDate, reqParams.EndDate);
            return list;
        }

        [Route("/CommonDataService/GetFDMarginReqDetails")]
        [HttpPost]
        public IList<MarginReqDetailsTO> GetFDMarginReqDetails(InputParameters reqParams)
        {
            IList<MarginReqDetailsTO> list = _brokerDataDao.GetFDMarginReqDetails(reqParams.StartDate, reqParams.EndDate);
            return list;
        }

        [Route("/CommonDataService/GetFDSecurityMarginDetails")]
        [HttpPost]
        public IList<SecurityMarginDetailsTO> GetFDSecurityMarginDetails(InputParameters reqParams)
        {
            IList<SecurityMarginDetailsTO> list = _brokerDataDao.GetFDSecurityMarginDetails(reqParams.StartDate, reqParams.EndDate);
            return list;
        }

        [Route("/CommonDataService/GetFDTaxLotDetails")]
        [HttpPost]
        public IList<FidelityTaxlotDetailsTO> GetFDTaxLotDetails(InputParameters reqParams)
        {
            IList<FidelityTaxlotDetailsTO> list = _brokerDataDao.GetFDTaxLotDetails(reqParams.StartDate, reqParams.EndDate);
            return list;
        }

        [Route("/CommonDataService/GetFDPositionTypeDetails")]
        [HttpPost]
        public IList<PositionTypeDetailsTO> GetFDPositionTypeDetails(InputParameters reqParams)
        {
            IList<PositionTypeDetailsTO> list = _brokerDataDao.GetFDPositionTypeDetails(reqParams.StartDate, reqParams.EndDate);
            return list;
        }

        [Route("/CommonDataService/GetFDValuationDetails")]
        [HttpPost]
        public IList<ValuationDetailsTO> GetFDValuationDetails(InputParameters reqParams)
        {
            IList<ValuationDetailsTO> list = _brokerDataDao.GetFDValuationDetails(reqParams.StartDate, reqParams.EndDate);
            return list;
        }

        [Route("/CommonDataService/GetFDIntEarningDetails")]
        [HttpPost]
        public IList<IntEarningDetailsTO> FDIntEarningDetails(InputParameters reqParams)
        {
            IList<IntEarningDetailsTO> list = _brokerDataDao.FDIntEarningDetails(reqParams.StartDate, reqParams.EndDate);
            return list;
        }

        [Route("/CommonDataService/GetFDPositionDetails")]
        [HttpPost]
        public IList<PositionDetailsTO> FDPositionDetails(InputParameters reqParams)
        {
            IList<PositionDetailsTO> list = _brokerDataDao.FDPositionDetails(reqParams.StartDate, reqParams.EndDate);
            return list;
        }

        [Route("/CommonDataService/GetFDTransactionDetails")]
        [HttpPost]
        public IList<TransactionDetailsTO> FDTransactionDetails(InputParameters reqParams)
        {
            IList<TransactionDetailsTO> list = _brokerDataDao.FDTransactionDetails(reqParams.StartDate, reqParams.EndDate);
            return list;
        }

        [Route("/CommonDataService/GetFDSecurityRebateRate")]
        [HttpPost]
        public IList<SecurityRebateRateTO> FDSecurityRebateRate(InputParameters reqParams)
        {
            IList<SecurityRebateRateTO> list = _brokerDataDao.FDSecurityRebateRate(reqParams.StartDate, reqParams.EndDate);
            return list;
        }

        [Route("/CommonDataService/FDRealizedGLDetails")]
        [HttpPost]
        public IList<FDRealizedGLDetailsTO> FDRealizedGLDetails(InputParameters reqParams)
        {
            IList<FDRealizedGLDetailsTO> list = _brokerDataDao.FDRealizedGLDetails(reqParams.StartDate, reqParams.EndDate);
            return list;
        }


        [Route("/CommonDataService/GetTDColleteralDetails")]
        [HttpPost]
        public IList<CollateralDetailsTO> GetTDColleteralDetails(InputParameters reqParams)
        {
            IList<CollateralDetailsTO> list = _brokerDataDao.GetTDColleteralDetails(reqParams.StartDate, reqParams.EndDate);
            return list;
        }

        [Route("/CommonDataService/GetTDTaxLotDetails")]
        [HttpPost]
        public IList<TaxLotDetailsTO> GetTDTaxLotDetails(InputParameters reqParams)
        {
            IList<TaxLotDetailsTO> list = _brokerDataDao.GetTDTaxLotDetails(reqParams.StartDate, reqParams.EndDate);
            return list;
        }
        [Route("/CommonDataService/GetIBPositionDetails")]
        [HttpPost]
        public IList<IBPositionDetailsTO> GetIBPositionDetails(InputParameters reqParams)
        {
            IList<IBPositionDetailsTO> list = _brokerDataDao.GetIBPositionDetails(reqParams.StartDate, reqParams.EndDate);
            return list;
        }

        [Route("/CommonDataService/GetIBTaxLotDetails")]
        [HttpPost]
        public IList<IBTaxLotDetailsTO> GetIBTaxLotDetails(InputParameters reqParams)
        {
            IList<IBTaxLotDetailsTO> list = _brokerDataDao.GetIBTaxLotDetails(reqParams.StartDate, reqParams.EndDate);
            return list;
        }

        [Route("/CommonDataService/GetTaylorCollisonDetails")]
        [HttpPost]
        public IList<TaylorCollisonTO> GetTaylorCollisonDetails(InputParameters reqParams)
        {
            IList<TaylorCollisonTO> list = _fundDao.GetTaylorCollisonDetails(reqParams.StartDate, reqParams.EndDate, reqParams.Ticker);
            return list;
        }

        [Route("/CommonDataService/GetBDCDataComparisionDetails")]
        [HttpPost]
        public IList<BDCDataComparisionTO> GetBDCDataComparisionDetails(InputParameters reqParams)
        {
            IList<BDCDataComparisionTO> list = _brokerDataDao.GetBDCDataComparisionDetails(reqParams.StartDate, reqParams.EndDate, reqParams.Ticker);
            return list;
        }

        [Route("/CommonDataService/GetBDCGlobalResearchDetails")]
        [HttpPost]
        public IList<BDCGlobalResearchTO> GetBDCGlobalResearchDetails(InputParameters reqParams)
        {
            IList<BDCGlobalResearchTO> list = _brokerDataDao.GetBDCGlobalResearchDetails(reqParams.StartDate, reqParams.EndDate, reqParams.Ticker);
            return list;
        }

        [Route("/CommonDataService/GetJPMSwapPositionDetails")]
        [HttpPost]
        public IList<JPMSwapPositionDetailsTO> GetJPMSwapPositionDetails(InputParameters reqParams)
        {
            IList<JPMSwapPositionDetailsTO> list = _brokerDataDao.GetJPMSwapPositionDetails(reqParams.StartDate, reqParams.EndDate);
            return list;
        }

        [Route("/CommonDataService/GetJPMSwapCorpActionDetails")]
        [HttpPost]
        public IList<JPMSwapCorpActionDetailsTO> GetJPMSwapCorpActionDetails(InputParameters reqParams)
        {
            IList<JPMSwapCorpActionDetailsTO> list = _brokerDataDao.GetJPMSwapCorpActionDetails(reqParams.StartDate, reqParams.EndDate);
            return list;
        }

        [Route("/CommonDataService/GetJPMSwapDividendDetails")]
        [HttpPost]
        public IList<JPMSwapDividendDetailsTO> GetJPMSwapDividendDetails(InputParameters reqParams)
        {
            IList<JPMSwapDividendDetailsTO> list = _brokerDataDao.GetJPMSwapDividendDetails(reqParams.StartDate, reqParams.EndDate);
            return list;
        }

        [Route("/CommonDataService/GetJPMPositionDetails")]
        [HttpPost]
        public IList<JPMPositionDetailsTO> GetJPMPositionDetails(InputParameters reqParams)
        {
            IList<JPMPositionDetailsTO> list = _brokerDataDao.GetJPMPositionDetails(reqParams.StartDate, reqParams.EndDate);
            return list;
        }

        [Route("/CommonDataService/GetJPMTaxlotDetails")]
        [HttpPost]
        public IList<JPMTaxlotDetailsTO> GetJPMTaxlotDetails(InputParameters reqParams)
        {
            IList<JPMTaxlotDetailsTO> list = _brokerDataDao.GetJPMTaxlotDetails(reqParams.StartDate, reqParams.EndDate);
            return list;
        }

        [Route("/CommonDataService/GetJPMSecurityPerfDetails")]
        [HttpPost]
        public IList<JPMSecurityPerfTO> GetJPMSecurityPerfDetails(InputParameters reqParams)
        {
            IList<JPMSecurityPerfTO> list = _brokerDataDao.GetJPMSecurityPerfDetails(reqParams.StartDate, reqParams.EndDate);
            return list;
        }

        [Route("/CommonDataService/GetJPMMarginExtDetails")]
        [HttpPost]
        public IList<JPMMarginDetailExtTO> GetJPMMarginExtDetails(InputParameters reqParams)
        {
            IList<JPMMarginDetailExtTO> list = _brokerDataDao.GetJPMMarginExtDetails(reqParams.StartDate, reqParams.EndDate);
            return list;
        }

        [Route("/CommonDataService/GetJPMMarginSummaryDetails")]
        [HttpPost]
        public IList<JPMMarginSummaryTO> GetJPMMarginSummaryDetails(InputParameters reqParams)
        {
            IList<JPMMarginSummaryTO> list = _brokerDataDao.GetJPMMarginSummaryDetails(reqParams.StartDate, reqParams.EndDate);
            return list;
        }

        [Route("/CommonDataService/GetJPMSecurityBorrowRateDetails")]
        [HttpPost]
        public IList<JPMSecurityBorrowRateTO> GetJPMSecurityBorrowRateDetails(InputParameters reqParams)
        {
            IList<JPMSecurityBorrowRateTO> list = _brokerDataDao.GetJPMSecurityBorrowRateDetails(reqParams.StartDate, reqParams.EndDate);
            return list;
        }

        [Route("/CommonDataService/GetJPMCorpActionDetails")]
        [HttpPost]
        public IList<JPMCorpActionDetailsTO> GetJPMCorpActionDetails(InputParameters reqParams)
        {
            IList<JPMCorpActionDetailsTO> list = _brokerDataDao.GetJPMCorpActionDetails(reqParams.StartDate, reqParams.EndDate);
            return list;
        }

        [Route("/CommonDataService/GetJPMFinanceSummaryDetails")]
        [HttpPost]
        public IList<JPMFinanceSummaryTO> GetJPMFinanceSummaryDetails(InputParameters reqParams)
        {
            IList<JPMFinanceSummaryTO> list = _brokerDataDao.GetJPMFinanceSummaryDetails(reqParams.StartDate, reqParams.EndDate);
            return list;
        }

        [Route("/CommonDataService/GetJPMMarginCurrency")]
        [HttpPost]
        public IList<JPMMarginCurrencyTO> GetJPMMarginCurrency(InputParameters reqParams)
        {
            IList<JPMMarginCurrencyTO> list = _brokerDataDao.GetJPMMarginCurrency(reqParams.StartDate, reqParams.EndDate);
            return list;
        }

        [Route("/CommonDataService/GetJPMMarginCurrencyDetails")]
        [HttpPost]
        public IList<JPMMarginCurrencyDetailsTO> GetJPMMarginCurrencyDetails(InputParameters reqParams)
        {
            IList<JPMMarginCurrencyDetailsTO> list = _brokerDataDao.GetJPMMarginCurrencyDetails(reqParams.StartDate, reqParams.EndDate);
            return list;
        }

        [Route("/CommonDataService/GetJPMRealizedGLDetails")]
        [HttpPost]
        public IList<JPMRealizedGLDetailsTO> GetJPMRealizedGLDetails(InputParameters reqParams)
        {
            IList<JPMRealizedGLDetailsTO> list = _brokerDataDao.GetJPMRealizedGLDetails(reqParams.StartDate, reqParams.EndDate);
            return list;
        }

        [Route("/CommonDataService/GetBloombergOverrides")]
        [HttpPost]
        public IList<BloombergOverridesTO> GetBloombergOverrides(InputParameters reqParams)
        {
            IList<BloombergOverridesTO> list = _commonDao.GetBloombergOverrides(reqParams.StartDate, reqParams.EndDate);
            return list;
        }

        [Route("/CommonDataService/GetNumisDataOverrides")]
        [HttpPost]
        public IList<NumisDataOverridesTO> GetNumisDataOverrides(InputParameters reqParams)
        {
            IList<NumisDataOverridesTO> list = _commonDao.GetNumisDataOverrides(reqParams.StartDate, reqParams.EndDate);
            return list;
        }

        [Route("/CommonDataService/SaveBloombergOverrides")]
        [HttpPost]
        public void SaveBloombergOverrides(IList<BloombergOverridesTO> list)
        {
            _commonDao.SaveBloombergOverrides(list);
        }

        [Route("/CommonDataService/SaveNumisOverrides")]
        [HttpPost]
        public void SaveNumisOverrides(IList<NumisDataOverridesTO> list)
        {
            _commonDao.SaveNumisOverrides(list);
        }

        [Route("/CommonDataService/SavePriceOverrides")]
        [HttpPost]
        public void SavePriceOverrides(IList<PriceOverrideTO> list)
        {
            _commonDao.SavePriceOverrides(list);
        }

        [Route("/CommonDataService/GetPriceOverrides")]
        [HttpPost]
        public IList<PriceOverrideTO> GetPriceOverrides(InputParameters reqParams)
        {
            IList<PriceOverrideTO> list = _commonDao.GetPriceOverrides(reqParams.StartDate, reqParams.EndDate);
            return list;
        }

        [Route("/CommonDataService/GetCEFAReport")]
        [HttpPost]
        public IList<CEFAReportTO> GetCEFAReport(InputParameters reqParams)
        {
            IList<CEFAReportTO> list = _commonDao.GetCEFAReport(reqParams.Ticker, reqParams.StartDate, reqParams.EndDate);
            return list;
        }


        [Route("/CommonDataService/GetCANTenderReport")]
        [HttpPost]
        public IList<CANTenderReportTO> GetCANTenderReport(InputParameters regParams)
        {
            IList<CANTenderReportTO> list = _commonDao.GetCANTenderReport(regParams.Ticker);
            return list;
        }

        [Route("/CommonDataService/GetUSATenderReport")]
        [HttpPost]
        public IList<USATenderReportTO> GetUSATenderReport(InputParameters reqParams)
        {
            IList<USATenderReportTO> list = _commonDao.GetUSATenderReport(reqParams.Ticker);
            return list;
        }

        [Route("/CommonDataService/GetEDFFundDetails")]
        [HttpPost]
        public IList<EDFFundDetailsTO> GetEDFFundDetails(InputParameters reqParams)
        {
            IList<EDFFundDetailsTO> list = _brokerDataDao.GetEDFFundDetails(reqParams.StartDate, reqParams.EndDate);
            return list;
        }

        [Route("/CommonDataService/GetEDFPositionDetails")]
        [HttpPost]
        public IList<EDFPositionDetailsTO> GetEDFPositionDetails(InputParameters reqParams)
        {
            IList<EDFPositionDetailsTO> list = _brokerDataDao.GetEDFPositionDetails(reqParams.StartDate, reqParams.EndDate);
            return list;
        }
        [Route("/CommonDataService/GetEDFPrelimDetails")]
        [HttpPost]
        public IList<EDFPrelimDetailsTO> GetEDFPrelimDetails(InputParameters reqParams)
        {
            IList<EDFPrelimDetailsTO> list = _brokerDataDao.GetEDFPrelimDetails(reqParams.StartDate, reqParams.EndDate);
            return list;
        }
        [Route("/CommonDataService/GetEDFPfdfDetails")]
        [HttpPost]
        public IList<EDFPfdfDetailsTO> GetEDFPfdfDetails(InputParameters reqParams)
        {
            IList<EDFPfdfDetailsTO> list = _brokerDataDao.GetEDFPfdfDetails(reqParams.StartDate, reqParams.EndDate);
            return list;
        }

        [Route("/CommonDataService/GetBrokerSecurityMapping")]
        [HttpGet]
        public IList<SecurityMasterTO> GetBrokerSecurityMapping()
        {
            IList<SecurityMasterTO> list = _commonDao.GetBrokerSecurityMapping();
            return list;
        }

        [Route("/CommonDataService/SaveBrokerSecurityMapping")]
        [HttpPost]
        public void SaveBrokerSecurityMapping(IList<SecurityMasterTO> list)
        {
            _commonDao.UpdateBrokerSecurityMapping(list);
            _commonDao.UpdateBrokerSecurityPositionMapping(list);
            _commonOperations.UpdateALMHoldings();
            //await _notificationService.PositionUpdate();
        }

        [Route("/CommonDataService/GetBrokerCommissionRates")]
        [HttpGet]
        public IList<BrokerCommission> GetBrokerCommissionRates()
        {
            IDictionary<string, BrokerCommission> dict = _cache.Get<IDictionary<string, BrokerCommission>>(CacheKeys.BROKER_COMMISSION_RATES);
            return dict.Values.ToList<BrokerCommission>();
        }

        [Route("/CommonDataService/GetTaxLotSummary")]
        [HttpGet]
        public IList<TaxLotSummaryTO> GetTaxLotSummary()
        {
            IList<TaxLotSummaryTO> list = _baseDao.GetTaxLotSummary();
            return list;
        }

        [Route("/CommonDataService/GetBMOTradeDetails")]
        [HttpPost]
        public IList<BMOTradeDetailsTO> GetBMOTradeDetails(InputParameters reqParams)
        {
            IList<BMOTradeDetailsTO> list = _brokerDataDao.GetBMOTradeDetails(reqParams.StartDate, reqParams.EndDate);
            return list;
        }

        [Route("/CommonDataService/GetBMOMTMDetails")]
        [HttpPost]
        public IList<BMOMTMDetailsTO> GetBMOMTMDetails(InputParameters reqParams)
        {
            IList<BMOMTMDetailsTO> list = _brokerDataDao.GetBMOMTMDetails(reqParams.StartDate, reqParams.EndDate);
            return list;
        }

        [Route("/CommonDataService/GetBMOTradeRTDDetails")]
        [HttpPost]
        public IList<BMOTradeRTDDetailsTO> GetBMOTradeRTDDetails(InputParameters reqParams)
        {
            IList<BMOTradeRTDDetailsTO> list = _brokerDataDao.GetBMOTradeRTDDetails(reqParams.StartDate, reqParams.EndDate);
            return list;
        }

        [Route("/CommonDataService/GetBMOUnwindDetails")]
        [HttpPost]
        public IList<BMOUnwindDetailsTO> GetBMOUnwindDetails(InputParameters reqParams)
        {
            IList<BMOUnwindDetailsTO> list = _brokerDataDao.GetBMOUnwindDetails(reqParams.StartDate, reqParams.EndDate);
            return list;
        }
        
        [Route("/CommonDataService/GetBMOPosExpDetails")]
        [HttpPost]
        public IList<BMOPosExpDetailsTO> GetBMOPosExpDetails(InputParameters reqParams)
        {
            IList<BMOPosExpDetailsTO> list = _brokerDataDao.GetBMOPosExpDetails(reqParams.StartDate, reqParams.EndDate);
            return list;
        }


        [Route("/CommonDataService/GetFundSupplementalData")]
        [HttpPost]
        public IList<FundSupplementalDataTO> GetFundSupplementalData(InputParameters reqParams)
        {
            IList<FundSupplementalDataTO> list = _fundDao.GetFundSupplementalData(reqParams.Ticker);
            return list;
        }

        [Route("/CommonDataService/GetUBSSwapTradeDetails")]
        [HttpPost]
        public IList<UBSSwapTradeDetailsTO> GetUBSSwapTradeDetails(InputParameters reqParams)
        {
            IList<UBSSwapTradeDetailsTO> list = _brokerDataDao.GetUBSSwapTradeDetails(reqParams.StartDate, reqParams.EndDate);
            return list;
        }

        [Route("/CommonDataService/GetUBSOpenTransactionDetails")]
        [HttpPost]
        public IList<UBSOpenTransactionDetailsTO> GetUBSOpenTransactionDetails(InputParameters reqParams)
        {
            IList<UBSOpenTransactionDetailsTO> list = _brokerDataDao.GetUBSOpenTransactionDetails(reqParams.StartDate, reqParams.EndDate);
            return list;
        }

        [Route("/CommonDataService/GetUBSOpenPositionDetails")]
        [HttpPost]
        public IList<UBSOpenPositionDetailsTO> GetUBSOpenPositionDetails(InputParameters reqParams)
        {
            IList<UBSOpenPositionDetailsTO> list = _brokerDataDao.GetUBSOpenPositionDetails(reqParams.StartDate, reqParams.EndDate);
            return list;
        }

        [Route("/CommonDataService/GetUBSValuationDetails")]
        [HttpPost]
        public IList<UBSValuationDetailsTO> GetUBSValuationDetails(InputParameters reqParams)
        {
            IList<UBSValuationDetailsTO> list = _brokerDataDao.GetUBSValuationDetails(reqParams.StartDate, reqParams.EndDate);
            return list;
        }

        [Route("/CommonDataService/GetUBSValuationSummaryDetails")]
        [HttpPost]
        public IList<UBSValuationSummaryDetailsTO> GetUBSValuationSummaryDetails(InputParameters reqParams)
        {
            IList<UBSValuationSummaryDetailsTO> list = _brokerDataDao.GetUBSValuationSummaryDetails(reqParams.StartDate, reqParams.EndDate);
            return list;
        }

        [Route("/CommonDataService/GetUBSMarginDetails")]
        [HttpPost]
        public IList<UBSMarginDetailsTO> GetUBSMarginDetails(InputParameters reqParams)
        {
            IList<UBSMarginDetailsTO> list = _brokerDataDao.GetUBSMarginDetails(reqParams.StartDate, reqParams.EndDate);
            return list;
        }

        [Route("/CommonDataService/GetUBSCrossMarginDetails")]
        [HttpPost]
        public IList<UBSCrossMarginDetailsTO> GetUBSCrossMarginDetails(InputParameters reqParams)
        {
            IList<UBSCrossMarginDetailsTO> list = _brokerDataDao.GetUBSCrossMarginDetails(reqParams.StartDate, reqParams.EndDate);
            return list;
        }

        [Route("/CommonDataService/GetTenderOfferHistory")]
        [HttpPost]
        public IList<TenderOfferHistoryTO> GetTenderOfferHistory(InputParameters reqParams)
        {
            IList<TenderOfferHistoryTO> list = _fundDao.GetTenderOfferHistory(reqParams.Ticker);
            return list;
        }

        [Route("/CommonDataService/GetMSPositionDetails")]
        [HttpPost]
        public IList<MSPositionDetailsTO> GetMSPositionDetails(InputParameters reqParams)
        {
            IList<MSPositionDetailsTO> list = _brokerDataDao.GetMSPositionDetails(reqParams.StartDate, reqParams.EndDate);
            return list;
        }

        [Route("/CommonDataService/GetMSFundDetails")]
        [HttpPost]
        public IList<MSFundDetailsTO> GetMSFundDetails(InputParameters reqParams)
        {
            IList<MSFundDetailsTO> list = _brokerDataDao.GetMSFundDetails(reqParams.StartDate, reqParams.EndDate);
            return list;
        }

        [Route("/CommonDataService/GetMSTaxLotDetails")]
        [HttpPost]
        public IList<MSTaxLotDetailsTO> GetMSTaxLotDetails(InputParameters reqParams)
        {
            IList<MSTaxLotDetailsTO> list = _brokerDataDao.GetMSTaxLotDetails(reqParams.StartDate, reqParams.EndDate);
            return list;
        }

        [Route("/CommonDataService/GetMSTradeHistDetails")]
        [HttpPost]
        public IList<MSTradeHistDetailsTO> GetMSTradeHistDetails(InputParameters reqParams)
        {
            IList<MSTradeHistDetailsTO> list = _brokerDataDao.GetMSTradeHistDetails(reqParams.StartDate, reqParams.EndDate);
            return list;
        }


        [Route("/CommonDataService/GetBAMLPerformanceDetails")]
        [HttpPost]
        public IList<BAMLPerformanceDetailsTO> GetBAMLPerformanceDetails(InputParameters reqParams)
        {
            IList<BAMLPerformanceDetailsTO> list = _brokerDataDao.GetBAMLPerformanceDetails(reqParams.StartDate, reqParams.EndDate);
            return list;
        }

        [Route("/CommonDataService/GetBAMLSwapActivityDetails")]
        [HttpPost]
        public IList<BAMLSwapActivityDetailsTO> GetBAMLSwapActivityDetails(InputParameters reqParams)
        {
            IList<BAMLSwapActivityDetailsTO> list = _brokerDataDao.GetBAMLSwapActivityDetails(reqParams.StartDate, reqParams.EndDate);
            return list;
        }
    }
}