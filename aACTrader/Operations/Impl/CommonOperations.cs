using aACTrader.DAO.Repository;
using aACTrader.Model;
using aACTrader.Operations.Interface;
using aACTrader.Operations.Reports;
using aACTrader.Utils;
using aCommons;
using aCommons.Alerts;
using aCommons.Cef;
using aCommons.Crypto;
using aCommons.Derivatives;
using aCommons.DTO;
using aCommons.Fund;
using aCommons.MarketMonitor;
using aCommons.Trading;
using aCommons.Utils;
using aCommons.Web;
using LazyCache;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;

namespace aACTrader.Operations.Impl
{
    public class CommonOperations : ICommonOperations
    {
        private readonly ILogger<CommonOperations> _logger;

        protected static readonly string pattern = @"([a-zA-Z]+)([\s]*)([a-zA-Z]*)[*]?([+-]?[0-9]*\.?[0-9]*)";
        protected static readonly string pattern1 = @"([a-zA-Z]+)([\s]*)([a-zA-Z]*)[*]?";
        protected static readonly string pattern2 = @"[*]([+-]?[0-9]*\.?[0-9]*)";
        private static readonly DateTime TodaysDate = DateTime.Now.Date;

        private readonly CommonDao _commonDao;
        private readonly BaseDao _baseDao;
        private readonly OptionDao _optionDao;
        private readonly FundDao _fundDao;
        private readonly FundCashDao _fundCashDao;
        private readonly FundHistoryDao _fundHistoryDao;
        private readonly StatsDao _statsDao;
        private readonly SecurityPriceDao _securityPriceDao;
        private readonly SecurityRiskDao _securityRiskDao;
        private readonly DataOverrideDao _dataOverrideDao;
        private readonly HoldingsDao _holdingsDao;
        private readonly TradingDao _tradingDao;
        private readonly SecurityAlertDao _securityAlertDao;
        private readonly FundSupplementalDataDao _fundSupplementalDataDao;
        private readonly CachingService _cache;
        private readonly FundIRRCalculator _irrCalculator;
        private readonly FundAlertManager _fundAlertManager;
        private readonly FundAlertManagerNew _fundAlertManagerNew;
        private readonly DataValidationChecks _dataValidationChecks;
        private readonly AdminDao _adminDao;
        private readonly CryptoDao _cryptoDao;
        private readonly BrokerDataDao _brokerDataDao;
        private readonly FundForecastEngine _fundForecastEngine;
        private readonly FundHistoryOperations _fundHistoryOperations;
        private readonly IConfiguration _configuration;
        private readonly DailyPnLReport _dailyPnLReport;

        public CommonOperations(ILogger<CommonOperations> logger
            , CommonDao commonDao
            , BaseDao baseDao
            , OptionDao optionDao
            , FundDao fundDao
            , FundCashDao fundCashDao
            , FundHistoryDao fundHistoryDao
            , StatsDao statsDao
            , SecurityPriceDao securityPriceDao
            , SecurityRiskDao securityRiskDao
            , DataOverrideDao dataOverrideDao
            , HoldingsDao holdingsDao
            , TradingDao tradingDao
            , SecurityAlertDao securityAlertDao
            , FundSupplementalDataDao fundSupplementalDataDao
            , CachingService cache
            , FundIRRCalculator irrCalculator
            , FundAlertManager fundAlertManager
            , FundAlertManagerNew fundAlertManagerNew
            , DataValidationChecks dataValidationChecks
            , AdminDao adminDao
            , CryptoDao cryptoDao
            , BrokerDataDao brokerDataDao
            , FundForecastEngine fundForecastEngine
            , FundHistoryOperations fundHistoryOperations
            , IConfiguration configuration
            , DailyPnLReport dailyPnLReport
            )
        {
            _logger = logger;
            _commonDao = commonDao;
            _baseDao = baseDao;
            _optionDao = optionDao;
            _fundDao = fundDao;
            _fundCashDao = fundCashDao;
            _fundHistoryDao = fundHistoryDao;
            _statsDao = statsDao;
            _securityPriceDao = securityPriceDao;
            _securityRiskDao = securityRiskDao;
            _dataOverrideDao = dataOverrideDao;
            _holdingsDao = holdingsDao;
            _tradingDao = tradingDao;
            _securityAlertDao = securityAlertDao;
            _fundSupplementalDataDao = fundSupplementalDataDao;
            _cache = cache;
            _irrCalculator = irrCalculator;
            _fundAlertManager = fundAlertManager;
            _fundAlertManagerNew = fundAlertManagerNew;
            _dataValidationChecks = dataValidationChecks;
            _adminDao = adminDao;
            _cryptoDao = cryptoDao;
            _brokerDataDao = brokerDataDao;
            _fundForecastEngine = fundForecastEngine;
            _fundHistoryOperations = fundHistoryOperations;
            _configuration = configuration;
            _dailyPnLReport = dailyPnLReport;
        }

        public void Start()
        {
            try
            {
                _logger.LogInformation("Starting Common Operations - STARTED");

                _logger.LogInformation("Setting Data Update Flag...");
                UpdateApplicationDataUpdateFlag("Y");

                _logger.LogInformation("Populating Fund Master...");
                PopulateFundMaster();

                _logger.LogInformation("Populating Fund Redemptions Details...");
                PopulateFundRedemptionDetails();

                _logger.LogInformation("Populating Fund Navs...");
                IDictionary<string, FundNav> fundNavDict = _commonDao.GetFundNavs();
                _cache.Add(CacheKeys.FUND_NAVS, fundNavDict, DateTimeOffset.MaxValue);

                _logger.LogInformation("Populating Fund Dividends...");
                IDictionary<string, FundDividend> fundDividendDict = _commonDao.GetFundDividends();
                _cache.Add(CacheKeys.FUND_DIVIDENDS, fundDividendDict, DateTimeOffset.MaxValue);

                _logger.LogInformation("Adjust Fund Navs for Ex-Dividends...");
                PopulateDividendAdjustedNavs(fundNavDict);

                _logger.LogInformation("Populating Expected Alpha Model Coefficients...");
                IDictionary<string, FundAlphaModelParams> fundAlphaModelParams = _commonDao.GetFundAlphaModelParams();
                _cache.Add(CacheKeys.ALPHA_MODEL_COEFFICIENTS, fundAlphaModelParams, DateTimeOffset.MaxValue);

                _logger.LogInformation("Populating Expected Alpha Model Scores...");
                IDictionary<string, FundAlphaModelScores> fundAlphaModelScores = _commonDao.GetFundAlphaModelScores();
                _cache.Add(CacheKeys.ALPHA_MODEL_SCORES, fundAlphaModelScores, DateTimeOffset.MaxValue);

                _logger.LogInformation("Populating Expected Alpha Model Params (NEW)...");
                IDictionary<string, ExpectedAlphaModelParams> expectedAlphaModelParams = _baseDao.GetExpectedAlphaModelParams();
                _cache.Add(CacheKeys.EXPECTED_ALPHA_MODEL_PARAMS, expectedAlphaModelParams, DateTimeOffset.MaxValue);

                _logger.LogInformation("Populating Fund Past and Future Dividends Payments...");
                IDictionary<string, FundDividendSchedule> fundDividendScheduleDict = _commonDao.GetFundDividendSchedule();
                _cache.Add(CacheKeys.FUND_DIVIDEND_SCHEDULE, fundDividendScheduleDict, DateTimeOffset.MaxValue);

                _logger.LogInformation("Populating Fund Stats...");
                IDictionary<string, FundHistStats> fundStatsDict = GetFundStats();
                _cache.Add(CacheKeys.FUND_STATS, fundStatsDict, DateTimeOffset.MaxValue);

                _logger.LogInformation("Populating ALM Holdings...");
                PopulateALMHoldings();

                _logger.LogInformation("Populating Broker Trading Volume...");
                IDictionary<string, BrokerTradingVolume> brokerTradingVolumeDict = _baseDao.GetBrokerTradingVolume();
                _cache.Add(CacheKeys.BROKER_TRADING_VOLUME, brokerTradingVolumeDict, DateTimeOffset.MaxValue);

                _logger.LogInformation("Populating Fund Rights Offers Details...");
                PopulateFundRightsOfferDetails();

                _logger.LogInformation("Populating Fund Tender Offers Details...");
                PopulateFundTenderOfferDetails();

                _logger.LogInformation("Populating Holdings and Supplemented Data in Fund Master...");
                PopulateFundSupplementalData();

                _logger.LogInformation("Populating FX Rates...");
                PopulateFXRates();

                _logger.LogInformation("Populating FX Rates (Previous Day)...");
                PopulateFXRatesPD();

                _logger.LogInformation("Populating ETF Returns...");
                PopulateETFReturns();

                _logger.LogInformation("Populating Proxy ETF Returns...");
                PopulateProxyETFReturns();

                _logger.LogInformation("Populating Alternate Proxy ETF Returns...");
                PopulateAltProxyETFReturns();

                _logger.LogInformation("Populating Port (Fixed Income port proxy) Proxy ETF Returns...");
                PopulatePortProxyETFReturns();

                _logger.LogInformation("Populating User Overrides...");
                PopulateUserOverrides();

                _logger.LogInformation("Populating Global Overrides...");
                IDictionary<string, GlobalDataOverride> globalDataOverrideDict = _dataOverrideDao.GetGlobalDataOverrides();
                _cache.Add(CacheKeys.GLOBAL_OVERRIDES, globalDataOverrideDict, DateTimeOffset.MaxValue);

                _logger.LogInformation("Populating Fund Forecasts...");
                IDictionary<string, FundForecast> fundForecastDict = GetFundForecasts();
                _cache.Add(CacheKeys.FUND_FORECASTS, fundForecastDict, DateTimeOffset.MaxValue);

                _logger.LogInformation("Populating Fund Alerts...");
                _baseDao.PopulateFundAlerts();

                _logger.LogInformation("Populating Fund Alert Targets...");
                _fundAlertManager.PopulateFundAlertTargets();

                _logger.LogInformation("Populating Security Master...");
                IDictionary<string, FundMaster> securityMasterDict = _baseDao.GetSecurityMaster();
                _cache.Add(CacheKeys.SECURITY_MASTER, securityMasterDict, DateTimeOffset.MaxValue);

                _logger.LogInformation("Populating Security Price Lookup Keys...");
                PopulatePriceTickerMap();

                _logger.LogInformation("Populating Fund Port Summary...");
                PopulateFundPortSummary();

                _logger.LogInformation("Populating ALM Fund Positions...");
                PopulateFundHoldingsSummary();

                _logger.LogInformation("Populating Holders Database...");
                IDictionary<string, ActivistHolding> holdersMap = _holdingsDao.GetActivistHoldings();
                _cache.Add(CacheKeys.HOLDERS_MAP, holdersMap, DateTimeOffset.MaxValue);

                _logger.LogInformation("Populating Fund Cash, Margin and Security Borrow Rates (Broker Reports)");
                PopulateBrokerReportDetails();

                _logger.LogInformation("Populating Position Security Details...");
                PopulatePositionSecurityDetails();

                _logger.LogInformation("Populating Pfd Common Share Map (South Korean Pfds)...");
                IDictionary<string, string> pfdCommonShareTickerMap = _baseDao.GetPfdCommonSharesMap();
                _cache.Add(CacheKeys.PFD_COMMON_SHARE_MAP, pfdCommonShareTickerMap, DateTimeOffset.MaxValue);

                _logger.LogInformation("Populating BDC Returns");
                IDictionary<string, FundReturn> bdcFundReturnsDict = _baseDao.GetBDCFundReturns();
                _cache.Add(CacheKeys.BDC_FUND_RETURNS, bdcFundReturnsDict, DateTimeOffset.MaxValue);

                _logger.LogInformation("Populating Watchlist Securities");
                _fundAlertManagerNew.PopulateWatchlistSecurities();

                _logger.LogInformation("Populating Historical Discount Stats");
                PopulateHistoricalDiscountStats();

                _logger.LogInformation("Populating Fund Sector Discount Stats");
                PopulateFundSectorHistoricalDiscountStats();

                _logger.LogInformation("Populating Historical FX Returns");
                IDictionary<string, IDictionary<DateTime, HistFXRate>> histFXReturnsDict = _securityRiskDao.GetHistFXRates();
                _cache.Add(CacheKeys.HIST_FX_RATES, histFXReturnsDict, DateTimeOffset.MaxValue);

                _logger.LogInformation("Populating Fund Port Dates");
                PopulateFundPortDates();

                _logger.LogInformation("Populating Fund Notes");
                PopulateFundNotes();

                _logger.LogInformation("Populating Fund Redemption Triggers");
                PopulateFundRedemptionTriggers();

                _logger.LogInformation("Populating Fund Nav Update Frequency");
                PopulateFundNavUpdateFrequency();

                _logger.LogInformation("Populating Fund Supplemental Data Details");
                PopulateFundSupplementalDetails();

                //if (_configuration["ConnectionStrings:ENV"].Equals("PROD", StringComparison.CurrentCultureIgnoreCase))
                //{
                //    _logger.LogInformation("Populating Global Market Month End History");
                //    PopulateGlobalMarketMonthEndLevels();
                //}

                _logger.LogInformation("Populating Option Details");
                IDictionary<string, OptionDetail> optionDetailsDict = _optionDao.GetOptionDetails();
                _cache.Add(CacheKeys.OPTION_DETAILS, optionDetailsDict, DateTimeOffset.MaxValue);

                _logger.LogInformation("Populating Security Alert Targets");
                PopulateSecurityAlertTargets();

                _logger.LogInformation("Running Security Data Validation Checks");
                RunDataValidationChecks();

                _logger.LogInformation("Populating ISIN -> Position Ticker Map");
                PopulatePositionTickerMap();

                _logger.LogInformation("Populating Crypto Security Mst");
                PopulateCryptoSecurityDetails();

                _logger.LogInformation("Populating Security Ext Details");
                PopulateSecurityExtDetails();

                _logger.LogInformation("Populating Security Risk Factors w/ Dates");
                PopulateSecurityRiskFactors();

                _logger.LogInformation("Populating Manual Trades");
                PopulateManualTrades();

                _logger.LogInformation("Populating Fund Forecast Calc Flags");
                UpdateFundForecastFlags();

                //if (_configuration["ConnectionStrings:ENV"].Equals("PROD"))
                //{
                //    _logger.LogInformation("Populating Full Fund History");
                //    PopulateFullFundHistory();
                //}

                PopulateLatestFundNavEstErrDetails();

                _logger.LogInformation("Populating Global Market Indicators");
                PopulateGlobalMarketIndicators();

                _logger.LogInformation("Populating Fund Currency Exposures");
                PopulateFundCurrencyExposures();

                _logger.LogInformation("Populating Broker Commission Rates");
                PopulateBrokerCommissionRates();

                _logger.LogInformation("Populating Broker Commission Rates");
                PopulateBatchMonitorDetails();

                _logger.LogInformation("Update Daily PnL Holdings");
                PopulateDailyPnLHoldings();

                _logger.LogInformation("Populating Fund PD Stats...");
                IDictionary<string, FundPDStats> fundPDStatsDict = new Dictionary<string, FundPDStats>();
                _cache.Add(CacheKeys.LIVE_FUND_PD_STATS, fundPDStatsDict, DateTimeOffset.MaxValue);

                _logger.LogInformation("Populating Trade Summary");
                IList<TradeSummaryReportTO> tradeSummaryList = new List<TradeSummaryReportTO>();
                _cache.Add(CacheKeys.TRADE_SUMMARY, tradeSummaryList, DateTimeOffset.MaxValue);

                _logger.LogInformation("Started Common Operations - DONE");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error initializing the application");
                throw;
            }
        }

        /// <summary>
        /// Populate list of active securities from [Security Master] table 
        /// and list of unique positions held in [ALM funds]
        /// and list of Crypto funds
        /// </summary>
        private void PopulateFundMaster()
        {
            _logger.LogInformation("Populating Fund Master... - STARTED");
            IDictionary<string, FundMaster> fundMasterDict = _commonDao.GetFundMaster();
            _cache.Remove(CacheKeys.FUND_MASTER);
            _cache.Add(CacheKeys.FUND_MASTER, fundMasterDict, DateTimeOffset.MaxValue);
            _logger.LogInformation("Populating Fund Master... - DONE");
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fundNavs"></param>
        private void PopulateDividendAdjustedNavs(IDictionary<string, FundNav> fundNavs)
        {
            IDictionary<string, FundDividend> fundDividendDict = _cache.Get<IDictionary<string, FundDividend>>(CacheKeys.FUND_DIVIDENDS);
            foreach (FundNav fundNav in fundNavs.Values)
            {
                if (fundDividendDict.TryGetValue(fundNav.Ticker, out FundDividend fundDividend))
                {
                    if (fundDividend.DvdFromLastNavDate.HasValue)
                    {
                        fundNav.DvdFromLastNavDate = fundDividend.DvdFromLastNavDate;
                        fundNav.ExDvdDates = fundDividend.ExDvdDates;
                    }
                }
            }
        }

        /// <summary>
        /// Creates fund forecast object for all active securities with initial data
        /// Builds regression equations and creates a map of fund ticker to etf regression tickers
        /// </summary>
        /// <returns></returns>
        public IDictionary<string, FundForecast> GetFundForecasts()
        {
            IDictionary<string, FundForecast> fundForecastDict = new Dictionary<string, FundForecast>(StringComparer.CurrentCultureIgnoreCase);

            //get regression coefficients for all funds
            IList<RegressionCoefficient> list = _commonDao.GetETFRegressionCoefficients(null);

            //create a map of fund ticker and etf tickers
            IDictionary<string, HashSet<string>> fundETFTickerMap = new Dictionary<string, HashSet<string>>(StringComparer.CurrentCultureIgnoreCase);
            _cache.Remove(CacheKeys.ETF_TICKERS);
            _cache.Add(CacheKeys.ETF_TICKERS, fundETFTickerMap, DateTimeOffset.MaxValue);

            IDictionary<string, FundMaster> fundMasterDict = _cache.Get<IDictionary<string, FundMaster>>(CacheKeys.FUND_MASTER);
            IDictionary<string, FundAlphaModelScores> fundAlphaModelScoresDict = _cache.Get<IDictionary<string, FundAlphaModelScores>>(CacheKeys.ALPHA_MODEL_SCORES);
            IDictionary<string, UserDataOverride> userDataOverrideDict = _cache.Get<IDictionary<string, UserDataOverride>>(CacheKeys.USER_OVERRIDES);

            //for each fund, build the regression equation from individual coefficients
            foreach (KeyValuePair<string, FundMaster> kvp in fundMasterDict)
            {
                string ticker = kvp.Key;
                FundMaster fundMaster = kvp.Value;

                //if (ticker.Equals("GBTC US", StringComparison.CurrentCultureIgnoreCase))
                //    _logger.LogDebug("Processing ticker: " + ticker);

                try
                {
                    if (!string.IsNullOrEmpty(fundMaster.Status)
                        && "ACTV".Equals(fundMaster.Status, StringComparison.CurrentCultureIgnoreCase))
                    {
                        FundForecast fundForecast;
                        if (!fundForecastDict.TryGetValue(ticker, out fundForecast))
                        {
                            fundForecast = new FundForecast
                            {
                                FIGI = fundMaster.FIGI,
                                Ticker = ticker,
                                Curr = fundMaster.Curr,
                                SecType = fundMaster.SecTyp,
                                HasRC = 0,
                                RODscntDispFlag = 0,

                                //master data (that could be overriden from user overrides screen)
                                NavEstMthd = fundMaster.NavEstMd,
                                ExpRatio = fundMaster.ExpRatio,
                                LevRatio = fundMaster.LevRatio,
                                ProxyForm = fundMaster.ProxyForm,
                                AltProxyForm = fundMaster.AltProxyForm,
                                PortFIProxyForm = fundMaster.PortFIProxyForm,
                                ExtraRedAdj = fundMaster.RedExtraAdj,
                                DvdFcst = fundMaster.DvdFcst
                            };

                            if (fundAlphaModelScoresDict.TryGetValue(ticker, out FundAlphaModelScores fundAlphaModelScores))
                            {
                                fundForecast.AScore = fundAlphaModelScores.ActivistScore;
                                fundForecast.RawAScore = fundAlphaModelScores.RawActivistScore;
                                fundForecast.LiqCost = fundAlphaModelScores.LiquidityCost;
                                fundForecast.ShareBB = fundAlphaModelScores.ShareBuyback;
                                fundForecast.ExpAlphaAdjFactor = fundAlphaModelScores.ExpectedAlphaAdjFactor;
                                fundForecast.DscntConvMult = fundAlphaModelScores.DiscountConvergenceMultiplier;
                                fundForecast.SecDScoreMult = fundAlphaModelScores.SecurityDScoreMultiplier;
                                fundForecast.FundCatDScoreMult = fundAlphaModelScores.FundCategoryDScoreMultiplier;
                                fundForecast.IRRMult = fundAlphaModelScores.IRRMultiplier;
                                fundForecast.AScoreMult = fundAlphaModelScores.ActivistScoreMultiplier;
                                fundForecast.MajVotHaircut = fundAlphaModelScores.MajorityVotingHaircut;
                                fundForecast.MajVotHaircutMin = fundAlphaModelScores.MajorityVotingHaircutMin;
                                fundForecast.BTAdjMult = fundAlphaModelScores.BoardTermAdjMultiplier;
                                fundForecast.BTAdjMin = fundAlphaModelScores.BoardTermAdjMin;
                                fundForecast.ExpDragMult = fundAlphaModelScores.ExpenseDragMultiplier;
                            }

                            //if (fundMaster.FundRedemption != null
                            //    && !string.IsNullOrEmpty(fundMaster.FundRedemption.PreferredShareSedol))
                            //{
                            //    fundForecast.PreferredShareTicker = fundMaster.FundRedemption.PreferredShareSedol;
                            //    fundForecast.PreferredSharePerCommonSplitTrust = fundMaster.FundRedemption.NumPreferredSharesPerCommonSplitTrust;
                            //}

                            //these fields are not only available as data overrides
                            if (userDataOverrideDict.TryGetValue(ticker, out UserDataOverride userOverride))
                            {
                                fundForecast.TaxLiability = userOverride.TaxLiability;
                                fundForecast.MgmtFee = userOverride.ManagementFee;
                                fundForecast.PerfFee = userOverride.PerformanceFee;
                                fundForecast.AccrRate = userOverride.AccrualRate;
                                fundForecast.AccrStartDt = userOverride.AccrualStartDate;
                                fundForecast.FundDscntGrp = userOverride.FundDiscountGroup;
                                fundForecast.FundCat = userOverride.FundCategory;
                                fundForecast.UKFundCat = userOverride.UKFundModelCategory;
                                fundForecast.AdjEstNav = userOverride.EstimatedNavAdjustment;
                                fundForecast.PortFIProxyForm = userOverride.PortFIProxyFormula;
                            }

                            fundForecastDict.Add(ticker, fundForecast);
                        }

                        //get etf regression coefficients for a fund
                        IList<RegressionCoefficient> coefficients = list.Where(e => e.Ticker.Equals(ticker, StringComparison.CurrentCultureIgnoreCase)).ToList();
                        if (coefficients != null && coefficients.Count > 0)
                        {
                            fundForecast.HasRC = 1;
                            BuildRegressionEquation(fundMaster, coefficients);

                            //populate list of unique etf tickers for each fund's regression equation
                            foreach (RegressionCoefficient regressionCoefficient in coefficients)
                            {
                                HashSet<string> etfTickers = null;
                                if (fundETFTickerMap.ContainsKey(ticker))
                                {
                                    etfTickers = fundETFTickerMap[ticker];
                                }
                                else
                                {
                                    etfTickers = new HashSet<string>();
                                    fundETFTickerMap.Add(ticker, etfTickers);
                                }

                                if (!etfTickers.Contains(regressionCoefficient.ETFTicker))
                                    etfTickers.Add(regressionCoefficient.ETFTicker);
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error generating Regression Equation for ticker: " + ticker);
                }
            }

            return fundForecastDict;
        }

        /// <summary>
        /// Build regression equation
        ///     Ex:- 0.192*BKLN US+0.085*SJNK US+0.04*JNK US
        /// </summary>
        /// <param name="fundMaster"></param>
        /// <param name="coefficients"></param>
        private void BuildRegressionEquation(FundMaster fundMaster, IList<RegressionCoefficient> coefficients)
        {
            Nullable<double> rSquared3M = null, rSquared6M = null, rSquared12M = null, rSquared24M = null, rSquared36M = null, rSquared60M = null, rSquaredLife = null;

            try
            {
                foreach (RegressionCoefficient regressionCoefficient in coefficients)
                {
                    switch (regressionCoefficient.Period.ToUpper())
                    {
                        case "3M":
                            {
                                if (string.IsNullOrEmpty(fundMaster.RegExp3M))
                                {
                                    fundMaster.RegExp3M = regressionCoefficient.Beta + "*" + regressionCoefficient.ETFTicker;
                                }
                                else
                                {
                                    string equation = (regressionCoefficient.Beta >= 0) ? "+" : "";
                                    equation += regressionCoefficient.Beta + "*" + regressionCoefficient.ETFTicker;
                                    fundMaster.RegExp3M += equation;
                                }
                                rSquared3M = regressionCoefficient.RSqrd;
                                break;
                            }
                        case "6M":
                            {
                                if (string.IsNullOrEmpty(fundMaster.RegExp6M))
                                {
                                    fundMaster.RegExp6M = regressionCoefficient.Beta + "*" + regressionCoefficient.ETFTicker;
                                }
                                else
                                {
                                    string equation = (regressionCoefficient.Beta >= 0) ? "+" : "";
                                    equation += regressionCoefficient.Beta + "*" + regressionCoefficient.ETFTicker;
                                    fundMaster.RegExp6M += equation;
                                }
                                rSquared6M = regressionCoefficient.RSqrd;
                                break;
                            }
                        case "12M":
                            {
                                if (string.IsNullOrEmpty(fundMaster.RegExp12M))
                                {
                                    fundMaster.RegExp12M = regressionCoefficient.Beta + "*" + regressionCoefficient.ETFTicker;
                                }
                                else
                                {
                                    string equation = (regressionCoefficient.Beta >= 0) ? "+" : "";
                                    equation += regressionCoefficient.Beta + "*" + regressionCoefficient.ETFTicker;
                                    fundMaster.RegExp12M += equation;
                                }
                                rSquared12M = regressionCoefficient.RSqrd;
                                break;
                            }
                        case "24M":
                            {
                                if (string.IsNullOrEmpty(fundMaster.RegExp24M))
                                {
                                    fundMaster.RegExp24M = regressionCoefficient.Beta + "*" + regressionCoefficient.ETFTicker;
                                }
                                else
                                {
                                    string equation = (regressionCoefficient.Beta >= 0) ? "+" : "";
                                    equation += regressionCoefficient.Beta + "*" + regressionCoefficient.ETFTicker;
                                    fundMaster.RegExp24M += equation;
                                }
                                rSquared24M = regressionCoefficient.RSqrd;
                                break;
                            }
                        case "36M":
                            {
                                if (string.IsNullOrEmpty(fundMaster.RegExp36M))
                                {
                                    fundMaster.RegExp36M = regressionCoefficient.Beta + "*" + regressionCoefficient.ETFTicker;
                                }
                                else
                                {
                                    string equation = (regressionCoefficient.Beta >= 0) ? "+" : "";
                                    equation += regressionCoefficient.Beta + "*" + regressionCoefficient.ETFTicker;
                                    fundMaster.RegExp36M += equation;
                                }
                                rSquared36M = regressionCoefficient.RSqrd;
                                break;
                            }
                        case "60M":
                            {
                                if (string.IsNullOrEmpty(fundMaster.RegExp60M))
                                {
                                    fundMaster.RegExp60M = regressionCoefficient.Beta + "*" + regressionCoefficient.ETFTicker;
                                }
                                else
                                {
                                    string equation = (regressionCoefficient.Beta >= 0) ? "+" : "";
                                    equation += regressionCoefficient.Beta + "*" + regressionCoefficient.ETFTicker;
                                    fundMaster.RegExp60M += equation;
                                }
                                rSquared60M = regressionCoefficient.RSqrd;
                                break;
                            }
                        case "ALL":
                            {
                                if (string.IsNullOrEmpty(fundMaster.RegExpLife))
                                {
                                    fundMaster.RegExpLife = regressionCoefficient.Beta + "*" + regressionCoefficient.ETFTicker;
                                }
                                else
                                {
                                    string equation = (regressionCoefficient.Beta >= 0) ? "+" : "";
                                    equation += regressionCoefficient.Beta + "*" + regressionCoefficient.ETFTicker;
                                    fundMaster.RegExpLife += equation;
                                }
                                rSquaredLife = regressionCoefficient.RSqrd;
                                break;
                            }
                        default:
                            break;
                    }
                }

                //3M
                if (rSquared3M.HasValue)
                {
                    fundMaster.RSqrd += rSquared3M.GetValueOrDefault() + "|";
                    fundMaster.RSqrd3M = rSquared3M.GetValueOrDefault();
                }
                else
                {
                    fundMaster.RSqrd += "|";
                }

                //6M
                if (rSquared6M.HasValue)
                {
                    fundMaster.RSqrd += rSquared6M.GetValueOrDefault() + "|";
                    fundMaster.RSqrd6M = rSquared6M.GetValueOrDefault();
                }
                else
                {
                    fundMaster.RSqrd += "|";
                }

                //12M
                if (rSquared12M.HasValue)
                {
                    fundMaster.RSqrd += rSquared12M.GetValueOrDefault() + "|";
                    fundMaster.RSqrd12M = rSquared12M.GetValueOrDefault();
                }
                else
                {
                    fundMaster.RSqrd += "|";
                }

                //24M
                if (rSquared24M.HasValue)
                {
                    fundMaster.RSqrd += rSquared24M.GetValueOrDefault() + "|";
                    fundMaster.RSqrd24M = rSquared24M.GetValueOrDefault();
                }
                else
                {
                    fundMaster.RSqrd += "|";
                }

                //36M
                if (rSquared36M.HasValue)
                {
                    fundMaster.RSqrd += rSquared36M.GetValueOrDefault() + "|";
                    fundMaster.RSqrd36M = rSquared36M.GetValueOrDefault();
                }
                else
                {
                    fundMaster.RSqrd += "|";
                }

                //60M
                if (rSquared60M.HasValue)
                {
                    fundMaster.RSqrd += rSquared60M.GetValueOrDefault() + "|";
                    fundMaster.RSqrd60M = rSquared60M.GetValueOrDefault();
                }
                else
                {
                    fundMaster.RSqrd += "|";
                }

                //Life
                if (rSquaredLife.HasValue)
                {
                    fundMaster.RSqrd += rSquaredLife.GetValueOrDefault() + "|";
                    fundMaster.RSqrdLife = rSquaredLife.GetValueOrDefault();
                }
                else
                {
                    fundMaster.RSqrd += "|";
                }

            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error generating regression equation for ticker: " + fundMaster.Ticker);
                throw;
            }
        }

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <returns></returns>
        //public IList<FundHoldingsReturn> GetFundHoldingsReturn()
        //{
        //    return _commonDao.GetFundHoldingsReturn();
        //}

        /// <summary>
        /// 
        /// </summary>
        /// <param name="Ticker"></param>
        /// <param name="StartDate"></param>
        /// <param name="EndDate"></param>
        /// <returns></returns>
        public IList<MSFundHistory> GetMSFundHistory(string Ticker, string StartDate, string EndDate)
        {
            return _commonDao.GetMSFundHistory(Ticker, StartDate, EndDate);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="Ticker"></param>
        /// <returns></returns>
        public IList<MSExpenseRatio> GetMSExpenseHistory(string Ticker)
        {
            return _commonDao.GetMSExpenseHistory(Ticker);
        }

        /// <summary>
        /// Gets fund historical discount mean and standard deviations for different time periods
        /// This data is used to calculate live Z-Score and D-Scores for different time periods
        ///     Z-Score = (Current Discount - Mean Discount)/Standard Deviation of Discount
        ///     D-Score = (Current Discount - Mean Discount)
        /// </summary>
        /// <returns></returns>
        public IDictionary<string, FundHistStats> GetFundStats()
        {
            IDictionary<string, FundHistStats> fundStatsDict = _commonDao.GetFundStats();

            foreach (string ticker in fundStatsDict.Keys)
            {
                try
                {
                    FundHistStats fundStats = fundStatsDict[ticker];

                    //Mean and Std Deviation 1W
                    if (!string.IsNullOrEmpty(fundStats.Stats1W))
                    {
                        string[] stats = fundStats.Stats1W.Split('|');
                        fundStats.Mean1W = DataConversionUtils.ConvertToDouble(stats[0], 100);
                        fundStats.StdDev1W = DataConversionUtils.ConvertToDouble(stats[1], 100);
                    }

                    //Mean and Std Deviation 2W
                    if (!string.IsNullOrEmpty(fundStats.Stats2W))
                    {
                        string[] stats = fundStats.Stats2W.Split('|');
                        fundStats.Mean2W = DataConversionUtils.ConvertToDouble(stats[0], 100);
                        fundStats.StdDev2W = DataConversionUtils.ConvertToDouble(stats[1], 100);
                    }

                    //Mean and Std Deviation 1M
                    if (!string.IsNullOrEmpty(fundStats.Stats1M))
                    {
                        string[] stats = fundStats.Stats1M.Split('|');
                        fundStats.Mean1M = DataConversionUtils.ConvertToDouble(stats[0], 100);
                        fundStats.StdDev1M = DataConversionUtils.ConvertToDouble(stats[1], 100);
                    }

                    //Mean and Std Deviation 3M
                    if (!string.IsNullOrEmpty(fundStats.Stats3M))
                    {
                        string[] stats = fundStats.Stats3M.Split('|');
                        fundStats.Mean3M = DataConversionUtils.ConvertToDouble(stats[0], 100);
                        fundStats.StdDev3M = DataConversionUtils.ConvertToDouble(stats[1], 100);
                    }

                    //Mean and Std Deviation 6M
                    if (!string.IsNullOrEmpty(fundStats.Stats6M))
                    {
                        string[] stats = fundStats.Stats6M.Split('|');
                        fundStats.Mean6M = DataConversionUtils.ConvertToDouble(stats[0], 100);
                        fundStats.StdDev6M = DataConversionUtils.ConvertToDouble(stats[1], 100);
                    }

                    //Mean and Std Deviation 12M
                    if (!string.IsNullOrEmpty(fundStats.Stats12M))
                    {
                        string[] stats = fundStats.Stats12M.Split('|');
                        fundStats.Mean12M = DataConversionUtils.ConvertToDouble(stats[0], 100);
                        fundStats.StdDev12M = DataConversionUtils.ConvertToDouble(stats[1], 100);
                    }

                    //Mean and Std Deviation 24M
                    if (!string.IsNullOrEmpty(fundStats.Stats24M))
                    {
                        string[] stats = fundStats.Stats24M.Split('|');
                        fundStats.Mean24M = DataConversionUtils.ConvertToDouble(stats[0], 100);
                        fundStats.StdDev24M = DataConversionUtils.ConvertToDouble(stats[1], 100);
                    }

                    //Mean and Std Deviation 36M
                    if (!string.IsNullOrEmpty(fundStats.Stats36M))
                    {
                        string[] stats = fundStats.Stats36M.Split('|');
                        fundStats.Mean36M = DataConversionUtils.ConvertToDouble(stats[0], 100);
                        fundStats.StdDev36M = DataConversionUtils.ConvertToDouble(stats[1], 100);
                    }

                    //Mean and Std Deviation 60M
                    if (!string.IsNullOrEmpty(fundStats.Stats60M))
                    {
                        string[] stats = fundStats.Stats60M.Split('|');
                        fundStats.Mean60M = DataConversionUtils.ConvertToDouble(stats[0], 100);
                        fundStats.StdDev60M = DataConversionUtils.ConvertToDouble(stats[1], 100);
                    }

                    //Mean and Std Deviation Life
                    if (!string.IsNullOrEmpty(fundStats.StatsLife))
                    {
                        string[] stats = fundStats.StatsLife.Split('|');
                        fundStats.MeanLife = DataConversionUtils.ConvertToDouble(stats[0], 100);
                        fundStats.StdDevLife = DataConversionUtils.ConvertToDouble(stats[1], 100);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error parsing fund stats for ticker " + ticker);
                }
            }
            return fundStatsDict;
        }

        /// <summary>
        /// Gets pre-computed historical data
        /// Fund Holder Summary
        /// Fund Performance Stats -- Price and Nav Returns, Sharpe Ratios and Volatilities (Closed End Funds)
        /// Fund Performance Stats (BDCs, REITs, Holding Companies)
        /// Fund Performance Ranks (Closed End Funds)
        /// Fund Performance Ranks (BDCs, REITs, Holding Companies)
        /// </summary>
        public void PopulateFundSupplementalData()
        {
            IDictionary<string, FundHolderSummary> fundHolderSummaryDict = _commonDao.GetFundHolderSummary();
            IDictionary<string, FundPerformanceReturn> fundPerformanceReturnsDict = _fundDao.GetFundPerformanceReturns();
            IDictionary<string, FundPerformanceRisk> fundPerformanceRiskStatsDict = _fundDao.GetFundPerformanceRiskStats();
            IDictionary<string, IDictionary<string, FundPerformanceRank>> fundPerformanceRanksDict = _fundDao.GetFundPerformanceRanks();

            IDictionary<string, FundMaster> fundMasterDict = _cache.Get<IDictionary<string, FundMaster>>(CacheKeys.FUND_MASTER);
            IDictionary<string, FundRedemption> fundRedemptionDict = _cache.Get<IDictionary<string, FundRedemption>>(CacheKeys.FUND_REDEMPTIONS);
            IDictionary<string, FundDividend> fundDividendDict = _cache.Get<IDictionary<string, FundDividend>>(CacheKeys.FUND_DIVIDENDS);
            IDictionary<string, FundAlphaModelScores> fundAlphaModelScoresDict = _cache.Get<IDictionary<string, FundAlphaModelScores>>(CacheKeys.ALPHA_MODEL_SCORES);
            IDictionary<string, FundHistStats> fundHistStatsDict = _cache.Get<IDictionary<string, FundHistStats>>(CacheKeys.FUND_STATS);
            IDictionary<string, BrokerTradingVolume> brokerTradingVolumeDict = _cache.Get<IDictionary<string, BrokerTradingVolume>>(CacheKeys.BROKER_TRADING_VOLUME);
            IDictionary<string, FundAlphaModelParams> fundAlphaModelParamsDict = _cache.Get<IDictionary<string, FundAlphaModelParams>>(CacheKeys.ALPHA_MODEL_COEFFICIENTS);

            //Proxy Formulas
            IDictionary<string, FundProxyFormula> fundProxyTickersDict = new Dictionary<string, FundProxyFormula>(StringComparer.CurrentCultureIgnoreCase);
            IDictionary<string, FundProxyFormula> fundAltProxyTickersDict = new Dictionary<string, FundProxyFormula>(StringComparer.CurrentCultureIgnoreCase);
            IDictionary<string, FundProxyFormula> fundPortProxyTickersDict = new Dictionary<string, FundProxyFormula>(StringComparer.CurrentCultureIgnoreCase);

            foreach (KeyValuePair<string, FundMaster> kvp in fundMasterDict)
            {
                string ticker = kvp.Key;

                //if (ticker.Equals("PSEC US", StringComparison.CurrentCultureIgnoreCase))
                //    _logger.LogInformation("Accrued Interest: " + ticker);

                try
                {
                    FundMaster fundMaster = kvp.Value;

                    //populate activist holdings
                    if (fundHolderSummaryDict.TryGetValue(ticker, out FundHolderSummary fundHolderSummary))
                        fundMaster.FundHolderSummary = fundHolderSummary;

                    //populate historical returns
                    if (fundPerformanceReturnsDict.TryGetValue(ticker, out FundPerformanceReturn fundPerformanceReturn))
                        fundMaster.FundPerfRtn = fundPerformanceReturn;

                    //populate historical risk metrics (volatilies, sharpe ratios)
                    if (fundPerformanceRiskStatsDict.TryGetValue(ticker, out FundPerformanceRisk fundPerformanceRisk))
                        fundMaster.FundPerfRisk = fundPerformanceRisk;

                    //populate fund performance ranks
                    if (fundPerformanceRanksDict.TryGetValue(ticker, out IDictionary<string, FundPerformanceRank> fundPerformanceByPeerGroupDict))
                        fundMaster.FundPerfRanks = fundPerformanceByPeerGroupDict.Values.ToList<FundPerformanceRank>();

                    //populate dividends
                    if (fundDividendDict.TryGetValue(ticker, out FundDividend fundDividend))
                        fundMaster.FundDvd = fundDividend;

                    //populate fund redemption details
                    fundMaster.FundStruct = "Standard"; //default
                    fundMaster.IsIRRFund = "N";
                    if (fundRedemptionDict.TryGetValue(ticker, out FundRedemption fundRedemption))
                    {
                        fundMaster.FundRedemption = fundRedemption;
                        if (!string.IsNullOrEmpty(fundRedemption.Structure))
                            fundMaster.FundStruct = fundRedemption.Structure;

                        if (fundRedemption.NextRedemptionDate.HasValue)
                            fundMaster.IsIRRFund = "Y";
                    }

                    //populate alpha model scores
                    if (fundAlphaModelScoresDict.TryGetValue(ticker, out FundAlphaModelScores fundAlphaModelScores))
                        fundMaster.FundAlphaModelScores = fundAlphaModelScores;

                    //populate alpha model params
                    if (fundAlphaModelParamsDict.TryGetValue(ticker, out FundAlphaModelParams fundAlphaModelParams))
                        fundMaster.FundAlphaModelParams = PopulateExpectedAlphaParams(fundAlphaModelParams);

                    //populate fund stats (discount mean and standard deviation)
                    if (fundHistStatsDict.TryGetValue(ticker, out FundHistStats fundHistStats))
                        fundMaster.FundHistStats = fundHistStats;

                    //proxy formula for the fund
                    //process proxy formula -- identify list of proxy tickers for later processing
                    if (!string.IsNullOrEmpty(fundMaster.ProxyForm))
                    {
                        try
                        {
                            IList<FundProxy> proxyTickersWithCoefficients = FundUtils.GetProxyTickers(ticker, fundMaster.ProxyForm);
                            FundProxyFormula fundProxyFormula = new FundProxyFormula
                            {
                                Ticker = ticker,
                                ProxyFormula = fundMaster.ProxyForm,
                                ProxyTickersWithCoefficients = proxyTickersWithCoefficients
                            };
                            fundProxyTickersDict.Add(ticker, fundProxyFormula);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError("Error parsing proxy formula for ticker: " + ticker + "/" + fundMaster.ProxyForm, ex);
                        }
                    }

                    //alternate proxy formula for the fund
                    //process alternate proxy formula -- identify list of proxy tickers for later processing
                    if (!string.IsNullOrEmpty(fundMaster.AltProxyForm))
                    {
                        try
                        {
                            IList<FundProxy> proxyTickersWithCoefficients = FundUtils.GetProxyTickers(ticker, fundMaster.AltProxyForm);
                            FundProxyFormula fundProxyFormula = new FundProxyFormula
                            {
                                Ticker = ticker,
                                ProxyFormula = fundMaster.AltProxyForm,
                                ProxyTickersWithCoefficients = proxyTickersWithCoefficients
                            };
                            fundAltProxyTickersDict.Add(ticker, fundProxyFormula);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError("Error parsing alt proxy formula for ticker: " + ticker + "/" + fundMaster.AltProxyForm, ex);
                        }
                    }

                    //proxy formula for the fund fixed income assets in the underlying portfolio
                    //process fixed income proxy formula -- identify list of proxy tickers for later processing
                    if (!string.IsNullOrEmpty(fundMaster.PortFIProxyForm))
                    {
                        try
                        {
                            IList<FundProxy> proxyTickersWithCoefficients = FundUtils.GetProxyTickers(ticker, fundMaster.PortFIProxyForm);
                            FundProxyFormula fundProxyFormula = new FundProxyFormula
                            {
                                Ticker = ticker,
                                ProxyFormula = fundMaster.PortFIProxyForm,
                                ProxyTickersWithCoefficients = proxyTickersWithCoefficients
                            };
                            fundPortProxyTickersDict.Add(ticker, fundProxyFormula);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError("Error parsing fi proxy formula for ticker: " + ticker + "/" + fundMaster.PortFIProxyForm, ex);
                        }
                    }

                    //populate broker trading volume
                    if (brokerTradingVolumeDict.TryGetValue(ticker, out BrokerTradingVolume brokerTradingVolume))
                        fundMaster.BkrTrdVol = brokerTradingVolume;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error populating fund master for ticker: " + ticker);
                }
            }

            //save fund proxy tickers
            _cache.Remove(CacheKeys.PROXY_FORMULAS);
            _cache.Add(CacheKeys.PROXY_FORMULAS, fundProxyTickersDict, DateTimeOffset.MaxValue);
            _commonDao.SaveFundProxyTickers(fundProxyTickersDict);

            //save fund alternate proxy tickers
            _cache.Remove(CacheKeys.ALT_PROXY_FORMULAS);
            _cache.Add(CacheKeys.ALT_PROXY_FORMULAS, fundAltProxyTickersDict, DateTimeOffset.MaxValue);
            _commonDao.SaveFundAltProxyTickers(fundAltProxyTickersDict);

            //save fund fixed income proxy tickers
            _cache.Remove(CacheKeys.PORT_PROXY_FORMULAS);
            _cache.Add(CacheKeys.PORT_PROXY_FORMULAS, fundPortProxyTickersDict, DateTimeOffset.MaxValue);
            _commonDao.SaveFundPortProxyTickers(fundPortProxyTickersDict);
        }

        /// <summary>
        /// Calculates next redemption notice date, redemption date and redemption payment dates for caclulating IRR's
        /// </summary>
        /// <param name="fundRedemptionDict"></param>
        private void ProcessFundRedemptionDetails(IDictionary<string, FundRedemption> fundRedemptionDict)
        {
            _irrCalculator.GenerateRedemptionSchedule(fundRedemptionDict);
            _commonDao.SaveFundRedemptionDates(fundRedemptionDict);
        }

        /// <summary>
        /// Gets list of securities based on the selections made in Trader Filter screen
        /// </summary>
        /// <param name="cefParams"></param>
        /// <returns></returns>
        public IList<FundGroup> GetCEFFundTickers(CEFParameters cefParams)
        {
            IDictionary<string, FundMaster> fundMasterDict = _cache.Get<IDictionary<string, FundMaster>>(CacheKeys.FUND_MASTER);
            IDictionary<string, FundForecast> fundForecastDict = _cache.Get<IDictionary<string, FundForecast>>(CacheKeys.FUND_FORECASTS);
            IDictionary<string, UserDataOverride> userOverrideDict = _cache.Get<IDictionary<string, UserDataOverride>>(CacheKeys.USER_OVERRIDES);
            IDictionary<string, PositionMaster> almPositionMasterDict = _cache.Get<IDictionary<string, PositionMaster>>(CacheKeys.POSITION_MASTER);
            IDictionary<string, FundRedemption> fundRedemptionDict = _cache.Get<IDictionary<string, FundRedemption>>(CacheKeys.FUND_REDEMPTIONS);

            IList<FundMaster> filteredFundMasterList = new List<FundMaster>();
            IList<FundGroup> fundGroupList = new List<FundGroup>();
            IList<FundGroup> fundGroupListSorted = new List<FundGroup>();
            IList<FundGroup> missingFundGroup = new List<FundGroup>();

            IList<CustomView> customViewList = null;

            if (!string.IsNullOrEmpty(cefParams.ViewName))
                customViewList = _baseDao.GetCustomViewDetails(cefParams.ViewName);

            if (cefParams.Market.Equals("AC Holdings Only")
                || cefParams.Market.Equals("Eur Holdings Only"))
            {
                ApplyHoldingsFilter(cefParams
                    , fundMasterDict
                    , almPositionMasterDict
                    , fundRedemptionDict
                    , missingFundGroup
                    , filteredFundMasterList);
            }
            else
            {
                //filter funds based on country and payment rank filter
                foreach (KeyValuePair<string, FundMaster> kvp in fundMasterDict)
                {
                    //string ticker = kvp.Key;
                    FundMaster fundMaster = kvp.Value;

                    if (!string.IsNullOrEmpty(fundMaster.Status)
                        && "ACTV".Equals(fundMaster.Status, StringComparison.CurrentCultureIgnoreCase))
                    {
                        if (!string.IsNullOrEmpty(cefParams.ViewName))
                        {
                            if (ApplyCustomFilters(fundMaster, fundForecastDict, customViewList, almPositionMasterDict, fundRedemptionDict))
                            {
                                filteredFundMasterList.Add(fundMaster);
                            }
                        }
                        else
                        {
                            if (ApplyStandardFilters(fundMaster, fundForecastDict, cefParams, almPositionMasterDict, fundRedemptionDict))
                            {
                                filteredFundMasterList.Add(fundMaster);
                            }
                        }
                    }
                }
            }

            //group
            foreach (FundMaster fundMaster in filteredFundMasterList)
            {
                FundGroup fundGroup = new FundGroup
                {
                    Ticker = fundMaster.Ticker
                };

                UserDataOverride userOverride;
                userOverrideDict.TryGetValue(fundMaster.Ticker, out userOverride);

                FundForecast fundForecast;
                fundForecastDict.TryGetValue(fundMaster.Ticker, out fundForecast);

                //group by column 1
                if (!string.IsNullOrEmpty(cefParams.GroupBy1))
                    GroupFunds(fundMaster, fundForecast, fundGroup, userOverride, cefParams.GroupBy1, "GroupBy1");

                //group by column 2
                if (!string.IsNullOrEmpty(cefParams.GroupBy2))
                    GroupFunds(fundMaster, fundForecast, fundGroup, userOverride, cefParams.GroupBy2, "GroupBy2");

                //group by column 3
                if (!string.IsNullOrEmpty(cefParams.GroupBy3))
                    GroupFunds(fundMaster, fundForecast, fundGroup, userOverride, cefParams.GroupBy3, "GroupBy3");

                //group by column 4
                if (!string.IsNullOrEmpty(cefParams.GroupBy4))
                    GroupFunds(fundMaster, fundForecast, fundGroup, userOverride, cefParams.GroupBy4, "GroupBy4");

                //default group
                if (string.IsNullOrEmpty(fundGroup.GroupName))
                    fundGroup.GroupName = "All";

                fundGroupList.Add(fundGroup);
            }

            //populate sort field
            if (!string.IsNullOrEmpty(cefParams.SortBy))
            {
                PopulateSortField(fundGroupList, cefParams.SortBy);

                if (string.IsNullOrEmpty(cefParams.SortOrder))
                    cefParams.SortOrder = "Desc";

                if (cefParams.SortOrder.Equals("Desc", StringComparison.CurrentCultureIgnoreCase))
                {
                    fundGroupListSorted = fundGroupList
                        .OrderBy(x => x.GroupByColumn1)
                        .ThenBy(x => x.GroupByColumn2)
                        .ThenBy(x => x.GroupByColumn3)
                        .ThenBy(x => x.GroupByColumn4)
                        .ThenByDescending(x => x.SortFieldValue)
                        .ThenBy(x => x.Ticker)
                        .ToList<FundGroup>();
                }
                else
                {
                    fundGroupListSorted = fundGroupList
                        .OrderBy(x => x.GroupByColumn1)
                        .ThenBy(x => x.GroupByColumn2)
                        .ThenBy(x => x.GroupByColumn3)
                        .ThenBy(x => x.GroupByColumn4)
                        .ThenBy(x => x.SortFieldValue)
                        .ThenBy(x => x.Ticker)
                        .ToList<FundGroup>();
                }
            }
            else
            {
                fundGroupListSorted = fundGroupList
                    .OrderBy(x => x.GroupByColumn1)
                    .ThenBy(x => x.GroupByColumn2)
                    .ThenBy(x => x.GroupByColumn3)
                    .ThenBy(x => x.GroupByColumn4)
                    .ThenBy(x => x.Ticker)
                    .ToList<FundGroup>();
            }

            //add missing funds
            foreach (FundGroup fundGroup in missingFundGroup)
                fundGroupListSorted.Add(fundGroup);

            return fundGroupListSorted;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fundMaster"></param>
        /// <param name="fundForecast"></param>
        /// <param name="fundGroup"></param>
        /// <param name="userOverride"></param>
        /// <param name="selectedGroup"></param>
        /// <param name="groupByColumn"></param>
        private void GroupFunds(FundMaster fundMaster, FundForecast fundForecast, FundGroup fundGroup, UserDataOverride userOverride, string selectedGroup, string groupByColumn)
        {
            string groupName = string.Empty;

            if ("Country".Equals(selectedGroup, StringComparison.CurrentCultureIgnoreCase))
                groupName = fundMaster.CntryCd;
            else if ("Currency".Equals(selectedGroup, StringComparison.CurrentCultureIgnoreCase))
                groupName = fundMaster.Curr;
            else if ("Fund Level 1".Equals(selectedGroup, StringComparison.CurrentCultureIgnoreCase))
                groupName = fundMaster.AssetLvl1;
            else if ("Fund Level 2".Equals(selectedGroup, StringComparison.CurrentCultureIgnoreCase))
                groupName = fundMaster.AssetLvl2;
            else if ("Fund Level 3".Equals(selectedGroup, StringComparison.CurrentCultureIgnoreCase))
                groupName = fundMaster.AssetLvl3;
            else if ("Geo Level 1".Equals(selectedGroup, StringComparison.CurrentCultureIgnoreCase))
                groupName = fundMaster.GeoLvl1;
            else if ("Geo Level 2".Equals(selectedGroup, StringComparison.CurrentCultureIgnoreCase))
                groupName = fundMaster.GeoLvl2;
            else if ("Geo Level 3".Equals(selectedGroup, StringComparison.CurrentCultureIgnoreCase))
                groupName = fundMaster.GeoLvl3;
            else if ("Fund Category".Equals(selectedGroup, StringComparison.CurrentCultureIgnoreCase))
                groupName = FundForecastOperations.GetFundCategory(fundForecast, userOverride);
            else if ("UK Model Category".Equals(selectedGroup, StringComparison.CurrentCultureIgnoreCase))
                groupName = FundForecastOperations.GetUKFundModelCategory(fundForecast, userOverride);
            else if ("Parent Company".Equals(selectedGroup, StringComparison.CurrentCultureIgnoreCase))
                groupName = fundMaster.ParentComp;
            else if ("Numis FundGroup".Equals(selectedGroup, StringComparison.CurrentCultureIgnoreCase))
                groupName = fundMaster.NumisFundGrp;
            else if ("Jefferies FundGroup".Equals(selectedGroup, StringComparison.CurrentCultureIgnoreCase))
                groupName = fundMaster.JeffFundGrp;
            else if ("PeelHunt FundGroup".Equals(selectedGroup, StringComparison.CurrentCultureIgnoreCase))
                groupName = fundMaster.PHFundGrp;

            if ("GroupBy1".Equals(groupByColumn, StringComparison.CurrentCultureIgnoreCase))
                fundGroup.GroupByColumn1 = groupName;
            else if ("GroupBy2".Equals(groupByColumn, StringComparison.CurrentCultureIgnoreCase))
                fundGroup.GroupByColumn2 = groupName;
            else if ("GroupBy3".Equals(groupByColumn, StringComparison.CurrentCultureIgnoreCase))
                fundGroup.GroupByColumn3 = groupName;
            else if ("GroupBy4".Equals(groupByColumn, StringComparison.CurrentCultureIgnoreCase))
                fundGroup.GroupByColumn4 = groupName;

            if (string.IsNullOrEmpty(fundGroup.GroupName))
                fundGroup.GroupName = groupName;
            else
                fundGroup.GroupName += "/" + groupName;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="cefParams"></param>
        /// <param name="fundMasterDict"></param>
        /// <param name="almPositionMasterDict"></param>
        /// <param name="fundRedemptionDict"></param>
        /// <param name="missingFundGroup"></param>
        /// <param name="filteredFundMasterList"></param>
        private void ApplyHoldingsFilter(CEFParameters cefParams
            , IDictionary<string, FundMaster> fundMasterDict
            , IDictionary<string, PositionMaster> almPositionMasterDict
            , IDictionary<string, FundRedemption> fundRedemptionDict
            , IList<FundGroup> missingFundGroup
            , IList<FundMaster> filteredFundMasterList)
        {
            bool matched = false;
            foreach (KeyValuePair<string, PositionMaster> kvp in almPositionMasterDict)
            {
                PositionMaster positionMaster = kvp.Value;
                string holdingTicker = positionMaster.Ticker;
                string securityTicker = positionMaster.SecTicker;
                string countryCode = "Unknown";

                if (positionMaster.FundAll.MV.HasValue)
                {
                    FundMaster fundMaster;
                    fundMasterDict.TryGetValue(holdingTicker, out fundMaster);
                    if (fundMaster == null && securityTicker != null)
                        fundMasterDict.TryGetValue(securityTicker, out fundMaster);

                    matched = false;
                    if (fundMaster != null)
                    {
                        countryCode = fundMaster.CntryCd;
                        if (ApplyAssetTypeFilter(fundMaster, cefParams.AssetType, fundRedemptionDict))
                        {
                            if (ApplyPaymentRankFilter(fundMaster, cefParams.PaymentRank))
                                matched = true;
                        }
                    }
                    else
                    {
                        FundGroup fundGroup = new FundGroup
                        {
                            Ticker = !string.IsNullOrEmpty(securityTicker) ? securityTicker : holdingTicker,
                            GroupName = "Other"
                        };
                        missingFundGroup.Add(fundGroup);
                    }

                    if (matched)
                    {
                        if (cefParams.Market.Equals("AC Holdings Only"))
                        {
                            filteredFundMasterList.Add(fundMaster);
                        }
                        else if (cefParams.Market.Equals("Eur Holdings Only"))
                        {
                            if (!("CA".Equals(countryCode, StringComparison.CurrentCultureIgnoreCase)
                                || "US".Equals(countryCode, StringComparison.CurrentCultureIgnoreCase)
                                || "Australia".Equals(countryCode, StringComparison.CurrentCultureIgnoreCase)
                                || "Bangladesh".Equals(countryCode, StringComparison.CurrentCultureIgnoreCase)))
                            {
                                filteredFundMasterList.Add(fundMaster);
                            }
                        }
                    }
                    //else
                    //{
                    //    _logger.LogInformation("Ignoring Ticker: " + holdingTicker + "/" + securityTicker);
                    //}
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fundMaster"></param>
        /// <param name="fundForecastDict"></param>
        /// <param name="customViewList"></param>
        /// <param name="almPositionMasterDict"></param>
        /// <param name="fundRedemptionDict"></param>
        /// <returns></returns>
        private bool ApplyCustomFilters(FundMaster fundMaster, IDictionary<string, FundForecast> fundForecastDict, IList<CustomView> customViewList, IDictionary<string, PositionMaster> almPositionMasterDict, IDictionary<string, FundRedemption> fundRedemptionDict)
        {
            bool matched = false;
            bool prevMatched = false;
            string prevCondition = string.Empty;
            int firstCondition = 0;

            try
            {
                if (customViewList != null && customViewList.Count > 0)
                {
                    foreach (CustomView customView in customViewList)
                    {
                        firstCondition++;

                        if (customView.FieldName.Equals("Market", StringComparison.CurrentCultureIgnoreCase))
                        {
                            prevMatched = ApplyCountryFilter(fundMaster, customView.FieldValue1, almPositionMasterDict, fundForecastDict);
                        }
                        else if (customView.FieldName.Equals("Security Type", StringComparison.CurrentCultureIgnoreCase))
                        {
                            prevMatched = ApplyAssetTypeFilter(fundMaster, customView.FieldValue1, fundRedemptionDict);
                        }
                        else if (customView.FieldName.Equals("Payment Rank", StringComparison.CurrentCultureIgnoreCase))
                        {
                            prevMatched = ApplyPaymentRankFilter(fundMaster, customView.FieldValue1);
                        }
                        else if (customView.FieldName.Equals("Fund Structure", StringComparison.CurrentCultureIgnoreCase))
                        {
                            prevMatched = ApplyFundStructureFilter(fundMaster, customView.FieldValue1);
                        }
                        else
                        {
                            prevMatched = ApplyFieldFilter(fundMaster, fundForecastDict, customView.FieldName, customView.FieldValue1AsDouble, customView.FieldValue2AsDouble, customView.FieldOperator);
                        }

                        if (firstCondition == 1)
                        {
                            matched = prevMatched;
                            prevCondition = customView.FieldCondition;
                        }
                        else
                        {
                            if (prevCondition.Equals("And", StringComparison.CurrentCultureIgnoreCase))
                            {
                                matched = (matched && prevMatched);
                            }
                            else
                            {
                                matched = (matched || prevMatched);
                            }

                            prevCondition = customView.FieldCondition;
                        }
                    }
                }
            }
            catch (Exception)
            {
            }

            return matched;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fundMaster"></param>
        /// <param name="fundForecastDict"></param>
        /// <param name="cefParams"></param>
        /// <param name="almPositionMasterDict"></param>
        /// <param name="fundRedemptionDict"></param>
        /// <returns></returns>
        private bool ApplyStandardFilters(FundMaster fundMaster, IDictionary<string, FundForecast> fundForecastDict, CEFParameters cefParams, IDictionary<string, PositionMaster> almPositionMasterDict, IDictionary<string, FundRedemption> fundRedemptionDict)
        {
            bool matched = false;

            try
            {
                if (ApplyCountryFilter(fundMaster, cefParams.Market, almPositionMasterDict, fundForecastDict))
                {
                    if (ApplyAssetTypeFilter(fundMaster, cefParams.AssetType, fundRedemptionDict))
                    {
                        if (ApplyPaymentRankFilter(fundMaster, cefParams.PaymentRank))
                        {
                            matched = true;
                        }
                    }
                }
            }
            catch (Exception)
            {
            }

            return matched;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fundGroupList"></param>
        /// <param name="sortField"></param>
        private void PopulateSortField(IList<FundGroup> fundGroupList, string sortField)
        {
            IDictionary<string, FundForecast> fundForecastDict = _cache.Get<IDictionary<string, FundForecast>>(CacheKeys.FUND_FORECASTS);
            IDictionary<string, SecurityPrice> securityPriceDict = _cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);
            IDictionary<string, string> priceTickerMap = _cache.Get<IDictionary<string, string>>(CacheKeys.PRICE_TICKER_MAP);

            foreach (FundGroup fundGroup in fundGroupList)
            {
                string ticker = fundGroup.Ticker;

                FundForecast fundForecast;
                if (fundForecastDict.TryGetValue(ticker, out fundForecast))
                {
                    if ("MCap".Equals(sortField, StringComparison.CurrentCultureIgnoreCase))
                    {
                        fundGroup.SortFieldValue = fundForecast.MV;
                    }
                    else if ("Dscnt To Last Price".Equals(sortField, StringComparison.CurrentCultureIgnoreCase))
                    {
                        fundGroup.SortFieldValue = fundForecast.PDLastPrc;
                    }
                    else if ("Unlev Dscnt To Last Price".Equals(sortField, StringComparison.CurrentCultureIgnoreCase))
                    {
                        fundGroup.SortFieldValue = fundForecast.PDLastPrcUnLev;
                    }
                    else if ("IRR".Equals(sortField, StringComparison.CurrentCultureIgnoreCase))
                    {
                        fundGroup.SortFieldValue = fundForecast.IRRLastPrc;
                    }
                    else if ("Expected Alpha".Equals(sortField, StringComparison.CurrentCultureIgnoreCase))
                    {
                        fundGroup.SortFieldValue = fundForecast.EAFinalAlpha;
                    }
                    else if ("Expected Alpha NEW".Equals(sortField, StringComparison.CurrentCultureIgnoreCase))
                    {
                        fundGroup.SortFieldValue = fundForecast.EAFinalAlpha;
                    }
                    else if ("Price Change".Equals(sortField, StringComparison.CurrentCultureIgnoreCase))
                    {
                        SecurityPrice securityPrice = SecurityPriceLookupOperations.GetSecurityPrice(ticker, priceTickerMap, securityPriceDict);
                        if (securityPrice != null)
                        {
                            fundGroup.SortFieldValue = securityPrice.PrcRtn;
                        }
                    }
                    else if ("Discount Change".Equals(sortField, StringComparison.CurrentCultureIgnoreCase))
                    {
                        fundGroup.SortFieldValue = fundForecast.PDChng;
                    }
                    else if ("Z-Score 1m".Equals(sortField, StringComparison.CurrentCultureIgnoreCase))
                    {
                        fundGroup.SortFieldValue = fundForecast.ZS1M;
                    }
                    else if ("Z-Score 6m".Equals(sortField, StringComparison.CurrentCultureIgnoreCase))
                    {
                        fundGroup.SortFieldValue = fundForecast.ZS6M;
                    }
                    else if ("Z-Score 12m".Equals(sortField, StringComparison.CurrentCultureIgnoreCase))
                    {
                        fundGroup.SortFieldValue = fundForecast.ZS12M;
                    }
                    else if ("Z-Score 24m".Equals(sortField, StringComparison.CurrentCultureIgnoreCase))
                    {
                        fundGroup.SortFieldValue = fundForecast.ZS24M;
                    }
                    else if ("D-Score 1m".Equals(sortField, StringComparison.CurrentCultureIgnoreCase))
                    {
                        fundGroup.SortFieldValue = fundForecast.DS1M;
                    }
                    else if ("D-Score 6m".Equals(sortField, StringComparison.CurrentCultureIgnoreCase))
                    {
                        fundGroup.SortFieldValue = fundForecast.DS6M;
                    }
                    else if ("D-Score 12m".Equals(sortField, StringComparison.CurrentCultureIgnoreCase))
                    {
                        fundGroup.SortFieldValue = fundForecast.DS12M;
                    }
                    else if ("D-Score 24m".Equals(sortField, StringComparison.CurrentCultureIgnoreCase))
                    {
                        fundGroup.SortFieldValue = fundForecast.DS24M;
                    }
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fundMaster"></param>
        /// <param name="fundForecastDict"></param>
        /// <param name="fieldName"></param>
        /// <param name="value1"></param>
        /// <param name="value2"></param>
        /// <param name="op"></param>
        /// <returns></returns>
        private bool ApplyFieldFilter(FundMaster fundMaster, IDictionary<string, FundForecast> fundForecastDict, string fieldName, double? value1, double? value2, string op)
        {
            bool fieldFilter = false;

            try
            {
                double fieldValue = 0;

                FundForecast fundForecast;
                fundForecastDict.TryGetValue(fundMaster.Ticker, out fundForecast);

                if ("MCap".Equals(fieldName, StringComparison.CurrentCultureIgnoreCase))
                {
                    if (fundForecast != null)
                        fieldValue = fundForecast.MV.GetValueOrDefault();
                }
                else if ("Discount".Equals(fieldName, StringComparison.CurrentCultureIgnoreCase))
                {
                    if (fundForecast != null)
                        fieldValue = fundForecast.PDLastPrc.GetValueOrDefault();
                }
                else if ("Z Score 1Yr".Equals(fieldName, StringComparison.CurrentCultureIgnoreCase))
                {
                    if (fundForecast != null)
                        fieldValue = fundForecast.ZS12M.GetValueOrDefault();
                }
                else if ("Z Score 3Yr".Equals(fieldName, StringComparison.CurrentCultureIgnoreCase))
                {
                    if (fundForecast != null)
                        fieldValue = fundForecast.ZS36M.GetValueOrDefault();
                }

                if (">=".Equals(op, StringComparison.CurrentCultureIgnoreCase))
                {
                    if (fieldValue >= value1.GetValueOrDefault())
                    {
                        fieldFilter = true;
                    }
                }
                else if ("<=".Equals(op, StringComparison.CurrentCultureIgnoreCase))
                {
                    if (fieldValue <= value1.GetValueOrDefault())
                    {
                        fieldFilter = true;
                    }
                }
                else if ("Btwn".Equals(op, StringComparison.CurrentCultureIgnoreCase))
                {
                    if (fieldValue > value1.GetValueOrDefault() && fieldValue < value2.GetValueOrDefault())
                    {
                        fieldFilter = true;
                    }
                }
            }
            catch (Exception)
            {
            }

            return fieldFilter;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fundMaster"></param>
        /// <param name="selectedPaymentRank"></param>
        /// <returns></returns>
        private bool ApplyPaymentRankFilter(FundMaster fundMaster, string selectedPaymentRank)
        {
            bool paymentRankFilter = false;
            try
            {
                if ("All".Equals(selectedPaymentRank, StringComparison.CurrentCultureIgnoreCase))
                {
                    paymentRankFilter = true;
                }
                else if ("Non Equity".Equals(selectedPaymentRank, StringComparison.CurrentCultureIgnoreCase))
                {
                    if (!"Equity".Equals(fundMaster.PayRank, StringComparison.CurrentCultureIgnoreCase))
                    {
                        paymentRankFilter = true;
                    }
                }
                else if (selectedPaymentRank.Equals(fundMaster.PayRank, StringComparison.CurrentCultureIgnoreCase))
                {
                    paymentRankFilter = true;
                }
            }
            catch (Exception)
            {
            }
            return paymentRankFilter;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fundMaster"></param>
        /// <param name="selectedFundStructure"></param>
        /// <returns></returns>
        private bool ApplyFundStructureFilter(FundMaster fundMaster, string selectedFundStructure)
        {
            bool matched = false;

            try
            {
                if (selectedFundStructure.Equals(fundMaster.FundStruct, StringComparison.CurrentCultureIgnoreCase))
                {
                    matched = true;
                }
            }
            catch (Exception)
            {
            }
            return matched;
        }

        /// <summary>
        /// Applies Asset Type Filter
        /// </summary>
        /// <param name="fundMaster"></param>
        /// <param name="selectedAssetType"></param>
        /// <param name="fundRedemptionDict"></param>
        /// <returns></returns>
        private bool ApplyAssetTypeFilter(FundMaster fundMaster, string selectedAssetType, IDictionary<string, FundRedemption> fundRedemptionDict)
        {
            bool assetType = false;
            try
            {
                if ("All".Equals(selectedAssetType, StringComparison.CurrentCultureIgnoreCase))
                {
                    assetType = true;
                }
                else if ("Non CEF".Equals(selectedAssetType, StringComparison.CurrentCultureIgnoreCase))
                {
                    if ("Y".Equals(fundMaster.NonCEFTyp, StringComparison.CurrentCultureIgnoreCase))
                        assetType = true;

                    //if (!"CEF".Equals(fundMaster.AssetType, StringComparison.CurrentCultureIgnoreCase))
                    //{
                    //    assetType = true;
                    //}
                }
                else if ("BDC & Reit".Equals(selectedAssetType, StringComparison.CurrentCultureIgnoreCase))
                {
                    if ("BDC".Equals(fundMaster.AssetTyp, StringComparison.CurrentCultureIgnoreCase)
                        || "Reit".Equals(fundMaster.AssetTyp, StringComparison.CurrentCultureIgnoreCase))
                    {
                        assetType = true;
                    }
                }
                else if (selectedAssetType.Equals(fundMaster.AssetTyp, StringComparison.CurrentCultureIgnoreCase))
                {
                    assetType = true;
                }
                else if ("IRR".Equals(selectedAssetType, StringComparison.CurrentCultureIgnoreCase))
                {
                    string countryCode = fundMaster.CntryCd;

                    if (fundRedemptionDict.TryGetValue(fundMaster.Ticker, out FundRedemption fundRedemption)
                        && fundRedemption.NextRedemptionDate.HasValue
                        && !("Bangladesh".Equals(countryCode, StringComparison.CurrentCultureIgnoreCase)))
                        assetType = true;
                }
            }
            catch (Exception)
            {
            }
            return assetType;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fundMaster"></param>
        /// <param name="selectedMarket"></param>
        /// <param name="almPositionMasterDict"></param>
        /// <param name="fundForecastDict"></param>
        /// <returns></returns>
        private bool ApplyCountryFilter(FundMaster fundMaster
            , string selectedMarket
            , IDictionary<string, PositionMaster> almPositionMasterDict
            , IDictionary<string, FundForecast> fundForecastDict)
        {
            bool marketFilter = false;
            try
            {
                string countryCode = fundMaster.CntryCd;

                if ("US".Equals(selectedMarket, StringComparison.CurrentCultureIgnoreCase)
                    && "US".Equals(countryCode, StringComparison.CurrentCultureIgnoreCase))
                {
                    marketFilter = true;
                }
                else if ("UK".Equals(selectedMarket, StringComparison.CurrentCultureIgnoreCase)
                    && "UK".Equals(countryCode, StringComparison.CurrentCultureIgnoreCase))
                {
                    marketFilter = true;
                }
                else if ("Canada".Equals(selectedMarket, StringComparison.CurrentCultureIgnoreCase)
                    && "CA".Equals(countryCode, StringComparison.CurrentCultureIgnoreCase))
                {
                    marketFilter = true;
                }
                else if ("AUS".Equals(selectedMarket, StringComparison.CurrentCultureIgnoreCase)
                    && "Australia".Equals(countryCode, StringComparison.CurrentCultureIgnoreCase))
                {
                    marketFilter = true;
                }
                else if ("Asia Pacific".Equals(selectedMarket, StringComparison.CurrentCultureIgnoreCase)
                    && ("Australia".Equals(countryCode, StringComparison.CurrentCultureIgnoreCase)
                        || "Bangladesh".Equals(countryCode, StringComparison.CurrentCultureIgnoreCase)))
                {
                    marketFilter = true;
                }
                else if ("North America".Equals(selectedMarket, StringComparison.CurrentCultureIgnoreCase)
                    && ("CA".Equals(countryCode, StringComparison.CurrentCultureIgnoreCase)
                        || "US".Equals(countryCode, StringComparison.CurrentCultureIgnoreCase)))
                {
                    marketFilter = true;
                }
                else if ("Major European".Equals(selectedMarket, StringComparison.CurrentCultureIgnoreCase)
                    && !("CA".Equals(countryCode, StringComparison.CurrentCultureIgnoreCase)
                        || "US".Equals(countryCode, StringComparison.CurrentCultureIgnoreCase)
                        || "Australia".Equals(countryCode, StringComparison.CurrentCultureIgnoreCase)
                        || "Bangladesh".Equals(countryCode, StringComparison.CurrentCultureIgnoreCase)))
                {
                    marketFilter = true;
                }
                else if ("World".Equals(selectedMarket, StringComparison.CurrentCultureIgnoreCase))
                {
                    marketFilter = true;
                }
                else if ("AC Holdings Only".Equals(selectedMarket, StringComparison.CurrentCultureIgnoreCase))
                {
                    if (almPositionMasterDict.TryGetValue(fundMaster.Ticker, out PositionMaster positionMaster))
                        marketFilter = true;
                }
                else if ("SPACs".Equals(selectedMarket, StringComparison.CurrentCultureIgnoreCase))
                {
                    if (fundForecastDict.TryGetValue(fundMaster.Ticker, out FundForecast fundForecast))
                    {
                        if (fundForecast.FundCat.Equals("SPACs", StringComparison.CurrentCultureIgnoreCase))
                            marketFilter = true;
                    }
                }
            }
            catch (Exception)
            {
            }

            return marketFilter;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="reqParams"></param>
        /// <returns></returns>
        public IList<FundHoldingReturn> GetFundHoldingReturn(InputParameters reqParams)
        {
            return _commonDao.GetFundHoldingReturn(reqParams.Ticker);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="reqParams"></param>
        /// <returns></returns>
        public IList<FundHoldingReturn> GetFundHoldingReturnsNew(InputParameters reqParams)
        {
            return _commonDao.GetFundHoldingReturnsNew(reqParams.Ticker);
        }

        /// <summary>
        /// Save User Overrides
        /// </summary>
        /// <param name="overrides"></param>
        public void SaveUserOverrides(IList<UserDataOverride> overrides)
        {
            _logger.LogInformation("Saving User Overrides - STARTED");

            string ticker = string.Empty;
            int proxyFormulaUpdateCount = 0;
            int altProxyFormulaUpdateCount = 0;
            int portFIProxyFormulaUpdateCount = 0;

            try
            {
                if (overrides != null && overrides.Count > 0)
                {
                    IDictionary<string, FundMaster> fundMasterDict = _cache.Get<IDictionary<string, FundMaster>>(CacheKeys.FUND_MASTER);
                    IDictionary<string, FundForecast> fundForecastDict = _cache.Get<IDictionary<string, FundForecast>>(CacheKeys.FUND_FORECASTS);
                    IDictionary<string, FundNav> fundNavDict = _cache.Get<IDictionary<string, FundNav>>(CacheKeys.FUND_NAVS);
                    IDictionary<string, FundAlphaModelScores> fundAlphaModelScoresDict = _cache.Get<IDictionary<string, FundAlphaModelScores>>(CacheKeys.ALPHA_MODEL_SCORES);
                    IDictionary<string, FundProxyFormula> fundProxyFormulaDict = _cache.Get<IDictionary<string, FundProxyFormula>>(CacheKeys.PROXY_FORMULAS);
                    IDictionary<string, FundProxyFormula> fundAltProxyFormulaDict = _cache.Get<IDictionary<string, FundProxyFormula>>(CacheKeys.ALT_PROXY_FORMULAS);
                    IDictionary<string, FundProxyFormula> fundPortProxyFormulaDict = _cache.Get<IDictionary<string, FundProxyFormula>>(CacheKeys.PORT_PROXY_FORMULAS);

                    foreach (UserDataOverride userOverride in overrides)
                    {
                        ticker = userOverride.Ticker;
                        fundMasterDict.TryGetValue(ticker, out FundMaster fundMaster);
                        fundAlphaModelScoresDict.TryGetValue(ticker, out FundAlphaModelScores fundAlphaModelScores);

                        //override proxy formula if it exists or assign a new one
                        if (!string.IsNullOrEmpty(userOverride.ProxyFormula))
                            proxyFormulaUpdateCount++;

                        //override proxy formula if it exists or assign a new one
                        if (!string.IsNullOrEmpty(userOverride.AltProxyFormula))
                            altProxyFormulaUpdateCount++;

                        //override proxy formula if it exists or assign a new one
                        if (!string.IsNullOrEmpty(userOverride.PortFIProxyFormula))
                            portFIProxyFormulaUpdateCount++;

                        //proxy formula
                        PopulateProxyFormula(ticker, userOverride.ProxyFormula, fundProxyFormulaDict, "FundProxy");

                        //alternate proxy formula
                        PopulateProxyFormula(ticker, userOverride.ProxyFormula, fundAltProxyFormulaDict, "FundAltProxy");

                        //override port (Fixed Income) proxy formula if it exists or assign a new one
                        PopulateProxyFormula(ticker, userOverride.PortFIProxyFormula, fundPortProxyFormulaDict, "FundPortFIProxy");

                        if (fundForecastDict.TryGetValue(ticker, out FundForecast fundForecast))
                        {
                            FundMasterOperations.ApplyUserOverrides(
                                fundForecast, fundMaster, fundAlphaModelScores, userOverride);
                        }

                        if (fundNavDict.TryGetValue(userOverride.Ticker, out FundNav fundNav))
                        {
                            fundNav.OvrLastNav = userOverride.NavOverride;
                            fundNav.OvrEstimatedNav = userOverride.EstimatedNavOverride;
                            fundNav.IntrinsicValue = userOverride.IntrinsicValue;

                            if (!string.IsNullOrEmpty(userOverride.NavOverrideDateAsString))
                                fundNav.OvrLastNavDate = DateUtils.ConvertToDate(userOverride.NavOverrideDateAsString, "yyyy-MM-dd");
                        }

                        if (!string.IsNullOrEmpty(userOverride.NavOverrideDateAsString))
                            userOverride.NavOverrideDate = DateUtils.ConvertToDate(userOverride.NavOverrideDateAsString, "yyyy-MM-dd");

                        if (!string.IsNullOrEmpty(userOverride.NavOverrideExpiryDateAsString))
                            userOverride.NavOverrideExpiryDate = DateUtils.ConvertToDate(userOverride.NavOverrideExpiryDateAsString, "yyyy-MM-dd");

                        if (!string.IsNullOrEmpty(userOverride.EstimatedNavOverrideExpiryDateAsString))
                            userOverride.EstimatedNavOverrideExpiryDate = DateUtils.ConvertToDate(userOverride.EstimatedNavOverrideExpiryDateAsString, "yyyy-MM-dd");

                        if (!string.IsNullOrEmpty(userOverride.AccrualStartDateAsString))
                            userOverride.AccrualStartDate = DateUtils.ConvertToDate(userOverride.AccrualStartDateAsString, "yyyy-MM-dd");

                        if (!string.IsNullOrEmpty(userOverride.IntrinsicValueDateAsString))
                            userOverride.IntrinsicValueDate = DateUtils.ConvertToDate(userOverride.IntrinsicValueDateAsString, "yyyy-MM-dd");

                        if (userOverride.IntrinsicValue.HasValue && !userOverride.IntrinsicValueDate.HasValue)
                            userOverride.IntrinsicValueDate = TodaysDate;

                        if (!string.IsNullOrEmpty(userOverride.IntrinsicValueExpiryDateAsString))
                            userOverride.IntrinsicValueExpiryDate = DateUtils.ConvertToDate(userOverride.IntrinsicValueExpiryDateAsString, "yyyy-MM-dd");
                    }

                    //save user overrides
                    _commonDao.SaveUserOverrides(overrides);

                    //save proxy tickers
                    if (proxyFormulaUpdateCount > 0)
                        _commonDao.SaveFundProxyTickers(fundProxyFormulaDict);

                    //save alternate proxy tickers
                    if (altProxyFormulaUpdateCount > 0)
                        _commonDao.SaveFundProxyTickers(fundAltProxyFormulaDict);

                    //save port FI proxy tickers
                    if (portFIProxyFormulaUpdateCount > 0)
                        _commonDao.SaveFundPortProxyTickers(fundProxyFormulaDict);

                    //refresh data cache
                    RefreshUserOverrides(overrides);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving data overrides");
            }

            _logger.LogInformation("Saving User Overrides - DONE");
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ticker"></param>
        /// <param name="proxyFormula"></param>
        /// <param name="dict"></param>
        /// <param name="proxyType"></param>
        private void PopulateProxyFormula(string ticker, string proxyFormula, IDictionary<string, FundProxyFormula> dict, string proxyType)
        {
            if (dict.TryGetValue(ticker, out FundProxyFormula fundProxyFormula))
            {
                fundProxyFormula.ProxyFormula = proxyFormula;
            }
            else if (!string.IsNullOrEmpty(proxyFormula))
            {
                fundProxyFormula = new FundProxyFormula();
                fundProxyFormula.Ticker = ticker;
                fundProxyFormula.ProxyFormula = proxyFormula;
                dict.Add(ticker, fundProxyFormula);

                //update fund forecast proxy flag
                if ("FundProxy".Equals(proxyType))
                    _fundForecastEngine.ResetFundProxyDetails(ticker);
                else if ("FundAltProxy".Equals(proxyType))
                    _fundForecastEngine.ResetFundAltProxyDetails(ticker);
                else if ("FundPortFIProxy".Equals(proxyType))
                    _fundForecastEngine.ResetFundPortProxyDetails(ticker);
            }

            if (fundProxyFormula != null && !string.IsNullOrEmpty(fundProxyFormula.ProxyFormula))
            {
                IList<FundProxy> proxyTickersWithCoefficients = FundUtils.GetProxyTickers(ticker, fundProxyFormula.ProxyFormula);
                fundProxyFormula.ProxyTickersWithCoefficients = proxyTickersWithCoefficients;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="overrides"></param>
        /// <returns></returns>
        private IList<UserDataOverride> CaptureDifferences(IList<UserDataOverride> overrides)
        {
            IList<UserDataOverride> filteredOverrides = new List<UserDataOverride>();
            IDictionary<string, UserDataOverride> overridesDict = _cache.Get<IDictionary<string, UserDataOverride>>(CacheKeys.USER_OVERRIDES);

            foreach (UserDataOverride dataOverride in overrides)
            {
                string ticker = dataOverride.Ticker;

                UserDataOverride obj;
                if (overridesDict.TryGetValue(ticker, out obj))
                {
                    bool flag = true;
                    try
                    {
                        flag = obj.Equals(dataOverride);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error comparing user override object for ticker: " + ticker);
                    }

                    if (!flag)
                    {
                        filteredOverrides.Add(dataOverride);
                    }
                }
                else
                {
                    filteredOverrides.Add(dataOverride);
                }
            }

            return filteredOverrides;
        }

        /// <summary>
        /// Updates User overrides in the data cache
        /// Update navs, estimated returns since last nav date if there is a change to published nav, estimated nav, proxy rule
        /// </summary>
        private void RefreshUserOverrides(IList<UserDataOverride> overrides)
        {
            IList<UserDataOverride> filteredDataOverrides = CaptureDifferences(overrides);
            if (filteredDataOverrides != null && filteredDataOverrides.Count > 0)
            {
                UpdateFundNavs();
                PopulateProxyETFReturns();
                PopulateAltProxyETFReturns();
                PopulatePortProxyETFReturns();
                _commonDao.UpdateFundNavOverrides();
            }

            PopulateUserOverrides();
        }

        /// <summary>
        /// Populate User Overrides 
        /// </summary>
        public void PopulateUserOverrides()
        {
            _logger.LogInformation("Populating User Overrides - STARTED");
            try
            {
                IDictionary<string, UserDataOverride> dict = _commonDao.GetUserDataOverrides();
                _cache.Remove(CacheKeys.USER_OVERRIDES);
                _cache.Add(CacheKeys.USER_OVERRIDES, dict, DateTimeOffset.MaxValue);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error populating User Overrides");
            }
            _logger.LogInformation("Populating User Overrides - DONE");
        }

        /// <summary>
        /// Updates latest fund navs and ex dividends
        /// Navs are adjusted for ex dividends
        /// </summary>
        public void UpdateFundNavs()
        {
            _logger.LogInformation("Updating Fund Navs - STARTED");
            try
            {
                IDictionary<string, FundNav> fundNavDict = _commonDao.GetFundNavs();
                IDictionary<string, FundDividend> fundDividendDict = _commonDao.GetFundDividends();
                foreach (KeyValuePair<string, FundNav> kvp in fundNavDict)
                {
                    string key = kvp.Key;
                    FundNav fundNav = kvp.Value;

                    if (fundDividendDict.TryGetValue(key, out FundDividend fundDividend))
                    {
                        if (fundDividend.DvdFromLastNavDate.HasValue)
                        {
                            fundNav.DvdFromLastNavDate = fundDividend.DvdFromLastNavDate;
                            fundNav.ExDvdDates = fundDividend.ExDvdDates;
                        }
                    }
                }

                //_cache.Remove(CacheKeys.FUND_NAVS);
                _cache.Add(CacheKeys.FUND_NAVS, fundNavDict, DateTimeOffset.MaxValue);
                IDictionary<string, FundNav> dict1 = _cache.Get<IDictionary<string, FundNav>>(CacheKeys.FUND_NAVS);
                _logger.LogInformation("Fund Navs Count: " + dict1.Count);

                //_cache.Remove(CacheKeys.FUND_DIVIDENDS);
                _cache.Add(CacheKeys.FUND_DIVIDENDS, fundDividendDict, DateTimeOffset.MaxValue);
                IDictionary<string, FundDividend> dict2 = _cache.Get<IDictionary<string, FundDividend>>(CacheKeys.FUND_DIVIDENDS);
                _logger.LogInformation("Fund Dividends Count: " + dict2.Count);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error updating Fund Navs & Dividends");
            }
            _logger.LogInformation("Updating Fund Navs - DONE");
        }

        /// <summary>
        /// Populates latest FX rates (from BBG)
        /// Some of the FX rates (30+ currencies) are updated live from Neovest
        /// </summary>
        public void PopulateFXRates()
        {
            _logger.LogInformation("Populating FX Rates - STARTED");
            try
            {
                IDictionary<string, FXRate> dict = _commonDao.GetFXRates();
                _cache.Remove(CacheKeys.FX_RATES);
                _cache.Add(CacheKeys.FX_RATES, dict, DateTimeOffset.MaxValue);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error updating FX rates");
            }
            _logger.LogInformation("Populating FX Rates - DONE");
        }

        /// <summary>
        /// Update latest FX rates (from BBG)
        /// Some of the FX rates (30+ currencies) are updated live from Neovest
        /// </summary>
        public void UpdateFXRates()
        {
            _logger.LogInformation("Update FX Rates - STARTED");
            try
            {
                IDictionary<string, FXRate> inputDict = _commonDao.GetFXRates();
                IDictionary<string, FXRate> cachedDict = _cache.Get<IDictionary<string, FXRate>>(CacheKeys.FX_RATES);
                foreach (FXRate iFXRate in inputDict.Values)
                {
                    if (cachedDict.TryGetValue(iFXRate.Currency, out FXRate oFXRate))
                    {
                        oFXRate.FXRatePD = iFXRate.FXRatePD;
                        oFXRate.FXRateLatest = iFXRate.FXRateLatest;
                        oFXRate.FXReturn = iFXRate.FXReturn;
                    }
                    else
                    {
                        _logger.LogInformation("FX not found in the cache: " + iFXRate.Currency);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error updating FX rates");
            }
            _logger.LogInformation("Update FX Rates - DONE");
        }

        /// <summary>
        /// Populates latest FX rates (from BBG)
        /// Some of the FX rates (30+ currencies) are updated live from Neovest
        /// </summary>
        public void PopulateFXRatesPD()
        {
            _logger.LogInformation("Populating FX Rates (Previous Day) - STARTED");
            try
            {
                IDictionary<string, FXRate> dict = _commonDao.GetFXRatesPD();
                _cache.Remove(CacheKeys.FX_RATES_PD);
                _cache.Add(CacheKeys.FX_RATES_PD, dict, DateTimeOffset.MaxValue);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error updating FX rates (Previous Day)");
            }
            _logger.LogInformation("Populating FX Rates (Previous Day)- DONE");
        }

        /// <summary>
        /// Populates ETF returns since last nav date
        /// Since navs are updated from BBG every 'x' minutes, this call is to re-calc the cumulative returns since last nav date
        /// </summary>
        public void PopulateETFReturns()
        {
            _logger.LogInformation("Populating ETF Reg Returns - STARTED");
            try
            {
                IDictionary<string, FundETFReturn> dict = _commonDao.GetFundRegETFReturns();
                //_cache.Remove(CacheKeys.ETF_RETURNS);
                _cache.Add(CacheKeys.ETF_RETURNS, dict, DateTimeOffset.MaxValue);
                IDictionary<string, FundETFReturn> dict1 = _cache.Get<IDictionary<string, FundETFReturn>>(CacheKeys.ETF_RETURNS);
                _logger.LogInformation("ETF Reg Returns Count: " + dict1.Count);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error updating ETF Reg Returns");
            }
            _logger.LogInformation("Populating ETF Reg Returns - DONE");
        }

        /// <summary>
        /// Populates Proxy ETF returns since last nav date 
        /// </summary>
        public void PopulateProxyETFReturns()
        {
            _logger.LogInformation("Populating Proxy ETF Returns - STARTED");
            try
            {
                IDictionary<string, FundETFReturn> dict = _commonDao.GetFundProxyETFReturns();
                //_cache.Remove(CacheKeys.PROXY_ETF_RETURNS);
                _cache.Add(CacheKeys.PROXY_ETF_RETURNS, dict, DateTimeOffset.MaxValue);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error updating Proxy ETF returns");
            }
            _logger.LogInformation("Populating Proxy ETF Returns - DONE");
        }

        /// <summary>
        /// Populates Alternate Proxy ETF returns since last nav date 
        /// </summary>
        public void PopulateAltProxyETFReturns()
        {
            _logger.LogInformation("Populating Alternate Proxy ETF Returns - STARTED");
            try
            {
                IDictionary<string, FundETFReturn> dict = _commonDao.GetFundAltProxyETFReturns();
                //_cache.Remove(CacheKeys.ALT_PROXY_ETF_RETURNS);
                _cache.Add(CacheKeys.ALT_PROXY_ETF_RETURNS, dict, DateTimeOffset.MaxValue);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error updating Alternate Proxy ETF returns");
            }
            _logger.LogInformation("Populating Alternate Proxy ETF Returns - DONE");
        }

        /// <summary>
        /// Populates Port Proxy ETF returns since last nav date
        /// This is to model returns of Fixed Income securities in fund portfolio
        /// </summary>
        public void PopulatePortProxyETFReturns()
        {
            _logger.LogInformation("Populating Port Proxy ETF Returns - STARTED");
            try
            {
                IDictionary<string, FundETFReturn> dict = _commonDao.GetFundPortProxyETFReturns();
                //_cache.Remove(CacheKeys.PORT_PROXY_ETF_RETURNS);
                _cache.Add(CacheKeys.PORT_PROXY_ETF_RETURNS, dict, DateTimeOffset.MaxValue);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error updating Port Proxy ETF returns");
            }
            _logger.LogInformation("Populating Port Proxy ETF Returns - DONE");
        }

        /// <summary>
        /// Updates security real-time pricing flags for display in ACTrader
        /// Security pricing is split into 2 web service calls for performance reasons
        /// One call returns prices for Funds (that are displayed in AC Trader) and another one to get prices for underlying Port holdings
        /// 
        /// Live Prices (for Funds, ETFs, ALM Positions) are updated with 25 ms delay from Neovest and 20 min delay from BBG
        /// Delayed Prices (Port Holdins) are updated with a delay of 2.5 secs from Neovest and 20 min delay from BBG
        /// </summary>
        public void UpdateSecurityPriceFlags()
        {
            _logger.LogInformation("Populating Security Price Flags - STARTED");
            try
            {
                IDictionary<string, SecurityPrice> securityPriceMasterDict = _securityPriceDao.GetSecurityPriceMaster();
                IDictionary<string, SecurityPrice> securityPriceDict = _cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);
                foreach (KeyValuePair<string, SecurityPrice> kvp in securityPriceMasterDict)
                {
                    SecurityPrice securityPriceMaster = kvp.Value;
                    if (securityPriceDict.TryGetValue(securityPriceMaster.Ticker, out SecurityPrice securityPrice))
                        securityPrice.RTFlag = securityPriceMaster.RTFlag;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error updating Security Price flags");
            }
            _logger.LogInformation("Populating Security Price Flags - DONE");
        }

        /// <summary>
        /// Save Fund Redemption Details (Database)
        /// Refresh Fund Redemption Details Cache
        /// Calculate Redemption Dates
        /// Refresh Fund Supplemental Details Cache
        /// </summary>
        /// <param name="fundRedemptionList"></param>
        public void SaveFundRedemptionRules(IList<FundRedemption> fundRedemptionList)
        {
            _baseDao.SaveFundRedemptions(fundRedemptionList);
            PopulateFundRedemptionDetails();
            PopulateFundSupplementalDetails();
        }

        /// <summary>
        /// Populate and Process Fund Redemption Details
        /// </summary>
        public void PopulateFundRedemptionDetails()
        {
            _logger.LogInformation("Populating Fund Redemption Details - STARTED");
            try
            {
                IDictionary<string, FundRedemption> fundRedemptionDict = _commonDao.GetFundRedemptionDetails();
                ProcessFundRedemptionDetails(fundRedemptionDict);
                _cache.Remove(CacheKeys.FUND_REDEMPTIONS);
                _cache.Add(CacheKeys.FUND_REDEMPTIONS, fundRedemptionDict, DateTimeOffset.MaxValue);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error populating Fund Redemption Details");
            }
            _logger.LogInformation("Populating Fund Redemption Details - DONE");
        }

        /// <summary>
        /// Updates ALM Funds Holdings
        /// </summary>
        public void PopulateALMHoldings()
        {
            _logger.LogInformation("Populating ALM Holdings - STARTED");
            try
            {
                //combined holdings of all ALM funds
                IDictionary<string, Holding> almHoldingsDict = _commonDao.GetHoldings();
                if (almHoldingsDict != null && almHoldingsDict.Count > 0)
                {
                    _cache.Remove(CacheKeys.ALM_HOLDINGS);
                    _cache.Add(CacheKeys.ALM_HOLDINGS, almHoldingsDict, DateTimeOffset.MaxValue);

                    //holdings by individual fund
                    IDictionary<string, Holding> almHoldingByPortDict = _commonDao.GetHoldingDetailsByPort();
                    _cache.Remove(CacheKeys.ALM_HOLDINGS_BY_PORT);
                    _cache.Add(CacheKeys.ALM_HOLDINGS_BY_PORT, almHoldingByPortDict, DateTimeOffset.MaxValue);

                    //map identifiers in position table to security master table
                    //populate positions that are not in Security Master table (these are most likely missing in Security table or do not have common security identifiers)
                    //IDictionary<string, string> almPositionSecurityMap = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);
                    IDictionary<string, SecurityIdentifier> almPositionSecurityMap = new Dictionary<string, SecurityIdentifier>(StringComparer.CurrentCultureIgnoreCase);
                    foreach (KeyValuePair<string, Holding> kvp in almHoldingsDict)
                    {
                        Holding holding = kvp.Value;
                        if (!almPositionSecurityMap.TryGetValue(holding.HoldingTicker, out SecurityIdentifier securityIdentifier))
                        {
                            securityIdentifier = new SecurityIdentifier
                            {
                                PositionId = holding.HoldingTicker,
                                Ticker = holding.SecurityTicker
                            };
                            almPositionSecurityMap.Add(holding.HoldingTicker, securityIdentifier);
                        }
                    }
                    _cache.Remove(CacheKeys.POSITION_SECURITY_MAP);
                    _cache.Add(CacheKeys.POSITION_SECURITY_MAP, almPositionSecurityMap, DateTimeOffset.MaxValue);
                }
                else
                {
                    _logger.LogInformation("No holdings found");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error populating ALM Holdings");
            }
            _logger.LogInformation("Populating ALM Holdings - DONE");
        }

        /// <summary>
        /// Updates ALM fund holdings
        /// This is done to capture any changes to fund holdings update after running PH loader
        /// Most likely a result of updated file sent by broker or changes to security mapping (broker ticker to ALM ticker)
        /// </summary>
        public void UpdateALMHoldings()
        {
            try
            {
                PopulateALMHoldings();
                PopulateFundHoldingsSummary();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error updating ALM holdings");
            }
        }

        /// <summary>
        /// Save Global Overrides
        /// </summary>
        /// <param name="overrides"></param>
        public void SaveGlobalOverrides(IList<GlobalDataOverride> overrides)
        {
            try
            {
                _dataOverrideDao.SaveGlobalOverrides(overrides);

                _logger.LogInformation("Refreshing Global Overrides...");
                IDictionary<string, GlobalDataOverride> globalDataOverridesDict = _dataOverrideDao.GetGlobalDataOverrides();
                _cache.Remove(CacheKeys.GLOBAL_OVERRIDES);
                _cache.Add(CacheKeys.GLOBAL_OVERRIDES, globalDataOverridesDict, DateTimeOffset.MaxValue);
                _logger.LogInformation("Refreshed Global Overrides");
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error refreshing Global Overrides");
            }
        }


        /// <summary>
        /// Populating Position Master, MVs, Currency Exposures and Risk Factors
        /// </summary>
        private void PopulateFundHoldingsSummary()
        {
            PopulateFundHoldings();
            PopulateEODFundHoldings();
            //PopulateRiskFactors();
        }

        /// <summary>
        /// Calculate MVs of ALM funds
        /// Populate broker price if security price is missing
        /// 
        /// </summary>
        private void PopulateFundHoldings()
        {
            double totalFundMarketValue = 0.0;
            double totalOppFundMarketValue = 0.0;
            double totalTacFundMarketValue = 0.0;

            _logger.LogInformation("Populating ALM Fund MVs, Broker Prices and Currency Exposures - STARTED");

            IDictionary<string, Holding> almHoldingsByPortDict = _cache.Get<IDictionary<string, Holding>>(CacheKeys.ALM_HOLDINGS_BY_PORT);

            //calculate MVs of ALM funds
            try
            {
                IDictionary<string, double> almHoldingsMarketValuesDict = _commonDao.GetFundMarketValues();
                _cache.Remove(CacheKeys.ALM_FUND_MARKET_VALUES);
                _cache.Add(CacheKeys.ALM_FUND_MARKET_VALUES, almHoldingsMarketValuesDict, DateTimeOffset.MaxValue);

                totalFundMarketValue = almHoldingsMarketValuesDict["All"];
                totalOppFundMarketValue = almHoldingsMarketValuesDict["OPP"];
                totalTacFundMarketValue = almHoldingsMarketValuesDict["TAC"];
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error calculating MVs of ALM funds");
            }

            //populate position master (holdings summary)
            //populate broker prices if security is not already getting priced
            try
            {
                IDictionary<string, SecurityPrice> securityPriceDict = _cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);
                IDictionary<string, PositionMaster> almPositionMasterDict = new Dictionary<string, PositionMaster>(StringComparer.CurrentCultureIgnoreCase);
                IDictionary<string, string> priceTickerMap = _cache.Get<IDictionary<string, string>>(CacheKeys.PRICE_TICKER_MAP);

                foreach (KeyValuePair<string, Holding> kvp in almHoldingsByPortDict)
                {
                    Holding holding = kvp.Value;
                    string ticker = holding.HoldingTicker;

                    try
                    {
                        //if (ticker.Equals("AKP US", StringComparison.CurrentCultureIgnoreCase))
                        //    _logger.LogDebug("Processing ticker: " + ticker);

                        PositionMaster positionMaster = null;
                        if (!almPositionMasterDict.TryGetValue(holding.HoldingTicker, out positionMaster))
                        {
                            positionMaster = new PositionMaster
                            {
                                Ticker = holding.HoldingTicker,
                                SecTicker = holding.SecurityTicker,
                                FIGI = holding.FIGI,
                                Curr = holding.Currency,
                                ShOut = holding.SharesOutstanding,
                                Avg20DayVol = holding.Average20DayVolume,
                                FxRate = holding.FX,
                                Price = holding.Price
                            };

                            almPositionMasterDict.Add(holding.HoldingTicker, positionMaster);
                        }

                        if (positionMaster != null)
                        {
                            //add to ALL
                            PositionMasterOperations.AddPosition(positionMaster.FundAll, holding, totalFundMarketValue);

                            //add to OPP Fund
                            if ("Y".Equals(holding.InOpportunityFund, StringComparison.CurrentCultureIgnoreCase))
                                PositionMasterOperations.AddPosition(positionMaster.FundOpp, holding, totalOppFundMarketValue);

                            //add to TAC Fund
                            if ("Y".Equals(holding.InTacticalFund, StringComparison.CurrentCultureIgnoreCase))
                                PositionMasterOperations.AddPosition(positionMaster.FundTac, holding, totalTacFundMarketValue);
                        }

                        if (securityPriceDict != null)
                        {
                            SecurityPrice securityPrice = SecurityPriceLookupOperations.GetSecurityPrice(ticker, priceTickerMap, securityPriceDict);
                            if (securityPrice == null)
                            {
                                securityPrice = new SecurityPrice
                                {
                                    Ticker = ticker,
                                    LastPrc = holding.Price
                                };

                                string source = string.Empty;
                                if (holding.Portfolio.IndexOf("Jeff", StringComparison.CurrentCultureIgnoreCase) >= 0)
                                    source = "Jeff";
                                else if (holding.Portfolio.IndexOf("JPM", StringComparison.CurrentCultureIgnoreCase) >= 0)
                                    source = "JPM";
                                else if (holding.Portfolio.IndexOf("IB", StringComparison.CurrentCultureIgnoreCase) >= 0)
                                    source = "IB";
                                else if (holding.Portfolio.IndexOf("Fido", StringComparison.CurrentCultureIgnoreCase) >= 0)
                                    source = "Fido";
                                securityPrice.Src = source;
                                securityPrice.RTFlag = 1;
                                securityPrice.MktCls = 0; //0 - Open, 1 - Closed

                                _logger.LogInformation("Added security to Security Price object: " + ticker);

                                if (!securityPriceDict.ContainsKey(ticker))
                                    securityPriceDict.Add(ticker, securityPrice);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error processing Position for ticker: " + ticker);
                    }
                }

                _cache.Remove(CacheKeys.POSITION_MASTER);
                _cache.Add(CacheKeys.POSITION_MASTER, almPositionMasterDict, DateTimeOffset.MaxValue);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error populating ALM Positions in Position Master");
            }
            _logger.LogInformation("Populating ALM Fund MVs, Broker Prices and Currency Exposures - DONE");
        }

        /// <summary>
        /// Calculate MVs of ALM funds
        /// Populate broker price if security price is missing
        /// 
        /// </summary>
        public IDictionary<string, PositionMaster> GetPositions()
        {
            IDictionary<string, PositionMaster> almPositionMasterDict = new Dictionary<string, PositionMaster>(StringComparer.CurrentCultureIgnoreCase);

            double totalFundMarketValue = 0.0;
            double totalOppFundMarketValue = 0.0;
            double totalTacFundMarketValue = 0.0;

            //calculate MVs of ALM funds
            try
            {
                IDictionary<string, double> almHoldingsMarketValuesDict = _commonDao.GetFundMarketValues();
                totalFundMarketValue = almHoldingsMarketValuesDict["All"];
                totalOppFundMarketValue = almHoldingsMarketValuesDict["OPP"];
                totalTacFundMarketValue = almHoldingsMarketValuesDict["TAC"];
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error calculating MVs of ALM funds");
            }

            //populate position master (holdings summary)
            try
            {
                IDictionary<string, Holding> almHoldingsByPortDict = _cache.Get<IDictionary<string, Holding>>(CacheKeys.ALM_HOLDINGS_BY_PORT);
                foreach (KeyValuePair<string, Holding> kvp in almHoldingsByPortDict)
                {
                    Holding holding = kvp.Value;
                    string ticker = holding.HoldingTicker;

                    try
                    {
                        //if (ticker.Equals("AKP US", StringComparison.CurrentCultureIgnoreCase))
                        //    _logger.LogDebug("Processing ticker: " + ticker);

                        PositionMaster positionMaster = null;
                        if (!almPositionMasterDict.TryGetValue(holding.HoldingTicker, out positionMaster))
                        {
                            positionMaster = new PositionMaster
                            {
                                Ticker = holding.HoldingTicker,
                                SecTicker = holding.SecurityTicker,
                                FIGI = holding.FIGI,
                                Curr = holding.Currency,
                                ShOut = holding.SharesOutstanding,
                                Avg20DayVol = holding.Average20DayVolume,
                                FxRate = holding.FX,
                                Price = holding.Price
                            };

                            almPositionMasterDict.Add(holding.HoldingTicker, positionMaster);
                        }

                        if (positionMaster != null)
                        {
                            //add to ALL
                            PositionMasterOperations.AddPosition(positionMaster.FundAll, holding, totalFundMarketValue);

                            //add to OPP Fund
                            if ("Y".Equals(holding.InOpportunityFund, StringComparison.CurrentCultureIgnoreCase))
                                PositionMasterOperations.AddPosition(positionMaster.FundOpp, holding, totalOppFundMarketValue);

                            //add to TAC Fund
                            if ("Y".Equals(holding.InTacticalFund, StringComparison.CurrentCultureIgnoreCase))
                                PositionMasterOperations.AddPosition(positionMaster.FundTac, holding, totalTacFundMarketValue);
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error processing Position for ticker: " + ticker);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error populating ALM Positions in Position Master");
            }
            return almPositionMasterDict;
        }

        /// <summary>
        /// Populates previous day (EOD) holdings
        /// </summary>
        private void PopulateEODFundHoldings()
        {
            _logger.LogInformation("Populating EOD Fund Holdings - STARTED");
            try
            {
                IDictionary<string, PositionMaster> almPositionMasterDict = _cache.Get<IDictionary<string, PositionMaster>>(CacheKeys.POSITION_MASTER);

                IDictionary<string, Holding> almEODHoldingsByPortDict = _commonDao.GetEODHoldingDetailsByPort();
                foreach (KeyValuePair<string, Holding> kvp in almEODHoldingsByPortDict)
                {
                    Holding holding = kvp.Value;
                    string ticker = holding.HoldingTicker;

                    try
                    {
                        PositionMaster positionMaster = null;
                        if (!almPositionMasterDict.TryGetValue(holding.HoldingTicker, out positionMaster))
                        {
                            positionMaster = new PositionMaster
                            {
                                Ticker = holding.HoldingTicker
                            };

                            almPositionMasterDict.Add(holding.HoldingTicker, positionMaster);
                        }

                        if (positionMaster != null)
                        {
                            //add to ALL
                            PositionMasterOperations.AddEODPosition(positionMaster.FundEODAll, holding);

                            //add to OPP Fund
                            if ("Y".Equals(holding.InOpportunityFund, StringComparison.CurrentCultureIgnoreCase))
                                PositionMasterOperations.AddEODPosition(positionMaster.FundEODOpp, holding);

                            //add to TAC Fund
                            if ("Y".Equals(holding.InTacticalFund, StringComparison.CurrentCultureIgnoreCase))
                                PositionMasterOperations.AddEODPosition(positionMaster.FundEODTac, holding);
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error processing Position for ticker: " + ticker);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error populating EOD Positions in Position Master");
            }
            _logger.LogInformation("Populating EOD Fund Holdings - DONE");
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fundAlphaModelParams"></param>
        /// <returns></returns>
        public FundAlphaModelParamsList PopulateExpectedAlphaParams(FundAlphaModelParams fundAlphaModelParams)
        {
            try
            {
                FundAlphaModelParamsList eaParams = new FundAlphaModelParamsList
                {
                    Ticker = fundAlphaModelParams.Ticker,
                    Model = fundAlphaModelParams.Model,
                    ModelLevel = fundAlphaModelParams.ModelLevel
                };

                foreach (KeyValuePair<string, FundAlphaModelCoefficients> ckvp in fundAlphaModelParams.Coefficients)
                {
                    FundAlphaModelCoefficients coefficients = ckvp.Value;
                    if (coefficients.Beta.HasValue)
                    {
                        if (coefficients.Regressor.Equals("disct", StringComparison.CurrentCultureIgnoreCase))
                            eaParams.DiscountRegressor = coefficients;
                        else if (coefficients.Regressor.Equals("zs24m", StringComparison.CurrentCultureIgnoreCase))
                            eaParams.ZScore24MRegressor = coefficients;
                        else if (coefficients.Regressor.Equals("zs36m", StringComparison.CurrentCultureIgnoreCase))
                            eaParams.ZScore36MRegressor = coefficients;
                    }
                }

                return eaParams;
            }
            catch (Exception)
            {
                return null;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="list"></param>
        public void SaveSecurityRiskFactors(IList<SecurityRiskFactor> list)
        {
            try
            {
                _securityRiskDao.SaveSecurityRiskFactors(list);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving Security Risk Factors");
            }
        }

        /// <summary>
        /// Updates data from broker files (JPM, Fidelity, Jefferies and IB)
        /// 
        /// Fund Details
        /// Fund Cash Details
        /// Fund Margin Attribution Details
        /// Fund Finance Details
        /// Fund Interest Earnings Details
        /// Fund Currency Details
        /// Fund Swap Position Details
        /// Fund Position Details
        /// Security Borrow Rates
        /// Security Margin Rates
        /// </summary>
        public void PopulateBrokerReportDetails()
        {
            _logger.LogInformation("Updating Fund Cash, Margin and Security Borrow Rates (Broker Reports) Details - STARTED");
            PopulateFundDetails();
            PopulateFundCashDetails();
            PopulateSecurityMarginRates();
            PopulateFundMarginAttributionDetails();
            PopulateFundFinanceDetails();
            PopulateFundInterestEarningsDetails();
            PopulateFundCurrencyDetails();
            PopulateFundSwapDetails();
            PopulateFundPositionDetails();
            PopulateSecurityRebateRates();
            PopulateSecurityActualRebateRates();
            _logger.LogInformation("Updating Fund Cash, Margin and Security Borrow Rates (Broker Reports) Details - DONE");
        }

        /// <summary>
        /// Gets fund's cash, long and short MV, nav and margin (by fund)
        /// call almitasc_ACTradingBBGData.spGetFundDetails('JPM');
        /// </summary>
        public void PopulateFundDetails()
        {
            _logger.LogInformation("Populating Fund Details - STARTED");
            IList<FundDetail> fidelityFundDetails = _fundCashDao.GetFidelityFundDetails("Fidelity");
            _cache.Remove(CacheKeys.FIDELITY_FUND_DETAILS);
            _cache.Add(CacheKeys.FIDELITY_FUND_DETAILS, fidelityFundDetails, DateTimeOffset.MaxValue);

            IList<FundDetail> jpmFundDetails = _fundCashDao.GetJPMFundDetails("JPMNew");
            _cache.Remove(CacheKeys.JPM_FUND_DETAILS);
            _cache.Add(CacheKeys.JPM_FUND_DETAILS, jpmFundDetails, DateTimeOffset.MaxValue);

            IList<FundDetail> ibFundDetails = _fundCashDao.GetIBFundDetails("IB");
            _cache.Remove(CacheKeys.IB_FUND_DETAILS);
            _cache.Add(CacheKeys.IB_FUND_DETAILS, ibFundDetails, DateTimeOffset.MaxValue);

            IList<FundDetail> jefferiesFundDetails = _fundCashDao.GetJefferiesFundDetails("Jefferies");
            _cache.Remove(CacheKeys.JEFFERIES_FUND_DETAILS);
            _cache.Add(CacheKeys.JEFFERIES_FUND_DETAILS, jefferiesFundDetails, DateTimeOffset.MaxValue);

            IList<FundDetail> edfFundDetails = _fundCashDao.GetEDFFundDetails("EDF");
            _cache.Remove(CacheKeys.EDF_FUND_DETAILS);
            _cache.Add(CacheKeys.EDF_FUND_DETAILS, edfFundDetails, DateTimeOffset.MaxValue);

            _logger.LogInformation("Populating Fund Details - DONE");
        }

        /// <summary>
        /// Gets fund's trade and settle data cash position (by fund and currency)
        /// call almitasc_ACTradingBBGData.spGetFundCashDetails('JPM');
        /// </summary>
        public void PopulateFundCashDetails()
        {
            _logger.LogInformation("Populating Fund Cash Details - STARTED");
            IList<FundCashDetail> fidelityFundCashDetails = _fundCashDao.GetFidelityFundCashDetails("Fidelity");
            _cache.Remove(CacheKeys.FIDELITY_MARGIN_DETAILS);
            _cache.Add(CacheKeys.FIDELITY_MARGIN_DETAILS, fidelityFundCashDetails, DateTimeOffset.MaxValue);

            IList<FundCashDetail> jpmFundCashDetails = _fundCashDao.GetJPMFundCashDetails("JPM");
            _cache.Remove(CacheKeys.JPM_MARGIN_DETAILS);
            _cache.Add(CacheKeys.JPM_MARGIN_DETAILS, jpmFundCashDetails, DateTimeOffset.MaxValue);

            IList<FundCashDetail> ibFundCashDetails = _fundCashDao.GetIBFundCashDetails("IB");
            _cache.Remove(CacheKeys.IB_MARGIN_DETAILS);
            _cache.Add(CacheKeys.IB_MARGIN_DETAILS, ibFundCashDetails, DateTimeOffset.MaxValue);

            IList<FundCashDetail> jefferiesFundCashDetails = _fundCashDao.GetJefferiesFundCashDetails("Jefferies");
            _cache.Remove(CacheKeys.JEFFERIES_MARGIN_DETAILS);
            _cache.Add(CacheKeys.JEFFERIES_MARGIN_DETAILS, jefferiesFundCashDetails, DateTimeOffset.MaxValue);
            _logger.LogInformation("Populating Fund Cash Details - DONE");
        }

        /// <summary>
        /// Gets funds swap position details (for JPM)
        /// By Fund, Fund and Ticker and Fund, Ticker and Side
        /// 
        /// call almitasc_ACTradingBBGData.spGetFundSwapDetails('JPM');
        /// call almitasc_ACTradingBBGData.spGetFundSwapDetails('JPM_BY_FUND');
        /// call almitasc_ACTradingBBGData.spGetFundSwapDetails('JPM_BY_FUND_BY_SIDE');
        /// </summary>
        public void PopulateFundSwapDetails()
        {
            _logger.LogInformation("Populating Fund Swap Details - STARTED");
            IList<FundSwapDetail> jpmFundSwapDetails = _fundCashDao.GetJPMFundSwapDetails("JPM");
            _cache.Remove(CacheKeys.JPM_SWAP_DETAILS);
            _cache.Add(CacheKeys.JPM_SWAP_DETAILS, jpmFundSwapDetails, DateTimeOffset.MaxValue);

            IList<FundSwapDetail> jpmFundTotalSwapDetails = _fundCashDao.GetJPMFundSwapDetails();
            _cache.Remove(CacheKeys.JPM_SWAP_DETAILS_BY_FUND);
            _cache.Add(CacheKeys.JPM_SWAP_DETAILS_BY_FUND, jpmFundTotalSwapDetails, DateTimeOffset.MaxValue);

            IList<FundSwapDetail> jpmFundTotalSwapDetailsByType = _fundCashDao.GetJPMFundSwapDetailsByType();
            _cache.Remove(CacheKeys.JPM_SWAP_DETAILS_BY_FUND_BY_TYPE);
            _cache.Add(CacheKeys.JPM_SWAP_DETAILS_BY_FUND_BY_TYPE, jpmFundTotalSwapDetailsByType, DateTimeOffset.MaxValue);
            _logger.LogInformation("Populating Fund Swap Details - DONE");
        }

        /// <summary>
        /// Gets security margin rates
        /// call almitasc_ACTradingBBGData.spGetSecurityMarginRates('JPM');
        /// </summary>
        public void PopulateSecurityMarginRates()
        {
            _logger.LogInformation("Populating Security Margin Rates - STARTED");
            IDictionary<string, SecurityMarginDetail> jpmSecurityMarginRates = _fundCashDao.GetJPMSecurityMarginDetails();
            _cache.Remove(CacheKeys.JPM_SECURITY_MARGIN_RATES);
            _cache.Add(CacheKeys.JPM_SECURITY_MARGIN_RATES, jpmSecurityMarginRates, DateTimeOffset.MaxValue);

            _logger.LogInformation("Populating Security Margin Rates - STARTED");
            IDictionary<string, JPMSecurityMarginDetail> latestJpmSecurityMarginDetails = _fundCashDao.GetLatestJPMSecurityMarginDetails();
            _cache.Remove(CacheKeys.LATEST_JPM_SECURITY_MARGIN_DETAILS);
            _cache.Add(CacheKeys.LATEST_JPM_SECURITY_MARGIN_DETAILS, latestJpmSecurityMarginDetails, DateTimeOffset.MaxValue);

            IDictionary<string, SecurityMargin> fidelitySecurityMarginRates = _fundCashDao.GetFidelitySecurityMarginRates("Fidelity");
            _cache.Remove(CacheKeys.FIDELITY_SECURITY_MARGIN_RATES);
            _cache.Add(CacheKeys.FIDELITY_SECURITY_MARGIN_RATES, fidelitySecurityMarginRates, DateTimeOffset.MaxValue);
            _logger.LogInformation("Populating Security Margin Rates - DONE");
        }

        /// <summary>
        /// Gets fund currency details
        /// call almitasc_ACTradingBBGData.spGetFundCurrencyDetails('JPM');
        /// </summary>
        public void PopulateFundCurrencyDetails()
        {
            _logger.LogInformation("Populating Fund Currency Details - STARTED");
            IList<FundCurrencyDetail> jpmCurrencyDetails = _fundCashDao.GetJPMCurrencyDetails("JPM");
            _cache.Remove(CacheKeys.JPM_CURRENCY_DETAILS);
            _cache.Add(CacheKeys.JPM_CURRENCY_DETAILS, jpmCurrencyDetails, DateTimeOffset.MaxValue);
            _logger.LogInformation("Populating Fund Currency Details - DONE");
        }

        /// <summary>
        /// Gets fund margin attribution details
        /// call almitasc_ACTradingBBGData.spGetFundMarginAttributionDetail('Fidelity');
        /// </summary>
        public void PopulateFundMarginAttributionDetails()
        {
            _logger.LogInformation("Populating Fund Margin Attribution Details - STARTED");
            IList<FundMarginAttributionDetail> list = _fundCashDao.GetFidelityMarginAttributionDetails("Fidelity");
            _cache.Remove(CacheKeys.FIDELITY_MARGIN_ATTRIBUTION_DETAILS);
            _cache.Add(CacheKeys.FIDELITY_MARGIN_ATTRIBUTION_DETAILS, list, DateTimeOffset.MaxValue);
            _logger.LogInformation("Populating Fund Margin Attribution Details - DONE");
        }

        /// <summary>
        /// Gets funds finance details
        /// call almitasc_ACTradingBBGData.spGetFundFinanceDetails('JPM');
        /// </summary>
        public void PopulateFundFinanceDetails()
        {
            _logger.LogInformation("Populating Fund Finance Details - STARTED");
            IList<FundFinanceDetail> list = _fundCashDao.GetJPMFundFinanceDetails("JPM");
            _cache.Remove(CacheKeys.JPM_FINANCE_DETAILS);
            _cache.Add(CacheKeys.JPM_FINANCE_DETAILS, list, DateTimeOffset.MaxValue);
            _logger.LogInformation("Populating Fund Finance Details - DONE");
        }

        /// <summary>
        /// Gets fund interest earnings details
        /// call almitasc_ACTradingBBGData.spGetFundInterestEarningDetails('Fidelity');
        /// </summary>
        public void PopulateFundInterestEarningsDetails()
        {
            _logger.LogInformation("Populating Fund Interest Earnings Details - STARTED");
            IList<FundInterestEarningDetail> list = _fundCashDao.GetFidelityFundInterestEarningsDetails("Fidelity");
            _cache.Remove(CacheKeys.FIDELITY_INTEREST_EARNING_DETAILS);
            _cache.Add(CacheKeys.FIDELITY_INTEREST_EARNING_DETAILS, list, DateTimeOffset.MaxValue);
            _logger.LogInformation("Populating Fund Interest Earnings Details - DONE");
        }

        /// <summary>
        /// Gets fund position details
        /// call almitasc_ACTradingBBGData.spGetFundBrokerPositionDetails('IB');
        /// </summary>
        public void PopulateFundPositionDetails()
        {
            _logger.LogInformation("Populating Fund Position Details - STARTED");
            IList<FundPositionDetail> list = _fundCashDao.GetIBPositionDetails("IB");
            _cache.Remove(CacheKeys.IB_POSITION_DETAILS);
            _cache.Add(CacheKeys.IB_POSITION_DETAILS, list, DateTimeOffset.MaxValue);
            _logger.LogInformation("Populating Fund Position Details - DONE");
        }

        /// <summary>
        /// Gets security short borrow rates
        /// call almitasc_ACTradingBBGData.spGetSecurityShortBorrowRates();
        /// </summary>
        public void PopulateSecurityRebateRates()
        {
            _logger.LogInformation("Populating Security Rebate Rates - STARTED");
            IDictionary<string, SecurityRebateRate> dict = _fundCashDao.GetSecurityRebateRates();
            _cache.Remove(CacheKeys.SECURITY_REBATE_RATES);
            _cache.Add(CacheKeys.SECURITY_REBATE_RATES, dict, DateTimeOffset.MaxValue);
            _logger.LogInformation("Populating Security Rebate Rates - DONE");
        }

        /// <summary>
        /// Gets security actual rebate rates paid by ALM funds
        /// call almitasc_ACTradingBBGData.spGetSecurityRebateRates();
        /// </summary>
        public void PopulateSecurityActualRebateRates()
        {
            _logger.LogInformation("Populating Security Actual Rebate Rates - STARTED");
            IList<SecurityActualRebateRate> list = _fundCashDao.GetSecurityActualRebateRates();
            _cache.Remove(CacheKeys.SECURITY_ACTUAL_REBATE_RATES);
            _cache.Add(CacheKeys.SECURITY_ACTUAL_REBATE_RATES, list, DateTimeOffset.MaxValue);
            _logger.LogInformation("Populating Security Actual Borrow Rates - DONE");
        }

        /// <summary>
        /// Populates Fund Rights Offer Details
        /// </summary>
        public void PopulateFundRightsOfferDetails()
        {
            _logger.LogInformation("Populating Fund Rights Offer Details - STARTED");
            IDictionary<string, FundRightsOffer> dict = _fundSupplementalDataDao.GetFundRightsOffers();
            _cache.Remove(CacheKeys.FUND_RIGHTS_OFFERS);
            _cache.Add(CacheKeys.FUND_RIGHTS_OFFERS, dict, DateTimeOffset.MaxValue);

            _logger.LogInformation("Resetting Fund Rights Offer Details...");
            _fundForecastEngine.ResetRightsOfferDetails();
            _logger.LogInformation("Populating Fund Rights Offer Details - DONE");
        }

        /// <summary>
        /// Populates Fund Tender Offer Details
        /// </summary>
        public void PopulateFundTenderOfferDetails()
        {
            _logger.LogInformation("Populating Fund Tender Offer Details - STARTED");
            IDictionary<string, FundTenderOffer> dict = _fundSupplementalDataDao.GetFundTenderOffers();
            _cache.Remove(CacheKeys.FUND_TENDER_OFFERS);
            _cache.Add(CacheKeys.FUND_TENDER_OFFERS, dict, DateTimeOffset.MaxValue);

            _logger.LogInformation("Resetting Fund Tender Offer Details...");
            _fundForecastEngine.ResetTenderOfferDetails();
            _logger.LogInformation("Populating Fund Tender Offer Details - DONE");
        }

        /// <summary>
        /// Populate Port Summary (pre-calculated by Frank)
        /// Summary Asset, Geo and Currrency Exposures
        /// </summary>
        public void PopulateFundPortSummary()
        {
            IDictionary<string, FundHoldingSummary> dict = _holdingsDao.GetFundHoldingSummary();
            _cache.Remove(CacheKeys.FUND_HOLDING_SUMMARY);
            _cache.Add(CacheKeys.FUND_HOLDING_SUMMARY, dict, DateTimeOffset.MaxValue);
        }

        /// <summary>
        /// Populates Security details of ALM positions
        /// </summary>
        public void PopulatePositionSecurityDetails()
        {
            IDictionary<string, SecurityMaster> dict = _baseDao.GetPositionSecurityDetails();
            _cache.Remove(CacheKeys.POSITION_SECURITY_MASTER_DETAILS);
            _cache.Add(CacheKeys.POSITION_SECURITY_MASTER_DETAILS, dict, DateTimeOffset.MaxValue);
        }

        /// <summary>
        /// Saves Fund Rights Offer Details
        /// </summary>
        /// <param name="list"></param>
        public void SaveFundRightsOfferDetails(IList<FundRightsOffer> list)
        {
            try
            {
                _fundSupplementalDataDao.SaveFundRightsOffers(list);
                PopulateFundRightsOfferDetails();
                PopulateFundSupplementalDetails();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving Fund Rights Offers");
            }
        }

        /// <summary>
        /// Populate Fund Supplemental Data
        ///     Populate Fund Redemptions
        ///     Populate Fund Rights Offer details
        ///     Populate Fund Tender Offer details
        ///     Populate Fund Port Dates
        ///     Popluate Security Rebate Rates
        ///     Populate Security Actual Rebate Rates
        ///     Populate Fund Nav Update Frequency Details
        /// </summary>
        /// <returns></returns>
        public void PopulateFundSupplementalDetails()
        {
            _logger.LogInformation("Updating Fund Supplemental Details - STARTED");
            string ticker = string.Empty;
            IDictionary<string, FundSupplementalData> fundSupplementalDataDict = new Dictionary<string, FundSupplementalData>(StringComparer.CurrentCultureIgnoreCase);

            //Populate Fund Redemptions
            try
            {
                IDictionary<string, FundRedemption> fundRedemptionsDict = _cache.Get<IDictionary<string, FundRedemption>>(CacheKeys.FUND_REDEMPTIONS);
                foreach (KeyValuePair<string, FundRedemption> kvp in fundRedemptionsDict)
                {
                    ticker = kvp.Key;
                    if (!fundSupplementalDataDict.TryGetValue(ticker, out FundSupplementalData fundSupplementalData))
                    {
                        fundSupplementalData = new FundSupplementalData
                        {
                            Ticker = ticker,
                            FundRedemption = kvp.Value
                        };
                        fundSupplementalDataDict.Add(ticker, fundSupplementalData);
                    }
                    else
                    {
                        fundSupplementalData.FundRedemption = kvp.Value;
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error populating Fund Redemption Details for ticker: " + ticker);
            }

            //Populate Fund Rights Offers
            try
            {
                IDictionary<string, FundRightsOffer> fundRightsOffersDict = _cache.Get<IDictionary<string, FundRightsOffer>>(CacheKeys.FUND_RIGHTS_OFFERS);
                foreach (KeyValuePair<string, FundRightsOffer> kvp in fundRightsOffersDict)
                {
                    ticker = kvp.Key;
                    if (!fundSupplementalDataDict.TryGetValue(ticker, out FundSupplementalData fundSupplementalData))
                    {
                        fundSupplementalData = new FundSupplementalData
                        {
                            Ticker = ticker,
                            FundRightsOffer = kvp.Value
                        };
                        fundSupplementalDataDict.Add(ticker, fundSupplementalData);
                    }
                    else
                    {
                        fundSupplementalData.FundRightsOffer = kvp.Value;
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error getting Fund Right Offer details for ticker: " + ticker);
            }

            //Populate Fund Tender Offers
            try
            {
                IDictionary<string, FundTenderOffer> fundTenderOfferDict = _cache.Get<IDictionary<string, FundTenderOffer>>(CacheKeys.FUND_TENDER_OFFERS);
                foreach (KeyValuePair<string, FundTenderOffer> kvp in fundTenderOfferDict)
                {
                    ticker = kvp.Key;
                    if (!fundSupplementalDataDict.TryGetValue(ticker, out FundSupplementalData fundSupplementalData))
                    {
                        fundSupplementalData = new FundSupplementalData
                        {
                            Ticker = ticker,
                            FundTenderOffer = kvp.Value
                        };
                        fundSupplementalDataDict.Add(ticker, fundSupplementalData);
                    }
                    else
                    {
                        fundSupplementalData.FundTenderOffer = kvp.Value;
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error getting Fund Tender Offer details for ticker: " + ticker);
            }

            //Populate Fund Port Dates
            try
            {
                IDictionary<string, FundPortDate> fundPortDateDict = _cache.Get<IDictionary<string, FundPortDate>>(CacheKeys.FUND_PORT_DATES);
                foreach (KeyValuePair<string, FundPortDate> kvp in fundPortDateDict)
                {
                    ticker = kvp.Key;
                    if (!fundSupplementalDataDict.TryGetValue(ticker, out FundSupplementalData fundSupplementalData))
                    {
                        fundSupplementalData = new FundSupplementalData
                        {
                            Ticker = ticker,
                            FundPortDate = kvp.Value
                        };
                        fundSupplementalDataDict.Add(ticker, fundSupplementalData);
                    }
                    else
                    {
                        fundSupplementalData.FundPortDate = kvp.Value;
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error getting Fund Port Dates for ticker: " + ticker);
            }

            //Security Rebate Rates
            try
            {
                IDictionary<string, SecurityRebateRate> securityShortBorrowRateDict = _cache.Get<IDictionary<string, SecurityRebateRate>>(CacheKeys.SECURITY_REBATE_RATES);
                foreach (KeyValuePair<string, SecurityRebateRate> kvp in securityShortBorrowRateDict)
                {
                    ticker = kvp.Value.Ticker;
                    if (!fundSupplementalDataDict.TryGetValue(ticker, out FundSupplementalData fundSupplementalData))
                    {
                        fundSupplementalData = new FundSupplementalData
                        {
                            Ticker = ticker,
                            SecurityRebateRate = kvp.Value
                        };
                        fundSupplementalDataDict.Add(ticker, fundSupplementalData);
                    }
                    else
                    {
                        fundSupplementalData.SecurityRebateRate = kvp.Value;
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error getting Security Rebate Rate(s) for ticker: " + ticker);
            }

            //Security Actual Rebate Rates
            try
            {
                IList<SecurityActualRebateRate> securityActualRebateRateList = _cache.Get<IList<SecurityActualRebateRate>>(CacheKeys.SECURITY_ACTUAL_REBATE_RATES);
                foreach (SecurityActualRebateRate data in securityActualRebateRateList)
                {
                    ticker = data.Ticker;
                    string source = data.Source;

                    if (!fundSupplementalDataDict.TryGetValue(ticker, out FundSupplementalData fundSupplementalData))
                    {
                        fundSupplementalData = new FundSupplementalData();
                        fundSupplementalData.Ticker = ticker;
                        if ("JPM".Equals(source, StringComparison.CurrentCultureIgnoreCase))
                            fundSupplementalData.SecurityActualRateJPM = data;
                        if ("Fidelity".Equals(source, StringComparison.CurrentCultureIgnoreCase))
                            fundSupplementalData.SecurityActualRateFidelity = data;

                        fundSupplementalDataDict.Add(ticker, fundSupplementalData);
                    }
                    else
                    {
                        if ("JPM".Equals(source, StringComparison.CurrentCultureIgnoreCase))
                            fundSupplementalData.SecurityActualRateJPM = data;
                        if ("Fidelity".Equals(source, StringComparison.CurrentCultureIgnoreCase))
                            fundSupplementalData.SecurityActualRateFidelity = data;
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error getting Existing Security Rebate Rate(s) for ticker: " + ticker);
            }

            //Populate Fund Nav Update Frequency Details
            try
            {
                IDictionary<string, FundNavUpdate> fundNavUpdateFreqDict = _cache.Get<IDictionary<string, FundNavUpdate>>(CacheKeys.FUND_NAV_UPDATE_FREQUENCY);
                foreach (KeyValuePair<string, FundNavUpdate> kvp in fundNavUpdateFreqDict)
                {
                    ticker = kvp.Key;
                    if (!fundSupplementalDataDict.TryGetValue(ticker, out FundSupplementalData fundSupplementalData))
                    {
                        fundSupplementalData = new FundSupplementalData
                        {
                            Ticker = ticker,
                            FundNavUpdate = kvp.Value
                        };
                        fundSupplementalDataDict.Add(ticker, fundSupplementalData);
                    }
                    else
                    {
                        fundSupplementalData.FundNavUpdate = kvp.Value;
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error getting Fund Nav Update details for ticker: " + ticker);
            }

            //Populate Fund Notes
            try
            {
                IDictionary<string, FundNotes> fundNotesDict = _cache.Get<IDictionary<string, FundNotes>>(CacheKeys.FUND_NOTES);
                foreach (KeyValuePair<string, FundNotes> kvp in fundNotesDict)
                {
                    ticker = kvp.Key;
                    FundNotes fundNotes = kvp.Value;
                    if (!string.IsNullOrEmpty(fundNotes.FundComments))
                    {
                        if (!fundSupplementalDataDict.TryGetValue(ticker, out FundSupplementalData fundSupplementalData))
                        {
                            fundSupplementalData = new FundSupplementalData
                            {
                                Ticker = ticker,
                                FundComments = fundNotes.FundComments
                            };
                            fundSupplementalDataDict.Add(ticker, fundSupplementalData);
                        }
                        else
                        {
                            fundSupplementalData.FundComments = fundNotes.FundComments;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error populating Fund Comments for ticker: " + ticker);
            }

            _cache.Remove(CacheKeys.FUND_SUPPLEMENTAL_DETAILS);
            _cache.Add(CacheKeys.FUND_SUPPLEMENTAL_DETAILS, fundSupplementalDataDict, DateTimeOffset.MaxValue);

            _logger.LogInformation("Updating Fund Supplemental Details - DONE");
        }

        /// <summary>
        /// Saves Fund Tender Offers
        /// </summary>
        /// <param name="list"></param>
        public void SaveFundTenderOfferDetails(IList<FundTenderOffer> list)
        {
            try
            {
                _fundSupplementalDataDao.SaveFundTenderOffers(list);
                PopulateFundTenderOfferDetails();
                PopulateFundSupplementalDetails();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving Fund Tender Offer details");
            }
        }

        /// <summary>
        /// Populates historical discount median stats for CEF groups
        /// The median discounts and quartiles are calculated as part of JAVA process
        /// </summary>
        private void PopulateHistoricalDiscountStats()
        {
            try
            {
                IDictionary<string, FundGroupDiscountStats> fundGroupDiscountStatsDict = new Dictionary<string, FundGroupDiscountStats>(StringComparer.CurrentCultureIgnoreCase);

                IList<FundGroupDiscountStats> list = _statsDao.GetFundDiscountStats("Closed End Fund", "United States");
                foreach (FundGroupDiscountStats stats in list)
                {
                    if (!fundGroupDiscountStatsDict.TryGetValue(stats.Id, out FundGroupDiscountStats fundGroupDiscountStats))
                        fundGroupDiscountStatsDict.Add(stats.Id, stats);
                }

                _cache.Remove(CacheKeys.HIST_DISCOUNT_STATS);
                _cache.Add(CacheKeys.HIST_DISCOUNT_STATS, fundGroupDiscountStatsDict, DateTimeOffset.MaxValue);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error getting Historical Discount Stats");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        private void PopulateFundSectorHistoricalDiscountStats()
        {
            IDictionary<string, IDictionary<string, FundGroupHistStats>> fundSectorDiscountStatsDict = new Dictionary<string, IDictionary<string, FundGroupHistStats>>();
            IDictionary<string, FundGroupDiscountStats> sectorDiscountStatsDict = _cache.Get<IDictionary<string, FundGroupDiscountStats>>(CacheKeys.HIST_DISCOUNT_STATS);
            IDictionary<string, FundMaster> fundMasterDict = _cache.Get<IDictionary<string, FundMaster>>(CacheKeys.FUND_MASTER);
            IDictionary<string, FundForecast> fundForecastDict = _cache.Get<IDictionary<string, FundForecast>>(CacheKeys.FUND_FORECASTS);

            foreach (FundMaster fundMaster in fundMasterDict.Values)
            {
                string ticker = fundMaster.Ticker;
                if (!string.IsNullOrEmpty(fundMaster.Cntry) && "United States".Equals(fundMaster.Cntry, StringComparison.CurrentCultureIgnoreCase))
                {
                    if (fundForecastDict.TryGetValue(ticker, out FundForecast fundForecast))
                    {
                        AddSectorStatsToFund(ticker, "MTD", fundForecast, sectorDiscountStatsDict, fundSectorDiscountStatsDict);
                        AddSectorStatsToFund(ticker, "1M", fundForecast, sectorDiscountStatsDict, fundSectorDiscountStatsDict);
                        AddSectorStatsToFund(ticker, "3M", fundForecast, sectorDiscountStatsDict, fundSectorDiscountStatsDict);
                        AddSectorStatsToFund(ticker, "6M", fundForecast, sectorDiscountStatsDict, fundSectorDiscountStatsDict);
                        AddSectorStatsToFund(ticker, "1Yr", fundForecast, sectorDiscountStatsDict, fundSectorDiscountStatsDict);
                        AddSectorStatsToFund(ticker, "2Yr", fundForecast, sectorDiscountStatsDict, fundSectorDiscountStatsDict);
                        AddSectorStatsToFund(ticker, "3Yr", fundForecast, sectorDiscountStatsDict, fundSectorDiscountStatsDict);
                        AddSectorStatsToFund(ticker, "5Yr", fundForecast, sectorDiscountStatsDict, fundSectorDiscountStatsDict);
                        AddSectorStatsToFund(ticker, "10Yr", fundForecast, sectorDiscountStatsDict, fundSectorDiscountStatsDict);
                    }
                }
            }

            _cache.Remove(CacheKeys.FUND_SECTOR_DISCOUNT_STATS);
            _cache.Add(CacheKeys.FUND_SECTOR_DISCOUNT_STATS, fundSectorDiscountStatsDict, DateTimeOffset.MaxValue);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ticker"></param>
        /// <param name="period"></param>
        /// <param name="fundForecast"></param>
        /// <param name="sectorDiscountStatsDict"></param>
        /// <param name="fundSectorDiscountStatsDict"></param>
        private void AddSectorStatsToFund(string ticker, string period,
            FundForecast fundForecast,
            IDictionary<string, FundGroupDiscountStats> sectorDiscountStatsDict,
            IDictionary<string, IDictionary<string, FundGroupHistStats>> fundSectorDiscountStatsDict
            )
        {
            //string id = string.Join("|", "CEF", "US", fundForecast.FundCat, period);
            string id = string.Join("|", "CEF", "US", fundForecast.FundDscntGrp, period);
            if (sectorDiscountStatsDict.TryGetValue(id, out FundGroupDiscountStats stats))
            {
                if (!fundSectorDiscountStatsDict.TryGetValue(ticker, out IDictionary<string, FundGroupHistStats> statsDict))
                {
                    statsDict = new Dictionary<string, FundGroupHistStats>();
                    fundSectorDiscountStatsDict.Add(ticker, statsDict);
                }

                FundGroupHistStats histStats = new FundGroupHistStats
                {
                    Id = stats.Id,
                    Mean = stats.Mean / 100.0,
                    StdDev = stats.StdDev / 100.0,
                };
                statsDict.Add(period, histStats);
            }
        }

        /// <summary>
        /// Populates ALM Ticker to Source Ticker Map and vice versa
        /// </summary>
        public void PopulatePriceTickerMap()
        {
            //ALM Ticker to Source Ticker Map (Neovest/BBG)
            //Maps ALM Identifiers to Neovest/BBG Identifiers
            //Ex:- CIC CT (CIC CN Equity)
            IDictionary<string, string> almTickerToSourceTickerDict = _securityPriceDao.GetPriceTickerMap();
            _cache.Remove(CacheKeys.PRICE_TICKER_MAP);
            _cache.Add(CacheKeys.PRICE_TICKER_MAP, almTickerToSourceTickerDict, DateTimeOffset.MaxValue);

            //Creating the list object for data transfer
            IList<SecurityPriceMap> almTickerToSourceTickerList = new List<SecurityPriceMap>();
            foreach (KeyValuePair<string, string> kvp in almTickerToSourceTickerDict)
            {
                SecurityPriceMap priceMap = new SecurityPriceMap
                {
                    InputTicker = kvp.Key,
                    SourceTicker = kvp.Value
                };
                almTickerToSourceTickerList.Add(priceMap);
            }

            _cache.Remove(CacheKeys.PRICE_TICKER_LIST);
            _cache.Add(CacheKeys.PRICE_TICKER_LIST, almTickerToSourceTickerList, DateTimeOffset.MaxValue);

            //Neovest Ticker Map
            //Maps Source Ticker (without market sector) to Source Ticker
            //Ex:- ALACN V3.06 PERP A (ALACN V3.06 PERP A Equity)
            IDictionary<string, string> nvTickerToSourceTickerDict = _securityPriceDao.GetNVPriceTickerMap();
            _cache.Remove(CacheKeys.NV_PRICE_TICKER_MAP);
            _cache.Add(CacheKeys.NV_PRICE_TICKER_MAP, nvTickerToSourceTickerDict, DateTimeOffset.MaxValue);

            //Creating the list object for data transfer
            IList<SecurityPriceMap> nvTickerToSourceTickerList = new List<SecurityPriceMap>();
            foreach (KeyValuePair<string, string> kvp in nvTickerToSourceTickerDict)
            {
                SecurityPriceMap priceMap = new SecurityPriceMap
                {
                    InputTicker = kvp.Key,
                    SourceTicker = kvp.Value
                };
                nvTickerToSourceTickerList.Add(priceMap);
            }

            _cache.Remove(CacheKeys.NV_PRICE_TICKER_LIST);
            _cache.Add(CacheKeys.NV_PRICE_TICKER_LIST, nvTickerToSourceTickerList, DateTimeOffset.MaxValue);


            //Source Ticker (Neovest/BBG) to ALM Ticker Map
            IDictionary<string, string> sourceTickerToAlmTickerDict = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);
            foreach (KeyValuePair<string, string> kvp in almTickerToSourceTickerDict)
            {
                string almTicker = kvp.Key;
                string sourceTicker = kvp.Value;

                if (!sourceTickerToAlmTickerDict.ContainsKey(sourceTicker))
                    sourceTickerToAlmTickerDict.Add(sourceTicker, almTicker);
            }
            _cache.Remove(CacheKeys.SOURCE_PRICE_TICKER_MAP);
            _cache.Add(CacheKeys.SOURCE_PRICE_TICKER_MAP, sourceTickerToAlmTickerDict, DateTimeOffset.MaxValue);
        }

        ///// <summary>
        ///// Saves Fund Holdings (User)
        ///// </summary>
        ///// <param name="fundHoldings"></param>
        //public void SaveFundHoldings(IList<FundHolding> fundHoldings)
        //{
        //    _baseDao.SaveFundHoldings(fundHoldings, "Y");
        //    PopulateFundPortDates();
        //    PopulateFundSupplementalDetails();
        //}

        /// <summary>
        /// Populate Fund Port Dates
        /// </summary>
        public void PopulateFundPortDates()
        {
            _logger.LogInformation("Populating Fund Port Dates - STARTED");
            IDictionary<string, FundPortDate> dict = _fundSupplementalDataDao.GetFundPortDates();
            _cache.Remove(CacheKeys.FUND_PORT_DATES);
            _cache.Add(CacheKeys.FUND_PORT_DATES, dict, DateTimeOffset.MaxValue);
            _logger.LogInformation("Populating Fund Port Dates - DONE");
        }

        /// <summary>
        /// Populate Fund Nav Update Frequency
        /// </summary>
        public void PopulateFundNavUpdateFrequency()
        {
            _logger.LogInformation("Populating Fund Nav Update Frequency - STARTED");
            IDictionary<string, FundNavUpdate> dict = _fundSupplementalDataDao.GetFundNavUpdateFrequency();
            _cache.Remove(CacheKeys.FUND_NAV_UPDATE_FREQUENCY);
            _cache.Add(CacheKeys.FUND_NAV_UPDATE_FREQUENCY, dict, DateTimeOffset.MaxValue);
            _logger.LogInformation("Populating Fund Nav Update Frequency - DONE");
        }

        /// <summary>
        /// Populate Fund Redemption Triggers
        /// </summary>
        public void PopulateFundRedemptionTriggers()
        {
            _logger.LogInformation("Populating Fund Redemption Triggers - STARTED");
            IDictionary<string, FundRedemptionTrigger> dict = _fundSupplementalDataDao.GetFundRedemptionTriggers();
            _cache.Remove(CacheKeys.FUND_REDEMPTION_TRIGGERS);
            _cache.Add(CacheKeys.FUND_REDEMPTION_TRIGGERS, dict, DateTimeOffset.MaxValue);
            _logger.LogInformation("Populating Fund Redemption Triggers - DONE");
        }

        /// <summary>
        /// Populate Security Alert Targets
        /// </summary>
        public void PopulateSecurityAlertTargets()
        {
            _logger.LogInformation("Populating Security Alert Targets - STARTED");
            IList<SecurityAlert> securityAlertsList = _securityAlertDao.GetSecurityAlerts();
            _cache.Remove(CacheKeys.SECURITY_ALERT_TARGETS);
            _cache.Add(CacheKeys.SECURITY_ALERT_TARGETS, securityAlertsList, DateTimeOffset.MaxValue);
            _logger.LogInformation("Populating Security Alert Targets - DONE");
        }

        /// <summary>
        /// Populate Fund Notes
        /// </summary>
        public void PopulateFundNotes()
        {
            _logger.LogInformation("Populating Fund Notes - STARTED");
            IDictionary<string, FundNotes> fundNotesDict = _fundDao.GetFundNotes();
            _cache.Remove(CacheKeys.FUND_NOTES);
            _cache.Add(CacheKeys.FUND_NOTES, fundNotesDict, DateTimeOffset.MaxValue);
            _logger.LogInformation("Populating Fund Notes - DONE");
        }

        /// <summary>
        /// Populate Fund Comments in Fund Supplemental Data
        /// </summary>
        private void RefreshFundNotes()
        {
            string ticker = string.Empty;
            try
            {
                IDictionary<string, FundNotes> fundNotesDict = _cache.Get<IDictionary<string, FundNotes>>(CacheKeys.FUND_NOTES);
                IDictionary<string, FundSupplementalData> fundSupplementalDataDict = _cache.Get<IDictionary<string, FundSupplementalData>>(CacheKeys.FUND_SUPPLEMENTAL_DETAILS);
                foreach (KeyValuePair<string, FundNotes> kvp in fundNotesDict)
                {
                    ticker = kvp.Key;
                    FundNotes fundNotes = kvp.Value;
                    if (!string.IsNullOrEmpty(fundNotes.FundComments))
                    {
                        if (!fundSupplementalDataDict.TryGetValue(ticker, out FundSupplementalData fundSupplementalData))
                        {
                            fundSupplementalData = new FundSupplementalData
                            {
                                Ticker = ticker,
                                FundComments = fundNotes.FundComments
                            };
                            fundSupplementalDataDict.Add(ticker, fundSupplementalData);
                        }
                        else
                        {
                            fundSupplementalData.FundComments = fundNotes.FundComments;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error populating Fund Comments for ticker: " + ticker);
            }
        }

        /// <summary>
        /// Populate Position Ticker Map (ISIN -> Position Ticker)
        /// </summary>
        public void PopulatePositionTickerMap()
        {
            _logger.LogInformation("Populating ISIN->Position Ticker Map - STARTED");
            IDictionary<string, string> dict = _tradingDao.GetISINPositionTickerMap();
            _cache.Remove(CacheKeys.POSITION_IDENTIFIER_MAP);
            _cache.Add(CacheKeys.POSITION_IDENTIFIER_MAP, dict, DateTimeOffset.MaxValue);
            _logger.LogInformation("Populating ISIN->Position Ticker Map - DONE");
        }

        /// <summary>
        /// Save Fund Notes
        /// </summary>
        /// <param name="fundNotes"></param>
        public void SaveFundNotes(IList<FundNotes> fundNotesList)
        {
            try
            {
                _logger.LogInformation("Saving Fund Notes...");
                _fundDao.SaveFundNotes(fundNotesList);
                _logger.LogInformation("Refreshing Fund Notes...");
                PopulateFundNotes();
                RefreshFundNotes();
                _logger.LogInformation("Refreshed Fund Notes");
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error refreshing Fund Notes");
            }
        }

        /// <summary>
        /// Refresh Fund Master
        /// </summary>
        public void RefreshFundMaster()
        {
            PopulateFundMaster();
        }

        /// <summary>
        /// Runs Data Validation Checks
        /// </summary>
        public void RunDataValidationChecks()
        {
            _dataValidationChecks.RunValidationChecks();
        }

        /// <summary>
        /// Populate Global Market Month End Levels
        /// </summary>
        public void PopulateGlobalMarketMonthEndLevels()
        {
            _logger.LogInformation("Populating Global Market Month End Levels - STARTED");
            IDictionary<string, IDictionary<DateTime, Nullable<double>>> globalMarketMonthEndHist = _statsDao.GetGlobalMarketHistory();
            //_cache.Remove(CacheKeys.GMM_MONTH_END_HIST);
            _cache.Add(CacheKeys.GMM_MONTH_END_HIST, globalMarketMonthEndHist, DateTimeOffset.MaxValue);
            _logger.LogInformation("Populating Global Market Month End Levels - DONE");
        }

        /// <summary>
        /// Populate Global Market Indicators
        /// </summary>
        public void PopulateGlobalMarketIndicators()
        {
            _logger.LogInformation("Populating Global Market Indicators - STARTED");
            IDictionary<string, GlobalMarketIndicator> dict = _statsDao.GetGlobalMarketIndicators();
            _cache.Remove(CacheKeys.GMM_INDICATORS);
            _cache.Add(CacheKeys.GMM_INDICATORS, dict, DateTimeOffset.MaxValue);
            _logger.LogInformation("Populating Global Market Indicators - DONE");
        }

        /// <summary>
        /// Updates application data update flag
        /// This flag is to use to pull latest data from RTD server(s)
        /// </summary>
        /// <param name="dataUpdateFlag"></param>
        public void UpdateApplicationDataUpdateFlag(string dataUpdateFlag)
        {
            ApplicationData applicationData = new ApplicationData
            {
                ApplicationName = "AC Trader",
                DataUpdateFlag = dataUpdateFlag
            };
            applicationData.PfdDataLastUpdateTime = _baseDao.GetPfdCurvesUpdateTime();

            //_cache.Remove(CacheKeys.APPLICATION_DATA_FLAG);
            _cache.Add(CacheKeys.APPLICATION_DATA_FLAG, applicationData, DateTimeOffset.MaxValue);
            _adminDao.SaveApplicationDataUpdateFlag(applicationData);
        }

        /// <summary>
        /// Populate Crypto Security Mst
        /// </summary>
        public void PopulateCryptoSecurityDetails()
        {
            _logger.LogInformation("Populating Crypto Security List - STARTED");
            IDictionary<string, CryptoSecMst> cryptoSecurityList = _cryptoDao.GetCryptoSecurityDetails();
            _cache.Remove(CacheKeys.CRYPTO_SECURITY_MST);
            _cache.Add(CacheKeys.CRYPTO_SECURITY_MST, cryptoSecurityList, DateTimeOffset.MaxValue);
            _logger.LogInformation("Populating Crypto Security List - DONE");
        }

        /// <summary>
        /// Populate Manual Trades
        /// Trades are done outside of Neovest
        /// </summary>
        public void PopulateManualTrades()
        {
            _logger.LogInformation("Populating Manual Trades - STARTED");
            IList<ASTrade> tradeList;
            if (_configuration["ConnectionStrings:ENV"].Equals("PROD"))
                tradeList = _tradingDao.GetASTrades(null, TodaysDate, TodaysDate, "'Manual'");
            else
                tradeList = _tradingDao.GetASTrades(null, DateTime.Now.AddDays(-1), DateTime.Now.AddDays(-1), "'Manual'");
            _cache.Remove(CacheKeys.MANUAL_TRADES);
            _cache.Add(CacheKeys.MANUAL_TRADES, tradeList, DateTimeOffset.MaxValue);
            _logger.LogInformation("Populating Manual Trades - DONE");
        }

        /// <summary>
        /// Save Crypto Security Details
        /// Refreshes Cache
        /// </summary>
        /// <param name="cryptoSecurityList"></param>
        public void SaveCryptoSecurityDetails(IList<CryptoSecMst> cryptoSecurityList)
        {
            try
            {
                _logger.LogInformation("Saving Crypto Security Details...");
                _cryptoDao.SaveCryptoSecurityDetails(cryptoSecurityList);
                _logger.LogInformation("Refreshing Crypto Security Details...");
                PopulateCryptoSecurityDetails();
                _logger.LogInformation("Refreshed Crypto Security Details");
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error saving Crypto Security Details");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void UpdateFundForecastFlags()
        {
            _logger.LogInformation("Updating Fund Forecast Flags...");
            _fundForecastEngine.ResetFundProxyDetails();
            _fundForecastEngine.ResetFundPortProxyDetails();
            _fundForecastEngine.ResetRightsOfferDetails();
            _fundForecastEngine.ResetTenderOfferDetails();
            _logger.LogInformation("Updating Fund Forecast Flags - DONE");
        }

        /// <summary>
        /// Populate Security Master Extension Details
        /// </summary>
        public void PopulateSecurityExtDetails()
        {
            _logger.LogInformation("Populating Security Ext Details - STARTED");
            IDictionary<string, SecurityMasterExt> securityExtDict = _securityRiskDao.GetSecurityMasterExtDetails();
            //_cache.Remove(CacheKeys.SECURITY_EXT_DETAILS);
            _cache.Add(CacheKeys.SECURITY_EXT_DETAILS, securityExtDict, DateTimeOffset.MaxValue);
            _logger.LogInformation("Populating Security Ext Details - DONE");
        }

        /// <summary>
        /// Populate Security Risk Factors (Risk Beta w/ Start and End Dates)
        /// </summary>
        public void PopulateSecurityRiskFactors()
        {
            _logger.LogInformation("Populating Security Risk Factors - STARTED");
            IDictionary<string, SecurityRiskFactor> dict = _securityRiskDao.GetSecurityRiskFactorsWithDates();
            _cache.Remove(CacheKeys.SECURITY_RISK_FACTORS_WITH_DATES);
            _cache.Add(CacheKeys.SECURITY_RISK_FACTORS_WITH_DATES, dict, DateTimeOffset.MaxValue);
            _logger.LogInformation("Populating Security Risk Factors - DONE");
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IDictionary<string, SecurityMasterExt> GetSecurityExtDetails()
        {
            return _cache.Get<IDictionary<string, SecurityMasterExt>>(CacheKeys.SECURITY_EXT_DETAILS);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="list"></param>
        public void SaveSecurityExtDetails(IList<SecurityMasterExt> list)
        {
            _securityRiskDao.SaveSecurityMasterExtDetails(list);
            PopulateSecurityExtDetails();
            //_commonDao.UpdateFundPortDates();
            //PopulateFundPortDates();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="list"></param>
        public void SaveSecurityRiskFactorsWithDates(IList<SecurityRiskFactor> list)
        {
            _securityRiskDao.SaveSecurityRiskFactorsWithDates(list);
            PopulateSecurityRiskFactors();
        }

        /// <summary>
        /// 
        /// </summary>
        public void PopulateFullFundHistory()
        {
            _logger.LogInformation("Populating Full Fund History - STARTED");
            IDictionary<string, IDictionary<DateTime, FundHistoryScalar>> dict = _fundHistoryDao.GetFundHistoryFull();
            //_cache.Remove(CacheKeys.FUND_FULL_HISTORY);
            _cache.Add(CacheKeys.FUND_FULL_HISTORY, dict, DateTimeOffset.MaxValue);
            _logger.LogInformation("Populating Full Fund History - DONE");
        }

        /// <summary>
        /// 
        /// </summary>
        public void PopulateCEFFullFundHistory()
        {
            _logger.LogInformation("Populating CEF Full Fund History - STARTED");
            IDictionary<string, IList<FundPDHistory>> dict = _fundHistoryDao.GetCEFFundHistoryFull();
            //_cache.Remove(CacheKeys.CEF_FUND_FULL_HISTORY);
            _cache.Add(CacheKeys.CEF_FUND_FULL_HISTORY, dict, DateTimeOffset.MaxValue);
            _logger.LogInformation("Populating CEF  Full Fund History - DONE");
        }

        /// <summary>
        /// Populate Fund Nav Estimation Error Details
        /// </summary>
        public void PopulateLatestFundNavEstErrDetails()
        {
            _logger.LogInformation("Populating Fund Nav Estimation Error Details - STARTED");
            IDictionary<string, FundForecast> fundForecastDict = _cache.Get<IDictionary<string, FundForecast>>(CacheKeys.FUND_FORECASTS);

            //UNITED STATES
            IList<FundNavReportTO> list = _fundHistoryOperations.GetLatestFundNavEstDetails("United States", null, DateTime.Today);
            foreach (FundNavReportTO data in list)
            {
                if (fundForecastDict.TryGetValue(data.Ticker, out FundForecast fundForecast))
                {
                    fundForecast.PubNavChng = data.PubNavChng;
                    fundForecast.EstNavChng = data.EstNavChng;
                    fundForecast.EstNavErr = data.EstNavErr;
                }
            }

            //UNITED KINGDOM
            list = _fundHistoryOperations.GetLatestFundNavEstDetails("United Kingdom", null, DateTime.Today);
            foreach (FundNavReportTO data in list)
            {
                if (fundForecastDict.TryGetValue(data.Ticker, out FundForecast fundForecast))
                {
                    fundForecast.PubNavChng = data.PubNavChng;
                    fundForecast.EstNavChng = data.EstNavChng;
                    fundForecast.EstNavErr = data.EstNavErr;
                }
            }

            //CANADA
            list = _fundHistoryOperations.GetLatestFundNavEstDetails("Canada", null, DateTime.Today);
            foreach (FundNavReportTO data in list)
            {
                if (fundForecastDict.TryGetValue(data.Ticker, out FundForecast fundForecast))
                {
                    fundForecast.PubNavChng = data.PubNavChng;
                    fundForecast.EstNavChng = data.EstNavChng;
                    fundForecast.EstNavErr = data.EstNavErr;
                }
            }
            _logger.LogInformation("Populating Fund Nav Estimation Error Details - DONE");
        }

        /// <summary>
        /// Populate Fund Currency Exposures (User provided)
        /// </summary>
        public void PopulateFundCurrencyExposures()
        {
            _logger.LogInformation("Populating Fund Currency Exposures (User Provided) - STARTED");
            IDictionary<string, FundCurrExpTO> dict = _fundDao.GetPortCurrencyExposureOverrides();
            _cache.Remove(CacheKeys.FUND_CURRENCY_EXPOSURES);
            _cache.Add(CacheKeys.FUND_CURRENCY_EXPOSURES, dict, DateTimeOffset.MaxValue);
            _logger.LogInformation("Populating Fund Currency Exposures (User Provided) - DONE");
        }

        /// <summary>
        /// Populate Broker Commission Rates
        /// </summary>
        public void PopulateBrokerCommissionRates()
        {
            _logger.LogInformation("Populating Broker Commission Rates - STARTED");
            IDictionary<string, BrokerCommission> dict = _brokerDataDao.GetBrokerCommissionRates();
            _cache.Remove(CacheKeys.BROKER_COMMISSION_RATES);
            _cache.Add(CacheKeys.BROKER_COMMISSION_RATES, dict, DateTimeOffset.MaxValue);
            _logger.LogInformation("Populating Broker Commission Rates - DONE");
        }

        public void PopulateBatchMonitorDetails()
        {
            _logger.LogInformation("Populating Batch Monitor Details - STARTED");
            IDictionary<string, BatchMonitorTO> dict = new Dictionary<string, BatchMonitorTO>();
            dict.Add("Neovest Live Prices", new BatchMonitorTO("Neovest Live Prices"));
            dict.Add("Neovest Delayed Prices", new BatchMonitorTO("Neovest Delayed Prices"));
            dict.Add("Neovest FX Rates", new BatchMonitorTO("Neovest FX Rates"));
            dict.Add("Neovest Orders", new BatchMonitorTO("Neovest Orders"));
            dict.Add("Crypto Prices", new BatchMonitorTO("Crypto Prices"));
            dict.Add("EMSX Orders", new BatchMonitorTO("EMSX Orders"));
            dict.Add("BBG Prices/FX Rates", new BatchMonitorTO("BBG Prices/FX Rates"));
            dict.Add("BBG Nav Update", new BatchMonitorTO("BBG Nav Update"));
            dict.Add("BBG Curve Update", new BatchMonitorTO("BBG Curve Update"));
            dict.Add("Fund Estimated Nav Update", new BatchMonitorTO("Fund Estimated Nav Update"));
            dict.Add("Daily PnL Update", new BatchMonitorTO("Daily PnL Update"));
            _cache.Remove(CacheKeys.BATCH_MONITOR);
            _cache.Add(CacheKeys.BATCH_MONITOR, dict, DateTimeOffset.MaxValue);
            _logger.LogInformation("Populating Batch Monitor Details - DONE");
        }

        /// <summary>
        /// 
        /// </summary>
        public void RefreshOverrides()
        {
            IDictionary<string, FundMaster> fundMasterDict = _cache.Get<IDictionary<string, FundMaster>>(CacheKeys.FUND_MASTER);
            IDictionary<string, FundForecast> fundForecastDict = _cache.Get<IDictionary<string, FundForecast>>(CacheKeys.FUND_FORECASTS);
            IDictionary<string, FundAlphaModelScores> fundAlphaModelScoresDict = _cache.Get<IDictionary<string, FundAlphaModelScores>>(CacheKeys.ALPHA_MODEL_SCORES);
            IDictionary<string, UserDataOverride> userDataOverrideDict = _cache.Get<IDictionary<string, UserDataOverride>>(CacheKeys.USER_OVERRIDES);

            string ticker = string.Empty;
            foreach (FundForecast fundForecast in fundForecastDict.Values)
            {
                try
                {
                    ticker = fundForecast.Ticker;
                    fundMasterDict.TryGetValue(ticker, out FundMaster fundMaster);
                    fundAlphaModelScoresDict.TryGetValue(ticker, out FundAlphaModelScores fundAlphaModelScores);
                    userDataOverrideDict.TryGetValue(ticker, out UserDataOverride userOverride);
                    FundMasterOperations.ApplyUserOverrides(fundForecast, fundMaster, fundAlphaModelScores, userOverride);
                }
                catch (Exception ex)
                {
                    _logger.LogError("Error refresing Overrides for Ticker: " + ticker, ex);
                }
            }
        }

        public IList<FundHistStatsTO> GetFundHistoricalStats(string country, string securityType, string ticker)
        {
            ticker = string.IsNullOrEmpty(ticker) ? "" : ticker;
            IDictionary<string, FundMaster> fundMasterDict = _cache.Get<IDictionary<string, FundMaster>>(CacheKeys.FUND_MASTER);
            IDictionary<string, FundHistStats> fundHistStatsDict = _cache.Get<IDictionary<string, FundHistStats>>(CacheKeys.FUND_STATS);

            IList<FundHistStatsTO> list = new List<FundHistStatsTO>();
            bool includeFund = false;
            foreach (FundMaster fundMaster in fundMasterDict.Values)
            {
                includeFund = false;
                try
                {
                    if ("All".Equals(country))
                        includeFund = true;
                    else if (country.Equals(fundMaster.Cntry, StringComparison.CurrentCultureIgnoreCase))
                        includeFund = true;

                    if (includeFund)
                    {
                        if (!"All".Equals(securityType))
                            if (!securityType.Equals(fundMaster.AssetTyp, StringComparison.CurrentCultureIgnoreCase))
                                includeFund = false;
                    }
                    if (includeFund)
                    {
                        if (!"".Equals(ticker))
                            if (!ticker.Equals(fundMaster.Ticker, StringComparison.CurrentCultureIgnoreCase))
                                includeFund = false;
                    }

                    if (includeFund)
                    {
                        FundHistStatsTO data = new FundHistStatsTO();
                        data.Ticker = fundMaster.Ticker;
                        data.SecType = fundMaster.SecTyp;
                        data.Cntry = fundMaster.Cntry;
                        data.Name = fundMaster.Name;
                        data.Curr = fundMaster.Curr;

                        list.Add(data);

                        if (fundHistStatsDict.TryGetValue(fundMaster.Ticker, out FundHistStats fundHistStats))
                        {
                            data.LastUpdate = fundHistStats.StatsLastUpdate;
                            data.Mean1W = fundHistStats.Mean1W;
                            data.Mean2W = fundHistStats.Mean2W;
                            data.Mean1M = fundHistStats.Mean1M;
                            data.Mean3M = fundHistStats.Mean3M;
                            data.Mean6M = fundHistStats.Mean6M;
                            data.Mean12M = fundHistStats.Mean12M;
                            data.Mean24M = fundHistStats.Mean24M;
                            data.Mean36M = fundHistStats.Mean36M;
                            data.Mean48M = fundHistStats.Mean48M;
                            data.Mean60M = fundHistStats.Mean60M;
                            data.Mean84M = fundHistStats.Mean84M;
                            data.Mean120M = fundHistStats.Mean120M;
                            data.Mean180M = fundHistStats.Mean180M;
                            data.MeanLife = fundHistStats.MeanLife;

                            data.StdDev1W = fundHistStats.StdDev1W;
                            data.StdDev2W = fundHistStats.StdDev2W;
                            data.StdDev1M = fundHistStats.StdDev1M;
                            data.StdDev3M = fundHistStats.StdDev3M;
                            data.StdDev6M = fundHistStats.StdDev6M;
                            data.StdDev12M = fundHistStats.StdDev12M;
                            data.StdDev24M = fundHistStats.StdDev24M;
                            data.StdDev36M = fundHistStats.StdDev36M;
                            data.StdDev48M = fundHistStats.StdDev48M;
                            data.StdDev60M = fundHistStats.StdDev60M;
                            data.StdDev84M = fundHistStats.StdDev84M;
                            data.StdDev120M = fundHistStats.StdDev120M;
                            data.StdDev180M = fundHistStats.StdDev180M;
                            data.StdDevLife = fundHistStats.StdDevLife;
                        }

                        if (fundMaster.FundPerfRtn != null)
                        {
                            data.PriceRtnMTD = fundMaster.FundPerfRtn.PriceRtnMTD;
                            data.PriceRtnQTD = fundMaster.FundPerfRtn.PriceRtnQTD;
                            data.PriceRtnYTD = fundMaster.FundPerfRtn.PriceRtnYTD;
                            data.PriceRtn6Mnths = fundMaster.FundPerfRtn.PriceRtn6Mnths;
                            data.PriceRtn12Mnths = fundMaster.FundPerfRtn.PriceRtn12Mnths;
                            data.PriceRtn24Mnths = fundMaster.FundPerfRtn.PriceRtn24Mnths;
                            data.PriceRtn36Mnths = fundMaster.FundPerfRtn.PriceRtn36Mnths;
                            data.PriceRtn60Mnths = fundMaster.FundPerfRtn.PriceRtn60Mnths;
                            data.PriceRtn120Mnths = fundMaster.FundPerfRtn.PriceRtn120Mnths;
                            data.PriceRtnLife = fundMaster.FundPerfRtn.PriceRtnLife;

                            data.NavRtnMTD = fundMaster.FundPerfRtn.NavRtnMTD;
                            data.NavRtnQTD = fundMaster.FundPerfRtn.NavRtnQTD;
                            data.NavRtnYTD = fundMaster.FundPerfRtn.NavRtnYTD;
                            data.NavRtn6Mnths = fundMaster.FundPerfRtn.NavRtn6Mnths;
                            data.NavRtn12Mnths = fundMaster.FundPerfRtn.NavRtn12Mnths;
                            data.NavRtn24Mnths = fundMaster.FundPerfRtn.NavRtn24Mnths;
                            data.NavRtn36Mnths = fundMaster.FundPerfRtn.NavRtn36Mnths;
                            data.NavRtn60Mnths = fundMaster.FundPerfRtn.NavRtn60Mnths;
                            data.NavRtn120Mnths = fundMaster.FundPerfRtn.NavRtn120Mnths;
                            data.NavRtnLife = fundMaster.FundPerfRtn.NavRtnLife;
                        }

                        if (fundMaster.FundPerfRisk != null)
                        {
                            data.PriceVolMTD = fundMaster.FundPerfRisk.PriceVolMTD;
                            data.PriceVolQTD = fundMaster.FundPerfRisk.PriceVolQTD;
                            data.PriceVolYTD = fundMaster.FundPerfRisk.PriceVolYTD;
                            data.PriceVol6Mnths = fundMaster.FundPerfRisk.PriceVolMTD;
                            data.PriceVol12Mnths = fundMaster.FundPerfRisk.PriceVol12Mnths;
                            data.PriceVol24Mnths = fundMaster.FundPerfRisk.PriceVol24Mnths;
                            data.PriceVol36Mnths = fundMaster.FundPerfRisk.PriceVol36Mnths;
                            data.PriceVol60Mnths = fundMaster.FundPerfRisk.PriceVol60Mnths;
                            data.PriceVol120Mnths = fundMaster.FundPerfRisk.PriceVol120Mnths;
                            data.PriceVolLife = fundMaster.FundPerfRisk.PriceVolLife;

                            data.NavVolMTD = fundMaster.FundPerfRisk.NavVolMTD;
                            data.NavVolQTD = fundMaster.FundPerfRisk.NavVolQTD;
                            data.NavVolYTD = fundMaster.FundPerfRisk.NavVolYTD;
                            data.NavVol6Mnths = fundMaster.FundPerfRisk.NavVolMTD;
                            data.NavVol12Mnths = fundMaster.FundPerfRisk.NavVol12Mnths;
                            data.NavVol24Mnths = fundMaster.FundPerfRisk.NavVol24Mnths;
                            data.NavVol36Mnths = fundMaster.FundPerfRisk.NavVol36Mnths;
                            data.NavVol60Mnths = fundMaster.FundPerfRisk.NavVol60Mnths;
                            data.NavVol120Mnths = fundMaster.FundPerfRisk.NavVol120Mnths;
                            data.NavVolLife = fundMaster.FundPerfRisk.NavVolLife;

                            data.PriceSharpeRatioMTD = fundMaster.FundPerfRisk.PriceSharpeRatioMTD;
                            data.PriceSharpeRatioQTD = fundMaster.FundPerfRisk.PriceSharpeRatioQTD;
                            data.PriceSharpeRatioYTD = fundMaster.FundPerfRisk.PriceSharpeRatioYTD;
                            data.PriceSharpeRatio6Mnths = fundMaster.FundPerfRisk.PriceSharpeRatio6Mnths;
                            data.PriceSharpeRatio12Mnths = fundMaster.FundPerfRisk.PriceSharpeRatio12Mnths;
                            data.PriceSharpeRatio24Mnths = fundMaster.FundPerfRisk.PriceSharpeRatio24Mnths;
                            data.PriceSharpeRatio36Mnths = fundMaster.FundPerfRisk.PriceSharpeRatio36Mnths;
                            data.PriceSharpeRatio60Mnths = fundMaster.FundPerfRisk.PriceSharpeRatio60Mnths;
                            data.PriceSharpeRatio120Mnths = fundMaster.FundPerfRisk.PriceSharpeRatio120Mnths;
                            data.PriceSharpeRatioLife = fundMaster.FundPerfRisk.PriceSharpeRatioLife;

                            data.NavSharpeRatioMTD = fundMaster.FundPerfRisk.NavSharpeRatioMTD;
                            data.NavSharpeRatioQTD = fundMaster.FundPerfRisk.NavSharpeRatioQTD;
                            data.NavSharpeRatioYTD = fundMaster.FundPerfRisk.NavSharpeRatioYTD;
                            data.NavSharpeRatio6Mnths = fundMaster.FundPerfRisk.NavSharpeRatio6Mnths;
                            data.NavSharpeRatio12Mnths = fundMaster.FundPerfRisk.NavSharpeRatio12Mnths;
                            data.NavSharpeRatio24Mnths = fundMaster.FundPerfRisk.NavSharpeRatio24Mnths;
                            data.NavSharpeRatio36Mnths = fundMaster.FundPerfRisk.NavSharpeRatio36Mnths;
                            data.NavSharpeRatio60Mnths = fundMaster.FundPerfRisk.NavSharpeRatio60Mnths;
                            data.NavSharpeRatio120Mnths = fundMaster.FundPerfRisk.NavSharpeRatio120Mnths;
                            data.NavSharpeRatioLife = fundMaster.FundPerfRisk.NavSharpeRatioLife;
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error getting Fund Stats", ex);
                }
            }
            return list;
        }

        /// <summary>
        /// Populate Daily PnL Holdings
        /// </summary>
        public void PopulateDailyPnLHoldings()
        {
            _logger.LogInformation("Populate Daily PnL Holdings - STARTED");
            _dailyPnLReport.UpdateHoldings();
            _logger.LogInformation("Populate Daily PnL Holdings- DONE");
        }
    }
}