using aACTrader.DAO.Interface;
using aCommons;
using aCommons.Admin;
using aCommons.Cef;
using aCommons.DTO;
using aCommons.Web;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;

namespace aACTrader.DAO.Repository
{
    public class WebDao : IWebDao
    {
        private readonly ILogger<WebDao> _logger;

        public WebDao(ILogger<WebDao> logger)
        {
            _logger = logger;
            _logger.LogInformation("Initializing WebDao...");
        }

        /// <summary>
        /// Gets Dividend Schedule (for all active securities)
        /// </summary>
        /// <returns></returns>
        public IList<DividendScheduleTO> GetDividendSchedule()
        {
            IList<DividendScheduleTO> list = new List<DividendScheduleTO>();
            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetDividendScheduleQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                DividendScheduleTO data = new DividendScheduleTO
                                {
                                    Ticker = reader["Ticker"] as string,
                                    SecName = reader["SecName"] as string,
                                    YellowKey = reader["YKey"] as string,
                                    SecType = reader["SecurityType"] as string,
                                    DecDate = reader.IsDBNull(reader.GetOrdinal("DecDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("DecDate")),
                                    ExDvdDate = reader.IsDBNull(reader.GetOrdinal("ExDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("ExDate")),
                                    RecordDate = reader.IsDBNull(reader.GetOrdinal("RecordDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("RecordDate")),
                                    PayDate = reader.IsDBNull(reader.GetOrdinal("PayDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("PayDate")),
                                    DvdAmt = (reader.IsDBNull(reader.GetOrdinal("DvdAmt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DvdAmt")),
                                    LastPrice = (reader.IsDBNull(reader.GetOrdinal("LastPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LastPrice")),
                                    PrevClosePrice = (reader.IsDBNull(reader.GetOrdinal("PrevClosePrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PrevClosePrice")),
                                    PriceChng = (reader.IsDBNull(reader.GetOrdinal("PriceChange"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceChange")),
                                    DvdFreq = reader["DvdFreq"] as string,
                                    DvdType = reader["DvdType"] as string
                                };
                                list.Add(data);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing query");
                throw;
            }
            return list;
        }

        public IList<CorporateAction> GetCorporateActions()
        {
            IList<CorporateAction> list = new List<CorporateAction>();
            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetCorporateActionsQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                CorporateAction data = new CorporateAction
                                {
                                    Source = reader["Source"] as string,
                                    Fund = reader["Fund"] as string,
                                    EffectiveDate = reader.IsDBNull(reader.GetOrdinal("EffectiveDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate")),
                                    Ticker = reader["Ticker"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    SecurityDesc = reader["SecurityDesc"] as string,
                                    Currency = reader["Currency"] as string,
                                    CorpActionType = reader["CorpActionType"] as string,
                                    CorpActionTypeDesc = reader["CorpActionTypeDesc"] as string,
                                    ExDate = reader.IsDBNull(reader.GetOrdinal("ExDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("ExDate")),
                                    RecordDate = reader.IsDBNull(reader.GetOrdinal("RecordDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("RecordDate")),
                                    PayDate = reader.IsDBNull(reader.GetOrdinal("PayDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("PayDate")),
                                    TradePos = (reader.IsDBNull(reader.GetOrdinal("TradePos"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TradePos")),
                                    ShortPos = (reader.IsDBNull(reader.GetOrdinal("ShortPos"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ShortPos")),
                                    LocalDvdAmt = (reader.IsDBNull(reader.GetOrdinal("ProjLocalDvdAmt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ProjLocalDvdAmt")),
                                    BaseDvdAmt = (reader.IsDBNull(reader.GetOrdinal("ProjBaseDvdAmt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ProjBaseDvdAmt")),
                                    FxRate = (reader.IsDBNull(reader.GetOrdinal("FxRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FxRate")),
                                    ISIN = reader["ISIN"] as string,
                                    Cusip = reader["Cusip"] as string,
                                };
                                list.Add(data);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing query");
                throw;
            }
            return list;
        }

        public IList<SecurityDataErrorTO> GetSecurityDataCheckReport(string country, string ticker)
        {
            IList<SecurityDataErrorTO> list = new List<SecurityDataErrorTO>();
            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    string sql = GetSecurtiyDataErrorsQuery;
                    if (!string.IsNullOrEmpty(country))
                        sql += " and s.Country = '" + country + "'";
                    sql += " order by 2";

                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                SecurityDataErrorTO data = new SecurityDataErrorTO
                                {
                                    FIGI = reader["FIGI"] as string,
                                    Ticker = reader["Ticker"] as string,
                                    Desc = reader["SecDesc"] as string,
                                    Curr = reader["Curr"] as string,
                                    SecType = reader["SecurityType"] as string,
                                    Cntry = reader["Country"] as string,
                                    NavEstMethod = reader["NavEstimationMethod"] as string,
                                    PortComments = reader["Notes"] as string,
                                    ProxyFormula = reader["ProxyFormula"] as string,
                                    //Port Source
                                    PortSrc = reader["PortSource"] as string,
                                    ALMPortDt = reader.IsDBNull(reader.GetOrdinal("ALMPortDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("ALMPortDate")),
                                    BBGPortDt = reader.IsDBNull(reader.GetOrdinal("BBGPortDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("BBGPortDate")),
                                    UserPortDt = reader.IsDBNull(reader.GetOrdinal("UserPortDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("UserPortDate")),
                                    //Nav
                                    NavDt = reader.IsDBNull(reader.GetOrdinal("NavDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("NavDate")),
                                    Nav = (reader.IsDBNull(reader.GetOrdinal("Nav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav")),
                                    PD = (reader.IsDBNull(reader.GetOrdinal("NavPD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NavPD")),
                                    NavSrc = reader["NavSource"] as string,
                                    NavFreq = reader["NavFreq"] as string,
                                    NavFreqType = reader["NavFreqType"] as string,
                                    NavLag = (reader.IsDBNull(reader.GetOrdinal("NavLag"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("NavLag")),
                                    //BBG Nav
                                    BBGNavDt = reader.IsDBNull(reader.GetOrdinal("BBGNavDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("BBGNavDate")),
                                    BBGNav = (reader.IsDBNull(reader.GetOrdinal("BBGNavAdj"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BBGNavAdj")),
                                    BBGPD = (reader.IsDBNull(reader.GetOrdinal("BBGPD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BBGPD")),
                                    BBGNavCurr = reader["BBGNavCurrency"] as string,
                                    //Numis Nav
                                    NumisNavDt = reader.IsDBNull(reader.GetOrdinal("NumisNavDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("NumisNavDate")),
                                    NumisNav = (reader.IsDBNull(reader.GetOrdinal("NumisPubNavAdj"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NumisPubNavAdj")),
                                    NumisPD = (reader.IsDBNull(reader.GetOrdinal("NumisPDCalculated"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NumisPDCalculated")),
                                    NumisNavCurr = reader["NumisNavCurrency"] as string,
                                    //PH Nav
                                    PHNavDt = reader.IsDBNull(reader.GetOrdinal("PHNavDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("PHNavDate")),
                                    PHNav = (reader.IsDBNull(reader.GetOrdinal("PHAdjNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PHAdjNav")),
                                    PHPD = (reader.IsDBNull(reader.GetOrdinal("PHPD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PHPD")),
                                    PHNavCurr = reader["PHCurrency"] as string,
                                    //Cefa Nav
                                    CEFANavDt = reader.IsDBNull(reader.GetOrdinal("CefaNavDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("CefaNavDate")),
                                    CEFANav = (reader.IsDBNull(reader.GetOrdinal("CefaNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CefaNav")),
                                    CEFAPD = (reader.IsDBNull(reader.GetOrdinal("CefaNavPD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CefaNavPD")),
                                };
                                list.Add(data);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing query");
                throw;
            }
            return list;
        }

        public IList<ALMFunction> GetALMFunctions(string funcType, string funcCategory, string funcName, string dataSrc)
        {
            IList<ALMFunction> list = new List<ALMFunction>();
            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    string sql = GetALMFunctionsQuery;

                    if (!string.IsNullOrEmpty(funcType) && !funcType.Equals("All", StringComparison.CurrentCultureIgnoreCase))
                        sql += " and FuncType = '" + funcType + "'";
                    if (!string.IsNullOrEmpty(funcCategory) && !funcCategory.Equals("All", StringComparison.CurrentCultureIgnoreCase))
                        sql += " and FuncCategory = '" + funcCategory + "'";
                    if (!string.IsNullOrEmpty(funcName) && !funcName.Equals("All", StringComparison.CurrentCultureIgnoreCase))
                        sql += " and FuncName = '" + funcName + "'";
                    if (!string.IsNullOrEmpty(dataSrc) && !dataSrc.Equals("All", StringComparison.CurrentCultureIgnoreCase))
                        sql += " and DataSrc = '" + dataSrc + "'";

                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                ALMFunction data = new ALMFunction
                                {
                                    FuncName = reader["FuncName"] as string,
                                    FuncCategory = reader["FuncCategory"] as string,
                                    FuncType = reader["FuncType"] as string,
                                    FuncSample = reader["FuncSample"] as string,
                                    FuncDesc = reader["FuncDesc"] as string,
                                    DataSrc = reader["DataSrc"] as string,
                                    RealTime = reader["RealTime"] as string,
                                    Delay = reader["Delay"] as string,
                                    Comments = reader["Comments"] as string,
                                };

                                list.Add(data);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing query");
                throw;
            }
            return list;
        }

        public IList<string> GetALMFunctionCategories(string funcType)
        {
            IList<string> list = new List<string>();
            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    string sql = GetALMFunctionCategoriesQuery;
                    if (!string.IsNullOrEmpty(funcType) && !funcType.Equals("All", StringComparison.CurrentCultureIgnoreCase))
                        sql += " where FuncType = '" + funcType + "'";
                    sql += " order by FuncCategory";

                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                string funcCategory = reader["FuncCategory"] as string;
                                list.Add(funcCategory);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing query");
                throw;
            }
            return list;
        }

        public IList<FundHoldingReturn> GetPortHoldingDataChecks()
        {
            IList<FundHoldingReturn> list = new List<FundHoldingReturn>();
            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetPortHoldingDataChecksQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundHoldingReturn data = new FundHoldingReturn
                                {
                                    FundTicker = reader["FundTicker"] as string,
                                    Ticker = reader["Ticker"] as string,
                                    ProxyTicker = reader["ProxyTicker"] as string,
                                    Cusip = reader["Cusip"] as string,
                                    FundCurr = reader["FundCurr"] as string,
                                    SecCurr = reader["SecCurr"] as string,
                                    ProxySecCurr = reader["ProxySecCurr"] as string,
                                    Cntry = reader["Country"] as string,
                                    AssetType = reader["AssetType"] as string,
                                    SecType = reader["SecType"] as string,
                                    Name = reader["SecName"] as string,
                                    BroadInd = reader["BroadIndustry"] as string,
                                    Industry = reader["Industry"] as string,
                                    Sector = reader["Sector"] as string,
                                    RatingClass = reader["RatingClass"] as string,
                                    LongCurr = reader["LongCurr"] as string,
                                    ShortCurr = reader["ShortCurr"] as string,
                                    LongShortPosInd = reader["LongShortPosInd"] as string,

                                    Pos = (reader.IsDBNull(reader.GetOrdinal("ReportedPos"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ReportedPos")),
                                    Wt = (reader.IsDBNull(reader.GetOrdinal("ReportedWt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ReportedWt")),
                                    LatestWt = (reader.IsDBNull(reader.GetOrdinal("LatestWt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LatestWt")),
                                    FinalWt = (reader.IsDBNull(reader.GetOrdinal("FinalWt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FinalWt")),
                                    Prc = (reader.IsDBNull(reader.GetOrdinal("ReportedPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ReportedPrice")),
                                    Dur = (reader.IsDBNull(reader.GetOrdinal("Duration"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Duration")),
                                    YTM = (reader.IsDBNull(reader.GetOrdinal("YTM"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("YTM")),
                                    YTW = (reader.IsDBNull(reader.GetOrdinal("YTW"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("YTW")),
                                    M2M = (reader.IsDBNull(reader.GetOrdinal("M2M"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("M2M")),
                                    NavDt = (reader.IsDBNull(reader.GetOrdinal("NavDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("NavDate")),
                                    FxRateNavDt = (reader.IsDBNull(reader.GetOrdinal("FxRateNavDate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FxRateNavDate")),
                                    FxRateLast = (reader.IsDBNull(reader.GetOrdinal("FxRateLast"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FxRateLast")),
                                    FxRtn = (reader.IsDBNull(reader.GetOrdinal("FxRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FxRtn")),
                                    RptdMV = (reader.IsDBNull(reader.GetOrdinal("ReportedMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ReportedMV")),
                                    LatestMV = (reader.IsDBNull(reader.GetOrdinal("LatestMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LatestMV")),

                                    HistEqRtnLcl = (reader.IsDBNull(reader.GetOrdinal("EqCumRtnNavDateLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EqCumRtnNavDateLocal")),
                                    HistEqFxRtn = (reader.IsDBNull(reader.GetOrdinal("FxCumRtnNavDate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FxCumRtnNavDate")),
                                    HistEqRtn = (reader.IsDBNull(reader.GetOrdinal("EqCumRtnNavDate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EqCumRtnNavDate")),

                                    CurrEqRtnLcl = (reader.IsDBNull(reader.GetOrdinal("EqCurrRtnLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EqCurrRtnLocal")),
                                    CurrEqRtn = (reader.IsDBNull(reader.GetOrdinal("EqCurrRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EqCurrRtn")),
                                    CurrFxRtn = (reader.IsDBNull(reader.GetOrdinal("FxCurrRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FxCurrRtn")),

                                    CurrEqRtnLclPrxy = (reader.IsDBNull(reader.GetOrdinal("EqCurrRtnLocalProxy"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EqCurrRtnLocalProxy")),
                                    CurrEqRtnPrxy = (reader.IsDBNull(reader.GetOrdinal("EqCurrRtnProxy"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EqCurrRtnProxy")),
                                    CurrFxRtnPrxy = (reader.IsDBNull(reader.GetOrdinal("FxCurrRtnProxy"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FxCurrRtnProxy")),

                                    TotEqRtn = (reader.IsDBNull(reader.GetOrdinal("EqRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EqRtn")),
                                    TotEqRtnPrxy = (reader.IsDBNull(reader.GetOrdinal("EqRtnProxy"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EqRtnProxy")),
                                    SpreadRtn = (reader.IsDBNull(reader.GetOrdinal("SpreadRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SpreadRtn")),
                                    CurveRtn = (reader.IsDBNull(reader.GetOrdinal("CurveRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CurveRtn")),
                                    FIRtn = (reader.IsDBNull(reader.GetOrdinal("FIRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FIRtn")),
                                    CashRtn = (reader.IsDBNull(reader.GetOrdinal("CashRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CashRtn")),
                                    CurrHedgeRtn = (reader.IsDBNull(reader.GetOrdinal("CurrencyHedgeRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CurrencyHedgeRtn")),
                                    TotRtn = (reader.IsDBNull(reader.GetOrdinal("TotalRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotalRtn")),
                                    WtdTotalRtn = (reader.IsDBNull(reader.GetOrdinal("WtdTotalRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("WtdTotalRtn")),
                                    TotRtnPrxy = (reader.IsDBNull(reader.GetOrdinal("TotalRtnProxy"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotalRtnProxy")),
                                    WtdTotRtnPrxy = (reader.IsDBNull(reader.GetOrdinal("WtdTotalRtnProxy"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("WtdTotalRtnProxy")),
                                    PctHedged = (reader.IsDBNull(reader.GetOrdinal("PctHedged"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PctHedged")),
                                    LastPrc = (reader.IsDBNull(reader.GetOrdinal("LastPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LastPrice")),
                                    Beta = (reader.IsDBNull(reader.GetOrdinal("Beta"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Beta")),
                                    PrcSrc = reader["PriceSource"] as string,
                                    LastTrdTm = reader["LastTradeTime"] as string,
                                    LastTrdDt = (reader.IsDBNull(reader.GetOrdinal("LastTradeDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("LastTradeDate")),
                                    PortDt = (reader.IsDBNull(reader.GetOrdinal("PortDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("PortDate")),
                                    CheckFlag = (reader.IsDBNull(reader.GetOrdinal("CheckFlag"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("CheckFlag")),
                                    Comments = reader["Comments"] as string,
                                    PortSrc = reader["PortSrc"] as string,
                                };
                                list.Add(data);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing query");
                throw;
            }
            return list;
        }

        public IList<ManualDataOverrideTO> GetManualDataOverrides()
        {
            IList<ManualDataOverrideTO> list = new List<ManualDataOverrideTO>();
            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetManualDataOverridesQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                ManualDataOverrideTO data = new ManualDataOverrideTO
                                {
                                    Ticker = reader["Ticker"] as string,
                                    Source = reader["Source"] as string,
                                    DataType = reader["DataType"] as string,
                                    Freq = reader["Frequency"] as string,
                                    DataLag = (reader.IsDBNull(reader.GetOrdinal("DataLag"))) ? (Int16?)null : reader.GetInt16(reader.GetOrdinal("DataLag")),
                                    Nav = (reader.IsDBNull(reader.GetOrdinal("Nav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav")),
                                    NavDate = reader.IsDBNull(reader.GetOrdinal("NavDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("NavDate")),
                                    CoinsPerUnit = (reader.IsDBNull(reader.GetOrdinal("CoinsPerUnit"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CoinsPerUnit")),
                                    AsOfDate = reader.IsDBNull(reader.GetOrdinal("AsOfDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    Notes = reader["Notes"] as string,
                                    LastUpdateDate = reader.IsDBNull(reader.GetOrdinal("LastUpdateDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("LastUpdateDate")),
                                };
                                list.Add(data);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing query");
                throw;
            }
            return list;
        }

        public IList<NavOverridesTO> GetNavDataOverrides(string startDate, string endDate, string Ticker)
        {
            IList<NavOverridesTO> list = new List<NavOverridesTO>();
            try
            {
                string sql = GetNavDataOverridesQuery + " where FundNavDate >= '" + startDate + "' and FundNavDate <= '" + endDate + "'";
                if (!string.IsNullOrEmpty(Ticker))
                    sql += " and FundTicker = '" + Ticker + "'";
                sql += " order by FundNavDate desc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                NavOverridesTO data = new NavOverridesTO
                                {
                                    Ticker = reader["FundTicker"] as string,
                                    Source = reader["Src"] as string,
                                    Nav = (reader.IsDBNull(reader.GetOrdinal("FundNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FundNav")),
                                    Price = (reader.IsDBNull(reader.GetOrdinal("Price"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price")),
                                    NavDate = reader.IsDBNull(reader.GetOrdinal("FundNavDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FundNavDate")),
                                    CreateDate = reader.IsDBNull(reader.GetOrdinal("CreateDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("CreateDate")),
                                };
                                list.Add(data);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing Nav overrides query");
                throw;
            }
            return list;
        }

        public IList<SwapMarginDetTO> GetSwapMarginDetails(string broker, string ticker)
        {
            IList<SwapMarginDetTO> list = new List<SwapMarginDetTO>();
            try
            {
                string sql = GetSwapMarginDetailsQuery;
                if (!"All".Equals(broker))
                    sql += " and Broker = '" + broker + "'";
                if (!string.IsNullOrEmpty(ticker))
                    sql += " and Ticker = '" + ticker + "'";
                sql += " order by Ticker, Broker";
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                SwapMarginDetTO data = new SwapMarginDetTO
                                {
                                    Ticker = reader["Ticker"] as string,
                                    SecName = reader["SecName"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    Cusip = reader["Cusip"] as string,
                                    ISIN = reader["ISIN"] as string,
                                    Curr = reader["Curr"] as string,
                                    Fund = reader["Fund"] as string,
                                    Bkr = reader["Broker"] as string,
                                    AsOfDate = reader.IsDBNull(reader.GetOrdinal("AsOfDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    RptDate = reader.IsDBNull(reader.GetOrdinal("ReportDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("ReportDate")),
                                    Pos = (reader.IsDBNull(reader.GetOrdinal("Pos"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Pos")),
                                    BenchmarkRate = (reader.IsDBNull(reader.GetOrdinal("BenchmarkRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BenchmarkRate")),
                                    FinancingSpread = (reader.IsDBNull(reader.GetOrdinal("FinancingSpread"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FinancingSpread")),
                                    FinancingRate = (reader.IsDBNull(reader.GetOrdinal("FinancingRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FinancingRate")),
                                    IAPct = (reader.IsDBNull(reader.GetOrdinal("IAPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IAPct")),
                                    IAAmtLocal = (reader.IsDBNull(reader.GetOrdinal("IAAmtLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IAAmtLocal")),
                                    IAAmt = (reader.IsDBNull(reader.GetOrdinal("IAAmt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IAAmt")),
                                    MVLocal = (reader.IsDBNull(reader.GetOrdinal("MVLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MVLocal")),
                                    MV = (reader.IsDBNull(reader.GetOrdinal("MV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MV")),
                                    GroupId = (reader.IsDBNull(reader.GetOrdinal("GroupId"))) ? (Int16?)null : reader.GetInt16(reader.GetOrdinal("GroupId")),
                                };
                                list.Add(data);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing query");
                throw;
            }
            return list;
        }

        public IList<SwapMarginDetTO> GetCryptoMarginDetails(string broker, string ticker)
        {
            IList<SwapMarginDetTO> list = new List<SwapMarginDetTO>();
            try
            {
                string sql = GetCryptoMarginDetailsQuery;
                if (!"All".Equals(broker))
                    sql += " and Broker = '" + broker + "'";
                if (!string.IsNullOrEmpty(ticker))
                    sql += " and Ticker = '" + ticker + "'";
                sql += " order by SortId asc, CoinType, Ticker";
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                SwapMarginDetTO data = new SwapMarginDetTO
                                {
                                    Ticker = reader["Ticker"] as string,
                                    SecName = reader["SecName"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    Cusip = reader["Cusip"] as string,
                                    ISIN = reader["ISIN"] as string,
                                    Curr = reader["Curr"] as string,
                                    Fund = reader["Fund"] as string,
                                    Bkr = reader["Broker"] as string,
                                    CoinType = reader["CoinType"] as string,
                                    AsOfDate = reader.IsDBNull(reader.GetOrdinal("AsOfDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    RptDate = reader.IsDBNull(reader.GetOrdinal("ReportDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("ReportDate")),
                                    Pos = (reader.IsDBNull(reader.GetOrdinal("Pos"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Pos")),
                                    IAPct = (reader.IsDBNull(reader.GetOrdinal("IAPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IAPct")),
                                    IAAmtLocal = (reader.IsDBNull(reader.GetOrdinal("IAAmtLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IAAmtLocal")),
                                    IAAmt = (reader.IsDBNull(reader.GetOrdinal("IAAmt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IAAmt")),
                                    MVLocal = (reader.IsDBNull(reader.GetOrdinal("MVLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MVLocal")),
                                    MV = (reader.IsDBNull(reader.GetOrdinal("MV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MV")),
                                    GroupId = (reader.IsDBNull(reader.GetOrdinal("GroupId"))) ? (Int16?)null : reader.GetInt16(reader.GetOrdinal("GroupId")),
                                };
                                list.Add(data);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing query");
                throw;
            }
            return list;
        }

        public IList<DailyDataLoadSummaryTO> GetDailyDataLoadSummary(string startDate, string endDate)
        {
            IList<DailyDataLoadSummaryTO> list = new List<DailyDataLoadSummaryTO>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    string sql = GetDailyDataLoadSummaryQuery + " and EffectiveDate >= '" + startDate + "' and EffectiveDate <= '" + endDate + "' order by TableName, EffectiveDate";

                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                DailyDataLoadSummaryTO data = new DailyDataLoadSummaryTO
                                {
                                    TableName = reader["TableName"] as string,
                                    SchemaName = reader["SchemaName"] as string,
                                    EffectiveDate = reader.IsDBNull(reader.GetOrdinal("EffectiveDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate")),
                                    AsOfDate = reader.IsDBNull(reader.GetOrdinal("AsOfDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    DataCount = (reader.IsDBNull(reader.GetOrdinal("DataCount"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("DataCount")),
                                    ProcessName = reader["ProcessName"] as string,
                                    ComponentName = reader["ComponentName"] as string,
                                };
                                list.Add(data);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing query");
                throw;
            }
            return list;
        }

        public IList<PFICPositionDetailsTO> GetPFICPositionDetails(string startDate, string endDate, string FundName)
        {
            IList<PFICPositionDetailsTO> list = new List<PFICPositionDetailsTO>();
            try
            {
                string actualFundName = FundName == "Tac" ? "ALMITAS TACTICAL FUND LP" : (FundName == "Opp" ? "ALMITAS OPPORTUNITY FUND LP" : "");
                string sql = GetPFICPositionDetailsQuery + " where MonthEndDate1 >= '" + startDate + "' and MonthEndDate1 <= '" + endDate + "'";
                if (!string.IsNullOrEmpty(actualFundName))
                    sql += " and FundName = '" + actualFundName + "'";
                sql += " order by AsOfDate desc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                PFICPositionDetailsTO data = new PFICPositionDetailsTO
                                {
                                    AsOfDate = (reader.IsDBNull(reader.GetOrdinal("AsOfDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    FundName = reader["Fund"] as string,
                                    ReportHeader = reader["ReportHeader"] as string,
                                    Broker = reader["Broker"] as string,
                                    SecType = reader["SecType"] as string,
                                    SecDesc = reader["SecDesc"] as string,
                                    Symbol = reader["Symbol"] as string,
                                    SedolCusip = reader["SedolCusip"] as string,
                                    Curr = reader["Curr"] as string,
                                    LongShortInd = reader["LongShortInd"] as string,
                                    MonthDesc1 = reader["MonthDesc1"] as string,
                                    MonthEndDate1 = (reader.IsDBNull(reader.GetOrdinal("MonthEndDate1"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("MonthEndDate1")),
                                    Qty1 = (reader.IsDBNull(reader.GetOrdinal("Qty1"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Qty1")),
                                    QtyAdj1 = (reader.IsDBNull(reader.GetOrdinal("QtyAdj1"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("QtyAdj1")),
                                    AdjQty1 = (reader.IsDBNull(reader.GetOrdinal("AdjQty1"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AdjQty1")),
                                    Cost1 = (reader.IsDBNull(reader.GetOrdinal("Cost1"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Cost1")),
                                    Adjustments1 = (reader.IsDBNull(reader.GetOrdinal("Adjustments1"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Adjustments1")),
                                    AdjustedCost1 = (reader.IsDBNull(reader.GetOrdinal("AdjustedCost1"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AdjustedCost1")),
                                    AdjustedPrice1 = (reader.IsDBNull(reader.GetOrdinal("AdjustedPrice1"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AdjustedPrice1")),
                                    MV1 = (reader.IsDBNull(reader.GetOrdinal("MV1"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MV1")),
                                    PerClientMV1 = (reader.IsDBNull(reader.GetOrdinal("PerClientMV1"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PerClientMV1")),
                                    AdjustedMV1 = (reader.IsDBNull(reader.GetOrdinal("AdjustedMV1"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AdjustedMV1")),
                                    AdjustedPnL1 = (reader.IsDBNull(reader.GetOrdinal("AdjustedPnL1"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AdjustedPnL1")),
                                    MonthDesc2 = reader["MonthDesc2"] as string,
                                    MonthEndDate2 = (reader.IsDBNull(reader.GetOrdinal("MonthEndDate2"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("MonthEndDate2")),
                                    Qty2 = (reader.IsDBNull(reader.GetOrdinal("Qty2"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Qty2")),
                                    QtyAdj2 = (reader.IsDBNull(reader.GetOrdinal("QtyAdj2"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("QtyAdj2")),
                                    AdjQty2 = (reader.IsDBNull(reader.GetOrdinal("AdjQty2"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AdjQty2")),
                                    Cost2 = (reader.IsDBNull(reader.GetOrdinal("Cost2"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Cost2")),
                                    Adjustments2 = (reader.IsDBNull(reader.GetOrdinal("Adjustments2"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Adjustments2")),
                                    AdjustedCost2 = (reader.IsDBNull(reader.GetOrdinal("AdjustedCost2"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AdjustedCost2")),
                                    AdjustedPrice2 = (reader.IsDBNull(reader.GetOrdinal("AdjustedPrice2"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AdjustedPrice2")),
                                    MV2 = (reader.IsDBNull(reader.GetOrdinal("MV2"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MV2")),
                                    PerClientMV2 = (reader.IsDBNull(reader.GetOrdinal("PerClientMV2"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PerClientMV2")),
                                    AdjustedMV2 = (reader.IsDBNull(reader.GetOrdinal("AdjustedMV2"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AdjustedMV2")),
                                    AdjustedPnL2 = (reader.IsDBNull(reader.GetOrdinal("AdjustedPnL2"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AdjustedPnL2")),
                                    QtyChng = (reader.IsDBNull(reader.GetOrdinal("QtyChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("QtyChng")),
                                    CostChng = (reader.IsDBNull(reader.GetOrdinal("CostChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CostChng")),
                                    PriceChng = (reader.IsDBNull(reader.GetOrdinal("PriceChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceChng")),
                                    MVChng = (reader.IsDBNull(reader.GetOrdinal("MVChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MVChng")),
                                    QtyChngPct = (reader.IsDBNull(reader.GetOrdinal("QtyChngPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("QtyChngPct")),
                                    CostChngPct = (reader.IsDBNull(reader.GetOrdinal("CostChngPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CostChngPct")),
                                    PriceChngPct = (reader.IsDBNull(reader.GetOrdinal("PriceChngPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceChngPct")),
                                    MVChngPct = (reader.IsDBNull(reader.GetOrdinal("MVChngPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MVChngPct")),
                                    RealizedPnL = (reader.IsDBNull(reader.GetOrdinal("RealizedPnL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RealizedPnL")),
                                    ManualAdjustments = (reader.IsDBNull(reader.GetOrdinal("ManualAdjustments"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ManualAdjustments")),
                                    AdjRealizedPnL = (reader.IsDBNull(reader.GetOrdinal("AdjRealizedPnL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AdjRealizedPnL")),
                                    OtherIncome = (reader.IsDBNull(reader.GetOrdinal("OtherIncome"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OtherIncome")),
                                    UnrealizedPnLChng = (reader.IsDBNull(reader.GetOrdinal("UnrealizedPnLChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnrealizedPnLChng")),
                                    TotalPnL = (reader.IsDBNull(reader.GetOrdinal("TotalPnL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotalPnL")),
                                    Notes = reader["Notes"] as string,
                                    ProcessName = reader["ProcessName"] as string,

                                };
                                list.Add(data);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing PFIC position details query");
                throw;
            }
            return list;
        }

        private const string GetDividendScheduleQuery = "call almitasc_ACTradingBBGData.spGetExDividendSchedule";
        private const string GetCorporateActionsQuery = "call almitasc_ACTradingBBGData.spGetCorpActions";
        private const string GetSecurtiyDataErrorsQuery = "select"
            + " s.FIGI, s.Ticker, s.securitydescription as SecDesc, s.currency as Curr, s.yellowkey as YKey, fp.PortSource"
            + " , (case when PortSource = 'User' then UserPortDate when PortSource = 'BBG' then BBGPortDate else null end) as ALMPortDate"
            + " , sn.FundPortDate as BBGPortDate, fp.UserPortDate, s.SecurityType, s.Country, fp.NavEstimationMethod"
            + " , fp.Notes, fn.NavDate, fn.Nav, fn.NavPD, fn.NavSource, fn.NavFreq, fn.NavFreqType, fn.NavLag"
            + " , fn.BBGNavAdj, fn.BBGNavDate, fn.BBGNavCurrency, fn.BBGPD, fn.NumisPubNavAdj, fn.NumisNavDate, fn.NumisPDCalculated, fn.NumisNavCurrency"
            + " , fn.PHAdjNav, fn.PHNavDate, fn.PHPD, fn.PHCurrency, fn.CefaNav, fn.CefaNavDate, fn.CefaNavPD"
            + " , ifnull(u.ProxyFormula, fr.ProxyFormula) as ProxyFormula"
            + " from almitasc_ACTradingBBGLink.globaltrading_securitymaster s"
            + " left join almitasc_ACTradingBBGData.SecurityNAV sn on(s.figi = sn.figi)"
            + " left join almitasc_ACTradingBBGData.FundPortDate fp on(s.figi = fp.figi)"
            + " left join almitasc_ACTradingBBGData.FundLatestNAVNew fn on(s.figi = fn.figi)"
            + " left join almitasc_ACTradingBBGData.FundNavEstRule fr on(s.figi = fr.figi)"
            + " left join almitasc_ACTradingBBGData.UserOverrideNew u on(s.ticker = u.ticker)"
            + " where s.securitytype = 'Closed End Fund'"
            + " and ifnull(sn.marketstatus, s.marketstatus) = 'ACTV'"
            + " and s.yellowkey = 'Equity'"
            + " and s.country not in ('Bangladesh')"
            + " and s.paymentrank not in ('Preferred')";

        private const string GetALMFunctionsQuery = "select * from almitasc_ACTradingBBGLink.ALMFunc where 1=1";
        private const string GetALMFunctionCategoriesQuery = "select distinct FuncCategory from almitasc_ACTradingBBGLink.ALMFunc";
        private const string GetPortHoldingDataChecksQuery = "select * from almitasc_ACTradingBBGData.FundPortReturnsCheck where CheckFlag is not null";
        private const string GetManualDataOverridesQuery = "select * from almitasc_ACTradingBBGData.ManualDataOverride order by LastUpdateDate desc";
        private const string GetSwapMarginDetailsQuery = "select * from Primebrokerfiles.ALMSwapMarginDet where 1=1";
        private const string GetCryptoMarginDetailsQuery = "select * from Primebrokerfiles.ALMCryptoMarginDet where 1=1";
        private const string GetDailyDataLoadSummaryQuery = "select * from Reporting.DailyDataLoadSummary where 1=1";
        private const string GetNavDataOverridesQuery = "select * from almitasc_ACTradingBBGData.FundNavHist_MiscDataSource";
        private const string GetPFICPositionDetailsQuery = "select (case when FundName = 'ALMITAS TACTICAL FUND LP' then 'TAC' when FundName = 'ALMITAS OPPORTUNITY FUND LP' then 'OPP' else FundName end) as Fund, a.* from Primebrokerfiles.PFICPositionDet a";
    }
}
