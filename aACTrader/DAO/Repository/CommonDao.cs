using aACTrader.DAO.Interface;
using aCommons;
using aCommons.Cef;
using aCommons.DTO;
using aCommons.DTO.TenderHistory;
using aCommons.Utils;
using Microsoft.Extensions.Logging;
using Microsoft.VisualBasic;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Text;

namespace aACTrader.DAO.Repository
{
    public class CommonDao : ICommonDao
    {
        private readonly ILogger<CommonDao> _logger;
        private const string DELIMITER = ",";
        private const string DATEFORMAT = "yyyy-MM-dd";
        public CommonDao(ILogger<CommonDao> logger, MySqlConnection connection)
        {
            _logger = logger;
            _logger.LogInformation("Initializing CommonDao...");
        }

        public IList<BuyBackSummaryTO> GetBuyBackSummary()
        {
            IList<BuyBackSummaryTO> buybacklist = new List<BuyBackSummaryTO>();
            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetBuyBackSummaryQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                BuyBackSummaryTO buyback = new BuyBackSummaryTO
                                {
                                    Ticker = reader["Ticker"] as string,
                                    Currency = reader["Currency"] as string,
                                    LastDate = reader.IsDBNull(reader.GetOrdinal("LastDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("LastDate")),
                                    SharesLast10D = (reader.IsDBNull(reader.GetOrdinal("SharesLast10D"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("SharesLast10D")),
                                    AvgPriceLast10D = (reader.IsDBNull(reader.GetOrdinal("AvgPriceLast10D"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AvgPriceLast10D")),
                                    FreqLast10D = (reader.IsDBNull(reader.GetOrdinal("FreqLast10D"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FreqLast10D")),
                                    SharesLast1M = (reader.IsDBNull(reader.GetOrdinal("SharesLast1M"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("SharesLast1M")),
                                    AvgPriceLast1M = (reader.IsDBNull(reader.GetOrdinal("AvgPriceLast1M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AvgPriceLast1M")),
                                    FreqLast1M = (reader.IsDBNull(reader.GetOrdinal("FreqLast1M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FreqLast1M")),
                                    SharesLast3M = (reader.IsDBNull(reader.GetOrdinal("SharesLast3M"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("SharesLast3M")),
                                    AvgPriceLast3M = (reader.IsDBNull(reader.GetOrdinal("AvgPriceLast3M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AvgPriceLast3M")),
                                    FreqLast3M = (reader.IsDBNull(reader.GetOrdinal("FreqLast3M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FreqLast3M")),
                                    SharesLast6M = (reader.IsDBNull(reader.GetOrdinal("SharesLast6M"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("SharesLast6M")),
                                    AvgPriceLast6M = (reader.IsDBNull(reader.GetOrdinal("AvgPriceLast6M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AvgPriceLast6M")),
                                    FreqLast6M = (reader.IsDBNull(reader.GetOrdinal("FreqLast6M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FreqLast6M")),
                                    SharesLast1Y = (reader.IsDBNull(reader.GetOrdinal("SharesLast1Y"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("SharesLast1Y")),
                                    AvgPriceLast1Y = (reader.IsDBNull(reader.GetOrdinal("AvgPriceLast1Y"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AvgPriceLast1Y")),
                                    FreqLast1Y = (reader.IsDBNull(reader.GetOrdinal("FreqLast1Y"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FreqLast1Y")),
                                    SharesLast3Y = (reader.IsDBNull(reader.GetOrdinal("SharesLast3Y"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("SharesLast3Y")),
                                    AvgPriceLast3Y = (reader.IsDBNull(reader.GetOrdinal("AvgPriceLast3Y"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AvgPriceLast3Y")),
                                    FreqLast3Y = (reader.IsDBNull(reader.GetOrdinal("FreqLast3Y"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FreqLast3Y")),
                                    SharesAll = (reader.IsDBNull(reader.GetOrdinal("SharesAll"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("SharesAll")),
                                    AvgPriceAll = (reader.IsDBNull(reader.GetOrdinal("AvgPriceAll"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AvgPriceAll")),
                                    Count = (reader.IsDBNull(reader.GetOrdinal("Count"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("Count")),
                                    MinEffDate = reader.IsDBNull(reader.GetOrdinal("MinEffDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("MinEffDate")),
                                    MaxEffDate = reader.IsDBNull(reader.GetOrdinal("MaxEffDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("MaxEffDate")),
                                };

                                buybacklist.Add(buyback);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing buyback ");
                throw;
            }

            return buybacklist;
        }

        public IDictionary<string, FundNav> GetFundNavs()
        {
            IDictionary<string, FundNav> fundNavDict = new Dictionary<string, FundNav>(StringComparer.CurrentCultureIgnoreCase);
            string ticker = string.Empty;
            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetFundNavsQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                ticker = reader["Ticker"] as string;
                                FundNav fundNav = new FundNav
                                {
                                    Ticker = reader["Ticker"] as string,
                                    EquitySharesOutstanding = (reader.IsDBNull(reader.GetOrdinal("EquitySharesOutstanding"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EquitySharesOutstanding")),
                                    PriceCurrency = reader["PriceCurrency"] as string,

                                    //Numis Navs
                                    NumisNavCurrency = reader["NumisNavCurrency"] as string,
                                    NumisPublishedNavDate = reader.IsDBNull(reader.GetOrdinal("NumisNavDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("NumisNavDate")),
                                    NumisPublishedNavReported = (reader.IsDBNull(reader.GetOrdinal("NumisPubNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NumisPubNav")),
                                    NumisPublishedNavAdjusted = (reader.IsDBNull(reader.GetOrdinal("NumisPubNavAdj"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NumisPubNavAdj")),
                                    NumisEstimatedNavReported = (reader.IsDBNull(reader.GetOrdinal("NumisEstNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NumisEstNav")),
                                    NumisEstimatedNavAdjusted = (reader.IsDBNull(reader.GetOrdinal("NumisEstNavAdj"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NumisEstNavAdj")),
                                    NumisPDReported = (reader.IsDBNull(reader.GetOrdinal("NumisPD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NumisPD")),
                                    NumisPDCalculated = (reader.IsDBNull(reader.GetOrdinal("NumisPDCalculated"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NumisPDCalculated")),

                                    //PeelHunt Navs
                                    PHNavCurrency = reader["PHCurrency"] as string,
                                    PHPublishedNavDate = reader.IsDBNull(reader.GetOrdinal("PHNavDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("PHNavDate")),
                                    PHPublishedNavReported = (reader.IsDBNull(reader.GetOrdinal("PHNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PHNav")),
                                    PHPublishedNavAdjusted = (reader.IsDBNull(reader.GetOrdinal("PHAdjNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PHAdjNav")),
                                    PHPDReported = (reader.IsDBNull(reader.GetOrdinal("PHPD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PHPD")),

                                    //BBG Navs
                                    BBGNavCurrency = reader["BBGNavCurrency"] as string,
                                    BBGPublishedNavDate = reader.IsDBNull(reader.GetOrdinal("BBGNavDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("BBGNavDate")),
                                    BBGPublishedNavReported = (reader.IsDBNull(reader.GetOrdinal("BBGNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BBGNav")),
                                    BBGPublishedNavAdjusted = (reader.IsDBNull(reader.GetOrdinal("BBGNavAdj"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BBGNavAdj")),
                                    BBGPDReported = (reader.IsDBNull(reader.GetOrdinal("BBGPD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BBGPD")),

                                    //Final Nav (after applying Nav hierarchy)
                                    LastNav = (reader.IsDBNull(reader.GetOrdinal("Nav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav")),
                                    OvrLastNav = (reader.IsDBNull(reader.GetOrdinal("OvrNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OvrNav")),
                                    OvrLastNavDate = reader.IsDBNull(reader.GetOrdinal("OvrNavDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("OvrNavDate")),
                                    OvrEstimatedNav = (reader.IsDBNull(reader.GetOrdinal("OvrEstimatedNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OvrEstimatedNav")),
                                    LastPctPremium = (reader.IsDBNull(reader.GetOrdinal("NavPD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NavPD")),
                                    LastNavDate = reader.IsDBNull(reader.GetOrdinal("NavDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("NavDate")),
                                    LastNavSource = reader["NavSource"] as string,
                                    NavUpdateTime = reader.IsDBNull(reader.GetOrdinal("NavUpdateTime")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("NavUpdateTime")),

                                    //Intrinsic Value
                                    IntrinsicValue = (reader.IsDBNull(reader.GetOrdinal("IntrinsicValue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IntrinsicValue")),

                                    //Short Rebate Rate
                                    ShortRebateRateQty = (reader.IsDBNull(reader.GetOrdinal("ShortRebateRateQty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ShortRebateRateQty")),
                                    ShortRebateRate = (reader.IsDBNull(reader.GetOrdinal("ShortRebateRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ShortRebateRate")),
                                    ShortRebateRateDate = (reader.IsDBNull(reader.GetOrdinal("ShortRebateRateDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("ShortRebateRateDate"))
                                };

                                if (!fundNavDict.TryGetValue(fundNav.Ticker, out FundNav fundNavLookup))
                                    fundNavDict.Add(fundNav.Ticker, fundNav);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing query: " + ticker);
                throw;
            }

            return fundNavDict;
        }

        public IList<RegressionCoefficient> GetETFRegressionCoefficients(string ticker)
        {
            IList<RegressionCoefficient> regressionCoefficients = new List<RegressionCoefficient>();
            try
            {
                string sql = GetRegressionCoefficientsQuery;

                if (!string.IsNullOrEmpty(ticker))
                    sql += " where fund_ticker = '" + ticker + "' order by period";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                RegressionCoefficient regressionCoefficient = new RegressionCoefficient
                                {
                                    Ticker = reader["fund_ticker"] as string,
                                    Period = reader["period"] as string,
                                    RSqrd = reader["rsqrd"] as double? ?? default(double),
                                    ETFTicker = reader["etf_ticker"] as string,
                                    Beta = reader["beta"] as double? ?? default(double)
                                };
                                regressionCoefficients.Add(regressionCoefficient);
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
            return regressionCoefficients;
        }

        public IList<FundHoldingsReturn> GetFundHoldingsReturn()
        {
            IList<FundHoldingsReturn> list = new List<FundHoldingsReturn>();
            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetPortfolioNavsQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundHoldingsReturn data = new FundHoldingsReturn
                                {
                                    Ticker = reader["Ticker"] as string,
                                    TotalReturn = (reader.IsDBNull(reader.GetOrdinal("TotalRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotalRtn")),
                                    TotalRtnExFIRtn = (reader.IsDBNull(reader.GetOrdinal("TotalRtnExFIRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotalRtnExFIRtn")),
                                    TotalReturnProxy = (reader.IsDBNull(reader.GetOrdinal("TotalRtnProxy"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotalRtnProxy")),
                                    EquityReturn = (reader.IsDBNull(reader.GetOrdinal("EqRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EqRtn")),
                                    FIReturn = (reader.IsDBNull(reader.GetOrdinal("FIRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FIRtn")),
                                    CashReturn = reader.IsDBNull(reader.GetOrdinal("CashRtn")) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CashRtn")),
                                    FXHedgeReturn = reader.IsDBNull(reader.GetOrdinal("FXHedgeRtn")) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXHedgeRtn")),
                                    Weight = (reader.IsDBNull(reader.GetOrdinal("Wt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Wt")),
                                    EquityWeight = (reader.IsDBNull(reader.GetOrdinal("EqWt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EqWt")),
                                    FIWeight = (reader.IsDBNull(reader.GetOrdinal("FIWt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FIWt")),
                                    CashWeight = (reader.IsDBNull(reader.GetOrdinal("CashWt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CashWt")),
                                    FXHedgeWeight = (reader.IsDBNull(reader.GetOrdinal("FXHedgeWt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXHedgeWt")),
                                    EquityPriceCoverage = (reader.IsDBNull(reader.GetOrdinal("EqPriceCoverage"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EqPriceCoverage")),
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

        public IList<MSFundHistory> GetMSFundHistory(string Ticker, string StartDate, string EndDate)
        {
            IList<MSFundHistory> msFundHistoryList = new List<MSFundHistory>();
            try
            {
                DateTime currentTime = DateTime.Now;
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    DateTime startDate = Convert.ToDateTime(StartDate);
                    DateTime endDate = Convert.ToDateTime(EndDate);

                    string startDateFormatted = String.Format("{0:yyyy-MM-dd}", startDate);
                    string endDateFormatted = String.Format("{0:yyyy-MM-dd}", endDate);

                    string sql = GetMSFundHistoryQuery + " where s.Ticker = '" + Ticker + "' and m.date >= '" + startDateFormatted + "' and m.Date <= '" + endDateFormatted + "'";
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                MSFundHistory msFundHistory = new MSFundHistory
                                {
                                    Ticker = reader["Ticker"] as string,
                                    SecId = reader["SecId"] as string,
                                    EffectiveDate = reader.IsDBNull(reader.GetOrdinal("Date")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("Date")),
                                    Price = (reader.IsDBNull(reader.GetOrdinal("Price"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price")),
                                    Nav = (reader.IsDBNull(reader.GetOrdinal("Nav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav")),
                                    Dividend = (reader.IsDBNull(reader.GetOrdinal("Dvd"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Dvd")),
                                    Discount = reader.IsDBNull(reader.GetOrdinal("Discount")) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Discount")),
                                    PriceReturn = (reader.IsDBNull(reader.GetOrdinal("TrrPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TrrPrice")),
                                    NavReturn = (reader.IsDBNull(reader.GetOrdinal("TrrNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TrrNav"))
                                };

                                msFundHistoryList.Add(msFundHistory);
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

            return msFundHistoryList;
        }

        public IList<MSExpenseRatio> GetMSExpenseHistory(string Ticker)
        {
            IList<MSExpenseRatio> msExpenseHistoryList = new List<MSExpenseRatio>();
            try
            {
                DateTime currentTime = DateTime.Now;
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    string sql = GetMSExpenseHistoryQuery + " where s.Ticker = '" + Ticker + "'";
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                MSExpenseRatio msExpenseHistory = new MSExpenseRatio
                                {
                                    Ticker = reader["Ticker"] as string,
                                    SecId = reader["SecId"] as string,
                                    Year = (reader.IsDBNull(reader.GetOrdinal("YrDate"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("YrDate")),
                                    ExpenseRatio = (reader.IsDBNull(reader.GetOrdinal("ExpenseRatio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExpenseRatio"))
                                };
                                msExpenseHistoryList.Add(msExpenseHistory);
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

            return msExpenseHistoryList;
        }

        public IDictionary<string, FundHistStats> GetFundStats()
        {
            IDictionary<string, FundHistStats> fundStatsDict = new Dictionary<string, FundHistStats>(StringComparer.CurrentCultureIgnoreCase);
            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetFundStatsQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                string ticker = reader["Ticker"] as string;
                                FundHistStats fundStats = new FundHistStats
                                {
                                    Ticker = ticker.Trim(),
                                    Stats1W = reader["Stat1W"] as string,
                                    Stats2W = reader["Stat2W"] as string,
                                    Stats1M = reader["Stat1M"] as string,
                                    Stats3M = reader["Stat3M"] as string,
                                    Stats6M = reader["Stat6M"] as string,
                                    Stats12M = reader["Stat12M"] as string,
                                    Stats24M = reader["Stat24M"] as string,
                                    Stats36M = reader["Stat36M"] as string,
                                    Stats60M = reader["Stat60M"] as string,
                                    StatsLife = reader["StatLife"] as string,
                                    StatsLastUpdate = reader.IsDBNull(reader.GetOrdinal("LastDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("LastDate"))
                                };

                                fundStats.StatsLastUpdateAsString = DateUtils.ConvertDate(fundStats.StatsLastUpdate, "yyyy-MM-dd");
                                if (!fundStatsDict.TryGetValue(ticker, out FundHistStats fundHistStatsLookup))
                                    fundStatsDict.Add(fundStats.Ticker, fundStats);
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

            return fundStatsDict;
        }


        public IDictionary<string, FundMaster> GetFundMaster()
        {
            IDictionary<string, FundMaster> fundMasterDict = new Dictionary<string, FundMaster>(StringComparer.CurrentCultureIgnoreCase);
            string ticker = string.Empty;

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetFundMasterQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                ticker = reader["Ticker"] as string;
                                int displayFlag = reader.GetInt16("DisplayFlag");
                                if (displayFlag == 1)
                                {
                                    FundMaster fundMaster = new FundMaster
                                    {
                                        FIGI = reader["FIGI"] as string,
                                        Ticker = reader["Ticker"] as string,
                                        ISIN = reader["ISIN"] as string,
                                        YKey = reader["YellowKey"] as string,
                                        Name = reader["SecurityDescription"] as string,
                                        Curr = reader["Currency"] as string,
                                        Cntry = reader["Country"] as string,
                                        GeoLvl1 = reader["GeoLevel1"] as string,
                                        GeoLvl2 = reader["GeoLevel2"] as string,
                                        GeoLvl3 = reader["GeoLevel3"] as string,
                                        AssetLvl1 = reader["AssetClassLevel1"] as string,
                                        AssetLvl2 = reader["AssetClassLevel2"] as string,
                                        AssetLvl3 = reader["AssetClassLevel3"] as string,
                                        Status = reader["MarketStatus"] as string,
                                        PayRank = reader["PaymentRank"] as string,
                                        AssetTyp = reader["AssetType"] as string,
                                        NonCEFTyp = reader["NonCEFType"] as string,
                                        SecTyp = reader["SecurityType"] as string,
                                        CntryCd = reader["CountryCode"] as string,
                                        CEFTyp = reader["CefInstrumentType"] as string,
                                        ParentComp = reader["ParentCompany"] as string,
                                        CorpBkr = reader["CorpBroker"] as string,
                                        PosId = reader["PositionId"] as string,
                                        Sec1940Act = reader["Sec1940Act"] as string,
                                        ALMPNotes = reader["ALMPortNotes"] as string,
                                        IRRModel = reader["IRRModel"] as string,
                                        IncDt = (reader.IsDBNull(reader.GetOrdinal("InceptionDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("InceptionDate")),
                                        TermDt = (reader.IsDBNull(reader.GetOrdinal("TerminationDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("TerminationDate")),
                                        NavPortDt = (reader.IsDBNull(reader.GetOrdinal("NavPortDate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NavPortDate")),
                                        LevRatio = (reader.IsDBNull(reader.GetOrdinal("LeverageRatio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LeverageRatio")),
                                        ExpRatio = (reader.IsDBNull(reader.GetOrdinal("ExpenseRatio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExpenseRatio")),
                                        RedExtraAdj = (reader.IsDBNull(reader.GetOrdinal("RedempExtraAdjustment"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RedempExtraAdjustment")),
                                        ExpRatioSrc = reader["ExpenseRatioSrc"] as string,
                                        LevRatioSrc = reader["LeverageRatioSrc"] as string,
                                        NavEstMd = reader["NavEstimationMethod"] as string,
                                        ProxyForm = reader["ProxyFormula"] as string,
                                        AltProxyForm = reader["AltProxyFormula"] as string,
                                        PortFIProxyForm = reader["FIProxyFormula"] as string,
                                        BoardVotPeriod = reader["BoardVotingPeriod"] as string,
                                        BoardVotType = reader["BoardVotingType"] as string,
                                        CorpStruct = reader["CorpStructure"] as string,
                                        ControlShares = reader["ControlShares"] as string,
                                        ContestedElectionOvr = reader["ContestedElectionOverride"] as string,
                                        VotingPrefShares = reader["VotingPrefShares"] as string,
                                        BoardType = reader["BoardType"] as string,
                                        DvdCov = (reader.IsDBNull(reader.GetOrdinal("DividendCoverage"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DividendCoverage")),
                                        DvdTrail12M = (reader.IsDBNull(reader.GetOrdinal("DvdTrailing12M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DvdTrailing12M")),
                                        DvdFcst12M = (reader.IsDBNull(reader.GetOrdinal("DvdForecast12M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DvdForecast12M")),
                                        DvdTrail12MALM = (reader.IsDBNull(reader.GetOrdinal("TTM"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TTM")),
                                        DvdFcst = (reader.IsDBNull(reader.GetOrdinal("DividendForecast"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DividendForecast")),

                                        //Model Scores
                                        MdlScore = (reader.IsDBNull(reader.GetOrdinal("ModelScore"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ModelScore")),
                                        MdlUnderEstRt = (reader.IsDBNull(reader.GetOrdinal("ModelUnderEstimationRatio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ModelUnderEstimationRatio")),
                                        MdlUnderEst = (reader.IsDBNull(reader.GetOrdinal("ModelUnderEstimation"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ModelUnderEstimation")),
                                        MdlOverEstRt = (reader.IsDBNull(reader.GetOrdinal("ModelOverEstimationRatio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ModelOverEstimationRatio")),
                                        MdlOverEst = (reader.IsDBNull(reader.GetOrdinal("ModelOverEstimation"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ModelOverEstimation")),

                                        //CEFA
                                        UNII = (reader.IsDBNull(reader.GetOrdinal("UNII"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UNII")),
                                        UNIITrendPct = (reader.IsDBNull(reader.GetOrdinal("UNIITrendPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UNIITrendPct")),
                                        EarningsYld = (reader.IsDBNull(reader.GetOrdinal("EarningsYield"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EarningsYield")),
                                        NavYld = (reader.IsDBNull(reader.GetOrdinal("NavYield"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NavYield")),
                                        EarningsCovRatio = (reader.IsDBNull(reader.GetOrdinal("EarningsCoverageRatio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EarningsCoverageRatio")),
                                        UNIITrend = reader["UNIITrend"] as string,
                                        DistrPolicy = reader["DistributionPolicy"] as string,
                                        DistrFreq = reader["DistributionFrequency"] as string,
                                        MuniState = reader["MuniState"] as string,
                                        BBGNavCorrFlag = reader["BBGNavCorrectFlag"] as string,

                                        ExpSrSecFirstLien = (reader.IsDBNull(reader.GetOrdinal("ExpSrSecuredFirstLien"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExpSrSecuredFirstLien")),
                                        ExpSrSubordSecondLien = (reader.IsDBNull(reader.GetOrdinal("ExpSrSubordinatedSecondLien"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExpSrSubordinatedSecondLien")),
                                        ExpUnsecuredSubord = (reader.IsDBNull(reader.GetOrdinal("ExpUnsecuredSubordinated"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExpUnsecuredSubordinated")),
                                        ExpCommonPfdEquity = (reader.IsDBNull(reader.GetOrdinal("ExpCommonPreferredEquity"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExpCommonPreferredEquity")),
                                        ExpWarrantsOptions = (reader.IsDBNull(reader.GetOrdinal("ExpWarrantsOptions"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExpWarrantsOptions")),
                                        ExpStrPrdOther = (reader.IsDBNull(reader.GetOrdinal("ExpStructuredProducsOther"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExpStructuredProducsOther")),
                                        ExpFI = (reader.IsDBNull(reader.GetOrdinal("ExpFixedIncome"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExpFixedIncome")),
                                        ExpEquity = (reader.IsDBNull(reader.GetOrdinal("ExpEquity"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExpEquity")),

                                        ExpLvl1Sec = (reader.IsDBNull(reader.GetOrdinal("ExpLevel1Securities"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExpLevel1Securities")),
                                        ExpLvl2Sec = (reader.IsDBNull(reader.GetOrdinal("ExpLevel2Securities"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExpLevel2Securities")),
                                        ExpLvl3Sec = (reader.IsDBNull(reader.GetOrdinal("ExpLevel3Securities"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExpLevel3Securities")),
                                        CefaPortDt = (reader.IsDBNull(reader.GetOrdinal("CefaPortDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("CefaPortDate")),
                                        CefaStateIncorp = reader["CefaStateIncorporated"] as string,
                                        CefaMarylandAct = reader["CefaMarylandAct"] as string,
                                        CefaStaggeredBoard = reader["CefaStaggeredBoard"] as string,
                                        CefaLevRatio = (reader.IsDBNull(reader.GetOrdinal("CefaLev"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CefaLev")),
                                        CefaAdjLevRatio = (reader.IsDBNull(reader.GetOrdinal("CefaAdjLev"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CefaAdjLev")),

                                        CreditQuality = reader["CefaCreditQuality"] as string,
                                        CreditQualityRBO = reader["CefaCreditQualityRBO"] as string,
                                        UnratedBondsPct = (reader.IsDBNull(reader.GetOrdinal("CefaUnratedBondsPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CefaUnratedBondsPct")),
                                        InvestmentGradePct = (reader.IsDBNull(reader.GetOrdinal("CefaInvestmentGradePct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CefaInvestmentGradePct")),
                                        NonInvestmentGradePct = (reader.IsDBNull(reader.GetOrdinal("CefaNonInvestmentGradePct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CefaNonInvestmentGradePct")),
                                        RatedAAA = (reader.IsDBNull(reader.GetOrdinal("CefaAAA"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CefaAAA")),
                                        RatedAA = (reader.IsDBNull(reader.GetOrdinal("CefaAA"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CefaAA")),
                                        RatedA = (reader.IsDBNull(reader.GetOrdinal("CefaA"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CefaA")),
                                        RatedBBB = (reader.IsDBNull(reader.GetOrdinal("CefaBBB"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CefaBBB")),
                                        RatedBB = (reader.IsDBNull(reader.GetOrdinal("CefaBB"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CefaBB")),
                                        RatedB = (reader.IsDBNull(reader.GetOrdinal("CefaB"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CefaB")),
                                        RatedCCC = (reader.IsDBNull(reader.GetOrdinal("CefaCCC"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CefaCCC")),
                                        RatedCC = (reader.IsDBNull(reader.GetOrdinal("CefaCC"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CefaCC")),
                                        RatedC = (reader.IsDBNull(reader.GetOrdinal("CefaCC"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CefaCC")),
                                        RatedD = (reader.IsDBNull(reader.GetOrdinal("CefaD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CefaD")),
                                        WARF = (reader.IsDBNull(reader.GetOrdinal("CefaWARF"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CefaWARF")),

                                        DvdChngDt = (reader.IsDBNull(reader.GetOrdinal("DvdChngDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("DvdChngDate")),
                                        DvdChngPct = (reader.IsDBNull(reader.GetOrdinal("DvdChngPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DvdChngPct")),
                                        DvdChng = reader["DvdChng"] as string,

                                        EarningsDt = (reader.IsDBNull(reader.GetOrdinal("EarningsDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EarningsDate")),
                                        UNIIDt = (reader.IsDBNull(reader.GetOrdinal("UNIIDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("UNIIDate")),
                                        RelUNII = (reader.IsDBNull(reader.GetOrdinal("RelUNII"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RelUNII")),
                                        UNIIFreq = reader["UNIIFrequency"] as string,
                                        NII = (reader.IsDBNull(reader.GetOrdinal("NII"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NII")),
                                        ShOut = (reader.IsDBNull(reader.GetOrdinal("SharesOutstanding"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SharesOutstanding")),
                                        NavFreq = reader["NavFreq"] as string,
                                        NavFreqType = reader["NavUpdateFreqType"] as string,
                                        CalcExpAlpha = reader.GetInt16("CalcEAFlag"),
                                        CalcPDStats = reader.GetInt16("CalcPDStats"),

                                        //Numis
                                        NumisKeyHldgs = reader["NumisKeyHoldings"] as string,
                                        NumisExpRatioExLev = (reader.IsDBNull(reader.GetOrdinal("NumisExpRatioExLev"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NumisExpRatioExLev")),
                                        NumisExpRatioExLevExPerfFee = (reader.IsDBNull(reader.GetOrdinal("NumisExpRatioExLevExPerfFee"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NumisExpRatioExLevExPerfFee")),
                                        NumisEffectiveGearing = (reader.IsDBNull(reader.GetOrdinal("NumisEffectiveGearing"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NumisEffectiveGearing")),
                                        NumisGearingLimit = reader["NumisGearingLimit"] as string,
                                        NumisCurrHedge = reader["NumisFXHedge"] as string,
                                        NumisBuybackPolicy = reader["NumisBuybackPolicy"] as string,
                                        NumisBuybackPolicyDetails = reader["NumisBuybackPolicyDetails"] as string,
                                        NumisPDDiscountTarget = reader["NumisPDDiscountTarget"] as string,
                                        NumisBuybacks = (reader.IsDBNull(reader.GetOrdinal("NumisBuybacks"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("NumisBuybacks")),
                                        NumisTenders = (reader.IsDBNull(reader.GetOrdinal("NumisTenders"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("NumisTenders")),
                                        NumisContVoteDate = reader["NumisContinuationVoteDate"] as string,
                                        NumisContVoteDetails = reader["NumisContinuationVoteDetails"] as string,
                                        NumisBroker = reader["NumisBroker"] as string,
                                        NumisKeyHolders = reader["NumisKeyHolders"] as string,
                                        NumisNotes = reader["NumisNotes"] as string,
                                        NumisListing = reader["NumisListing"] as string,
                                        NumisDomicile = reader["NumisDomicile"] as string,
                                        NumisFundManager = reader["NumisFundManager"] as string,
                                        NumisCoreInvstStrategy = reader["NumisCoreInvestmentStrategy"] as string,
                                        NumisBaseFee = reader["NumisBaseFee"] as string,
                                        NumisBaseFeeBasedOn = reader["NumisBaseFeeBasedOn"] as string,
                                        NumisPerfFeeOn = reader["NumisPerfFeeOn"] as string,
                                        NumisPerfFeeBasedOn = reader["NumisPerfFeeBasedOn"] as string,
                                        NumisPerfFeeTimePeriod = reader["NumisPerfFeeTimePeriod"] as string,
                                        NumisFXHedge = reader["NumisFXHedge"] as string,
                                        NumisGeoExpUK = (reader.IsDBNull(reader.GetOrdinal("NumisGeoExpUK"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NumisGeoExpUK")),
                                        NumisGeoExpEurope = (reader.IsDBNull(reader.GetOrdinal("NumisGeoExpEurope"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NumisGeoExpEurope")),
                                        NumisGeoExpNA = (reader.IsDBNull(reader.GetOrdinal("NumisGeoExpNorthAmerica"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NumisGeoExpNorthAmerica")),
                                        NumisGeoExpJapan = (reader.IsDBNull(reader.GetOrdinal("NumisGeoExpJapan"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NumisGeoExpJapan")),
                                        NumisGeoExpOtherAsiaPac = (reader.IsDBNull(reader.GetOrdinal("NumisGeoExpOtherAsiaPacific"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NumisGeoExpOtherAsiaPacific")),
                                        NumisGeoExpOtherEmg = (reader.IsDBNull(reader.GetOrdinal("NumisGeoExpOtherEmg"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NumisGeoExpOtherEmg")),
                                        NumisGeoExpGlobal = (reader.IsDBNull(reader.GetOrdinal("NumisGeoExpGlobal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NumisGeoExpGlobal")),
                                        NumisAssetExpListedEqPct = (reader.IsDBNull(reader.GetOrdinal("NumisAssetExpListedEquitiesPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NumisAssetExpListedEquitiesPct")),
                                        NumisAssetExpUnlistedEqPct = (reader.IsDBNull(reader.GetOrdinal("NumisAssetExpUnlistedEquitiesPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NumisAssetExpUnlistedEquitiesPct")),
                                        NumisAssetExpBondsPct = (reader.IsDBNull(reader.GetOrdinal("NumisAssetExpBondsPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NumisAssetExpBondsPct")),
                                        NumisAssetExpPropertyPct = (reader.IsDBNull(reader.GetOrdinal("NumisAssetExpPropertyPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NumisAssetExpPropertyPct")),
                                        NumisAssetExpHedgeFundsPct = (reader.IsDBNull(reader.GetOrdinal("NumisAssetExpHedgeFundsPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NumisAssetExpHedgeFundsPct")),

                                        MFStructTyp = reader["MutualFundStructureType"] as string,
                                        StateOfIncorp = reader["StateOfIncorporation"] as string,
                                        MCSAA = reader["MCSAA"] as string,
                                        SSParty = reader["StandstillParty"] as string,
                                        SSDur = reader["StandstillDuration"] as string,
                                        SSEndDt = reader["StandstillEndDate"] as string,
                                        SSStartDt = (reader.IsDBNull(reader.GetOrdinal("StandstillStartDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("StandstillStartDate")),

                                        //Previous Day Nav Estimates
                                        PDPubAdjNav = (reader.IsDBNull(reader.GetOrdinal("PDPublishedAdjNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PDPublishedAdjNav")),
                                        PDPubNavDt = (reader.IsDBNull(reader.GetOrdinal("PDPublishedNavDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("PDPublishedNavDate")),
                                        PDNavEstDt = (reader.IsDBNull(reader.GetOrdinal("PDNavEstimationDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("PDNavEstimationDate")),
                                        PDNavEstMethod = reader["PDNavEstimationMethod"] as string,
                                        PDPortAdjRtn = (reader.IsDBNull(reader.GetOrdinal("PDPortAdjustedReturn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PDPortAdjustedReturn")),
                                        PDETFRtn = (reader.IsDBNull(reader.GetOrdinal("PDETFReturn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PDETFReturn")),
                                        PDProxyRtn = (reader.IsDBNull(reader.GetOrdinal("PDProxyReturn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PDProxyReturn")),
                                        PDEstRtn = (reader.IsDBNull(reader.GetOrdinal("PDEstimatedReturn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PDEstimatedReturn")),
                                        PDPortNav = (reader.IsDBNull(reader.GetOrdinal("PDPortNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PDPortNav")),
                                        PDETFNav = (reader.IsDBNull(reader.GetOrdinal("PDETFNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PDETFNav")),
                                        PDProxyNav = (reader.IsDBNull(reader.GetOrdinal("PDProxyNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PDProxyNav")),
                                        PDEstNav = (reader.IsDBNull(reader.GetOrdinal("PDEstimatedNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PDEstimatedNav")),
                                        PDDscntToLastPrc = (reader.IsDBNull(reader.GetOrdinal("PDDscntToLastPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PDDscntToLastPrice")),

                                        NeedPH = reader["NeedPort"] as string,
                                        NeedReg = reader["NeedRegression"] as string,

                                        FundRedempDt = (reader.IsDBNull(reader.GetOrdinal("FundRedemptionDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FundRedemptionDate")),
                                        FundManager = reader["FundManager"] as string,

                                        //BDC Columns
                                        FundMgmtStruct = reader["ManagedInternally"] as string,
                                        InclCash = reader["IncludesCash"] as string,
                                        ExclCash = reader["ExcludesCash"] as string,
                                        BaseMgmtFee = (reader.IsDBNull(reader.GetOrdinal("BaseMgmtFee"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BaseMgmtFee")),
                                        PerfFeeExclInc = (reader.IsDBNull(reader.GetOrdinal("PerfFeeExclIncome"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PerfFeeExclIncome")),
                                        CapGainsPerfFee = (reader.IsDBNull(reader.GetOrdinal("CapitalGainsPerfFee"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CapitalGainsPerfFee")),
                                        HurdleRt = (reader.IsDBNull(reader.GetOrdinal("HurdleRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("HurdleRate")),
                                        TotRtnHurdle = reader["TotalReturnHurdle"] as string,
                                        CatchUpRt = (reader.IsDBNull(reader.GetOrdinal("CatchUp"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CatchUp")),
                                        CatchUpProv = reader["CatchUpProvision"] as string,
                                        CatchUpLowerBound = (reader.IsDBNull(reader.GetOrdinal("CatchUpLowerBound"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CatchUpLowerBound")),
                                        CatchUpUpperBound = (reader.IsDBNull(reader.GetOrdinal("CatchUpUpperBound"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CatchUpUpperBound")),
                                        PrevTicker = reader["PreviousTicker"] as string,
                                        PrimInvstTyp = reader["PrimaryInvestmentType"] as string,
                                        FundNotes = reader["FundNotes"] as string,
                                        MgmtFeeOtherProv = reader["MgmtFeeOtherProvisions"] as string,
                                        NetInvstIncRt = (reader.IsDBNull(reader.GetOrdinal("NetInvestmentIncomeRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NetInvestmentIncomeRate")),
                                        NetCapGainsRt = (reader.IsDBNull(reader.GetOrdinal("NetCapitalGainsRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NetCapitalGainsRate")),
                                        FeeWaiver = reader["FeeWaiver"] as string,
                                        TieredBaseMgmtFee = (reader.IsDBNull(reader.GetOrdinal("TieredBaseMgmtFee"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TieredBaseMgmtFee")),
                                        TieredFeeLevLvl = (reader.IsDBNull(reader.GetOrdinal("TieredFeeLeverageLevel"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TieredFeeLeverageLevel")),

                                        //Jefferies
                                        JeffPotEvtCatalyst = reader["JeffPotentialEventCatalyst"] as string,
                                        JeffWindupProv = reader["JeffWindupProvisions"] as string,
                                        JeffWindupProvCd = reader["JeffWindupProvisionCode"] as string,
                                        JeffLifeTriggEvtDt = (reader.IsDBNull(reader.GetOrdinal("JeffLifeTriggerEventDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("JeffLifeTriggerEventDate")),
                                        JeffOtherEvts = reader["JeffOtherEvents"] as string,
                                        JeffOtherEvtsCd = reader["JeffOtherEventsCode"] as string,
                                        JeffEvtTriggDt = (reader.IsDBNull(reader.GetOrdinal("JeffEventTriggerDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("JeffEventTriggerDate")),
                                        JeffTgtPDNormalMC = (reader.IsDBNull(reader.GetOrdinal("JeffTargetPDNormalMarketConditions"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("JeffTargetPDNormalMarketConditions")),

                                        //Earnings Announcement Date
                                        NextEarAnnDt = (reader.IsDBNull(reader.GetOrdinal("ExpectedEarningsReportDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("ExpectedEarningsReportDate")),
                                        NextEarAnnDtTyp = reader["ExpectedEarningsReportType"] as string,

                                        //Fund Group(s)
                                        NumisFundGrp = reader["NumisFundGroup"] as string,
                                        JeffFundGrp = reader["JeffFundGroup"] as string,
                                        PHFundGrp = reader["PeelHuntFundGroup"] as string,

                                        //Duration (CEFA)
                                        EffDur = (reader.IsDBNull(reader.GetOrdinal("EffDur"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EffDur")),
                                        EffDurCol = reader["EffDurCol"] as string,

                                        //Duration (after applying hierarchy)
                                        Dur = (reader.IsDBNull(reader.GetOrdinal("Dur"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Dur")),
                                        DurSrc = reader["DurSrc"] as string,
                                    };

                                    if (!fundMasterDict.TryGetValue(fundMaster.Ticker, out FundMaster fundMasterLookup))
                                        fundMasterDict.Add(fundMaster.Ticker, fundMaster);
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error getting Fund Master data for ticker: " + ticker);
                throw;
            }
            return fundMasterDict;
        }

        public IDictionary<string, Holding> GetHoldings()
        {
            IDictionary<string, Holding> holdingsDict = new Dictionary<string, Holding>(StringComparer.CurrentCultureIgnoreCase);
            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetHoldingsQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                Holding holding = new Holding
                                {
                                    HoldingTicker = reader["Ticker"] as string,
                                    SecurityTicker = reader["SecTicker"] as string,
                                    FIGI = reader["FIGI"] as string,
                                    Currency = reader["Currency"] as string,
                                    CountryCode = reader["CountryCode"] as string,
                                    YellowKey = reader["YellowKey"] as string,
                                    Security13FFlag = reader["Security13FFlag"] as string,
                                    Price = (reader.IsDBNull(reader.GetOrdinal("Price"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price")),
                                    FX = (reader.IsDBNull(reader.GetOrdinal("FXRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXRate")),
                                    Position = (reader.IsDBNull(reader.GetOrdinal("Position"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Position")),
                                    MarketValue = (reader.IsDBNull(reader.GetOrdinal("MktValue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MktValue")),
                                    MarketValueLocal = (reader.IsDBNull(reader.GetOrdinal("MktValueLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MktValueLocal")),
                                    PctOwnership = (reader.IsDBNull(reader.GetOrdinal("PctOwnership"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PctOwnership")),
                                    SharesOutstanding = (reader.IsDBNull(reader.GetOrdinal("EquitySharesOutstanding"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EquitySharesOutstanding")),
                                    Average20DayVolume = (reader.IsDBNull(reader.GetOrdinal("Avg20DVolume"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Avg20DVolume"))
                                };

                                if (holding.SharesOutstanding.HasValue)
                                    holding.SharesOutstandingAsString = DataConversionUtils.FormatNumberNoDecimals(holding.SharesOutstanding.GetValueOrDefault());

                                if (holding.Position.HasValue)
                                    holding.PositionAsString = DataConversionUtils.FormatNumberNoDecimals(holding.Position.GetValueOrDefault());

                                if (!holdingsDict.ContainsKey(holding.HoldingTicker))
                                    holdingsDict.Add(holding.HoldingTicker, holding);
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

            return holdingsDict;
        }

        public IDictionary<string, Holding> GetHoldingDetailsByPort()
        {
            IDictionary<string, Holding> holdingsDict = new Dictionary<string, Holding>(StringComparer.CurrentCultureIgnoreCase);
            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetHoldingDetailsByPortQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                Holding holding = new Holding
                                {
                                    PositionKey = reader["PositionKey"] as string,
                                    Portfolio = reader["Portfolio"] as string,
                                    HoldingTicker = reader["Ticker"] as string,
                                    Broker = reader["Broker"] as string,
                                    SecurityTicker = reader["SecTicker"] as string,
                                    InOpportunityFund = reader["FundOpt"] as string,
                                    InTacticalFund = reader["FundTac"] as string,
                                    FIGI = reader["FIGI"] as string,
                                    Currency = reader["Currency"] as string,
                                    CountryCode = reader["CountryCode"] as string,
                                    FundName = reader["FundName"] as string,
                                    YellowKey = reader["YellowKey"] as string,
                                    Security13FFlag = reader["Security13FFlag"] as string,

                                    Price = (reader.IsDBNull(reader.GetOrdinal("Price"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price")),
                                    FX = (reader.IsDBNull(reader.GetOrdinal("FXRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXRate")),
                                    Position = (reader.IsDBNull(reader.GetOrdinal("FinalPosition"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FinalPosition")),
                                    SwapPosition = (reader.IsDBNull(reader.GetOrdinal("SwapPos"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SwapPos")),
                                    MarketValue = (reader.IsDBNull(reader.GetOrdinal("MktValue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MktValue")),
                                    MarketValueLocal = (reader.IsDBNull(reader.GetOrdinal("MktValueLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MktValueLocal")),
                                    PctOwnership = (reader.IsDBNull(reader.GetOrdinal("PctOwnership"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PctOwnership")),
                                    SharesOutstanding = (reader.IsDBNull(reader.GetOrdinal("EquitySharesOutstanding"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EquitySharesOutstanding")),
                                    Average20DayVolume = (reader.IsDBNull(reader.GetOrdinal("Avg20DVolume"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Avg20DVolume")),
                                    AsofDate = (reader.IsDBNull(reader.GetOrdinal("AsofDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsofDate")),

                                    //Tendered Shares
                                    SharesTendered = (reader.IsDBNull(reader.GetOrdinal("SharesTendered"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SharesTendered")),
                                    DateTendered = (reader.IsDBNull(reader.GetOrdinal("DateTendered"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("DateTendered")),
                                    DateSettled = (reader.IsDBNull(reader.GetOrdinal("DateSettled"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("DateSettled")),

                                    //Recon
                                    Variance = (reader.IsDBNull(reader.GetOrdinal("Variance"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Variance")),
                                    DerivedPosition = (reader.IsDBNull(reader.GetOrdinal("DerivedPosition"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DerivedPosition")),
                                };

                                holding.AsofDateAsString = DateUtils.ConvertDate(holding.AsofDate, "yyyy-MM-dd");

                                if (holding.SharesOutstanding.HasValue)
                                    holding.SharesOutstandingAsString = DataConversionUtils.FormatNumberNoDecimals(holding.SharesOutstanding.GetValueOrDefault());

                                if (holding.Position.HasValue)
                                    holding.PositionAsString = DataConversionUtils.FormatNumberNoDecimals(holding.Position.GetValueOrDefault());

                                if (!holdingsDict.ContainsKey(holding.PositionKey))
                                    holdingsDict.Add(holding.PositionKey, holding);
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

            return holdingsDict;
        }

        public IDictionary<string, FundHolderSummary> GetFundHolderSummary()
        {
            IDictionary<string, FundHolderSummary> fundHolderSummaryDict = new Dictionary<string, FundHolderSummary>(StringComparer.CurrentCultureIgnoreCase);
            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetFundHolderSummaryQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundHolderSummary fundHolderSummary = new FundHolderSummary
                                {
                                    Ticker = reader["Ticker"] as string,
                                    ActivistList = reader["ActivistList"] as string,
                                    ActivistHolding = (reader.IsDBNull(reader.GetOrdinal("ActivistHolding"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ActivistHolding")),
                                    VotingShares = (reader.IsDBNull(reader.GetOrdinal("VotingShare"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("VotingShare"))
                                };
                                fundHolderSummaryDict.Add(fundHolderSummary.Ticker, fundHolderSummary);
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
            return fundHolderSummaryDict;
        }

        public void InitializeDailyBatch()
        {
            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetInitializeDataBatchQuery, connection))
                    {
                        command.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing query");
                throw;
            }
        }

        public IDictionary<string, FXRate> GetFXRates()
        {
            IDictionary<string, FXRate> dict = new Dictionary<string, FXRate>(StringComparer.CurrentCultureIgnoreCase);
            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetFXRatesQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FXRate fxRate = new FXRate
                                {
                                    Currency = reader["LocalCurrency"] as string,
                                    FXRatePD = (reader.IsDBNull(reader.GetOrdinal("FXRatePD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXRatePD")),
                                    FXRateLatest = (reader.IsDBNull(reader.GetOrdinal("FXRateLatest"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXRateLatest")),
                                    FXReturn = (reader.IsDBNull(reader.GetOrdinal("FXRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXRtn")),
                                    Source = "BBG"
                                };

                                fxRate.CurrencyCode = fxRate.Currency + "USD";
                                dict.Add(fxRate.Currency, fxRate);
                            }

                            //add USD to USD fx rate = 1
                            FXRate fxRate1 = new FXRate
                            {
                                Currency = "USD"
                            };
                            fxRate1.CurrencyCode = fxRate1.Currency + "USD";
                            fxRate1.FXRatePD = 1.0;
                            fxRate1.FXRateLatest = 1.0;
                            fxRate1.FXReturn = 0.0;
                            fxRate1.Source = "BBG";
                            dict.Add(fxRate1.Currency, fxRate1);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing query");
                throw;
            }
            return dict;
        }

        public IDictionary<string, FXRate> GetFXRatesPD()
        {
            IDictionary<string, FXRate> dict = new Dictionary<string, FXRate>(StringComparer.CurrentCultureIgnoreCase);
            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetFXRatesPDQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FXRate fxRate = new FXRate
                                {
                                    Currency = reader["LocalCurrency"] as string,
                                    FXRatePD = (reader.IsDBNull(reader.GetOrdinal("FXRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXRate")),
                                    Source = "BBG"
                                };

                                fxRate.CurrencyCode = fxRate.Currency + "USD";
                                dict.Add(fxRate.Currency, fxRate);
                            }

                            //add USD to USD fx rate = 1
                            FXRate fxRate1 = new FXRate
                            {
                                Currency = "USD"
                            };
                            fxRate1.CurrencyCode = fxRate1.Currency + "USD";
                            fxRate1.FXRatePD = 1.0;
                            fxRate1.Source = "BBG";
                            dict.Add(fxRate1.Currency, fxRate1);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing query");
                throw;
            }
            return dict;
        }

        public IDictionary<string, FundDividend> GetFundDividends()
        {
            IDictionary<string, FundDividend> dict = new Dictionary<string, FundDividend>(StringComparer.CurrentCultureIgnoreCase);
            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetFundDividendsQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundDividend data = new FundDividend
                                {
                                    Ticker = reader["Ticker"] as string,
                                    DvdFromLastNavDate = (reader.IsDBNull(reader.GetOrdinal("DvdFromLastNavDate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DvdFromLastNavDate")),
                                    DvdLast = (reader.IsDBNull(reader.GetOrdinal("LastDvd"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LastDvd")),
                                    ExDvdDates = reader["ExDates"] as string,
                                    DvdNavDate = reader.IsDBNull(reader.GetOrdinal("NavDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("NavDate")),
                                    LastExDvdDate = reader.IsDBNull(reader.GetOrdinal("LastExDvdDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("LastExDvdDate"))
                                };

                                if (!dict.TryGetValue(data.Ticker, out FundDividend fundDividendLookup))
                                    dict.Add(data.Ticker, data);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error getting Fund Dividends");
                throw;
            }
            return dict;
        }

        public IDictionary<string, FundETFReturn> GetFundRegETFReturns()
        {
            IDictionary<string, FundETFReturn> dict = new Dictionary<string, FundETFReturn>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetFundRegressionETFReturnsQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                string fundTicker = reader["fund_ticker"] as string;
                                FundETFReturn fundETFReturn;
                                if (!dict.TryGetValue(fundTicker, out fundETFReturn))
                                {
                                    fundETFReturn = new FundETFReturn
                                    {
                                        HistoricalETFReturn = new Dictionary<string, Nullable<double>>(StringComparer.CurrentCultureIgnoreCase)
                                    };
                                    dict.Add(fundTicker, fundETFReturn);
                                }

                                string etfTicker = reader["etf_ticker"] as string;
                                Nullable<double> etfReturn = (reader.IsDBNull(reader.GetOrdinal("etftrri"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("etftrri"));
                                if (!fundETFReturn.HistoricalETFReturn.ContainsKey(etfTicker))
                                    fundETFReturn.HistoricalETFReturn.Add(etfTicker, etfReturn);
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
            return dict;
        }

        public IDictionary<string, FundRedemption> GetFundRedemptionDetails()
        {
            IDictionary<string, FundRedemption> dict = new Dictionary<string, FundRedemption>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetFundRedemptionQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundRedemption data = new FundRedemption
                                {
                                    Ticker = reader["Ticker"] as string,
                                    RedemptionType = reader["RedemptionType"] as string,
                                    Structure = reader["Structure"] as string,
                                    FirstRedemptionDate = reader.IsDBNull(reader.GetOrdinal("FirstRedemptionDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FirstRedemptionDate")),
                                    LastRedemptionDate = reader.IsDBNull(reader.GetOrdinal("LastRedemptionDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("LastRedemptionDate")),
                                    RedemptionFrequency = reader["RedemptionFrequency"] as string,
                                    RedemptionDaysFromMonthEnd = (reader.IsDBNull(reader.GetOrdinal("RedemptionDaysFromMonthEnd"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("RedemptionDaysFromMonthEnd")),
                                    RedemptionNoticeDateType = reader["RedemptionNoticeDateType"] as string,
                                    RedemptionNoticeDays = (reader.IsDBNull(reader.GetOrdinal("NoticeDaysFromRedemption"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("NoticeDaysFromRedemption")),
                                    PaymentDelayDateType = reader["PaymentDelayDateType"] as string,
                                    PaymentDelay = (reader.IsDBNull(reader.GetOrdinal("PaymentDelay"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("PaymentDelay")),
                                    IsPreferredShareRedemptionValueIncludedInNav = reader["IsPreferredShareRedemptionValueIncludedInNAV"] as string,
                                    PreferredShareRedemptionValueAsString = reader["PreferredShareRedemptionValue"] as string,
                                    PreferredInterestOnRedemptionDateAsString = reader["PreferredInterestOnRedemptionDate"] as string,
                                    FundLiquidityTransactionCost = (reader.IsDBNull(reader.GetOrdinal("FundLiquidityTransactionCost"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FundLiquidityTransactionCost")),
                                    RedemptionFixedFee = (reader.IsDBNull(reader.GetOrdinal("FundRedemptionFixedFee"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FundRedemptionFixedFee")),
                                    RedemptionFeePct = (reader.IsDBNull(reader.GetOrdinal("FundRedemptionFeePct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FundRedemptionFeePct")),
                                    AddlRedemptionFeePct = (reader.IsDBNull(reader.GetOrdinal("AddlFundRedemptionFeePct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AddlFundRedemptionFeePct")),
                                    PerShareCommission = (reader.IsDBNull(reader.GetOrdinal("CommissionPerfShare"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CommissionPerfShare")),
                                    PreferredSharedRedemptionFee = (reader.IsDBNull(reader.GetOrdinal("PreferredSharedRedemptionFee"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PreferredSharedRedemptionFee")),
                                    NumPreferredSharesPerCommonSplitTrust = (reader.IsDBNull(reader.GetOrdinal("NumPreferredSharesPerCommonShare"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NumPreferredSharesPerCommonShare")),
                                    PreferredShareTicker = reader["PreferredShareTicker"] as string,
                                    PreferredShareSedol = reader["PreferredShareSedol"] as string,
                                    IsPreferredTakenOutAtRedemptionPrice = reader["PreferredTakenOutRedemptionPrice"] as string,
                                    PreferredShareMaturityDate = reader.IsDBNull(reader.GetOrdinal("PreferredMaturityDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("PreferredMaturityDate")),
                                    PreferredShareExDividendDate = reader.IsDBNull(reader.GetOrdinal("PreferredShareExDividendDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("PreferredShareExDividendDate")),
                                    PreferredDvdSharesLast = (reader.IsDBNull(reader.GetOrdinal("PreferredDvdShareLast"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PreferredDvdShareLast")),
                                    PreferredCoupon = (reader.IsDBNull(reader.GetOrdinal("PreferredCoupon"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PreferredCoupon")),
                                    StampTax = (reader.IsDBNull(reader.GetOrdinal("StampTax"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("StampTax")),
                                    OvrNextRedemptionNoticeDate = reader.IsDBNull(reader.GetOrdinal("OvrNextRedemptionNoticeDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("OvrNextRedemptionNoticeDate")),
                                    OvrNextRedemptionPaymentDate = reader.IsDBNull(reader.GetOrdinal("OvrNextRedemptionPaymentDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("OvrNextRedemptionPaymentDate"))
                                };

                                data.PreferredShareRedemptionValue = DataConversionUtils.ConvertToDouble(data.PreferredShareRedemptionValueAsString);
                                data.PreferredInterestOnRedemptionDate = DataConversionUtils.ConvertToDouble(data.PreferredInterestOnRedemptionDateAsString);
                                data.FirstRedemptionDateAsString = DateUtils.ConvertDate(data.FirstRedemptionDate, "yyyy-MM-dd");
                                data.LastRedemptionDateAsString = DateUtils.ConvertDate(data.LastRedemptionDate, "yyyy-MM-dd");
                                data.PreferredShareMaturityDateAsString = DateUtils.ConvertDate(data.PreferredShareMaturityDate, "yyyy-MM-dd");
                                data.PreferredShareExDividendDateAsString = DateUtils.ConvertDate(data.PreferredShareExDividendDate, "yyyy-MM-dd");
                                data.OvrNextRedemptionNoticeDateAsString = DateUtils.ConvertDate(data.OvrNextRedemptionNoticeDate, "yyyy-MM-dd");
                                data.OvrNextRedemptionPaymentDateAsString = DateUtils.ConvertDate(data.OvrNextRedemptionPaymentDate, "yyyy-MM-dd");

                                if (!dict.TryGetValue(data.Ticker, out FundRedemption fundRedemptionLookup))
                                    dict.Add(data.Ticker, data);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error getting Fund Redemption Details");
                throw;
            }
            return dict;
        }

        public void SaveFundRedemptionDates(IDictionary<string, FundRedemption> fundRedemptionDict)
        {
            string sql = "insert into almitasc_ACTradingBBGData.FundRedemptionDet (Ticker, SettlementDate, NextRedemptionDate, NextRedemptionNoticeDate, NextRedemptionPaymentDate, DaysUntilRedemption, DaysUntilNotification) values " +
                        "(@Ticker, @SettlementDate, @NextRedemptionDate, @NextRedemptionNoticeDate, @NextRedemptionPaymentDate, @DaysUntilRedemption, @DaysUntilNotification)";

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlTransaction trans = connection.BeginTransaction())
                    {
                        string sqlDelete = "truncate table almitasc_ACTradingBBGData.FundRedemptionDet";
                        using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                        {
                            command.ExecuteNonQuery();
                        }

                        using (MySqlCommand command = new MySqlCommand(sql, connection))
                        {
                            command.Parameters.Add(new MySqlParameter("@Ticker", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("@SettlementDate", MySqlDbType.Date));
                            command.Parameters.Add(new MySqlParameter("@NextRedemptionDate", MySqlDbType.Date));
                            command.Parameters.Add(new MySqlParameter("@NextRedemptionNoticeDate", MySqlDbType.Date));
                            command.Parameters.Add(new MySqlParameter("@NextRedemptionPaymentDate", MySqlDbType.Date));
                            command.Parameters.Add(new MySqlParameter("@DaysUntilRedemption", MySqlDbType.Int16));
                            command.Parameters.Add(new MySqlParameter("@DaysUntilNotification", MySqlDbType.Int16));

                            foreach (KeyValuePair<string, FundRedemption> kvp in fundRedemptionDict)
                            {
                                FundRedemption fundRedemption = kvp.Value;
                                command.Parameters[0].Value = fundRedemption.Ticker;
                                command.Parameters[1].Value = fundRedemption.SettlementDate;
                                command.Parameters[2].Value = fundRedemption.NextRedemptionDate;
                                command.Parameters[3].Value = fundRedemption.NextNotificationDate;
                                command.Parameters[4].Value = fundRedemption.NextRedemptionPaymentDate;
                                command.Parameters[5].Value = fundRedemption.DaysUntilRedemptionPayment;
                                command.Parameters[6].Value = fundRedemption.DaysUntilNotification;

                                command.ExecuteNonQuery();
                            }
                        }
                        trans.Commit();
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving data");
            }
        }

        public IDictionary<string, FundAlphaModelParams> GetFundAlphaModelParams()
        {
            IDictionary<string, FundAlphaModelParams> dict = new Dictionary<string, FundAlphaModelParams>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetFundAlphaModelParamsQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                string fundTicker = reader["Ticker"] as string;

                                FundAlphaModelParams fundAlphaModelParms;
                                if (!dict.TryGetValue(fundTicker, out fundAlphaModelParms))
                                {
                                    fundAlphaModelParms = new FundAlphaModelParams
                                    {
                                        Ticker = fundTicker,
                                        Model = reader["Model"] as string,
                                        ModelLevel = reader["ModelLevel"] as string,
                                        Coefficients = new Dictionary<string, FundAlphaModelCoefficients>(StringComparer.CurrentCultureIgnoreCase)
                                    };
                                    dict.Add(fundTicker, fundAlphaModelParms);
                                }

                                string regressor = reader["Regressor"] as string;
                                if (!fundAlphaModelParms.Coefficients.ContainsKey(regressor))
                                {
                                    FundAlphaModelCoefficients fundAlphaModelCoefficients = new FundAlphaModelCoefficients
                                    {
                                        Regressor = regressor,
                                        RSqrd = (reader.IsDBNull(reader.GetOrdinal("RSqrd"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RSqrd")),
                                        Beta = (reader.IsDBNull(reader.GetOrdinal("Beta"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Beta"))
                                    };

                                    fundAlphaModelParms.Coefficients.Add(regressor, fundAlphaModelCoefficients);
                                }
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

            return dict;
        }

        public IDictionary<string, FundAlphaModelScores> GetFundAlphaModelScores()
        {
            IDictionary<string, FundAlphaModelScores> dict = new Dictionary<string, FundAlphaModelScores>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetFundAlphaModelScoresQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundAlphaModelScores fundAlphaModelScores = new FundAlphaModelScores
                                {
                                    Ticker = reader["Ticker"] as string,
                                    ActivistScore = (reader.IsDBNull(reader.GetOrdinal("ActivistScore"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ActivistScore")),
                                    RawActivistScore = (reader.IsDBNull(reader.GetOrdinal("RawActivistScore"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RawActivistScore")),
                                    LiquidityCost = (reader.IsDBNull(reader.GetOrdinal("LiquidityCost"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LiquidityCost")),
                                    ShareBuyback = (reader.IsDBNull(reader.GetOrdinal("ShareBuyback"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ShareBuyback")),
                                    ExpectedAlphaAdjFactor = (reader.IsDBNull(reader.GetOrdinal("ExpectedAlphaAdjFactor"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExpectedAlphaAdjFactor")),
                                    DiscountConvergenceMultiplier = (reader.IsDBNull(reader.GetOrdinal("DiscountConvergenceMultiplier"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DiscountConvergenceMultiplier")),
                                    SecurityDScoreMultiplier = (reader.IsDBNull(reader.GetOrdinal("SecurityDScoreMultiplier"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SecurityDScoreMultiplier")),
                                    FundCategoryDScoreMultiplier = (reader.IsDBNull(reader.GetOrdinal("FundCategoryDScoreMultiplier"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FundCategoryDScoreMultiplier")),
                                    IRRMultiplier = (reader.IsDBNull(reader.GetOrdinal("IRRMultiplier"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IRRMultiplier")),
                                    ActivistScoreMultiplier = (reader.IsDBNull(reader.GetOrdinal("ActivistScoreMultiplier"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ActivistScoreMultiplier")),
                                    MajorityVotingHaircut = (reader.IsDBNull(reader.GetOrdinal("MajorityVotingHaircut"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MajorityVotingHaircut")),
                                    MajorityVotingHaircutMin = (reader.IsDBNull(reader.GetOrdinal("MajorityVotingHaircutMin"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MajorityVotingHaircutMin")),
                                    BoardTermAdjMultiplier = (reader.IsDBNull(reader.GetOrdinal("BoardTermAdjMultiplier"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BoardTermAdjMultiplier")),
                                    BoardTermAdjMin = (reader.IsDBNull(reader.GetOrdinal("BoardTermAdjMin"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BoardTermAdjMin")),
                                    ExpenseDragMultiplier = (reader.IsDBNull(reader.GetOrdinal("ExpenseDragMultiplier"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExpenseDragMultiplier")),
                                };

                                if (!dict.TryGetValue(fundAlphaModelScores.Ticker, out FundAlphaModelScores fundAlphaModelScoresLookup))
                                    dict.Add(fundAlphaModelScores.Ticker, fundAlphaModelScores);
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
            return dict;
        }

        public IDictionary<string, FundDividendSchedule> GetFundDividendSchedule()
        {
            IDictionary<string, FundDividendSchedule> dict = new Dictionary<string, FundDividendSchedule>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetFundDividendScheduleQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundDividendSchedule fundDividendSchedule = new FundDividendSchedule
                                {
                                    Ticker = reader["Ticker"] as string,
                                    Last12MonthDividendsPayString = reader["dvdstr"] as string,
                                    Last12MonthDividends = (reader.IsDBNull(reader.GetOrdinal("dvd12m"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("dvd12m")),
                                    FutureDividendPayString = reader["dvdlast"] as string
                                };

                                if (!dict.TryGetValue(fundDividendSchedule.Ticker, out FundDividendSchedule fundDividendScheduleLookup))
                                    dict.Add(fundDividendSchedule.Ticker, fundDividendSchedule);
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

            return dict;
        }

        public IList<FundHoldingReturn> GetFundHoldingReturn(string ticker)
        {
            IList<FundHoldingReturn> list = new List<FundHoldingReturn>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetFundHoldingReturnQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Ticker", ticker);

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
                                    FundCurr = reader["FundCurrency"] as string,
                                    SecCurr = reader["SecurityCurrency"] as string,
                                    ProxySecCurr = reader["ProxySecurityCurrency"] as string,
                                    Cntry = reader["Country"] as string,
                                    AssetType = reader["AssetType"] as string,
                                    SecType = reader["SecurityType"] as string,
                                    Name = reader["Name"] as string,
                                    BroadInd = reader["BroadIndustry"] as string,
                                    Industry = reader["Industry"] as string,
                                    Sector = reader["Sector"] as string,
                                    RatingClass = reader["RatingClass"] as string,
                                    LongCurr = reader["LongCurrency"] as string,
                                    ShortCurr = reader["ShortCurrency"] as string,
                                    LongShortPosInd = reader["LongShortPosIndicator"] as string,

                                    Pos = (reader.IsDBNull(reader.GetOrdinal("ReportedPosition"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ReportedPosition")),
                                    Wt = (reader.IsDBNull(reader.GetOrdinal("ReportedWeight"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ReportedWeight")),
                                    LatestWt = (reader.IsDBNull(reader.GetOrdinal("LatestWeight"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LatestWeight")),
                                    FinalWt = (reader.IsDBNull(reader.GetOrdinal("FinalWeight"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FinalWeight")),
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
                                    PortDt = (reader.IsDBNull(reader.GetOrdinal("PortDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("PortDate"))
                                };

                                data.NavDtAsString = DateUtils.ConvertDate(data.NavDt, "yyyy-MM-dd");
                                data.LastTrdDtAsString = DateUtils.ConvertDate(data.LastTrdDt, "yyyy-MM-dd");
                                data.PortDtAsString = DateUtils.ConvertDate(data.PortDt, "yyyy-MM-dd");

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

        public IList<FundHoldingReturn> GetFundHoldingReturnsNew(string ticker)
        {
            IList<FundHoldingReturn> list = new List<FundHoldingReturn>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetFundHoldingReturnNewQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Ticker", ticker);
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundHoldingReturn data = new FundHoldingReturn
                                {
                                    FundTicker = reader["FundTicker"] as string,
                                    Ticker = reader["Ticker"] as string,
                                    Cusip = reader["Cusip"] as string,
                                    FundCurr = reader["FundCurrency"] as string,
                                    SecCurr = reader["SecurityCurrency"] as string,
                                    Cntry = reader["Country"] as string,
                                    AssetType = reader["AssetType"] as string,
                                    SecType = reader["SecurityType"] as string,
                                    Name = reader["Name"] as string,
                                    BroadInd = reader["BroadIndustry"] as string,
                                    Industry = reader["Industry"] as string,
                                    Sector = reader["Sector"] as string,
                                    RatingClass = reader["RatingClass"] as string,
                                    LongCurr = reader["LongCurrency"] as string,
                                    ShortCurr = reader["ShortCurrency"] as string,
                                    LongShortPosInd = reader["LongShortPosIndicator"] as string,

                                    Pos = (reader.IsDBNull(reader.GetOrdinal("ReportedPosition"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ReportedPosition")),
                                    Wt = (reader.IsDBNull(reader.GetOrdinal("ReportedWeight"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ReportedWeight")),
                                    //LatestWt = (reader.IsDBNull(reader.GetOrdinal("LatestWeight"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LatestWeight")),
                                    FinalWt = (reader.IsDBNull(reader.GetOrdinal("FinalWeight"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FinalWeight")),
                                    Prc = (reader.IsDBNull(reader.GetOrdinal("ReportedPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ReportedPrice")),
                                    Dur = (reader.IsDBNull(reader.GetOrdinal("Duration"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Duration")),
                                    YTM = (reader.IsDBNull(reader.GetOrdinal("YTM"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("YTM")),
                                    YTW = (reader.IsDBNull(reader.GetOrdinal("YTW"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("YTW")),
                                    M2M = (reader.IsDBNull(reader.GetOrdinal("M2M"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("M2M")),
                                    NavDt = (reader.IsDBNull(reader.GetOrdinal("NavDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("NavDate")),
                                    FxRateNavDt = (reader.IsDBNull(reader.GetOrdinal("FxRateNavDate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FxRateNavDate")),
                                    FxRateLast = (reader.IsDBNull(reader.GetOrdinal("FxRateLast"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FxRateLast")),
                                    FxRtn = (reader.IsDBNull(reader.GetOrdinal("FxRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FxRtn")),

                                    HistEqRtnLcl = (reader.IsDBNull(reader.GetOrdinal("EqCumRtnNavDateLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EqCumRtnNavDateLocal")),
                                    HistEqFxRtn = (reader.IsDBNull(reader.GetOrdinal("FxCumRtnNavDate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FxCumRtnNavDate")),
                                    HistEqRtn = (reader.IsDBNull(reader.GetOrdinal("EqCumRtnNavDate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EqCumRtnNavDate")),

                                    CurrEqRtnLcl = (reader.IsDBNull(reader.GetOrdinal("EqCurrRtnLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EqCurrRtnLocal")),
                                    CurrEqRtn = (reader.IsDBNull(reader.GetOrdinal("EqCurrRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EqCurrRtn")),
                                    CurrFxRtn = (reader.IsDBNull(reader.GetOrdinal("FxCurrRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FxCurrRtn")),

                                    TotEqRtn = (reader.IsDBNull(reader.GetOrdinal("EqRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EqRtn")),
                                    SpreadRtn = (reader.IsDBNull(reader.GetOrdinal("SpreadRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SpreadRtn")),
                                    CurveRtn = (reader.IsDBNull(reader.GetOrdinal("CurveRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CurveRtn")),
                                    FIRtn = (reader.IsDBNull(reader.GetOrdinal("FIRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FIRtn")),
                                    CashRtn = (reader.IsDBNull(reader.GetOrdinal("CashRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CashRtn")),
                                    CurrHedgeRtn = (reader.IsDBNull(reader.GetOrdinal("CurrencyHedgeRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CurrencyHedgeRtn")),
                                    TotRtn = (reader.IsDBNull(reader.GetOrdinal("TotalRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotalRtn")),
                                    WtdTotalRtn = (reader.IsDBNull(reader.GetOrdinal("WtdTotalRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("WtdTotalRtn")),
                                    PctHedged = (reader.IsDBNull(reader.GetOrdinal("PctHedged"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PctHedged")),
                                    LastPrc = (reader.IsDBNull(reader.GetOrdinal("LastPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LastPrice")),
                                    Beta = (reader.IsDBNull(reader.GetOrdinal("Beta"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Beta"))
                                };

                                data.NavDtAsString = DateUtils.ConvertDate(data.NavDt, "yyyy-MM-dd");

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

        public void SaveUserOverrides(IList<UserDataOverride> overrides)
        {
            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlTransaction trans = connection.BeginTransaction())
                    {
                        using (MySqlCommand command = new MySqlCommand(SaveUserOverridesQuery, connection))
                        {
                            command.CommandType = System.Data.CommandType.StoredProcedure;

                            command.Parameters.Add(new MySqlParameter("p_Ticker", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_ProxyFormula", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_ActivistScore", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_InsiderScore", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_LiquidityCost", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_ShareBuyback", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_RedempExtraAdjustment", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_NavEstRule", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_NavOverride", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_NavOverrideDate", MySqlDbType.Date));
                            command.Parameters.Add(new MySqlParameter("p_NavOverrideExpiryDate", MySqlDbType.Date));
                            command.Parameters.Add(new MySqlParameter("p_EstimatedNavOverride", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_EstimatedNavOverrideExpiryDate", MySqlDbType.Date));
                            command.Parameters.Add(new MySqlParameter("p_ExpenseRatio", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_LeverageRatio", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_SecurityType", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_PaymentRank", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_Country", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_Currency", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_YellowKey", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_GeoLevel1", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_GeoLevel2", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_GeoLevel3", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_AssetClassLevel1", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_AssetClassLevel2", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_AssetClassLevel3", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_Duration", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_MuniState", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_DividendForecast", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_FundCategory", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_ExpectedAlphaAdjFactor", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_ExpectedDiscountLevelChange", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_ExpectedAlphaMultiplier", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_TaxStatus", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_QualifyingDividends", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_DvdYield", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_ExDvdDate", MySqlDbType.Date));
                            command.Parameters.Add(new MySqlParameter("p_NextDvdDate", MySqlDbType.Date));
                            command.Parameters.Add(new MySqlParameter("p_RecallDays", MySqlDbType.Int32));
                            command.Parameters.Add(new MySqlParameter("p_LastPrice", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_ManagementFee", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_PerformanceFee", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_TaxLiability", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_AccrualRate", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_AccrualStartDate", MySqlDbType.Date));
                            command.Parameters.Add(new MySqlParameter("p_FundDiscountGroup", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_DiscountConvergenceMultiplier", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_SecurityDScoreMultiplier", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_FundCategoryDScoreMultiplier", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_IRRMultiplier", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_ActivistScoreMultiplier", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_MajorityVotingHaircut", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_MajorityVotingHaircutMin", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_BoardTermAdjMultiplier", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_BoardTermAdjMin", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_ExpenseDragMultiplier", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_UserName", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_EstimatedNavAdjustment", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_IntrinsicValue", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_IntrinsicValueDate", MySqlDbType.Date));
                            command.Parameters.Add(new MySqlParameter("p_IntrinsicValueExpiryDate", MySqlDbType.Date));
                            command.Parameters.Add(new MySqlParameter("p_NII", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_FIProxyFormula", MySqlDbType.VarChar));

                            foreach (UserDataOverride data in overrides)
                            {
                                command.Parameters[0].Value = data.Ticker;
                                command.Parameters[1].Value = data.ProxyFormula;
                                command.Parameters[2].Value = data.ActivistScore;
                                command.Parameters[3].Value = data.InsiderScore;
                                command.Parameters[4].Value = data.LiquidityCost;
                                command.Parameters[5].Value = data.ShareBuyback;
                                command.Parameters[6].Value = data.RedempExtraAdjustment;
                                command.Parameters[7].Value = data.NavEstRule;
                                command.Parameters[8].Value = data.NavOverride;
                                command.Parameters[9].Value = data.NavOverrideDate;
                                command.Parameters[10].Value = data.NavOverrideExpiryDate;
                                command.Parameters[11].Value = data.EstimatedNavOverride;
                                command.Parameters[12].Value = data.EstimatedNavOverrideExpiryDate;
                                command.Parameters[13].Value = data.ExpenseRatio;
                                command.Parameters[14].Value = data.LeverageRatio;
                                command.Parameters[15].Value = data.SecurityType;
                                command.Parameters[16].Value = data.PaymentRank;
                                command.Parameters[17].Value = data.Country;
                                command.Parameters[18].Value = data.Currency;
                                command.Parameters[19].Value = data.YellowKey;
                                command.Parameters[20].Value = data.GeoLevel1;
                                command.Parameters[21].Value = data.GeoLevel2;
                                command.Parameters[22].Value = data.GeoLevel3;
                                command.Parameters[23].Value = data.AssetClassLevel1;
                                command.Parameters[24].Value = data.AssetClassLevel2;
                                command.Parameters[25].Value = data.AssetClassLevel3;
                                command.Parameters[26].Value = data.Duration;
                                command.Parameters[27].Value = data.MuniState;
                                command.Parameters[28].Value = data.DividendForecast;
                                command.Parameters[29].Value = data.FundCategory;
                                command.Parameters[30].Value = data.ExpectedAlphaAdjFactor;
                                command.Parameters[31].Value = data.ExpectedDiscountLevelChange;
                                command.Parameters[32].Value = data.ExpectedAlphaMultiplier;
                                command.Parameters[33].Value = data.TaxStatus;
                                command.Parameters[34].Value = data.QualifyingDividends;
                                command.Parameters[35].Value = data.DvdYield;
                                command.Parameters[36].Value = data.ExDvdDate;
                                command.Parameters[37].Value = data.NextDvdDate;
                                command.Parameters[38].Value = data.RecallDays;
                                command.Parameters[39].Value = data.LastPrice;
                                command.Parameters[40].Value = data.ManagementFee;
                                command.Parameters[41].Value = data.PerformanceFee;
                                command.Parameters[42].Value = data.TaxLiability;
                                command.Parameters[43].Value = data.AccrualRate;
                                command.Parameters[44].Value = data.AccrualStartDate;
                                command.Parameters[45].Value = data.FundDiscountGroup;
                                command.Parameters[46].Value = data.DiscountConvergenceMultiplier;
                                command.Parameters[47].Value = data.SecurityDScoreMultiplier;
                                command.Parameters[48].Value = data.FundCategoryDScoreMultiplier;
                                command.Parameters[49].Value = data.IRRMultiplier;
                                command.Parameters[50].Value = data.ActivistScoreMultiplier;
                                command.Parameters[51].Value = data.MajorityVotingHaircut;
                                command.Parameters[52].Value = data.MajorityVotingHaircutMin;
                                command.Parameters[53].Value = data.BoardTermAdjMultiplier;
                                command.Parameters[54].Value = data.BoardTermAdjMin;
                                command.Parameters[55].Value = data.ExpenseDragMultiplier;
                                command.Parameters[56].Value = data.UserName;
                                command.Parameters[57].Value = data.EstimatedNavAdjustment;
                                command.Parameters[58].Value = data.IntrinsicValue;
                                command.Parameters[59].Value = data.IntrinsicValueDate;
                                command.Parameters[60].Value = data.IntrinsicValueExpiryDate;
                                command.Parameters[61].Value = data.NII;
                                command.Parameters[62].Value = data.PortFIProxyFormula;

                                command.ExecuteNonQuery();
                            }
                        }
                        trans.Commit();
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving data");
            }
        }

        public IDictionary<string, UserDataOverride> GetUserDataOverrides()
        {
            IDictionary<string, UserDataOverride> dict = new Dictionary<string, UserDataOverride>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetUserOverridesQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                UserDataOverride data = new UserDataOverride
                                {
                                    Ticker = reader["Ticker"] as string,
                                    ProxyFormula = reader["ProxyFormula"] as string,
                                    AltProxyFormula = reader["AltProxyFormula"] as string,
                                    ActivistScore = (reader.IsDBNull(reader.GetOrdinal("ActivistScore"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ActivistScore")),
                                    InsiderScore = (reader.IsDBNull(reader.GetOrdinal("InsiderScore"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("InsiderScore")),
                                    LiquidityCost = (reader.IsDBNull(reader.GetOrdinal("LiquidityCost"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LiquidityCost")),
                                    ShareBuyback = (reader.IsDBNull(reader.GetOrdinal("ShareBuyback"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ShareBuyback")),
                                    RedempExtraAdjustment = (reader.IsDBNull(reader.GetOrdinal("RedempExtraAdjustment"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RedempExtraAdjustment")),
                                    NavEstRule = reader["NavEstRule"] as string,
                                    NavOverride = (reader.IsDBNull(reader.GetOrdinal("NavOverride"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NavOverride")),
                                    NavOverrideDate = (reader.IsDBNull(reader.GetOrdinal("NavOverrideDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("NavOverrideDate")),
                                    NavOverrideExpiryDate = (reader.IsDBNull(reader.GetOrdinal("NavOverrideExpiryDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("NavOverrideExpiryDate")),
                                    EstimatedNavOverride = (reader.IsDBNull(reader.GetOrdinal("EstimatedNavOverride"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EstimatedNavOverride")),
                                    EstimatedNavOverrideExpiryDate = (reader.IsDBNull(reader.GetOrdinal("EstimatedNavOverrideExpiryDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EstimatedNavOverrideExpiryDate")),
                                    EstimatedNavAdjustment = (reader.IsDBNull(reader.GetOrdinal("EstimatedNavAdjustment"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EstimatedNavAdjustment")),
                                    ExpenseRatio = (reader.IsDBNull(reader.GetOrdinal("ExpenseRatio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExpenseRatio")),
                                    LeverageRatio = (reader.IsDBNull(reader.GetOrdinal("LeverageRatio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LeverageRatio")),
                                    SecurityType = reader["SecurityType"] as string,
                                    PaymentRank = reader["PaymentRank"] as string,
                                    Country = reader["Country"] as string,
                                    Currency = reader["Currency"] as string,
                                    YellowKey = reader["YellowKey"] as string,
                                    GeoLevel1 = reader["GeoLevel1"] as string,
                                    GeoLevel2 = reader["GeoLevel2"] as string,
                                    GeoLevel3 = reader["GeoLevel3"] as string,
                                    AssetClassLevel1 = reader["AssetClassLevel1"] as string,
                                    AssetClassLevel2 = reader["AssetClassLevel2"] as string,
                                    AssetClassLevel3 = reader["AssetClassLevel3"] as string,
                                    Duration = (reader.IsDBNull(reader.GetOrdinal("Duration"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Duration")),
                                    MuniState = reader["MuniState"] as string,
                                    DividendForecast = (reader.IsDBNull(reader.GetOrdinal("DividendForecast"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DividendForecast")),
                                    FundCategory = reader["FundCategory"] as string,
                                    UKFundModelCategory = reader["UKFundModelCategory"] as string,
                                    ExpectedAlphaAdjFactor = (reader.IsDBNull(reader.GetOrdinal("ExpectedAlphaAdjFactor"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExpectedAlphaAdjFactor")),
                                    ExpectedDiscountLevelChange = (reader.IsDBNull(reader.GetOrdinal("ExpectedDiscountLevelChange"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExpectedDiscountLevelChange")),
                                    ExpectedAlphaMultiplier = (reader.IsDBNull(reader.GetOrdinal("ExpectedAlphaMultiplier"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExpectedAlphaMultiplier")),
                                    TaxStatus = reader["TaxStatus"] as string,
                                    QualifyingDividends = (reader.IsDBNull(reader.GetOrdinal("QualifyingDividends"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("QualifyingDividends")),
                                    DvdYield = (reader.IsDBNull(reader.GetOrdinal("DvdYield"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DvdYield")),
                                    ExDvdDate = (reader.IsDBNull(reader.GetOrdinal("ExDvdDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("ExDvdDate")),
                                    NextDvdDate = (reader.IsDBNull(reader.GetOrdinal("NextDvdDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("NextDvdDate")),
                                    RecallDays = (reader.IsDBNull(reader.GetOrdinal("RecallDays"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("RecallDays")),
                                    LastPrice = (reader.IsDBNull(reader.GetOrdinal("LastPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LastPrice")),
                                    ManagementFee = (reader.IsDBNull(reader.GetOrdinal("ManagementFee"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ManagementFee")),
                                    PerformanceFee = (reader.IsDBNull(reader.GetOrdinal("PerformanceFee"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PerformanceFee")),
                                    TaxLiability = (reader.IsDBNull(reader.GetOrdinal("TaxLiability"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TaxLiability")),
                                    AccrualRate = (reader.IsDBNull(reader.GetOrdinal("AccrualRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AccrualRate")),
                                    AccrualStartDate = (reader.IsDBNull(reader.GetOrdinal("AccrualStartDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AccrualStartDate")),
                                    NII = (reader.IsDBNull(reader.GetOrdinal("NII"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NII")),
                                    FundDiscountGroup = reader["FundDiscountGroup"] as string,
                                    DiscountConvergenceMultiplier = (reader.IsDBNull(reader.GetOrdinal("DiscountConvergenceMultiplier"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DiscountConvergenceMultiplier")),
                                    SecurityDScoreMultiplier = (reader.IsDBNull(reader.GetOrdinal("SecurityDScoreMultiplier"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SecurityDScoreMultiplier")),
                                    FundCategoryDScoreMultiplier = (reader.IsDBNull(reader.GetOrdinal("FundCategoryDScoreMultiplier"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FundCategoryDScoreMultiplier")),
                                    IRRMultiplier = (reader.IsDBNull(reader.GetOrdinal("IRRMultiplier"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IRRMultiplier")),
                                    ActivistScoreMultiplier = (reader.IsDBNull(reader.GetOrdinal("ActivistScoreMultiplier"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ActivistScoreMultiplier")),
                                    MajorityVotingHaircut = (reader.IsDBNull(reader.GetOrdinal("MajorityVotingHaircut"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MajorityVotingHaircut")),
                                    MajorityVotingHaircutMin = (reader.IsDBNull(reader.GetOrdinal("MajorityVotingHaircutMin"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MajorityVotingHaircutMin")),
                                    BoardTermAdjMultiplier = (reader.IsDBNull(reader.GetOrdinal("BoardTermAdjMultiplier"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BoardTermAdjMultiplier")),
                                    BoardTermAdjMin = (reader.IsDBNull(reader.GetOrdinal("BoardTermAdjMin"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BoardTermAdjMin")),
                                    ExpenseDragMultiplier = (reader.IsDBNull(reader.GetOrdinal("ExpenseDragMultiplier"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExpenseDragMultiplier")),
                                    UserName = reader["UserName"] as string,
                                    LastUpdated = (reader.IsDBNull(reader.GetOrdinal("LastUpdated"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("LastUpdated")),
                                    IntrinsicValue = (reader.IsDBNull(reader.GetOrdinal("IntrinsicValue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IntrinsicValue")),
                                    IntrinsicValueDate = (reader.IsDBNull(reader.GetOrdinal("IntrinsicValueDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("IntrinsicValueDate")),
                                    IntrinsicValueExpiryDate = (reader.IsDBNull(reader.GetOrdinal("IntrinsicValueExpiryDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("IntrinsicValueExpiryDate")),
                                    PortFIProxyFormula = reader["FIProxyFormula"] as string,
                                };

                                data.NavOverrideDateAsString = DateUtils.ConvertDate(data.NavOverrideDate, "yyyy-MM-dd");
                                data.NavOverrideExpiryDateAsString = DateUtils.ConvertDate(data.NavOverrideExpiryDate, "yyyy-MM-dd");
                                data.EstimatedNavOverrideExpiryDateAsString = DateUtils.ConvertDate(data.EstimatedNavOverrideExpiryDate, "yyyy-MM-dd");
                                data.ExDvdDateAsString = DateUtils.ConvertDate(data.ExDvdDate, "yyyy-MM-dd");
                                data.NextDvdDateAsString = DateUtils.ConvertDate(data.NextDvdDate, "yyyy-MM-dd");
                                data.LastUpdatedAsString = DateUtils.ConvertDate(data.LastUpdated, "yyyy-MM-dd");
                                data.AccrualStartDateAsString = DateUtils.ConvertDate(data.AccrualStartDate, "yyyy-MM-dd");
                                data.IntrinsicValueDateAsString = DateUtils.ConvertDate(data.IntrinsicValueDate, "yyyy-MM-dd");
                                data.IntrinsicValueExpiryDateAsString = DateUtils.ConvertDate(data.IntrinsicValueExpiryDate, "yyyy-MM-dd");

                                dict.Add(data.Ticker, data);
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
            return dict;
        }

        public IDictionary<string, FundETFReturn> GetFundProxyETFReturns()
        {
            IDictionary<string, FundETFReturn> dict = new Dictionary<string, FundETFReturn>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetFundProxyETFReturnsQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                string fundTicker = reader["fund_ticker"] as string;

                                FundETFReturn fundETFReturn = null;
                                if (!dict.TryGetValue(fundTicker, out fundETFReturn))
                                {
                                    fundETFReturn = new FundETFReturn
                                    {
                                        HistoricalETFReturn = new Dictionary<string, Nullable<double>>()
                                    };
                                    dict.Add(fundTicker, fundETFReturn);
                                }

                                string etfTicker = reader["etf_ticker"] as string;
                                Nullable<double> etfReturn = (reader.IsDBNull(reader.GetOrdinal("etftrri"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("etftrri"));
                                if (!fundETFReturn.HistoricalETFReturn.ContainsKey(etfTicker))
                                    fundETFReturn.HistoricalETFReturn.Add(etfTicker, etfReturn);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error getting Proxy ETF returns");
                throw;
            }
            return dict;
        }

        public IDictionary<string, FundETFReturn> GetFundAltProxyETFReturns()
        {
            IDictionary<string, FundETFReturn> dict = new Dictionary<string, FundETFReturn>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetFundAltProxyETFReturnsQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                string fundTicker = reader["fund_ticker"] as string;

                                FundETFReturn fundETFReturn = null;
                                if (!dict.TryGetValue(fundTicker, out fundETFReturn))
                                {
                                    fundETFReturn = new FundETFReturn
                                    {
                                        HistoricalETFReturn = new Dictionary<string, Nullable<double>>()
                                    };
                                    dict.Add(fundTicker, fundETFReturn);
                                }

                                string etfTicker = reader["etf_ticker"] as string;
                                Nullable<double> etfReturn = (reader.IsDBNull(reader.GetOrdinal("etftrri"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("etftrri"));
                                if (!fundETFReturn.HistoricalETFReturn.ContainsKey(etfTicker))
                                    fundETFReturn.HistoricalETFReturn.Add(etfTicker, etfReturn);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error getting Alternate Proxy ETF returns");
                throw;
            }
            return dict;
        }

        public void SaveFundProxyTickers(IDictionary<string, FundProxyFormula> fundProxyDict)
        {
            string sql = "insert into almitasc_ACTradingBBGData.FundProxyTickers (FundTicker, Ticker, Beta) values (@FundTicker, @Ticker, @Beta)";

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlTransaction trans = connection.BeginTransaction())
                    {

                        string sqlDelete = "truncate table almitasc_ACTradingBBGData.FundProxyTickers";
                        using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                        {
                            _logger.LogInformation("Truncating table almitasc_ACTradingBBGData.FundProxyTickers");
                            command.ExecuteNonQuery();
                        }

                        using (MySqlCommand command = new MySqlCommand(sql, connection))
                        {
                            command.Parameters.Add(new MySqlParameter("@FundTicker", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("@Ticker", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("@Beta", MySqlDbType.Decimal));

                            foreach (KeyValuePair<string, FundProxyFormula> kvp in fundProxyDict)
                            {
                                IList<FundProxy> fundProxyList = kvp.Value.ProxyTickersWithCoefficients;
                                foreach (FundProxy fundProxy in fundProxyList)
                                {
                                    command.Parameters[0].Value = kvp.Key;
                                    command.Parameters[1].Value = fundProxy.ETFTicker;
                                    command.Parameters[2].Value = fundProxy.Beta;
                                    command.ExecuteNonQuery();
                                }
                            }
                        }
                        trans.Commit();
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving data");
            }
        }

        public void SaveFundAltProxyTickers(IDictionary<string, FundProxyFormula> fundProxyDict)
        {
            string sql = "insert into almitasc_ACTradingBBGData.FundAltProxyTickers (FundTicker, Ticker, Beta) values (@FundTicker, @Ticker, @Beta)";

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlTransaction trans = connection.BeginTransaction())
                    {

                        string sqlDelete = "truncate table almitasc_ACTradingBBGData.FundAltProxyTickers";
                        using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                        {
                            _logger.LogInformation("Truncating table almitasc_ACTradingBBGData.FundAltProxyTickers");
                            command.ExecuteNonQuery();
                        }

                        using (MySqlCommand command = new MySqlCommand(sql, connection))
                        {
                            command.Parameters.Add(new MySqlParameter("@FundTicker", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("@Ticker", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("@Beta", MySqlDbType.Decimal));

                            foreach (KeyValuePair<string, FundProxyFormula> kvp in fundProxyDict)
                            {
                                IList<FundProxy> fundProxyList = kvp.Value.ProxyTickersWithCoefficients;
                                foreach (FundProxy fundProxy in fundProxyList)
                                {
                                    command.Parameters[0].Value = kvp.Key;
                                    command.Parameters[1].Value = fundProxy.ETFTicker;
                                    command.Parameters[2].Value = fundProxy.Beta;
                                    command.ExecuteNonQuery();
                                }
                            }
                        }
                        trans.Commit();
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving data");
            }
        }

        public void UpdateFundNavOverrides()
        {
            _logger.LogInformation("Update Fund Nav Overrides (Eq Cum Returns) - STARTED");

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetUpdateFundNavOverridesQuery, connection))
                    {
                        command.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing query");
                throw;
            }
            _logger.LogInformation("Update Fund Nav Overrides (Eq Cum Returns) - DONE");
        }

        public IDictionary<string, Dividend> GetDividends()
        {
            IDictionary<string, Dividend> dict = new Dictionary<string, Dividend>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                string sql = @"
				select s.ticker, d.ex_date, sum(d.amount) as amount
				from almitasc_ACTradingBBGLink.globaltrading_securitymaster s
				join almitasc_ACTradingBBGData.globalcef_fundhistorydvd d on(s.figi = d.figi)
				join almitasc_ACTrading.zzcodetable_dvd c on(d.type = c.type)
				where d.ex_date = current_date()
				and c.cash = 1
				group by s.ticker, d.ex_date";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                Dividend dividend = new Dividend
                                {
                                    Ticker = reader["Ticker"] as string,
                                    ExDate = (reader.IsDBNull(reader.GetOrdinal("Ex_Date"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("Ex_Date")),
                                    DividendAmount = (reader.IsDBNull(reader.GetOrdinal("Amount"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Amount"))
                                };
                                dict.Add(dividend.Ticker, dividend);
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
            return dict;
        }

        public IDictionary<string, Holding> GetEODHoldingDetailsByPort()
        {
            IDictionary<string, Holding> holdingsDict = new Dictionary<string, Holding>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetEODHoldingDetailsByPortQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                Holding holding = new Holding
                                {
                                    PositionKey = reader["PositionKey"] as string,
                                    Portfolio = reader["Portfolio"] as string,
                                    Broker = reader["Broker"] as string,
                                    HoldingTicker = reader["Ticker"] as string,
                                    InOpportunityFund = reader["FundOpt"] as string,
                                    InTacticalFund = reader["FundTac"] as string,
                                    Position = (reader.IsDBNull(reader.GetOrdinal("Position"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Position"))
                                };

                                if (!holdingsDict.ContainsKey(holding.PositionKey))
                                    holdingsDict.Add(holding.PositionKey, holding);
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
            return holdingsDict;
        }

        public IDictionary<string, Holding> GetHoldingHistoryByPort(DateTime asofDate)
        {
            IDictionary<string, Holding> holdingsDict = new Dictionary<string, Holding>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetHoldingHistoryByPortQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_AsofDate", asofDate);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                Holding holding = new Holding
                                {
                                    PositionKey = reader["PositionKey"] as string,
                                    Portfolio = reader["Portfolio"] as string,
                                    HoldingTicker = reader["Ticker"] as string,
                                    Broker = reader["Broker"] as string,
                                    SecurityTicker = reader["SecTicker"] as string,
                                    BrokerySystemId = reader["BrokerSystemId"] as string,
                                    FundName = reader["FundName"] as string,
                                    Currency = reader["Currency"] as string,
                                    YellowKey = reader["YellowKey"] as string,
                                    SecurityName = reader["SecurityName"] as string,
                                    FIGI = reader["Figi"] as string,

                                    AsofDate = (reader.IsDBNull(reader.GetOrdinal("AsofDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsofDate")),
                                    Price = (reader.IsDBNull(reader.GetOrdinal("Price"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price")),
                                    FX = (reader.IsDBNull(reader.GetOrdinal("FXRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FXRate")),
                                    Position = (reader.IsDBNull(reader.GetOrdinal("Position"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Position")),
                                    MarketValue = (reader.IsDBNull(reader.GetOrdinal("MktValue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MktValue")),
                                    MarketValueLocal = (reader.IsDBNull(reader.GetOrdinal("MktValueLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MktValueLocal")),
                                    PriceMultiplier = (reader.IsDBNull(reader.GetOrdinal("PriceMultiplier"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceMultiplier"))
                                };

                                if (!holdingsDict.ContainsKey(holding.PositionKey))
                                    holdingsDict.Add(holding.PositionKey, holding);
                                else
                                    _logger.LogError("Duplicate position key: " + holding.PositionKey);
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
            return holdingsDict;
        }

        public IList<Holding> GetHoldingsHistoryByPort(string fundName, string broker, string ticker, DateTime startDate, DateTime endDate)
        {
            IList<Holding> list = new List<Holding>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetHoldingHistoryQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Portfolio", fundName);
                        command.Parameters.AddWithValue("p_Broker", broker);
                        command.Parameters.AddWithValue("p_Ticker", ticker);
                        command.Parameters.AddWithValue("p_StartDate", startDate);
                        command.Parameters.AddWithValue("p_EndDate", endDate);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                Holding holding = new Holding
                                {
                                    AsofDate = (reader.IsDBNull(reader.GetOrdinal("AsOfDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    Portfolio = reader["FundName"] as string,
                                    Broker = reader["Broker"] as string,
                                    HoldingTicker = reader["Ticker"] as string,
                                    Currency = reader["Currency"] as string,
                                    Position = (reader.IsDBNull(reader.GetOrdinal("Position"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Position")),
                                    PrevPosition = (reader.IsDBNull(reader.GetOrdinal("PrevPosition"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PrevPosition")),
                                    Price = (reader.IsDBNull(reader.GetOrdinal("Price"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price")),
                                    FX = (reader.IsDBNull(reader.GetOrdinal("FxRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FxRate")),
                                    MarketValueLocal = (reader.IsDBNull(reader.GetOrdinal("MVLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MVLocal")),
                                    MarketValue = (reader.IsDBNull(reader.GetOrdinal("MVUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MVUSD")),
                                    SwapPosition = (reader.IsDBNull(reader.GetOrdinal("SwapPosition"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SwapPosition")),
                                    IsSwap = reader["IsSwap"] as string,

                                    TradePosition = (reader.IsDBNull(reader.GetOrdinal("TradePosition"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TradePosition")),
                                    TradePrice = (reader.IsDBNull(reader.GetOrdinal("TradePrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TradePrice")),
                                    TradeMV = (reader.IsDBNull(reader.GetOrdinal("TradeMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TradeMV")),
                                };

                                holding.AsofDateAsString = DateUtils.ConvertDate(holding.AsofDate, "yyyy-MM-dd");
                                list.Add(holding);
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

        public void SaveFundPortProxyTickers(IDictionary<string, FundProxyFormula> fundProxyTickersDict)
        {
            string sql = "insert into almitasc_ACTradingBBGData.FundPortProxyTickers (FundTicker, AssetType, Ticker, Beta) values (@FundTicker, @AssetType, @Ticker, @Beta)";

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlTransaction trans = connection.BeginTransaction())
                    {
                        string sqlDelete = "truncate table almitasc_ACTradingBBGData.FundPortProxyTickers";
                        using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                        {
                            _logger.LogInformation("Truncating table almitasc_ACTradingBBGData.FundPortProxyTickers");
                            command.ExecuteNonQuery();
                        }

                        using (MySqlCommand command = new MySqlCommand(sql, connection))
                        {
                            command.Parameters.Add(new MySqlParameter("@FundTicker", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("@AssetType", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("@Ticker", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("@Beta", MySqlDbType.Decimal));

                            foreach (KeyValuePair<string, FundProxyFormula> kvp in fundProxyTickersDict)
                            {
                                IList<FundProxy> fundProxyList = kvp.Value.ProxyTickersWithCoefficients;
                                foreach (FundProxy fundProxy in fundProxyList)
                                {
                                    command.Parameters[0].Value = kvp.Key;
                                    command.Parameters[1].Value = "FI";
                                    command.Parameters[2].Value = fundProxy.ETFTicker;
                                    command.Parameters[3].Value = fundProxy.Beta;
                                    command.ExecuteNonQuery();
                                }
                            }
                        }
                        trans.Commit();
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving data");
            }
        }

        public IDictionary<string, double> GetFundMarketValues()
        {
            IDictionary<string, double> dict = new Dictionary<string, double>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetFundMVsQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                string fundName = reader["FundName"] as string;
                                double? fundMarketValue = (reader.IsDBNull(reader.GetOrdinal("FundMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FundMV"));

                                if (!dict.ContainsKey(fundName))
                                {
                                    dict.Add(fundName + "_MM", fundMarketValue.GetValueOrDefault());
                                    dict.Add(fundName, fundMarketValue.GetValueOrDefault() / 1000000.0);
                                }
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
            return dict;
        }

        public IDictionary<string, FundETFReturn> GetFundPortProxyETFReturns()
        {
            IDictionary<string, FundETFReturn> dict = new Dictionary<string, FundETFReturn>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetFundPortProxyETFReturnsQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                string fundTicker = reader["fund_ticker"] as string;

                                FundETFReturn fundETFReturn = null;
                                if (!dict.TryGetValue(fundTicker, out fundETFReturn))
                                {
                                    fundETFReturn = new FundETFReturn
                                    {
                                        HistoricalETFReturn = new Dictionary<string, Nullable<double>>()
                                    };
                                    dict.Add(fundTicker, fundETFReturn);
                                }

                                string etfTicker = reader["etf_ticker"] as string;
                                Nullable<double> etfReturn = (reader.IsDBNull(reader.GetOrdinal("etftrri"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("etftrri"));
                                if (!fundETFReturn.HistoricalETFReturn.ContainsKey(etfTicker))
                                    fundETFReturn.HistoricalETFReturn.Add(etfTicker, etfReturn);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error getting Port Proxy ETF returns");
                throw;
            }
            return dict;
        }

        public IList<UserDataOverrideRpt> GetUserDataOverrideRpt()
        {
            IList<UserDataOverrideRpt> list = new List<UserDataOverrideRpt>();

            try
            {
                string sql = GetUserOverrideRptQuery;

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                UserDataOverrideRpt data = new UserDataOverrideRpt
                                {
                                    Ticker = reader["Ticker"] as string,
                                    FldName = reader["FieldName"] as string,
                                    SecTyp = reader["SecurityType"] as string,
                                    Curr = reader["Currency"] as string,
                                    Cntry = reader["Country"] as string,
                                    ChkFlag = (reader.IsDBNull(reader.GetOrdinal("CheckFlag"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("CheckFlag")),
                                    SortOrder = (reader.IsDBNull(reader.GetOrdinal("SortOrder"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("SortOrder")),

                                    RptDt = reader.IsDBNull(reader.GetOrdinal("ReportDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("ReportDate")),
                                    OvrVal = (reader.IsDBNull(reader.GetOrdinal("OvrValue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OvrValue")),
                                    OvrDt = reader.IsDBNull(reader.GetOrdinal("OvrDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("OvrDate")),
                                    OvrExpiryDt = reader.IsDBNull(reader.GetOrdinal("OvrExpiryDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("OvrExpiryDate")),
                                    BBGVal = (reader.IsDBNull(reader.GetOrdinal("BBGValue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BBGValue")),
                                    BBGDt = reader.IsDBNull(reader.GetOrdinal("BBGDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("BBGDate")),
                                    BBGCurr = reader["BBGCurr"] as string,

                                    CEFARptdVal = (reader.IsDBNull(reader.GetOrdinal("CEFARptValue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CEFARptValue")),
                                    CEFAVal = (reader.IsDBNull(reader.GetOrdinal("CEFAValue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CEFAValue")),
                                    CEFADt = reader.IsDBNull(reader.GetOrdinal("CEFADate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("CEFADate")),
                                    CEFACurr = reader["CEFACurr"] as string,

                                    NumisVal = (reader.IsDBNull(reader.GetOrdinal("NumisValue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NumisValue")),
                                    NumisDt = reader.IsDBNull(reader.GetOrdinal("NumisDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("NumisDate")),
                                    NumisCurr = reader["NumisCurr"] as string,

                                    PHVal = (reader.IsDBNull(reader.GetOrdinal("PHValue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PHValue")),
                                    PHDt = reader.IsDBNull(reader.GetOrdinal("PHDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("PHDate")),
                                    PHCurr = reader["PHCurr"] as string,
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


        public IList<BloombergOverridesTO> GetBloombergOverrides(string StartDate, string EndDate)
        {
            IList<BloombergOverridesTO> fulllist = new List<BloombergOverridesTO>();
            try
            {
                string sql = GetBloombergOverridesQuery;
                sql += " where Date between '" + StartDate + "' and '" + EndDate + "'";
                sql += " order by Date desc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                BloombergOverridesTO listitem = new BloombergOverridesTO
                                {
                                    System4data = reader["System4data"] as string,
                                    Ticker = reader["Ticker"] as string,
                                    Date1 = reader.IsDBNull(reader.GetOrdinal("Date")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("Date")),
                                    Price = (reader.IsDBNull(reader.GetOrdinal("Price"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price")),
                                    Nav = (reader.IsDBNull(reader.GetOrdinal("Nav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav")),
                                    Comments = reader["Comments"] as string,
                                };

                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing Bloomberg Overrides details ");
                throw;
            }
            return fulllist;
        }

        public void SaveBloombergOverrides(IList<BloombergOverridesTO> items)
        {
            try
            {
                StringBuilder sCommand = new StringBuilder("insert into almitasc_ACTradingData.globalcef_fundhistorydatacorrection"
                    + " (System4data, ticker, date, price, Nav, comments) values ");

                int rowsToInsert = 0;
                string tradeIds = string.Empty;

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlTransaction trans = connection.BeginTransaction())
                    {

                        IList<string> Rows = new List<string>();
                        StringBuilder sb = new StringBuilder();
                        foreach (BloombergOverridesTO data in items)
                        {
                            DateTime dt = data.Date1 ?? DateTime.Now;
                            string mdate = dt.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);

                            if (!string.IsNullOrEmpty(data.Action)
                               && !string.IsNullOrEmpty(data.Ticker)
                               && "Y".Equals(data.Action, StringComparison.CurrentCultureIgnoreCase))
                            {
                                rowsToInsert++;

                                if (!string.IsNullOrEmpty(data.System4data))
                                    sb.Append(string.Concat("'", data.System4data, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                if (!string.IsNullOrEmpty(data.Ticker))
                                    sb.Append(string.Concat("'", data.Ticker, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                if (mdate != "")
                                    sb.Append(string.Concat("'", mdate, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                if (data.Price.HasValue)
                                    sb.Append(data.Price).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                if (data.Nav.HasValue)
                                    sb.Append(data.Nav).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                if (!string.IsNullOrEmpty(data.Comments))
                                    sb.Append(string.Concat("'", data.Comments, "'"));
                                else
                                    sb.Append("null");

                                string row = sb.ToString();
                                Rows.Add(string.Concat("(", row, ")"));
                                sb.Clear();
                            }
                        }

                        sCommand.Append(string.Join(",", Rows));
                        sCommand.Append(";");

                        if (rowsToInsert > 0)
                        {
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), connection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                        }
                        trans.Commit();
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving Bloomberg overrides details into database");
                throw;
            }
        }

        public IList<NumisDataOverridesTO> GetNumisDataOverrides(string StartDate, string EndDate)
        {
            IList<NumisDataOverridesTO> fulllist = new List<NumisDataOverridesTO>();
            try
            {
                string sql = GetNumisdataOverridesQuery;
                sql += " where Date between '" + StartDate + "' and '" + EndDate + "'";
                sql += " order by Date desc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                NumisDataOverridesTO listitem = new NumisDataOverridesTO
                                {
                                    Ticker = reader["Ticker"] as string,
                                    Date1 = reader.IsDBNull(reader.GetOrdinal("Date")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("Date")),
                                    Nav = (reader.IsDBNull(reader.GetOrdinal("Nav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav")),
                                    Source = reader["Source"] as string,
                                };

                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing Numis Overrides details ");
                throw;
            }
            return fulllist;
        }

        public void SaveNumisOverrides(IList<NumisDataOverridesTO> items)
        {
            try
            {
                StringBuilder sCommand = new StringBuilder("insert into almitasc_ACTradingData.globalcef_fundhistorydatacorrection4uknav"
                    + " (ticker, date, Nav, source) values ");

                int rowsToInsert = 0;
                string tradeIds = string.Empty;

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlTransaction trans = connection.BeginTransaction())
                    {
                        IList<string> Rows = new List<string>();
                        StringBuilder sb = new StringBuilder();
                        foreach (NumisDataOverridesTO data in items)
                        {
                            DateTime dt = data.Date1 ?? DateTime.Now;
                            string mdate = dt.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);

                            if (!string.IsNullOrEmpty(data.Action)
                               && !string.IsNullOrEmpty(data.Ticker)
                               && "Y".Equals(data.Action, StringComparison.CurrentCultureIgnoreCase))
                            {
                                rowsToInsert++;

                                if (!string.IsNullOrEmpty(data.Ticker))
                                    sb.Append(string.Concat("'", data.Ticker, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                if (mdate != "")
                                    sb.Append(string.Concat("'", mdate, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                if (data.Nav.HasValue)
                                    sb.Append(data.Nav).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                if (!string.IsNullOrEmpty(data.Source))
                                    sb.Append(string.Concat("'", data.Source, "'"));
                                else
                                    sb.Append("null");

                                string row = sb.ToString();
                                Rows.Add(string.Concat("(", row, ")"));
                                sb.Clear();

                            }
                        }

                        sCommand.Append(string.Join(",", Rows));
                        sCommand.Append(";");

                        if (rowsToInsert > 0)
                        {
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), connection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                        }
                        trans.Commit();
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving Numis overrides details into database");
                throw;
            }
        }

        public IList<PriceOverrideTO> GetPriceOverrides(string StartDate, string EndDate)
        {
            IList<PriceOverrideTO> fulllist = new List<PriceOverrideTO>();
            try
            {
                string sql = GetPriceOverridesQuery;
                sql += " where PriceDate between '" + StartDate + "' and '" + EndDate + "'";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                PriceOverrideTO listitem = new PriceOverrideTO
                                {
                                    PriceId = reader.IsDBNull(reader.GetOrdinal("Id")) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("Id")),
                                    Ticker = reader["Ticker"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    Cusip = reader["Cusip"] as string,
                                    ISIN = reader["ISIN"] as string,
                                    Curr = reader["Curr"] as string,
                                    PriceDate = reader.IsDBNull(reader.GetOrdinal("PriceDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("PriceDate")),
                                    Price = (reader.IsDBNull(reader.GetOrdinal("Price"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price")),
                                    Comments = reader["Comments"] as string,
                                };

                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error getting Price Overrides details ");
                throw;
            }
            return fulllist;
        }

        public void SavePriceOverrides(IList<PriceOverrideTO> items)
        {
            try
            {
                StringBuilder sCommand = new StringBuilder("");
                StringBuilder sCommandInsert = new StringBuilder("insert into almitasc_ACTradingBBGData.PriceOverride"
                    + " (Ticker, Sedol, Cusip, ISIN, Curr, Pricedate, Price, Comments, ModifyDate) values ");

                StringBuilder sCommandUpdate = new StringBuilder("update almitasc_ACTradingBBGData.PriceOverride set ");

                int rowsToInsert = 0;
                int rowstoUpdate = 0;
                string tradeIds = string.Empty;

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlTransaction trans = connection.BeginTransaction())
                    {
                        IList<string> Rows = new List<string>();
                        IList<string> Rowsupdate = new List<string>();
                        StringBuilder sb = new StringBuilder();
                        StringBuilder sbupdate = new StringBuilder();
                        foreach (PriceOverrideTO data in items)
                        {
                            DateTime dt = data.PriceDate ?? DateTime.Now;
                            string pdate = dt.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
                            string mdate = dt.ToString("yyyy-MM-dd HH: mm: ss", CultureInfo.InvariantCulture);

                            if (!string.IsNullOrEmpty(data.Ticker)
                               && !string.IsNullOrEmpty(data.Ticker)
                               && "I".Equals(data.Action, StringComparison.CurrentCultureIgnoreCase))
                            {
                                rowsToInsert++;

                                if (!string.IsNullOrEmpty(data.Ticker))
                                    sb.Append(string.Concat("'", data.Ticker, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                if (!string.IsNullOrEmpty(data.Sedol))
                                    sb.Append(string.Concat("'", data.Sedol, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                if (!string.IsNullOrEmpty(data.Cusip))
                                    sb.Append(string.Concat("'", data.Cusip, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                if (!string.IsNullOrEmpty(data.ISIN))
                                    sb.Append(string.Concat("'", data.ISIN, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                if (!string.IsNullOrEmpty(data.Curr))
                                    sb.Append(string.Concat("'", data.Curr, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                if (pdate != "")
                                    sb.Append(string.Concat("'", pdate, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                if (data.Price.HasValue)
                                    sb.Append(data.Price).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                if (!string.IsNullOrEmpty(data.Comments))
                                    sb.Append(string.Concat("'", data.Comments, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                if (mdate != "")
                                    sb.Append(string.Concat("'", mdate, "'"));
                                else
                                    sb.Append("null");

                                string row = sb.ToString();
                                Rows.Add(string.Concat("(", row, ")"));
                                sb.Clear();
                            }

                            if (!string.IsNullOrEmpty(data.Ticker)
                               && !string.IsNullOrEmpty(data.Ticker)
                               && "U".Equals(data.Action, StringComparison.CurrentCultureIgnoreCase))
                            {
                                rowstoUpdate++;

                                if (!string.IsNullOrEmpty(data.Ticker))
                                    sbupdate.Append(string.Concat("Ticker = '", data.Ticker, "'")).Append(DELIMITER);
                                else
                                    sbupdate.Append("Ticker = null").Append(DELIMITER);

                                if (!string.IsNullOrEmpty(data.Sedol))
                                    sbupdate.Append(string.Concat("Sedol = '", data.Sedol, "'")).Append(DELIMITER);
                                else
                                    sbupdate.Append("Sedol = null").Append(DELIMITER);

                                if (!string.IsNullOrEmpty(data.Cusip))
                                    sbupdate.Append(string.Concat("Cusip = '", data.Cusip, "'")).Append(DELIMITER);
                                else
                                    sbupdate.Append("Cusip = null").Append(DELIMITER);

                                if (!string.IsNullOrEmpty(data.ISIN))
                                    sbupdate.Append(string.Concat("ISIN = '", data.ISIN, "'")).Append(DELIMITER);
                                else
                                    sbupdate.Append("ISIN = null").Append(DELIMITER);

                                if (!string.IsNullOrEmpty(data.Curr))
                                    sbupdate.Append(string.Concat("Curr = '", data.Curr, "'")).Append(DELIMITER);
                                else
                                    sbupdate.Append("Curr = null").Append(DELIMITER);

                                if (pdate != "")
                                    sbupdate.Append(string.Concat("PriceDate = '", pdate, "'")).Append(DELIMITER);
                                else
                                    sbupdate.Append("PriceDate = null").Append(DELIMITER);

                                if (data.Price.HasValue)
                                    sbupdate.Append("Price = " + data.Price).Append(DELIMITER);
                                else
                                    sbupdate.Append("Price = null").Append(DELIMITER);

                                if (!string.IsNullOrEmpty(data.Comments))
                                    sbupdate.Append(string.Concat("Comments = '", data.Comments, "'")).Append(DELIMITER);
                                else
                                    sb.Append("Comments = null").Append(DELIMITER);

                                sbupdate.Append(string.Concat("ModifyDate = '", mdate, "'"));
                                sbupdate.Append(string.Concat(" where Id = " + data.PriceId + ";"));

                                string row = sCommandUpdate.ToString() + sbupdate.ToString();
                                Rowsupdate.Add(string.Join("", row));
                                sbupdate.Clear();
                            }
                        }

                        sCommandInsert.Append(string.Join(",", Rows));
                        sCommandInsert.Append(";");

                        if (rowstoUpdate > 0)
                            sCommand.Append(string.Join("", Rowsupdate));

                        if (rowsToInsert > 0)
                            sCommand = sCommand.Append(sCommandInsert.ToString());

                        if (rowsToInsert > 0 || rowstoUpdate > 0)
                        {
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), connection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                        }
                        trans.Commit();
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving Price overrides details into database");
                throw;
            }
        }

        public IList<CEFAReportTO> GetCEFAReport(string Ticker, string StartDate, string EndDate)
        {
            IList<CEFAReportTO> fulllist = new List<CEFAReportTO>();
            try
            {
                string sql = GetCEFAReportQuery;
                sql += " where FileDate between '" + StartDate + "' and '" + EndDate + "'";
                sql += " and Ticker = '" + Ticker + "'";
                sql += " order by FileDate desc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                CEFAReportTO listitem = new CEFAReportTO
                                {
                                    Ticker = reader["Ticker"] as string,
                                    MainGroup = reader["MainGroup"] as string,
                                    SubGroup = reader["SubGroup"] as string,
                                    MarketPrice = (reader.IsDBNull(reader.GetOrdinal("MarketPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarketPrice")),
                                    IncYield = (reader.IsDBNull(reader.GetOrdinal("IncYield"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IncYield")),
                                    TotalYield = (reader.IsDBNull(reader.GetOrdinal("TotalYield"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotalYield")),
                                    NAVYield = (reader.IsDBNull(reader.GetOrdinal("NAVYield"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NAVYield")),
                                    LevAdjNAVYield = (reader.IsDBNull(reader.GetOrdinal("LevAdjNAVYield"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LevAdjNAVYield")),
                                    DistribAmount = (reader.IsDBNull(reader.GetOrdinal("DistribAmount"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DistribAmount")),
                                    Earnings_Share = (reader.IsDBNull(reader.GetOrdinal("Earnings_Share"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Earnings_Share")),
                                    EarningsYield = (reader.IsDBNull(reader.GetOrdinal("EarningsYield"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EarningsYield")),
                                    DistributionPolicy = reader["DistributionPolicy"] as string,
                                    Frequency = reader["Frequency"] as string,
                                    PctRoC12m = (reader.IsDBNull(reader.GetOrdinal("PctRoC12m"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PctRoC12m")),
                                    PctIncome12m = (reader.IsDBNull(reader.GetOrdinal("PctIncome12m"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PctIncome12m")),
                                    PctShortGain12m = (reader.IsDBNull(reader.GetOrdinal("PctShortGain12m"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PctShortGain12m")),
                                    PctLongGain12m = (reader.IsDBNull(reader.GetOrdinal("PctLongGain12m"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PctLongGain12m")),
                                    Inc_DecPct = (reader.IsDBNull(reader.GetOrdinal("Inc_DecPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Inc_DecPct")),
                                    LastChange = (reader.IsDBNull(reader.GetOrdinal("LastChange"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("LastChange")),
                                    UNII = (reader.IsDBNull(reader.GetOrdinal("UNII"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UNII")),
                                    UNIIDate = (reader.IsDBNull(reader.GetOrdinal("UNIIDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("UNIIDate")),
                                    UNIIFrequency = reader["UNIIFrequency"] as string,
                                    UNIITrendPct = (reader.IsDBNull(reader.GetOrdinal("UNIITrendPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UNIITrendPct")),
                                    EarningCoverageRatioPct = (reader.IsDBNull(reader.GetOrdinal("EarningCoverageRatioPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EarningCoverageRatioPct")),
                                    CapGainPct = (reader.IsDBNull(reader.GetOrdinal("CapGainPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CapGainPct")),
                                    LevPct = (reader.IsDBNull(reader.GetOrdinal("LevPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LevPct")),
                                    StructuralLevPct = (reader.IsDBNull(reader.GetOrdinal("StructuralLevPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("StructuralLevPct")),
                                    PctLeverageisStructural = (reader.IsDBNull(reader.GetOrdinal("PctLeverageisStructural"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PctLeverageisStructural")),
                                    PortfolioLevPct = (reader.IsDBNull(reader.GetOrdinal("PortfolioLevPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PortfolioLevPct")),
                                    LeverageCostPct = (reader.IsDBNull(reader.GetOrdinal("LeverageCostPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LeverageCostPct")),
                                    LevTypeI = (reader.IsDBNull(reader.GetOrdinal("LevTypeI"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LevTypeI")),
                                    LevSubtypeI = (reader.IsDBNull(reader.GetOrdinal("LevSubtypeI"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LevSubtypeI")),
                                    LevTypeII = (reader.IsDBNull(reader.GetOrdinal("LevTypeII"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LevTypeII")),
                                    LevSubtypeII = (reader.IsDBNull(reader.GetOrdinal("LevSubtypeII"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LevSubtypeII")),
                                    LevTypeIII = (reader.IsDBNull(reader.GetOrdinal("LevTypeIII"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LevTypeIII")),
                                    LevSubtypeIII = (reader.IsDBNull(reader.GetOrdinal("LevSubtypeIII"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LevSubtypeIII")),
                                    ExpRatio = (reader.IsDBNull(reader.GetOrdinal("ExpRatio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExpRatio")),
                                    SharesOutstanding = (reader.IsDBNull(reader.GetOrdinal("SharesOutstanding"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SharesOutstanding")),
                                    PctSharesOwnedbyInstitutions = (reader.IsDBNull(reader.GetOrdinal("PctSharesOwnedbyInstitutions"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PctSharesOwnedbyInstitutions")),
                                    InstitutionsPctTrend = reader["InstitutionsPctTrend"] as string,
                                    PctSharesOwnedbyActivists = (reader.IsDBNull(reader.GetOrdinal("PctSharesOwnedbyActivists"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PctSharesOwnedbyActivists")),
                                    ActivistsPctTrend = (reader.IsDBNull(reader.GetOrdinal("ActivistsPctTrend"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ActivistsPctTrend")),
                                    AverageDuration = (reader.IsDBNull(reader.GetOrdinal("AverageDuration"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AverageDuration")),
                                    AverageMaturity = (reader.IsDBNull(reader.GetOrdinal("AverageMaturity"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AverageMaturity")),
                                    AverageBondPrice = (reader.IsDBNull(reader.GetOrdinal("AverageBondPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AverageBondPrice")),
                                    AverageCouponRate = (reader.IsDBNull(reader.GetOrdinal("AverageCouponRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AverageCouponRate")),
                                    CreditQuality = reader["CreditQuality"] as string,
                                    UnratedBondsPct = (reader.IsDBNull(reader.GetOrdinal("UnratedBondsPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnratedBondsPct")),
                                    InvestmentGradePct = (reader.IsDBNull(reader.GetOrdinal("InvestmentGradePct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("InvestmentGradePct")),
                                    NonInvestmentGradePct = (reader.IsDBNull(reader.GetOrdinal("NonInvestmentGradePct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NonInvestmentGradePct")),
                                    AAA = reader["AAA"] as string,
                                    AA = reader["AA"] as string,
                                    A = reader["A"] as string,
                                    BBB = reader["BBB"] as string,
                                    BB = reader["BB"] as string,
                                    B = reader["B"] as string,
                                    CCC = reader["CCC"] as string,
                                    CC = reader["CC"] as string,
                                    C = reader["C"] as string,
                                    D = reader["D"] as string,
                                    InsuredPct = (reader.IsDBNull(reader.GetOrdinal("InsuredPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("InsuredPct")),
                                    CA = reader["CA"] as string,
                                    CT = reader["CT"] as string,
                                    GU = reader["GU"] as string,
                                    IL = reader["IL"] as string,
                                    NJ = reader["NJ"] as string,
                                    NY = reader["NY"] as string,
                                    PR = reader["PR"] as string,
                                    VI = reader["VI"] as string,
                                    PctUSEquity = (reader.IsDBNull(reader.GetOrdinal("PctUSEquity"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PctUSEquity")),
                                    PctNonUSEquity = (reader.IsDBNull(reader.GetOrdinal("PctNonUSEquity"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PctNonUSEquity")),
                                    PctUSBonds = (reader.IsDBNull(reader.GetOrdinal("PctUSBonds"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PctUSBonds")),
                                    PctNonUSBonds = (reader.IsDBNull(reader.GetOrdinal("PctNonUSBonds"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PctNonUSBonds")),
                                    PctCash_ShortTermInvestment = (reader.IsDBNull(reader.GetOrdinal("PctCash_ShortTermInvestment"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PctCash_ShortTermInvestment")),
                                    PctOption = (reader.IsDBNull(reader.GetOrdinal("PctOption"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PctOption")),
                                    PctCommodities = (reader.IsDBNull(reader.GetOrdinal("PctCommodities"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PctCommodities")),
                                    InceptionDate = (reader.IsDBNull(reader.GetOrdinal("InceptionDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("InceptionDate")),
                                    NonLevExpenseRatio = (reader.IsDBNull(reader.GetOrdinal("NonLevExpenseRatio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NonLevExpenseRatio")),
                                    PctSharesOwnedbyInsiders = (reader.IsDBNull(reader.GetOrdinal("PctSharesOwnedbyInsiders"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PctSharesOwnedbyInsiders")),
                                    Energy = (reader.IsDBNull(reader.GetOrdinal("Energy"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Energy")),
                                    MLPs = (reader.IsDBNull(reader.GetOrdinal("MLPs"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MLPs")),
                                    Utilities_Infrastructure = (reader.IsDBNull(reader.GetOrdinal("Utilities_Infrastructure"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Utilities_Infrastructure")),
                                    Retail_Diversified = (reader.IsDBNull(reader.GetOrdinal("Retail_Diversified"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Retail_Diversified")),
                                    Transport_Education = (reader.IsDBNull(reader.GetOrdinal("Transport_Education"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Transport_Education")),
                                    InformationTechnology_TelecomServices = (reader.IsDBNull(reader.GetOrdinal("InformationTechnology_TelecomServices"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("InformationTechnology_TelecomServices")),
                                    Consumerstaples_ConsumerDiscretionary = (reader.IsDBNull(reader.GetOrdinal("Consumerstaples_ConsumerDiscretionary"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Consumerstaples_ConsumerDiscretionary")),
                                    Banking_Financialservices_Insurance = (reader.IsDBNull(reader.GetOrdinal("Banking_Financialservices_Insurance"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Banking_Financialservices_Insurance")),
                                    Industrials_Materials = (reader.IsDBNull(reader.GetOrdinal("Industrials_Materials"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Industrials_Materials")),
                                    Healthcare_BioTechStocks_Pharma = (reader.IsDBNull(reader.GetOrdinal("Healthcare_BioTechStocks_Pharma"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Healthcare_BioTechStocks_Pharma")),
                                    Entertainment_Cable_Satellite_Broadcasting = (reader.IsDBNull(reader.GetOrdinal("Entertainment_Cable_Satellite_ Broadcasting"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Entertainment_Cable_Satellite_ Broadcasting")),
                                    REITs_Others = (reader.IsDBNull(reader.GetOrdinal("REITs_Others"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("REITs_Others")),
                                    USREIT = (reader.IsDBNull(reader.GetOrdinal("USREIT"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("USREIT")),
                                    NonUSReit = (reader.IsDBNull(reader.GetOrdinal("NonUSReit"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NonUSReit")),
                                    Emerging_FrontierMarketsEquity = (reader.IsDBNull(reader.GetOrdinal("Emerging_FrontierMarketsEquity"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Emerging_FrontierMarketsEquity")),
                                    CommonStock = (reader.IsDBNull(reader.GetOrdinal("CommonStock"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CommonStock")),
                                    PreferredStock = (reader.IsDBNull(reader.GetOrdinal("PreferredStock"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PreferredStock")),
                                    CorporateDebtSecurities_CorporateBonds = (reader.IsDBNull(reader.GetOrdinal("CorporateDebtSecurities_CorporateBonds"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CorporateDebtSecurities_CorporateBonds")),
                                    MunicipalBonds = (reader.IsDBNull(reader.GetOrdinal("MunicipalBonds"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MunicipalBonds")),
                                    SecuritiesLendingcollateral = (reader.IsDBNull(reader.GetOrdinal("SecuritiesLendingcollateral"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SecuritiesLendingcollateral")),
                                    HighYieldMuniBonds = (reader.IsDBNull(reader.GetOrdinal("HighYieldMuniBonds"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("HighYieldMuniBonds")),
                                    InvestmentGradeBond = (reader.IsDBNull(reader.GetOrdinal("InvestmentGradeBond"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("InvestmentGradeBond")),
                                    NonUSSovereignBonds = (reader.IsDBNull(reader.GetOrdinal("NonUSSovereignBonds"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NonUSSovereignBonds")),
                                    US_GovtBonds = (reader.IsDBNull(reader.GetOrdinal("US GovtBonds"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("US GovtBonds")),
                                    HighYield_JunkBonds = (reader.IsDBNull(reader.GetOrdinal("HighYield_JunkBonds"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("HighYield_JunkBonds")),
                                    EmergingMarketBonds = (reader.IsDBNull(reader.GetOrdinal("EmergingMarketBonds"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EmergingMarketBonds")),
                                    MortgageBonds_ABS_MBS = (reader.IsDBNull(reader.GetOrdinal("MortgageBonds_ABS_MBS"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MortgageBonds_ABS_MBS")),
                                    Loan_SrLoan_FloatingRateLoan = (reader.IsDBNull(reader.GetOrdinal("Loan_SrLoan_FloatingRateLoan"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Loan_SrLoan_FloatingRateLoan")),
                                    Commodity = (reader.IsDBNull(reader.GetOrdinal("Commodity"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Commodity")),
                                    Cash_ShrtTerm = (reader.IsDBNull(reader.GetOrdinal("Cash_ShrtTerm"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Cash_ShrtTerm")),
                                    OptionWritten = (reader.IsDBNull(reader.GetOrdinal("OptionWritten"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OptionWritten")),
                                    Netotherassets = (reader.IsDBNull(reader.GetOrdinal("Netotherassets"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Netotherassets")),
                                    NorthAmerican = (reader.IsDBNull(reader.GetOrdinal("NorthAmerican"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NorthAmerican")),
                                    LatinAmerican = (reader.IsDBNull(reader.GetOrdinal("LatinAmerican"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LatinAmerican")),
                                    SouthAmerica = (reader.IsDBNull(reader.GetOrdinal("SouthAmerica"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SouthAmerica")),
                                    European = (reader.IsDBNull(reader.GetOrdinal("European"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("European")),
                                    WesternEurope = (reader.IsDBNull(reader.GetOrdinal("WesternEurope"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("WesternEurope")),
                                    EasternEurope = (reader.IsDBNull(reader.GetOrdinal("EasternEurope"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EasternEurope")),
                                    DevelopedMarketsUSWesternEuropeJapan = (reader.IsDBNull(reader.GetOrdinal("DevelopedMarketsUSWesternEuropeJapan"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DevelopedMarketsUSWesternEuropeJapan")),
                                    _13DHoldersPct = (reader.IsDBNull(reader.GetOrdinal("13DHoldersPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("13DHoldersPct")),
                                    _13GHoldersPct = (reader.IsDBNull(reader.GetOrdinal("13GHoldersPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("13GHoldersPct")),
                                    CombinedActivistHoldersPct = (reader.IsDBNull(reader.GetOrdinal("CombinedActivistHoldersPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CombinedActivistHoldersPct")),
                                    GrossExpRatioExLevCost = (reader.IsDBNull(reader.GetOrdinal("GrossExpRatioExLevCost"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("GrossExpRatioExLevCost")),
                                    Director_TrusteeCompensation = (reader.IsDBNull(reader.GetOrdinal("Director_TrusteeCompensation"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Director_TrusteeCompensation")),
                                    TenderFrom = reader["TenderFrom"] as string,
                                    TenderCommencedOn = (reader.IsDBNull(reader.GetOrdinal("TenderCommencedOn"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("TenderCommencedOn")),
                                    TenderExpirationDate = (reader.IsDBNull(reader.GetOrdinal("TenderExpirationDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("TenderExpirationDate")),
                                    TenderOfferPrice = reader["TenderOfferPrice"] as string,
                                    TenderIntendtobuyuptoPct = (reader.IsDBNull(reader.GetOrdinal("TenderIntendtobuyuptoPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TenderIntendtobuyuptoPct")),
                                    TenderedPct = (reader.IsDBNull(reader.GetOrdinal("TenderedPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TenderedPct")),
                                    TenderPurchasedPct = (reader.IsDBNull(reader.GetOrdinal("TenderPurchasedPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TenderPurchasedPct")),
                                    TenderPurchasePrice = (reader.IsDBNull(reader.GetOrdinal("TenderPurchasePrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TenderPurchasePrice")),
                                    RepurchasePrCommencedon = (reader.IsDBNull(reader.GetOrdinal("RepurchasePrCommencedon"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("RepurchasePrCommencedon")),
                                    RepurchasePrExpirationDate = (reader.IsDBNull(reader.GetOrdinal("RepurchasePrExpirationDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("RepurchasePrExpirationDate")),
                                    RepurchasePrIntendtobuyuptoPct = (reader.IsDBNull(reader.GetOrdinal("RepurchasePrIntendtobuyuptoPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RepurchasePrIntendtobuyuptoPct")),
                                    RepurchasePrTargetDiscountPct = (reader.IsDBNull(reader.GetOrdinal("RepurchasePrTargetDiscountPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RepurchasePrTargetDiscountPct")),
                                    RepurchasePrPurchasedPct = (reader.IsDBNull(reader.GetOrdinal("RepurchasePrPurchasedPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RepurchasePrPurchasedPct")),
                                    GrosstoNetAssetsLeverage = (reader.IsDBNull(reader.GetOrdinal("GrosstoNetAssetsLeverage"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("GrosstoNetAssetsLeverage")),
                                    AssetsReportedDate = (reader.IsDBNull(reader.GetOrdinal("AssetsReportedDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AssetsReportedDate")),
                                    RealizedCapGain = (reader.IsDBNull(reader.GetOrdinal("RealizedCapGain"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RealizedCapGain")),
                                    FileDate = (reader.IsDBNull(reader.GetOrdinal("FileDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),

                                };

                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing CEFA Report details ");
                throw;
            }
            return fulllist;
        }

        public IList<CANTenderReportTO> GetCANTenderReport(string ticker)
        {
            IList<CANTenderReportTO> fulllist = new List<CANTenderReportTO>();
            try
            {
                string sql = GetCANTenderHistoryQuery;
                if (!string.IsNullOrEmpty(ticker))
                    sql += " where Ticker = '" + ticker + "'";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                CANTenderReportTO listitem = new CANTenderReportTO
                                {
                                    Ticker = reader["Ticker"] as string,
                                    FundType = reader["FundType"] as string,
                                    Currency = reader["Currency"] as string,
                                    YearOfAnnouncement = (reader.IsDBNull(reader.GetOrdinal("YearOfAnnouncement"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("YearOfAnnouncement")),
                                    ExDateOfNotification = (reader.IsDBNull(reader.GetOrdinal("ExDateOfNotification"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("ExDateOfNotification")),
                                    OfferSize = (reader.IsDBNull(reader.GetOrdinal("OfferSize"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OfferSize")),
                                    NAVPercentage = (reader.IsDBNull(reader.GetOrdinal("NAVPercentage"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NAVPercentage")),
                                    RedemptionDate = (reader.IsDBNull(reader.GetOrdinal("RedemptionDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("RedemptionDate")),
                                    ALMPmtDate = (reader.IsDBNull(reader.GetOrdinal("ALMPmtDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("ALMPmtDate")),
                                    EstNav = (reader.IsDBNull(reader.GetOrdinal("EstNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EstNav")),
                                    RealizedNav = (reader.IsDBNull(reader.GetOrdinal("RealizedNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RealizedNav")),
                                    ImputedRedemptionFee = (reader.IsDBNull(reader.GetOrdinal("ImputedRedemptionFee"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ImputedRedemptionFee")),
                                    ALMQtyTendered = (reader.IsDBNull(reader.GetOrdinal("ALMQtyTendered"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ALMQtyTendered")),
                                    ALMQtyExecuted = (reader.IsDBNull(reader.GetOrdinal("ALMQtyExecuted"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ALMQtyExecuted")),
                                    ProRation = (reader.IsDBNull(reader.GetOrdinal("ProRation"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ProRation")),

                                };

                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing Canada Tendor Report details ");
                throw;
            }
            return fulllist;
        }
        public IList<USATenderReportTO> GetUSATenderReport(string ticker)
        {
            IList<USATenderReportTO> fulllist = new List<USATenderReportTO>();
            try
            {
                string sql = GetUSATenderHistoryQuery;
                if (!string.IsNullOrEmpty(ticker))
                    sql += " where Ticker = '" + ticker + "'";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                USATenderReportTO listitem = new USATenderReportTO
                                {
                                    Ticker = reader["Ticker"] as string,
                                    Currency = reader["Currency"] as string,
                                    DateAnnounced = (reader.IsDBNull(reader.GetOrdinal("DateAnnounced"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("DateAnnounced")),
                                    ExDate = (reader.IsDBNull(reader.GetOrdinal("ExDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("ExDate")),
                                    OfferSize = (reader.IsDBNull(reader.GetOrdinal("OfferSize"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OfferSize")),
                                    NAVPercentage = (reader.IsDBNull(reader.GetOrdinal("NAVPercentage"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NAVPercentage")),
                                    NAVLanguage = reader["NAVLanguage"] as string,
                                    ALMPmtDate = (reader.IsDBNull(reader.GetOrdinal("ALMPmtDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("ALMPmtDate")),
                                    EstNav = (reader.IsDBNull(reader.GetOrdinal("EstNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EstNav")),
                                    RealizedNav = (reader.IsDBNull(reader.GetOrdinal("RealizedNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RealizedNav")),
                                    ImputedRedemptionFee = (reader.IsDBNull(reader.GetOrdinal("ImputedRedemptionFee"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ImputedRedemptionFee")),
                                    ALMQtyTendered = (reader.IsDBNull(reader.GetOrdinal("ALMQtyTendered"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ALMQtyTendered")),
                                    ALMQtyExecuted = (reader.IsDBNull(reader.GetOrdinal("ALMQtyExecuted"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ALMQtyExecuted")),
                                    PercentTendered = (reader.IsDBNull(reader.GetOrdinal("PercentTendered"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PercentTendered")),
                                    ProRation = (reader.IsDBNull(reader.GetOrdinal("ProRation"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ProRation")),
                                };

                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing USA Tendor Report details ");
                throw;
            }
            return fulllist;
        }
        public IList<SecurityMasterTO> GetBrokerSecurityMapping()
        {
            IList<SecurityMasterTO> fulllist = new List<SecurityMasterTO>();
            try
            {
                string sql = GetBrokerSecurityMappingQuery;
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                SecurityMasterTO listitem = new SecurityMasterTO
                                {
                                    Broker = reader["broker"] as string,
                                    BrokerSystemId = reader["brokersystemid"] as string,
                                    Ticker = reader["ticker"] as string,
                                    YellowKey = reader["yellowkey"] as string,
                                    Description = reader["description"] as string,
                                    TradebookTicker = reader["tradebook_ticker"] as string,
                                    DateAdded = (reader.IsDBNull(reader.GetOrdinal("date_added"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("date_added")),
                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error getting Broker Security Mapping details", ex);
                throw;
            }
            return fulllist;
        }

        public void UpdateBrokerSecurityMapping(IList<SecurityMasterTO> items)
        {
            try
            {
                StringBuilder sCommand = new StringBuilder("");
                StringBuilder sCommandUpdate = new StringBuilder("update almitasc_ACTradingPM.portfoliomanager_securitymaster set ");

                int rowstoUpdate = 0;
                string tradeIds = string.Empty;

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlTransaction trans = connection.BeginTransaction())
                    {
                        IList<string> Rows = new List<string>();
                        IList<string> Rowsupdate = new List<string>();
                        StringBuilder sb = new StringBuilder();
                        StringBuilder sbupdate = new StringBuilder();
                        foreach (SecurityMasterTO data in items)
                        {
                            if (!string.IsNullOrEmpty(data.BrokerSystemId)
                               && !string.IsNullOrEmpty(data.BrokerSystemId)
                               && "U".Equals(data.Action, StringComparison.CurrentCultureIgnoreCase))
                            {
                                rowstoUpdate++;

                                //if (!string.IsNullOrEmpty(data.Broker))
                                //    sbupdate.Append(string.Concat("broker = '", data.Broker, "'")).Append(DELIMITER);
                                //else
                                //    sbupdate.Append("broker = null").Append(DELIMITER);

                                if (!string.IsNullOrEmpty(data.Ticker))
                                    sbupdate.Append(string.Concat("ticker = '", data.Ticker, "'")).Append(DELIMITER);
                                else
                                    sbupdate.Append("ticker = null").Append(DELIMITER);

                                if (!string.IsNullOrEmpty(data.YellowKey))
                                    sbupdate.Append(string.Concat("yellowkey = '", data.YellowKey, "'")).Append(DELIMITER);
                                else
                                    sbupdate.Append("yellowkey = null").Append(DELIMITER);

                                if (!string.IsNullOrEmpty(data.Description))
                                    sbupdate.Append(string.Concat("description = '", data.Description, "'")).Append(DELIMITER);
                                else
                                    sbupdate.Append("description = null").Append(DELIMITER);

                                DateTime dt = data.DateAdded ?? DateTime.Now;
                                string pdate = dt.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
                                if (pdate != "")
                                    sbupdate.Append(string.Concat("date_added = '", pdate, "'")).Append(DELIMITER);
                                else
                                    sbupdate.Append("date_added = null").Append(DELIMITER);

                                if (!string.IsNullOrEmpty(data.TradebookTicker))
                                    sbupdate.Append(string.Concat("tradebook_ticker = '", data.TradebookTicker, "'"));
                                else
                                    sbupdate.Append("tradebook_ticker = null");

                                sbupdate.Append(string.Concat(" where brokersystemid = '" + data.BrokerSystemId + "'"));
                                sbupdate.Append(string.Concat(" and broker = '" + data.Broker + "';"));

                                string row = sCommandUpdate.ToString() + sbupdate.ToString();
                                Rowsupdate.Add(string.Join("", row));
                                sbupdate.Clear();
                            }
                        }

                        if (rowstoUpdate > 0)
                        {
                            sCommand.Append(string.Join("", Rowsupdate));
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), connection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                        }
                        trans.Commit();
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving Broker Security Mapping details into database");
                throw;
            }
        }

        public void UpdateBrokerSecurityPositionMapping(IList<SecurityMasterTO> items)
        {
            try
            {
                StringBuilder sCommand = new StringBuilder("");
                StringBuilder sCommandUpdate = new StringBuilder("update almitasc_ACTradingPM.portfoliomanager_holdings set ");

                int rowstoUpdate = 0;
                string tradeIds = string.Empty;

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlTransaction trans = connection.BeginTransaction())
                    {
                        IList<string> Rows = new List<string>();
                        IList<string> Rowsupdate = new List<string>();
                        StringBuilder sb = new StringBuilder();
                        StringBuilder sbupdate = new StringBuilder();
                        foreach (SecurityMasterTO data in items)
                        {
                            if (!string.IsNullOrEmpty(data.BrokerSystemId)
                               && !string.IsNullOrEmpty(data.BrokerSystemId)
                               && "U".Equals(data.Action, StringComparison.CurrentCultureIgnoreCase))
                            {
                                rowstoUpdate++;

                                if (!string.IsNullOrEmpty(data.Ticker))
                                    sbupdate.Append(string.Concat("ticker = '", data.Ticker, "'")).Append(DELIMITER);

                                if (!string.IsNullOrEmpty(data.YellowKey))
                                    sbupdate.Append(string.Concat("ykey = '", data.YellowKey, "'"));

                                sbupdate.Append(string.Concat(" where brokersystemid = '" + data.BrokerSystemId + "'"));
                                sbupdate.Append(string.Concat(" and portfolio like '" + data.Broker + " %';"));

                                string row = sCommandUpdate.ToString() + sbupdate.ToString();
                                Rowsupdate.Add(string.Join("", row));
                                sbupdate.Clear();
                            }
                        }

                        if (rowstoUpdate > 0)
                        {
                            sCommand.Append(string.Join("", Rowsupdate));
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), connection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                        }
                        trans.Commit();
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error updating Broker Security Mapping in Position table", ex);
                throw;
            }
        }

        public void UpdateFundPortDates()
        {
            _logger.LogInformation("Update Fund Port Dates - STARTED");
            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetUpdateFundPortDatesQuery, connection))
                    {
                        command.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing query");
            }
            _logger.LogInformation("Update Fund Port Dates - DONE");
        }

        public IList<ALMTenderOfferHistTO> GetALMTenderOfferHistDetails()
        {
            IList<ALMTenderOfferHistTO> list = new List<ALMTenderOfferHistTO>();
            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetALMTenderOfferHistoryQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                ALMTenderOfferHistTO data = new ALMTenderOfferHistTO
                                {
                                    Ticker = reader["Ticker"] as string,
                                    Fund = reader["Fund"] as string,
                                    Broker = reader["Broker"] as string,
                                    CashSwapInd = reader["CashSwapInd"] as string,
                                    Qty = (reader.IsDBNull(reader.GetOrdinal("Qty"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("Qty")),
                                    TenderType = reader["TenderType"] as string,
                                    StartDate = (reader.IsDBNull(reader.GetOrdinal("StartDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("StartDate")),
                                    EndDate = (reader.IsDBNull(reader.GetOrdinal("EndDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EndDate")),
                                    ExpiryDate = (reader.IsDBNull(reader.GetOrdinal("ExpiryDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("ExpiryDate")),
                                    EffectiveDate = (reader.IsDBNull(reader.GetOrdinal("EffectiveDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate")),
                                    PaymentDate = (reader.IsDBNull(reader.GetOrdinal("PaymentDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("PaymentDate")),
                                    SharesSubmitted = (reader.IsDBNull(reader.GetOrdinal("SharesSubmitted"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("SharesSubmitted")),
                                    PctRedeemed = (reader.IsDBNull(reader.GetOrdinal("PctRedeemed"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PctRedeemed")),
                                    Status = reader["Status"] as string,
                                    UserId = reader["UserId"] as string,
                                    Notes = reader["Notes"] as string,

                                };
                                list.Add(data);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error Tender Offer history executing query");
                throw;
            }
            return list;
        }

        public void SaveALMTenderOfferHistDetails(IList<ALMTenderOfferHistTO> list)
        {
            StringBuilder sCommand = new StringBuilder("insert into almitasc_ACTradingBBGData.StgALMTenderOfferHist " +
                    "(Ticker,Fund,Broker,CashSwapInd,Qty,TenderType,StartDate,EndDate,ExpiryDate,EffectiveDate," +
                     "PaymentDate,SharesSubmitted,PctRedeemed,Status,UserId,Notes) values ");

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    if (list != null && list.Count > 0)
                    {
                        using (MySqlTransaction trans = connection.BeginTransaction())
                        {
                            _logger.LogInformation("Deleting data from almitasc_ACTradingBBGData.StgALMTenderOfferHist");
                            string sqlDelete = "delete from almitasc_ACTradingBBGData.StgALMTenderOfferHist";
                            using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                            {
                                command.ExecuteNonQuery();
                            }
                            _logger.LogInformation("Saving data to almitasc_ACTradingBBGData.StgALMTenderOfferHist");

                            List<string> Rows = new List<string>();
                            StringBuilder sb = new StringBuilder();
                            foreach (ALMTenderOfferHistTO data in list)
                            {
                                //Ticker
                                if (!string.IsNullOrEmpty(data.Ticker))
                                    sb.Append(string.Concat("'", data.Ticker, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                //Fund
                                if (!string.IsNullOrEmpty(data.Fund))
                                    sb.Append(string.Concat("'", data.Fund, "'")).Append(DELIMITER);
                                else

                                    sb.Append("null").Append(DELIMITER);

                                //Broker
                                if (!string.IsNullOrEmpty(data.Broker))
                                    sb.Append(string.Concat("'", data.Broker, "'")).Append(DELIMITER);
                                else

                                    sb.Append("null").Append(DELIMITER);

                                //CashSwapInd
                                if (!string.IsNullOrEmpty(data.CashSwapInd))
                                    sb.Append(string.Concat("'", data.CashSwapInd, "'")).Append(DELIMITER);
                                else

                                    sb.Append("null").Append(DELIMITER);

                                //Qty
                                if (data.Qty.HasValue)
                                    sb.Append(data.Qty).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                //TenderType
                                if (!string.IsNullOrEmpty(data.TenderType))
                                    sb.Append(string.Concat("'", data.TenderType, "'")).Append(DELIMITER);
                                else

                                    sb.Append("null").Append(DELIMITER);

                                //StartDate
                                if (data.StartDate.HasValue)
                                    sb.Append(string.Concat("'", DateUtils.ConvertDate(data.StartDate, DATEFORMAT, "0000-00-00"), "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                //EndDate
                                if (data.EndDate.HasValue)
                                    sb.Append(string.Concat("'", DateUtils.ConvertDate(data.EndDate, DATEFORMAT, "0000-00-00"), "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                //ExpiryDate
                                if (data.ExpiryDate.HasValue)
                                    sb.Append(string.Concat("'", DateUtils.ConvertDate(data.ExpiryDate, DATEFORMAT, "0000-00-00"), "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                //EffectiveDate
                                if (data.EffectiveDate.HasValue)
                                    sb.Append(string.Concat("'", DateUtils.ConvertDate(data.EffectiveDate, DATEFORMAT, "0000-00-00"), "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                //PaymentDate
                                if (data.PaymentDate.HasValue)
                                    sb.Append(string.Concat("'", DateUtils.ConvertDate(data.PaymentDate, DATEFORMAT, "0000-00-00"), "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                //SharesSubmitted
                                if (data.SharesSubmitted.HasValue)
                                    sb.Append(data.SharesSubmitted).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                //PctRedeemed
                                if (data.PctRedeemed.HasValue)
                                    sb.Append(data.PctRedeemed).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                //Status
                                if (!string.IsNullOrEmpty(data.Status))
                                    sb.Append(string.Concat("'", data.Status, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                //UserId
                                if (!string.IsNullOrEmpty(data.UserId))
                                    sb.Append(string.Concat("'", data.UserId, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                //Notes
                                if (!string.IsNullOrEmpty(data.Notes))
                                    sb.Append(string.Concat("'", data.Notes, "'"));
                                else
                                    sb.Append("null");

                                string row = sb.ToString();
                                Rows.Add(string.Concat("(", row, ")"));
                                sb.Clear();
                            }

                            sCommand.Append(string.Join(",", Rows));
                            sCommand.Append(";");

                            using (MySqlCommand command = new MySqlCommand(sCommand.ToString(), connection))
                            {
                                command.CommandType = CommandType.Text;
                                command.ExecuteNonQuery();
                            }

                            _logger.LogInformation("Moving data to almitasc_ACTradingBBGData.ALMTenderOfferHist");
                            string sql = "almitasc_ACTradingBBGData.spPopulateTenderOffersHistory";
                            using (MySqlCommand command = new MySqlCommand(sql, connection))
                            {
                                command.CommandType = System.Data.CommandType.StoredProcedure;
                                command.ExecuteNonQuery();
                            }
                            trans.Commit();
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving Tender Offer Details data");
            }
        }

        public string GetRegressionCoefficientsQuery = "select distinct fund_ticker, period, rsqrd, trim(replace(etf_ticker, '  ', ' ')) as etf_ticker, beta from almitasc_ACTradingModeling.globalcef_etfregression";
        public string GetFundNavsQuery = "call almitasc_ACTradingBBGData.spGetFundLatestNAVs";
        //public string GetPortfolioNavsQuery = "call almitasc_ACTradingBBGData.spCalcTotalFundReturnsTest";
        public string GetPortfolioNavsQuery = "call Model.spCalcTotalFundReturns";
        public string GetMSFundHistoryQuery = "select s.ticker, m.* from almitasc_ACTradingBBGLink.globaltrading_securitymaster s join almitasc_ACTrading.globalcef_fundhistory4morningstar m on (s.secid = m.secid)";
        public string GetMSExpenseHistoryQuery = "select s.ticker, m.* from almitasc_ACTradingBBGLink.globaltrading_securitymaster s join almitasc_ACTrading.globalcef_fundhistoryexpenseratio m on (s.secid = m.secid)";
        public string GetFundStatsQuery = "call almitasc_ACTradingBBGData.spGetFundStats";
        public string GetFundMasterQuery = "call almitasc_ACTradingBBGData.spGetFundMasterDetailsNew";
        public string GetHoldingsQuery = "call Reporting.spGetALMHoldings";
        public string GetHoldingDetailsByPortQuery = "call Reporting.spGetALMHoldingsByPort";
        public string GetHoldingHistoryQuery = "Reporting.spGetALMHoldingsHistoryByPort";
        public string GetHoldingHistoryByPortQuery = "Reporting.spGetALMHoldingsByPortNew";
        public string GetEODHoldingDetailsByPortQuery = "call Reporting.spGetEODAlmHoldingsByPort";
        ////////////////////////////////public string GetHoldingsQuery = "call almitasc_ACTradingBBGData.spGetALMHoldingsNew";
        ////////////////////////////////public string GetHoldingDetailsByPortQuery = "call almitasc_ACTradingBBGData.spGetALMHoldingsByPort";
        ////////////////////////////////public string GetHoldingHistoryQuery = "spGetALMHoldingsHistoryByPort";
        ////////////////////////////////public string GetHoldingHistoryByPortQuery = "spGetALMHoldingsByPortNew";
        ////////////////////////////////public string GetEODHoldingDetailsByPortQuery = "call almitasc_ACTradingBBGData.spGetEODAlmHoldingsByPort";
        public string GetFundMVsQuery = "call almitasc_ACTradingBBGData.spGetFundMarketValues";
        public string GetFundHolderSummaryQuery = "select * from almitasc_ACTradingBBGData.FundHolderSummary";
        public string GetInitializeDataBatchQuery = "call almitasc_ACTradingBBGData.spInitDailyBatch";
        public string GetFXRatesQuery = "select * from almitasc_ACTradingBBGData.FXReturn";
        public string GetFXRatesPDQuery = "select * from almitasc_ACTradingBBGData.FXRatePD";
        public string GetFundDividendsQuery = "call almitasc_ACTradingBBGData.spGetFundDividends";
        public string GetFundRegressionETFReturnsQuery = "call almitasc_ACTradingBBGData.spGetFundETFReturns";
        public string GetFundRedemptionQuery = "call almitasc_ACTradingBBGData.spGetFundRedemptionDetailsNew";
        public string GetFundAlphaModelParamsQuery = "call almitasc_ACTradingBBGData.spGetAlphaModelParameters";
        public string GetFundAlphaModelScoresQuery = "call almitasc_ACTradingBBGData.spGetAlphaModelScoresNew";
        public string GetFundDividendScheduleQuery = "call almitasc_ACTradingBBGData.spGetFutureDividends";
        public string GetFundHoldingReturnQuery = "Model.spGetFundHoldingReturns";
        public string GetFundHoldingReturnNewQuery = "Model.spGetFundHoldingReturnsNew";
        public string SaveUserOverridesQuery = "spSaveUserOverrides";
        public string GetUserOverridesQuery = "select * from almitasc_ACTradingBBGData.UserOverrideNew";
        public string GetFundProxyETFReturnsQuery = "call almitasc_ACTradingBBGData.spGetFundProxyETFReturns";
        public string GetFundAltProxyETFReturnsQuery = "call almitasc_ACTradingBBGData.spGetFundAltProxyETFReturns";
        public string GetFundPortProxyETFReturnsQuery = "call almitasc_ACTradingBBGData.spGetFundPortProxyReturns";
        public string GetUpdateFundNavOverridesQuery = "call almitasc_ACTradingBBGData.spUpdateOvrFundNavDates";
        public string GetUserOverrideRptQuery = "select * from almitasc_ACTradingBBGData.UserOverrideRpt Order By SortOrder asc";
        public string GetBuyBackSummaryQuery = "call almitasc_ACTradingBBGData.spGetFundBuybackSummary";
        public string GetUpdateFundPortDatesQuery = "call almitasc_ACTradingBBGData.spPopulateFundPortDates";
        public string GetBloombergOverridesQuery = "select * from almitasc_ACTradingData.globalcef_fundhistorydatacorrection";
        public string GetNumisdataOverridesQuery = "select * from almitasc_ACTradingData.globalcef_fundhistorydatacorrection4uknav";
        public string GetPriceOverridesQuery = "select * from almitasc_ACTradingBBGData.PriceOverride";
        public string GetCEFAReportQuery = "select * from almitasc_ACTradingBBGData.globalresearch_cefatable4cef";
        public string GetBrokerSecurityMappingQuery = "select * from almitasc_ACTradingPM.portfoliomanager_securitymaster order by date_added desc";
        private string GetCANTenderHistoryQuery = "SELECT * FROM almitasc_ACTradingBBGData.CANTenderHistory";
        private string GetUSATenderHistoryQuery = "SELECT * FROM almitasc_ACTradingBBGData.USATenderHistory";
        private string GetALMTenderOfferHistoryQuery = "SELECT * FROM almitasc_ACTradingBBGData.ALMTenderOfferHist";
    }
}