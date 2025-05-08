using aACTrader.DAO.Interface;
using aCommons;
using aCommons.Cef;
using aCommons.Utils;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace aACTrader.DAO.Repository
{
    public class FundSupplementalDataDao : IFundSupplementalDataDao
    {
        private readonly ILogger<FundSupplementalDataDao> _logger;

        private static readonly DateTime TodaysDate = DateTime.Now.Date;
        private const string DATEFORMAT = "yyyy-MM-dd";
        private const string DELIMITER = ",";

        public FundSupplementalDataDao(ILogger<FundSupplementalDataDao> logger)
        {
            _logger = logger;
            _logger.LogInformation("Initializing FundSupplementalDataDao...");
        }

        /// <summary>
        /// Gets Fund Buyback History (# of buybacks made by the fund)
        /// DataSource: Bloomberg (updated by Frank on a periodic basis)
        /// </summary>
        /// <returns></returns>
        public IList<FundBuyback> GetFundBuybackCounts()
        {
            IList<FundBuyback> list = new List<FundBuyback>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetFundBuybackCountsQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundBuyback data = new FundBuyback
                                {
                                    Ticker = reader["Ticker"] as string,
                                    Count = (reader.IsDBNull(reader.GetOrdinal("Count"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("Count"))
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

        /// <summary>
        /// Gets Fund Buyback Details
        /// </summary>
        /// <param name="ticker"></param>
        /// <returns></returns>
        public IList<FundBuyback> GetFundBuybackDetails(string ticker)
        {
            IList<FundBuyback> list = new List<FundBuyback>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetFundBuybackDetailsQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Ticker", ticker);
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundBuyback data = new FundBuyback
                                {
                                    Ticker = reader["Ticker"] as string,
                                    FIGI = reader["FIGI"] as string,
                                    BuybackAnnouncementDate = reader.IsDBNull(reader.GetOrdinal("BuybackAnnouncementDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("BuybackAnnouncementDate")),
                                    BuybackEffectiveDate = reader.IsDBNull(reader.GetOrdinal("BuybackEffectiveDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("BuybackEffectiveDate")),
                                    BuybackAmount = (reader.IsDBNull(reader.GetOrdinal("BuybackAmt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BuybackAmt")),
                                    BuybackCurrency = reader["BuybackCurrency"] as string,
                                    BuybackType = reader["BuybackType"] as string,
                                    BuybackShares = (reader.IsDBNull(reader.GetOrdinal("BuybackShares"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BuybackShares")),
                                    BuybackPct = (reader.IsDBNull(reader.GetOrdinal("BuybackPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BuybackPct")),
                                    BuybackPrice = (reader.IsDBNull(reader.GetOrdinal("BuybackPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BuybackPrice")),
                                    EffectiveDate = reader.IsDBNull(reader.GetOrdinal("EffectiveDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate")),
                                    ClosingPrice = (reader.IsDBNull(reader.GetOrdinal("ClosePrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ClosePrice")),
                                    VWAPPrice = (reader.IsDBNull(reader.GetOrdinal("VWAPPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("VWAPPrice")),
                                    Nav = (reader.IsDBNull(reader.GetOrdinal("Nav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav")),
                                    NavInterpolated = (reader.IsDBNull(reader.GetOrdinal("NavInterpolated"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NavInterpolated")),
                                    PD = (reader.IsDBNull(reader.GetOrdinal("PD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PD")),
                                    PDInterpolated = (reader.IsDBNull(reader.GetOrdinal("PDInterpolated"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PDInterpolated")),
                                    Volume = (reader.IsDBNull(reader.GetOrdinal("Volume"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Volume")),
                                    CEFASharesOutstanding = (reader.IsDBNull(reader.GetOrdinal("CEFASharesOutstanding"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CEFASharesOutstanding")),
                                    NumisSharesOutstanding = (reader.IsDBNull(reader.GetOrdinal("NumisSharesOutstanding"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NumisSharesOutstanding")),
                                    PDSharesOutstanding = (reader.IsDBNull(reader.GetOrdinal("PHSharesOutstanding"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PHSharesOutstanding")),
                                    FundCurrency = reader["FundCurrency"] as string,
                                    NumisNavCurrency = reader["NumisNavCurrency"] as string,
                                    PHNavCurrency = reader["PHCurrency"] as string,
                                    BBGNavCurrency = reader["BBGCurrency"] as string,
                                    NumisNavFrequency = reader["NumisNavFreq"] as string,
                                    NumisPubNavDate = (reader.IsDBNull(reader.GetOrdinal("NumisNavDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("NumisNavDate")),
                                    BBGPubNavDate = (reader.IsDBNull(reader.GetOrdinal("BBGNavDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("BBGNavDate")),
                                    NumisPubNav = (reader.IsDBNull(reader.GetOrdinal("NumisPubNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NumisPubNav")),
                                    PHCumFairNav = (reader.IsDBNull(reader.GetOrdinal("PHCumFairNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PHCumFairNav")),
                                    PHCumParNav = (reader.IsDBNull(reader.GetOrdinal("PHCumParNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PHCumParNav")),
                                    BBGNav = (reader.IsDBNull(reader.GetOrdinal("BBGPubNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BBGPubNav")),
                                    NumisEstNav = (reader.IsDBNull(reader.GetOrdinal("NumisEstNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NumisEstNav")),
                                    MSCumFairNav = (reader.IsDBNull(reader.GetOrdinal("MSCumFairNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MSCumFairNav")),
                                    MSExFairNav = (reader.IsDBNull(reader.GetOrdinal("MSExFairNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MSExFairNav")),
                                    MSExParNav = (reader.IsDBNull(reader.GetOrdinal("MSExParNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MSExParNav")),
                                    NumisPD = (reader.IsDBNull(reader.GetOrdinal("NumisPD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NumisPD")),
                                    BBGPD = (reader.IsDBNull(reader.GetOrdinal("BBGPD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BBGPD")),
                                    PHPD = (reader.IsDBNull(reader.GetOrdinal("PHPD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PHPD")),
                                    MSPD = (reader.IsDBNull(reader.GetOrdinal("MSPD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MSPD")),
                                };
                                data.EffectiveDateAsString = DateUtils.ConvertDate(data.EffectiveDate, "yyyy-MM-dd");
                                data.BuybackAnnouncementDateAsString = DateUtils.ConvertDate(data.BuybackAnnouncementDate, "yyyy-MM-dd");
                                data.BuybackEffectiveDateAsString = DateUtils.ConvertDate(data.BuybackEffectiveDate, "yyyy-MM-dd");
                                data.NumisPubNavDateAsString = DateUtils.ConvertDate(data.NumisPubNavDate, "yyyy-MM-dd");
                                data.BBGPubNavDateAsString = DateUtils.ConvertDate(data.BBGPubNavDate, "yyyy-MM-dd");
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

        /// <summary>
        /// Gets Fund Buyback Details
        /// </summary>
        /// <param name="ticker"></param>
        /// <returns></returns>
        public IList<FundBuyback> GetFundBuybackHistory(string ticker)
        {
            IList<FundBuyback> list = new List<FundBuyback>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    string sql = GetFundBuybackHistoryQuery + " where Ticker = '" + ticker + "' Order By 2 desc";

                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundBuyback data = new FundBuyback
                                {
                                    Ticker = reader["Ticker"] as string,
                                    BuybackAnnouncementDate = reader.IsDBNull(reader.GetOrdinal("announcementdate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("announcementdate")),
                                    BuybackEffectiveDate = reader.IsDBNull(reader.GetOrdinal("effectivedate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("effectivedate")),
                                    BuybackExpirationDate = reader.IsDBNull(reader.GetOrdinal("expirationdate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("expirationdate")),
                                    BuybackAmount = (reader.IsDBNull(reader.GetOrdinal("amount"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("amount")),
                                    BuybackCurrency = reader["currency"] as string,
                                    BuybackType = reader["buybacktype"] as string,
                                    BuybackShares = (reader.IsDBNull(reader.GetOrdinal("buybackshares"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("buybackshares")),
                                    BuybackPct = (reader.IsDBNull(reader.GetOrdinal("buybackpercent"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("buybackpercent")),
                                    BuybackPrice = (reader.IsDBNull(reader.GetOrdinal("buybackprice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("buybackprice")),
                                    BuybackShOut = (reader.IsDBNull(reader.GetOrdinal("shareout"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("shareout")),
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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ticker"></param>
        /// <param name="source"></param>
        /// <param name="startDate"></param>
        /// <param name="endDate"></param>
        /// <returns></returns>
        public IList<FundDiscountHist> GetFundDiscountHistory(string ticker, string source, DateTime startDate, DateTime endDate)
        {
            IList<FundDiscountHist> list = new List<FundDiscountHist>();

            try
            {
                DateTime currentTime = DateTime.Now;
                string startDateAsString = DateUtils.ConvertDate(startDate, "yyyy-MM-dd");
                string endDateAsString = DateUtils.ConvertDate(endDate, "yyyy-MM-dd");

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetFundDiscountHistoryQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Ticker", ticker);
                        command.Parameters.AddWithValue("p_Source", source);
                        command.Parameters.AddWithValue("p_StartDate", startDate);
                        command.Parameters.AddWithValue("p_EndDate", endDate);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundDiscountHist data = new FundDiscountHist
                                {
                                    EffectiveDate = (reader.IsDBNull(reader.GetOrdinal("EffectiveDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate")),
                                    PublishedNavDate = (reader.IsDBNull(reader.GetOrdinal("NavDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("NavDate")),
                                    PublishedNav = (reader.IsDBNull(reader.GetOrdinal("NavPublished"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NavPublished")),
                                    PublishedDiscount = (reader.IsDBNull(reader.GetOrdinal("PublishedDiscount"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PublishedDiscount"))
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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ticker"></param>
        /// <param name="startDate"></param>
        /// <param name="endDate"></param>
        /// <returns></returns>
        public IList<FundDividend> GetFundDividends(string ticker, DateTime startDate, DateTime endDate)
        {
            IList<FundDividend> list = new List<FundDividend>();

            try
            {
                DateTime currentTime = DateTime.Now;
                string startDateAsString = DateUtils.ConvertDate(startDate, "yyyy-MM-dd");
                string endDateAsString = DateUtils.ConvertDate(endDate, "yyyy-MM-dd");

                string sql = GetFundDividendsQuery + " and s.ticker = '" + ticker + "'";
                if (!string.IsNullOrEmpty(startDateAsString))
                    sql += " and d.ex_date >= '" + startDateAsString + "'";
                if (!string.IsNullOrEmpty(endDateAsString))
                    sql += " and d.ex_date <= '" + endDateAsString + "'";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundDividend data = new FundDividend
                                {
                                    Ticker = reader["Ticker"] as string,
                                    Figi = reader["Figi"] as string,
                                    DvdType = reader["DvdType"] as string,
                                    DvdFrequency = reader["DvdFrequency"] as string,
                                    DecDate = reader.IsDBNull(reader.GetOrdinal("DecDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("DecDate")),
                                    ExDate = reader.IsDBNull(reader.GetOrdinal("ExDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("ExDate")),
                                    RecordDate = (reader.IsDBNull(reader.GetOrdinal("RecordDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("RecordDate")),
                                    PayDate = (reader.IsDBNull(reader.GetOrdinal("PayDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("PayDate")),
                                    DvdAmount = (reader.IsDBNull(reader.GetOrdinal("DvdAmount"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DvdAmount"))
                                };
                                data.DecDateAsString = DateUtils.ConvertDate(data.DecDate, "yyyy-MM-dd");
                                data.ExDateAsString = DateUtils.ConvertDate(data.ExDate, "yyyy-MM-dd");
                                data.RecordDateAsString = DateUtils.ConvertDate(data.RecordDate, "yyyy-MM-dd");
                                data.PayDateAsString = DateUtils.ConvertDate(data.PayDate, "yyyy-MM-dd");
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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ticker"></param>
        /// <param name="startDate"></param>
        /// <param name="endDate"></param>
        /// <returns></returns>
        public IList<FundDividend> GetFundDividendsFinal(string ticker, DateTime startDate, DateTime endDate)
        {
            IList<FundDividend> list = new List<FundDividend>();

            try
            {
                DateTime currentTime = DateTime.Now;
                string startDateAsString = DateUtils.ConvertDate(startDate, "yyyy-MM-dd");
                string endDateAsString = DateUtils.ConvertDate(endDate, "yyyy-MM-dd");

                string sql = GetFundDividendsFinalQuery + " and s.ticker = '" + ticker + "'";
                if (!string.IsNullOrEmpty(startDateAsString))
                    sql += " and d.ex_date >= '" + startDateAsString + "'";
                if (!string.IsNullOrEmpty(endDateAsString))
                    sql += " and d.ex_date <= '" + endDateAsString + "'";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundDividend data = new FundDividend
                                {
                                    Ticker = reader["Ticker"] as string,
                                    Figi = reader["Figi"] as string,
                                    DvdType = reader["DvdType"] as string,
                                    DvdFrequency = reader["DvdFrequency"] as string,
                                    DecDate = reader.IsDBNull(reader.GetOrdinal("DecDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("DecDate")),
                                    ExDate = reader.IsDBNull(reader.GetOrdinal("ExDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("ExDate")),
                                    RecordDate = (reader.IsDBNull(reader.GetOrdinal("RecordDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("RecordDate")),
                                    PayDate = (reader.IsDBNull(reader.GetOrdinal("PayDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("PayDate")),
                                    DvdAmount = (reader.IsDBNull(reader.GetOrdinal("DvdAmount"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DvdAmount")),
                                    DvdAmountBBG = (reader.IsDBNull(reader.GetOrdinal("DvdAmountBBG"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DvdAmountBBG"))
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

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IDictionary<string, FundLeverage> GetFundLeverageRatios()
        {
            IDictionary<string, FundLeverage> dict = new Dictionary<string, FundLeverage>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetFundLeverageRatiosQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundLeverage data = new FundLeverage
                                {
                                    Ticker = reader["Ticker"] as string,
                                    ReportedLeverage = (reader.IsDBNull(reader.GetOrdinal("ReportedLeverage"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ReportedLeverage")),
                                    ImpliedDebt = (reader.IsDBNull(reader.GetOrdinal("ImpliedDebt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ImpliedDebt")),
                                    AssetsReportedDate = reader.IsDBNull(reader.GetOrdinal("AssetsReportedDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AssetsReportedDate")),
                                    ImpliedLeverage = (reader.IsDBNull(reader.GetOrdinal("ImpliedLeverage"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ImpliedLeverage")),
                                    UpdatedLeverage = (reader.IsDBNull(reader.GetOrdinal("UpdatedLeverage"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UpdatedLeverage")),
                                    AdjustedLeverage = (reader.IsDBNull(reader.GetOrdinal("AdjustedLeverage"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AdjustedLeverage")),
                                    AdjustedDebt = (reader.IsDBNull(reader.GetOrdinal("AdjustedDebt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AdjustedDebt")),
                                    ImpliedLeverageLast5Days = (reader.IsDBNull(reader.GetOrdinal("ImpliedLeverageLast5Days"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ImpliedLeverageLast5Days")),
                                    UpdatedLeverageLast5Days = (reader.IsDBNull(reader.GetOrdinal("UpdatedLeverageLast5Days"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UpdatedLeverageLast5Days")),
                                    AdjustedDebtLast5Days = (reader.IsDBNull(reader.GetOrdinal("AdjustedDebtLast5Days"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AdjustedDebtLast5Days")),
                                    AdjustedLeverageLast5Days = (reader.IsDBNull(reader.GetOrdinal("AdjustedLeverageLast5Days"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AdjustedLeverageLast5Days"))
                                };
                                if (!dict.ContainsKey(data.Ticker))
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

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IDictionary<string, FundPortDate> GetFundPortDates()
        {
            IDictionary<string, FundPortDate> dict = new Dictionary<string, FundPortDate>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetFundPortDatesQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundPortDate data = new FundPortDate
                                {
                                    Ticker = reader["Ticker"] as string,
                                    ALMPortfolioDate = reader.IsDBNull(reader.GetOrdinal("ALMPortDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("ALMPortDate")),
                                    UserPortfolioDate = reader.IsDBNull(reader.GetOrdinal("UserPortDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("UserPortDate")),
                                    BBGPortfolioDate = reader.IsDBNull(reader.GetOrdinal("BBGPortDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("BBGPortDate")),
                                    ALMPortSource = reader["PortSource"] as string,
                                    DurSrc = reader["DurSrc"] as string,
                                    Dur = reader.IsDBNull(reader.GetOrdinal("Dur")) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Dur")),
                                };
                                if (!string.IsNullOrEmpty(data.ALMPortSource))
                                    data.HasPortHoldings = "Y";
                                else
                                    data.HasPortHoldings = "N";
                                if (!dict.TryGetValue(data.Ticker, out FundPortDate fundPortDateLookup))
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

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IDictionary<string, FundRedemptionTrigger> GetFundRedemptionTriggers()
        {
            IDictionary<string, FundRedemptionTrigger> dict = new Dictionary<string, FundRedemptionTrigger>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetFundRedemptionTriggersQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundRedemptionTrigger data = new FundRedemptionTrigger
                                {
                                    Ticker = reader["Ticker"] as string,
                                    Benchmark = reader["Benchmark"] as string,
                                    TriggerType = reader["TriggerType"] as string,
                                    CalcType = reader["CalcType"] as string,
                                    Source = reader["Source"] as string,
                                    TriggerThreshold = (reader.IsDBNull(reader.GetOrdinal("TriggerThreshold"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TriggerThreshold")),
                                    DataStartDate = reader.IsDBNull(reader.GetOrdinal("DataStartDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("DataStartDate")),
                                    DataEndDate = reader.IsDBNull(reader.GetOrdinal("DataEndDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("DataEndDate")),
                                    FundDiscount = (reader.IsDBNull(reader.GetOrdinal("FundDiscount"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FundDiscount")),
                                    FundNavRtn = (reader.IsDBNull(reader.GetOrdinal("FundNavRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FundNavRtn")),
                                    BenchmarkRtn = (reader.IsDBNull(reader.GetOrdinal("BenchmarkRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BenchmarkRtn")),
                                };
                                data.DataStartDateAsString = DateUtils.ConvertDate(data.DataStartDate, "yyyy-MM-dd");
                                data.DataEndDateAsString = DateUtils.ConvertDate(data.DataEndDate, "yyyy-MM-dd");
                                data.Id = data.Ticker + "|" + data.TriggerType;
                                if (!dict.TryGetValue(data.Id, out FundRedemptionTrigger fundRedemptionTriggerLookup))
                                    dict.Add(data.Id, data);
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

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IDictionary<string, FundRightsOffer> GetFundRightsOffers()
        {
            IDictionary<string, FundRightsOffer> dict = new Dictionary<string, FundRightsOffer>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetFundRightOffersQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundRightsOffer data = new FundRightsOffer
                                {
                                    Ticker = reader["Ticker"] as string,
                                    SubscriptionStartDate = reader.IsDBNull(reader.GetOrdinal("SubscriptionStartDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("SubscriptionStartDate")),
                                    SubscriptionEndDate = reader.IsDBNull(reader.GetOrdinal("SubscriptionEndDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("SubscriptionEndDate")),
                                    SubscriptionRatio = (reader.IsDBNull(reader.GetOrdinal("SubscriptionRatio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SubscriptionRatio")),
                                    OverSubscriptionRatio = (reader.IsDBNull(reader.GetOrdinal("OversubscriptionRatio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OversubscriptionRatio")),
                                    SubscriptionPriceDiscount = (reader.IsDBNull(reader.GetOrdinal("SubscriptionPriceDiscount"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SubscriptionPriceDiscount")),
                                    SubscriptionPriceDiscountField = reader["SubscriptionPriceDiscountField"] as string,
                                    SharesOutstanding = (reader.IsDBNull(reader.GetOrdinal("SharesOutstanding"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SharesOutstanding")),
                                    DisplayPostRightsOfferDiscount = reader["ShowSubscriptionDiscount"] as string,
                                    SubscriptionPrice = (reader.IsDBNull(reader.GetOrdinal("SubscriptionPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SubscriptionPrice"))
                                };

                                if (string.IsNullOrEmpty(data.DisplayPostRightsOfferDiscount))
                                    data.DisplayPostRightsOfferDiscount = "N";
                                data.SubscriptionStartDateAsString = DateUtils.ConvertDate(data.SubscriptionStartDate, "yyyy-MM-dd");
                                data.SubscriptionEndDateAsString = DateUtils.ConvertDate(data.SubscriptionEndDate, "yyyy-MM-dd");
                                if (!dict.TryGetValue(data.Ticker, out FundRightsOffer fundRightsOfferTemp))
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

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IDictionary<string, FundTenderOffer> GetFundTenderOffers()
        {
            IDictionary<string, FundTenderOffer> dict = new Dictionary<string, FundTenderOffer>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetFundTenderOffersQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundTenderOffer data = new FundTenderOffer
                                {
                                    Ticker = reader["Ticker"] as string,
                                    TenderStartDate = reader.IsDBNull(reader.GetOrdinal("TenderStartDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("TenderStartDate")),
                                    TenderEndDate = reader.IsDBNull(reader.GetOrdinal("TenderEndDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("TenderEndDate")),
                                    SharesTendered = (reader.IsDBNull(reader.GetOrdinal("SharesTendered"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SharesTendered")),
                                    DiscountPostTender = (reader.IsDBNull(reader.GetOrdinal("DiscountPostTender"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DiscountPostTender")),
                                    TenderDiscount = (reader.IsDBNull(reader.GetOrdinal("TenderPriceDiscount"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TenderPriceDiscount")),
                                    TenderDiscountField = reader["TenderPriceDiscountField"] as string,
                                    InstitutionalHoldings = (reader.IsDBNull(reader.GetOrdinal("InstitutionalHoldings"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("InstitutionalHoldings")),
                                    RetailHoldings = (reader.IsDBNull(reader.GetOrdinal("RetailHoldings"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RetailHoldings")),
                                    InstitutionalHoldingsTendered = (reader.IsDBNull(reader.GetOrdinal("InstitutionalHoldingsTendered"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("InstitutionalHoldingsTendered")),
                                    RetailHoldingsTendered = (reader.IsDBNull(reader.GetOrdinal("RetailHoldingsTendered"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RetailHoldingsTendered")),
                                    ExpenseRatio = (reader.IsDBNull(reader.GetOrdinal("ExpenseRatio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExpenseRatio"))
                                };

                                data.TenderStartDateAsString = DateUtils.ConvertDate(data.TenderStartDate, "yyyy-MM-dd");
                                data.TenderEndDateAsString = DateUtils.ConvertDate(data.TenderEndDate, "yyyy-MM-dd");
                                if (!dict.TryGetValue(data.Ticker, out FundTenderOffer fundTenderOfferTemp))
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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="list"></param>
        public void SaveFundRedemptionTriggerDetails(IList<FundRedemptionTriggerDetail> list)
        {
            try
            {
                StringBuilder sCommand = new StringBuilder("insert into almitasc_ACTradingBBGData.FundRedemptionTriggerDetail " +
                    "(Ticker, TriggerType, ValueType, SecurityType, Price, FundNav, FundNavDate, FundDiscount, EffectiveDate, " +
                    "DvdExDate, DvdPayDate, DvdAmount, DvdFrequency, DvdType, DailyRtn) values ");

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlTransaction trans = connection.BeginTransaction())
                    {
                        _logger.LogInformation("Deleting data from almitasc_ACTradingBBGData.FundRedemptionTriggerDetail");
                        string sqlDelete = "delete from almitasc_ACTradingBBGData.FundRedemptionTriggerDetail";
                        using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                        {
                            command.ExecuteNonQuery();
                        }

                        List<string> Rows = new List<string>();
                        StringBuilder sb = new StringBuilder();
                        foreach (FundRedemptionTriggerDetail data in list)
                        {
                            //Ticker
                            if (!string.IsNullOrEmpty(data.Ticker))
                                sb.Append(string.Concat("'", data.Ticker, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //TriggerType
                            if (!string.IsNullOrEmpty(data.TriggerType))
                                sb.Append(string.Concat("'", data.TriggerType, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //ValueType
                            if (!string.IsNullOrEmpty(data.ValueType))
                                sb.Append(string.Concat("'", data.ValueType, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //SecurityType
                            if (!string.IsNullOrEmpty(data.SecurityType))
                                sb.Append(string.Concat("'", data.SecurityType, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //Price
                            if (data.Price.HasValue)
                                sb.Append(data.Price).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //FundNav
                            if (data.FundNav.HasValue)
                                sb.Append(data.FundNav).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //FundNavDate
                            if (data.FundNavDate.HasValue)
                                sb.Append(string.Concat("'", DateUtils.ConvertDate(data.FundNavDate, "yyyy-MM-dd"), "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //FundDiscount
                            if (data.FundDiscount.HasValue)
                                sb.Append(data.FundDiscount).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //EffectiveDate
                            if (data.EffectiveDate.HasValue)
                                sb.Append(string.Concat("'", DateUtils.ConvertDate(data.EffectiveDate, "yyyy-MM-dd"), "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //DvdExDate
                            if (data.DvdExDate.HasValue)
                                sb.Append(string.Concat("'", DateUtils.ConvertDate(data.DvdExDate, "yyyy-MM-dd"), "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //DvdPayDate
                            if (data.DvdPayDate.HasValue)
                                sb.Append(string.Concat("'", DateUtils.ConvertDate(data.DvdPayDate, "yyyy-MM-dd"), "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //DvdAmount
                            if (data.DvdAmount.HasValue)
                                sb.Append(data.DvdAmount).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //DvdFrequency
                            if (!string.IsNullOrEmpty(data.DvdFrequency))
                                sb.Append(string.Concat("'", data.DvdFrequency, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //DvdType
                            if (!string.IsNullOrEmpty(data.DvdType))
                                sb.Append(string.Concat("'", data.DvdType, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //DailyRtn
                            if (data.DailyRtn.HasValue)
                                sb.Append(data.DailyRtn);
                            else
                                sb.Append("null");

                            string row = sb.ToString();
                            Rows.Add(string.Concat("(", row, ")"));
                            sb.Clear();
                        }

                        sCommand.Append(string.Join(",", Rows));
                        sCommand.Append(";");
                        using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), connection))
                        {
                            myCmd.CommandType = CommandType.Text;
                            myCmd.ExecuteNonQuery();
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving Fund Redemption Details");
                throw;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="list"></param>
        public void SaveFundRightsOffers(IList<FundRightsOffer> list)
        {
            string sql = "insert into almitasc_ACTradingBBGData.StgRightsOfferingMst"
                + " (Ticker, SubscriptionStartDate, SubscriptionEndDate, SubscriptionRatio,"
                + " OverSubscriptionRatio, SubscriptionPriceDiscount, SubscriptionPriceDiscountField,"
                + " SharesOutstanding, ShowSubscriptionDiscount, SubscriptionPrice)"
                + " values (@Ticker, @SubscriptionStartDate, @SubscriptionEndDate, @SubscriptionRatio,"
                + " @OverSubscriptionRatio, @SubscriptionPriceDiscount, @SubscriptionPriceDiscountField,"
                + " @SharesOutstanding, @ShowSubscriptionDiscount, @SubscriptionPrice)";

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    if (list != null && list.Count > 0)
                    {
                        using (MySqlTransaction trans = connection.BeginTransaction())
                        {
                            _logger.LogInformation("Deleting data from almitasc_ACTradingBBGData.StgRightsOfferingMst");
                            string sqlDelete = "delete from almitasc_ACTradingBBGData.StgRightsOfferingMst";
                            using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                            {
                                command.ExecuteNonQuery();
                            }

                            _logger.LogInformation("Saving data to almitasc_ACTradingBBGData.StgRightsOfferingMst");
                            using (MySqlCommand command = new MySqlCommand(sql, connection))
                            {
                                command.Parameters.Add(new MySqlParameter("@Ticker", MySqlDbType.VarChar));
                                command.Parameters.Add(new MySqlParameter("@SubscriptionStartDate", MySqlDbType.Date));
                                command.Parameters.Add(new MySqlParameter("@SubscriptionEndDate", MySqlDbType.Date));
                                command.Parameters.Add(new MySqlParameter("@SubscriptionRatio", MySqlDbType.Decimal));
                                command.Parameters.Add(new MySqlParameter("@OverSubscriptionRatio", MySqlDbType.Decimal));
                                command.Parameters.Add(new MySqlParameter("@SubscriptionPriceDiscount", MySqlDbType.Decimal));
                                command.Parameters.Add(new MySqlParameter("@SubscriptionPriceDiscountField", MySqlDbType.VarChar));
                                command.Parameters.Add(new MySqlParameter("@SharesOutstanding", MySqlDbType.Decimal));
                                command.Parameters.Add(new MySqlParameter("@ShowSubscriptionDiscount", MySqlDbType.VarChar));
                                command.Parameters.Add(new MySqlParameter("@SubscriptionPrice", MySqlDbType.Decimal));

                                foreach (FundRightsOffer data in list)
                                {
                                    data.SubscriptionStartDate = DateUtils.ConvertToDate(data.SubscriptionStartDateAsString, "yyyy-MM-dd");
                                    data.SubscriptionEndDate = DateUtils.ConvertToDate(data.SubscriptionEndDateAsString, "yyyy-MM-dd");

                                    command.Parameters[0].Value = data.Ticker;
                                    command.Parameters[1].Value = data.SubscriptionStartDate;
                                    command.Parameters[2].Value = data.SubscriptionEndDate;
                                    command.Parameters[3].Value = data.SubscriptionRatio;
                                    command.Parameters[4].Value = data.OverSubscriptionRatio;
                                    command.Parameters[5].Value = data.SubscriptionPriceDiscount;
                                    command.Parameters[6].Value = data.SubscriptionPriceDiscountField;
                                    command.Parameters[7].Value = data.SharesOutstanding;
                                    command.Parameters[8].Value = data.DisplayPostRightsOfferDiscount;
                                    command.Parameters[9].Value = data.SubscriptionPrice;
                                    command.ExecuteNonQuery();
                                }
                            }

                            _logger.LogInformation("Moving data to almitasc_ACTradingBBGData.RightsOfferingMst");
                            sql = "spPopulateFundRightsOffers";
                            using (MySqlCommand command = new MySqlCommand(sql, connection))
                            {
                                command.CommandType = System.Data.CommandType.StoredProcedure;
                                command.ExecuteNonQuery();
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving data", ex);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="list"></param>
        public void SaveFundTenderOffers(IList<FundTenderOffer> list)
        {
            string sql = "insert into almitasc_ACTradingBBGData.StgFundTenderOfferMst"
                + " (Ticker, TenderStartDate, TenderEndDate, SharesTendered, DiscountPostTender,"
                + " TenderPriceDiscount, TenderPriceDiscountField, InstitutionalHoldings, RetailHoldings,"
                + " InstitutionalHoldingsTendered, RetailHoldingsTendered, ExpenseRatio)"
                + " values (@Ticker, @TenderStartDate, @TenderEndDate, @SharesTendered, @DiscountPostTender,"
                + " @TenderPriceDiscount, @TenderPriceDiscountField, @InstitutionalHoldings, @RetailHoldings,"
                + " @InstitutionalHoldingsTendered, @RetailHoldingsTendered, @ExpenseRatio)";

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    if (list != null && list.Count > 0)
                    {
                        using (MySqlTransaction trans = connection.BeginTransaction())
                        {
                            _logger.LogInformation("Deleting data from almitasc_ACTradingBBGData.StgFundTenderOfferMst");
                            string sqlDelete = "delete from almitasc_ACTradingBBGData.StgFundTenderOfferMst";
                            using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                            {
                                command.ExecuteNonQuery();
                            }

                            _logger.LogInformation("Saving data to almitasc_ACTradingBBGData.StgFundTenderOfferMst");
                            using (MySqlCommand command = new MySqlCommand(sql, connection))
                            {
                                command.Parameters.Add(new MySqlParameter("@Ticker", MySqlDbType.VarChar));
                                command.Parameters.Add(new MySqlParameter("@TenderStartDate", MySqlDbType.Date));
                                command.Parameters.Add(new MySqlParameter("@TenderEndDate", MySqlDbType.Date));
                                command.Parameters.Add(new MySqlParameter("@SharesTendered", MySqlDbType.Decimal));
                                command.Parameters.Add(new MySqlParameter("@DiscountPostTender", MySqlDbType.Decimal));
                                command.Parameters.Add(new MySqlParameter("@TenderPriceDiscount", MySqlDbType.Decimal));
                                command.Parameters.Add(new MySqlParameter("@TenderPriceDiscountField", MySqlDbType.VarChar));
                                command.Parameters.Add(new MySqlParameter("@InstitutionalHoldings", MySqlDbType.Decimal));
                                command.Parameters.Add(new MySqlParameter("@RetailHoldings", MySqlDbType.Decimal));
                                command.Parameters.Add(new MySqlParameter("@InstitutionalHoldingsTendered", MySqlDbType.Decimal));
                                command.Parameters.Add(new MySqlParameter("@RetailHoldingsTendered", MySqlDbType.Decimal));
                                command.Parameters.Add(new MySqlParameter("@ExpenseRatio", MySqlDbType.Decimal));

                                foreach (FundTenderOffer data in list)
                                {
                                    data.TenderStartDate = DateUtils.ConvertToDate(data.TenderStartDateAsString, "yyyy-MM-dd");
                                    data.TenderEndDate = DateUtils.ConvertToDate(data.TenderEndDateAsString, "yyyy-MM-dd");

                                    command.Parameters[0].Value = data.Ticker;
                                    command.Parameters[1].Value = data.TenderStartDate;
                                    command.Parameters[2].Value = data.TenderEndDate;
                                    command.Parameters[3].Value = data.SharesTendered;
                                    command.Parameters[4].Value = data.DiscountPostTender;
                                    command.Parameters[5].Value = data.TenderDiscount;
                                    command.Parameters[6].Value = data.TenderDiscountField;
                                    command.Parameters[7].Value = data.InstitutionalHoldings;
                                    command.Parameters[8].Value = data.RetailHoldings;
                                    command.Parameters[9].Value = data.InstitutionalHoldingsTendered;
                                    command.Parameters[10].Value = data.RetailHoldingsTendered;
                                    command.Parameters[11].Value = data.ExpenseRatio;
                                    command.ExecuteNonQuery();
                                }
                            }

                            _logger.LogInformation("Moving data to almitasc_ACTradingBBGData.FundTenderOfferMst");
                            sql = "spPopulateFundTenderOffers";
                            using (MySqlCommand command = new MySqlCommand(sql, connection))
                            {
                                command.CommandType = System.Data.CommandType.StoredProcedure;
                                command.ExecuteNonQuery();
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving data");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ticker"></param>
        /// <returns></returns>
        public IList<FundRedemptionTriggerDetail> GetFundRedemptionDiscountTriggerDetails(string ticker)
        {
            IList<FundRedemptionTriggerDetail> list = new List<FundRedemptionTriggerDetail>();

            try
            {
                string sql = GetFundRedemptionTriggerDetailsQuery
                    + " where TriggerType = 'Discount' and Ticker = '" + ticker + "'" + " order by ValueType desc, EffectiveDate desc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundRedemptionTriggerDetail data = new FundRedemptionTriggerDetail
                                {
                                    ValueType = reader["ValueType"] as string,
                                    FundNav = (reader.IsDBNull(reader.GetOrdinal("FundNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FundNav")),
                                    FundDiscount = (reader.IsDBNull(reader.GetOrdinal("FundDiscount"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FundDiscount")),
                                    FundNavDate = reader.IsDBNull(reader.GetOrdinal("FundNavDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FundNavDate")),
                                    EffectiveDate = (reader.IsDBNull(reader.GetOrdinal("EffectiveDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate")),
                                };

                                data.EffectiveDateAsString = DateUtils.ConvertDate(data.EffectiveDate, "yyyy-MM-dd");
                                data.FundNavDateAsString = DateUtils.ConvertDate(data.FundNavDate, "yyyy-MM-dd");
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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ticker"></param>
        /// <param name="securityType"></param>
        /// <param name="dataType"></param>
        /// <param name="source"></param>
        /// <param name="startDate"></param>
        /// <param name="endDate"></param>
        /// <returns></returns>
        public IList<FundDataHist> GetFundDataHistory(string ticker, string securityType, string dataType, string source, DateTime startDate, DateTime endDate)
        {
            IList<FundDataHist> list = new List<FundDataHist>();

            try
            {
                DateTime currentTime = DateTime.Now;
                string startDateAsString = DateUtils.ConvertDate(startDate, "yyyy-MM-dd");
                string endDateAsString = DateUtils.ConvertDate(endDate, "yyyy-MM-dd");

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetFundDataHistoryQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Ticker", ticker);
                        command.Parameters.AddWithValue("p_SecurityType", securityType);
                        command.Parameters.AddWithValue("p_DataType", dataType);
                        command.Parameters.AddWithValue("p_Source", source);
                        command.Parameters.AddWithValue("p_StartDate", startDate);
                        command.Parameters.AddWithValue("p_EndDate", endDate);

                        if (securityType.Equals("Fund") && dataType.Equals("Nav"))
                        {
                            using (MySqlDataReader reader = command.ExecuteReader())
                            {
                                while (reader.Read())
                                {
                                    FundDataHist data = new FundDataHist
                                    {
                                        EffectiveDate = (reader.IsDBNull(reader.GetOrdinal("EffectiveDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate")),
                                        PublishedNavDate = (reader.IsDBNull(reader.GetOrdinal("NavDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("NavDate")),
                                        PublishedNav = (reader.IsDBNull(reader.GetOrdinal("NavPublished"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NavPublished")),
                                        PublishedDiscount = (reader.IsDBNull(reader.GetOrdinal("PublishedDiscount"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PublishedDiscount")),
                                        DvdExDate = (reader.IsDBNull(reader.GetOrdinal("ExDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("ExDate")),
                                        DvdPayDate = (reader.IsDBNull(reader.GetOrdinal("PayDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("PayDate")),
                                        DvdAmount = (reader.IsDBNull(reader.GetOrdinal("DvdAmount"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DvdAmount")),
                                        DvdFreq = reader["DvdFrequency"] as string,
                                        DvdType = reader["DvdType"] as string,
                                    };
                                    list.Add(data);
                                }
                            }
                        }
                        else if (securityType.Equals("Fund") && dataType.Equals("Price"))
                        {
                            using (MySqlDataReader reader = command.ExecuteReader())
                            {
                                while (reader.Read())
                                {
                                    FundDataHist data = new FundDataHist
                                    {
                                        EffectiveDate = (reader.IsDBNull(reader.GetOrdinal("EffectiveDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate")),
                                        Price = (reader.IsDBNull(reader.GetOrdinal("Price"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price")),
                                        DvdExDate = (reader.IsDBNull(reader.GetOrdinal("ExDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("ExDate")),
                                        DvdPayDate = (reader.IsDBNull(reader.GetOrdinal("PayDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("PayDate")),
                                        DvdAmount = (reader.IsDBNull(reader.GetOrdinal("DvdAmount"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DvdAmount")),
                                        DvdFreq = reader["DvdFrequency"] as string,
                                        DvdType = reader["DvdType"] as string,
                                    };
                                    list.Add(data);
                                }
                            }
                        }
                        else if (securityType.Equals("Benchmark"))
                        {
                            using (MySqlDataReader reader = command.ExecuteReader())
                            {
                                while (reader.Read())
                                {
                                    FundDataHist data = new FundDataHist
                                    {
                                        EffectiveDate = (reader.IsDBNull(reader.GetOrdinal("EffectiveDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate")),
                                        DailyRtn = (reader.IsDBNull(reader.GetOrdinal("DailyRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DailyRtn")),
                                    };
                                    list.Add(data);
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
            return list;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ticker"></param>
        /// <param name="triggerType"></param>
        /// <param name="securityType"></param>
        /// <returns></returns>
        public IList<FundRedemptionTriggerDetail> GetFundRedemptionNavReturnTriggerDetails(string ticker, string triggerType, string securityType)
        {
            IList<FundRedemptionTriggerDetail> list = new List<FundRedemptionTriggerDetail>();

            try
            {
                string sql = GetFundRedemptionTriggerDetailsQuery
                    + " where TriggerType = '" + triggerType + "' and Ticker = '" + ticker + "'" + " order by ValueType desc, EffectiveDate desc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            if (securityType.Equals("Fund"))
                            {
                                while (reader.Read())
                                {
                                    FundRedemptionTriggerDetail data = new FundRedemptionTriggerDetail
                                    {
                                        ValueType = reader["ValueType"] as string,
                                        Price = (reader.IsDBNull(reader.GetOrdinal("Price"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price")),
                                        FundNav = (reader.IsDBNull(reader.GetOrdinal("FundNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FundNav")),
                                        FundDiscount = (reader.IsDBNull(reader.GetOrdinal("FundDiscount"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FundDiscount")),
                                        FundNavDate = reader.IsDBNull(reader.GetOrdinal("FundNavDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FundNavDate")),
                                        EffectiveDate = (reader.IsDBNull(reader.GetOrdinal("EffectiveDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate")),
                                        DvdExDate = (reader.IsDBNull(reader.GetOrdinal("DvdExDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("DvdExDate")),
                                        DvdPayDate = (reader.IsDBNull(reader.GetOrdinal("DvdPayDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("DvdPayDate")),
                                        DvdAmount = (reader.IsDBNull(reader.GetOrdinal("DvdAmount"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DvdAmount")),
                                        DvdFrequency = reader["DvdFrequency"] as string,
                                        DvdType = reader["DvdType"] as string,
                                    };
                                    data.EffectiveDateAsString = DateUtils.ConvertDate(data.EffectiveDate, "yyyy-MM-dd");
                                    data.FundNavDateAsString = DateUtils.ConvertDate(data.FundNavDate, "yyyy-MM-dd");
                                    data.DvdExDateAsString = DateUtils.ConvertDate(data.DvdExDate, "yyyy-MM-dd");
                                    data.DvdPayDateAsString = DateUtils.ConvertDate(data.DvdPayDate, "yyyy-MM-dd");
                                    list.Add(data);
                                }
                            }
                            else if (securityType.Equals("Benchmark"))
                            {
                                while (reader.Read())
                                {
                                    FundRedemptionTriggerDetail data = new FundRedemptionTriggerDetail
                                    {
                                        ValueType = reader["ValueType"] as string,
                                        EffectiveDate = (reader.IsDBNull(reader.GetOrdinal("EffectiveDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate")),
                                        DailyRtn = (reader.IsDBNull(reader.GetOrdinal("DailyRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DailyRtn")),
                                    };
                                    data.EffectiveDateAsString = DateUtils.ConvertDate(data.EffectiveDate, "yyyy-MM-dd");
                                    list.Add(data);
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
            return list;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IDictionary<string, FundNavUpdate> GetFundNavUpdateFrequency()
        {
            IDictionary<string, FundNavUpdate> dict = new Dictionary<string, FundNavUpdate>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                string sql = GetFundNavFrequencyQuery;
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundNavUpdate data = new FundNavUpdate
                                {
                                    Ticker = reader["Ticker"] as string,
                                    NavUpdateFreq = reader["NavUpdateFreq"] as string,
                                    NavUpdateLag = (reader.IsDBNull(reader.GetOrdinal("NavUpdateLag"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("NavUpdateLag")),
                                    NavEstErrorCode = (reader.IsDBNull(reader.GetOrdinal("NavEstErrorCode"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("NavEstErrorCode")),
                                    NavEstErrorAvg = (reader.IsDBNull(reader.GetOrdinal("NavEstErrorAvg"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NavEstErrorAvg")),
                                    NavEstErrorAvgVol = (reader.IsDBNull(reader.GetOrdinal("NavEstErrorAvgVol"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NavEstErrorAvgVol")),
                                };
                                if (!dict.ContainsKey(data.Ticker))
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

        public IList<FundBuybackTO> GetBuybackHistory(string ticker)
        {
            IList<FundBuybackTO> list = new List<FundBuybackTO>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetBuybackDetailsQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Ticker", ticker);
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundBuybackTO data = new FundBuybackTO
                                {
                                    Ticker = reader["Ticker"] as string,
                                    AnnDt = reader.IsDBNull(reader.GetOrdinal("AnnDt")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AnnDt")),
                                    EffDt = reader.IsDBNull(reader.GetOrdinal("EffDt")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffDt")),
                                    ExpDt = reader.IsDBNull(reader.GetOrdinal("ExpDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("ExpDate")),
                                    Amt = (reader.IsDBNull(reader.GetOrdinal("Amt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Amt")),
                                    Curr = reader["Curr"] as string,
                                    BuybackType = reader["BuybackType"] as string,
                                    BuybackShares = (reader.IsDBNull(reader.GetOrdinal("BuybackShares"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BuybackShares")),
                                    BuybackPct = (reader.IsDBNull(reader.GetOrdinal("BuybackPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BuybackPct")),
                                    BuybackPrice = (reader.IsDBNull(reader.GetOrdinal("BuybackPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BuybackPrice")),
                                    ClsPrc = (reader.IsDBNull(reader.GetOrdinal("ClsPrc"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ClsPrc")),
                                    AlmNav = (reader.IsDBNull(reader.GetOrdinal("AlmNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AlmNav")),
                                    AlmNavI = (reader.IsDBNull(reader.GetOrdinal("AlmNavI"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AlmNavI")),
                                    AlmPD = (reader.IsDBNull(reader.GetOrdinal("AlmPD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AlmPD")),
                                    AlmPDI = (reader.IsDBNull(reader.GetOrdinal("AlmPDI"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AlmPDI")),
                                    FundCurr = reader["FundCurr"] as string,
                                    NumisCurr = reader["NumisCurr"] as string,
                                    PHCurr = reader["PHCurr"] as string,
                                    NumisNav = (reader.IsDBNull(reader.GetOrdinal("NumisNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NumisNav")),
                                    PHCumFairNav = (reader.IsDBNull(reader.GetOrdinal("PHCumFairNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PHCumFairNav")),
                                    JPMCumFairNav = (reader.IsDBNull(reader.GetOrdinal("JPMCumFairNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("JPMCumFairNav")),
                                    NumisPD = (reader.IsDBNull(reader.GetOrdinal("NumisPD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NumisPD")),
                                    PHPD = (reader.IsDBNull(reader.GetOrdinal("PHPD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PHPD")),
                                    JPMPD = (reader.IsDBNull(reader.GetOrdinal("JPMPD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("JPMPD")),
                                    CEFASharesOut = (reader.IsDBNull(reader.GetOrdinal("CEFASharesOut"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CEFASharesOut")),
                                    NumisSharesOut = (reader.IsDBNull(reader.GetOrdinal("NumisSharesOut"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NumisSharesOut")),
                                    PHSharesOut = (reader.IsDBNull(reader.GetOrdinal("PHSharesOut"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PHSharesOut")),
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

        private const string GetFundRightOffersQuery = "select * from almitasc_ACTradingBBGData.RightsOfferingMst";
        private const string GetFundTenderOffersQuery = "select * from almitasc_ACTradingBBGData.FundTenderOfferMst where TenderEndDate > current_date()";
        private const string GetFundDividendsQuery = "select s.Ticker, s.Figi, d.frequency as DvdFrequency, d.type as DvdType, d.dec_date as DecDate"
                                + ", d.ex_date as ExDate, d.record_date as RecordDate, d.pay_date as PayDate, d.amount as DvdAmount"
                                + " from almitasc_ACTradingBBGLink.globaltrading_securitymaster s"
                                + " left join almitasc_ACTradingBBGData.globalcef_fundhistorydvd d on(s.figi = d.figi)"
                                + " left join almitasc_ACTrading.zzcodetable_dvd c on(d.type = c.type)"
                                + " where c.cash = 1";

        private const string GetFundDividendsFinalQuery = "select s.Ticker, d.frequency as DvdFrequency, d.type as DvdType, d.dec_date as DecDate"
                                + ", d.ex_date as ExDate,s.figi, d.record_date as RecordDate, d.pay_date as PayDate, d.amount as DvdAmount, d.amountbbg as DvdAmountBBG"
                                + " from almitasc_ACTradingBBGLink.globaltrading_securitymaster s"
                                + " left join almitasc_ACTradingData.globalcef_fundhistorydvd d on(s.figi = d.figi)"
                                + " where 1=1";
        private const string GetFundBuybackCountsQuery = "select b.ticker as Ticker, count(*) as Count from almitasc_ACTradingData.globalcef_fundhistory4buybacks b group by b.Ticker order by 1";
        private const string GetFundBuybackDetailsQuery = "spGetFundBuybackHistory";
        private const string GetBuybackDetailsQuery = "Reporting.spGetBuybackHistory";
        private const string GetFundLeverageRatiosQuery = "call almitasc_ACTradingBBGData.spGetFundLeverageRatios";
        private const string GetFundPortDatesQuery = "call almitasc_ACTradingBBGData.spGetFundPortDates";
        private const string GetFundRedemptionTriggersQuery = "call almitasc_ACTradingBBGData.spGetFundTriggers";
        private const string GetFundRedemptionTriggerDetailsQuery = "select * from almitasc_ACTradingBBGData.FundRedemptionTriggerDetail";
        private const string GetFundDiscountHistoryQuery = "spGetFundDiscountHistory";
        private const string GetFundDataHistoryQuery = "spGetFundDataHistory";
        private const string GetFundNavFrequencyQuery = "select Ticker, NavUpdateFreq, ifnull(NavUpdateLag,0) as NavUpdateLag, NavEstErrorCode, NavEstErrorAvg, NavEstErrorAvgVol from almitasc_ACTradingBBGData.FundSupplementalData";
        private const string GetFundBuybackHistoryQuery = "select * from almitasc_ACTradingData.globalcef_fundhistory4buybacks";
    }
}
