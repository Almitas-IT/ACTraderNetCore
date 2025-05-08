using aACTrader.DAO.Interface;
using aCommons;
using aCommons.MarketMonitor;
using aCommons.Utils;
using aCommons.Web;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;

namespace aACTrader.DAO.Repository
{
    public class StatsDao : IStatsDao
    {
        private readonly ILogger<StatsDao> _logger;

        public StatsDao(ILogger<StatsDao> logger)
        {
            _logger = logger;
            _logger.LogInformation("Initializing StatsDao...");
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="securityType"></param>
        /// <param name="country"></param>
        /// <returns></returns>
        public IList<FundGroupDiscountStats> GetFundDiscountStats(string securityType, string country)
        {
            IList<FundGroupDiscountStats> list = new List<FundGroupDiscountStats>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetFundDiscountStatsQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_SecurityType", securityType);
                        command.Parameters.AddWithValue("p_Country", country);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundGroupDiscountStats data = new FundGroupDiscountStats
                                {
                                    SecurityType = reader["SecurityType"] as string,
                                    Country = reader["Country"] as string,
                                    PaymentRank = reader["PaymentRank"] as string,
                                    GroupName = reader["GroupName"] as string,
                                    DiscountGroupName = reader["DiscountGroupName"] as string,
                                    GroupLevel1 = reader["GroupLevel1"] as string,
                                    GroupLevel2 = reader["GroupLevel2"] as string,
                                    GroupLevel3 = reader["GroupLevel3"] as string,
                                    PeriodType = reader["PeriodName"] as string,

                                    ReportDate = reader.IsDBNull(reader.GetOrdinal("ReportDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("ReportDate")),
                                    PeriodStartDate = reader.IsDBNull(reader.GetOrdinal("PeriodStartDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("PeriodStartDate")),
                                    PeriodEndDate = reader.IsDBNull(reader.GetOrdinal("PeriodEndDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("PeriodEndDate")),

                                    TotalFunds = (reader.IsDBNull(reader.GetOrdinal("TotalFunds"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("TotalFunds")),
                                    NumFunds = (reader.IsDBNull(reader.GetOrdinal("NumFunds"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("NumFunds")),
                                    NumObs = (reader.IsDBNull(reader.GetOrdinal("NumObs"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("NumObs")),
                                    Indent = (reader.IsDBNull(reader.GetOrdinal("Indent"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("Indent")),

                                    Min = (reader.IsDBNull(reader.GetOrdinal("MinPD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MinPD")),
                                    Max = (reader.IsDBNull(reader.GetOrdinal("MaxPD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MaxPD")),
                                    Mean = (reader.IsDBNull(reader.GetOrdinal("MeanPD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MeanPD")),
                                    Median = (reader.IsDBNull(reader.GetOrdinal("MedianPD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MedianPD")),
                                    StdDev = (reader.IsDBNull(reader.GetOrdinal("StdDevPD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("StdDevPD")),
                                    FirstQuartile = (reader.IsDBNull(reader.GetOrdinal("FirstQuartilePD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FirstQuartilePD")),
                                    SecondQuartile = (reader.IsDBNull(reader.GetOrdinal("SecondQuartilePD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SecondQuartilePD")),
                                    ThirdQuartile = (reader.IsDBNull(reader.GetOrdinal("ThirdQuartilePD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ThirdQuartilePD")),
                                    FourthQuartile = (reader.IsDBNull(reader.GetOrdinal("FourthQuartilePD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FourthQuartilePD")),
                                    ZScore = (reader.IsDBNull(reader.GetOrdinal("ZScorePD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ZScorePD")),

                                    MeanDiff = (reader.IsDBNull(reader.GetOrdinal("MeanPDChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MeanPDChng")),
                                    MedianDiff = (reader.IsDBNull(reader.GetOrdinal("MedianPDChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MedianPDChng")),
                                    FirstQuartileDiff = (reader.IsDBNull(reader.GetOrdinal("FirstQuartilePDChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FirstQuartilePDChng")),
                                    SecondQuartileDiff = (reader.IsDBNull(reader.GetOrdinal("SecondQuartilePDChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SecondQuartilePDChng")),
                                    ThirdQuartileDiff = (reader.IsDBNull(reader.GetOrdinal("ThirdQuartilePDChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ThirdQuartilePDChng")),
                                    FourthQuartileDiff = (reader.IsDBNull(reader.GetOrdinal("FourthQuartilePDChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FourthQuartilePDChng")),

                                    MeanRank = (reader.IsDBNull(reader.GetOrdinal("MeanPDRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MeanPDRank")),
                                    MedianRank = (reader.IsDBNull(reader.GetOrdinal("MedianPDRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MedianPDRank")),
                                    FirstQuartileRank = (reader.IsDBNull(reader.GetOrdinal("FirstQuartilePDRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FirstQuartilePDRank")),
                                    SecondQuartileRank = (reader.IsDBNull(reader.GetOrdinal("SecondQuartilePDRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SecondQuartilePDRank")),
                                    ThirdQuartileRank = (reader.IsDBNull(reader.GetOrdinal("ThirdQuartilePDRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ThirdQuartilePDRank")),
                                    FourthQuartileRank = (reader.IsDBNull(reader.GetOrdinal("FourthQuartilePDRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FourthQuartilePDRank"))
                                };

                                data.ReportDateAsString = DateUtils.ConvertDate(data.ReportDate, "yyyy-MM-dd");
                                data.PeriodStartDateAsString = DateUtils.ConvertDate(data.PeriodStartDate, "yyyy-MM-dd");
                                data.PeriodEndDateAsString = DateUtils.ConvertDate(data.PeriodEndDate, "yyyy-MM-dd");

                                string id = string.Empty;

                                if (data.SecurityType.Equals("Closed End Fund", StringComparison.CurrentCultureIgnoreCase))
                                    id = "CEF";

                                if (data.Country.Equals("United States", StringComparison.CurrentCultureIgnoreCase))
                                    id += "|" + "US";

                                id += "|" + data.DiscountGroupName + "|" + data.PeriodType;
                                data.Id = id;

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
        /// <param name="securityType"></param>
        /// <param name="country"></param>
        /// <returns></returns>
        public FundGroupDiscountStats GetFundDiscountStats(string securityType, string country, string fundGroup, DateTime asofDate)
        {
            FundGroupDiscountStats data = null;

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetFundDiscountStatsByDateQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_SecurityType", securityType);
                        command.Parameters.AddWithValue("p_Country", country);
                        command.Parameters.AddWithValue("p_FundGroup", fundGroup);
                        command.Parameters.AddWithValue("p_AsOfDate", asofDate);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                data = new FundGroupDiscountStats
                                {
                                    Mean = (reader.IsDBNull(reader.GetOrdinal("MeanPD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MeanPD")),
                                    Median = (reader.IsDBNull(reader.GetOrdinal("MedianPD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MedianPD")),
                                };
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

            return data;
        }

        /// <summary>
        /// Gets global market values from Global Market Monitor database
        /// </summary>
        /// <returns></returns>
        public IDictionary<string, IDictionary<DateTime, Nullable<double>>> GetGlobalMarketHistory()
        {
            IDictionary<string, IDictionary<DateTime, Nullable<double>>> result = new Dictionary<string, IDictionary<DateTime, Nullable<double>>>(StringComparer.CurrentCultureIgnoreCase);
            IDictionary<string, string> marketIndicatorsDict = new Dictionary<string, string>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlTransaction trans = connection.BeginTransaction())
                    {
                        using (MySqlCommand command = new MySqlCommand(GetGlobalMarketMonthEndHistQuery, connection))
                        {
                            command.CommandType = System.Data.CommandType.StoredProcedure;

                            using (MySqlDataReader reader = command.ExecuteReader())
                            {
                                while (reader.Read())
                                {
                                    string marketIndicatorName = reader["MarketIndicatorName"] as string;
                                    Nullable<DateTime> effectiveDate = reader.IsDBNull(reader.GetOrdinal("MonthEndDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("MonthEndDate"));
                                    Nullable<double> marketLevel = (reader.IsDBNull(reader.GetOrdinal("MarketLevel"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarketLevel"));

                                    IDictionary<DateTime, Nullable<double>> dict;
                                    if (result.TryGetValue(marketIndicatorName, out dict))
                                    {
                                        if (!dict.ContainsKey(effectiveDate.GetValueOrDefault()))
                                            dict.Add(effectiveDate.GetValueOrDefault(), marketLevel);
                                        //else
                                        //    _logger.LogDebug("Key already exists: " + marketIndicator + "/" + effectiveDate + "/" + marketLevel);
                                    }
                                    else
                                    {
                                        dict = new Dictionary<DateTime, Nullable<double>>
                                                {
                                                    { effectiveDate.GetValueOrDefault(), marketLevel }
                                                };
                                        result.Add(marketIndicatorName, dict);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error getting Market Indicators history");
                throw;
            }

            return result;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IList<GlobalMarketSummary> GetGlobalMarketStats()
        {
            IList<GlobalMarketSummary> list = new List<GlobalMarketSummary>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetGlobalMarketStatsQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                GlobalMarketSummary data = new GlobalMarketSummary
                                {
                                    MarketIndicator = reader["MarketIndicator"] as string,
                                    MarketIndicatorDisplayName = reader["DisplayName"] as string,
                                    MeasureType = reader["MeasureType"] as string,
                                    AsOfDate = reader.IsDBNull(reader.GetOrdinal("AsofDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsofDate")),
                                    Source = reader["DataSource"] as string,
                                    Currency = reader["Currency"] as string,

                                    MarketLevel = (reader.IsDBNull(reader.GetOrdinal("MarketLevel"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarketLevel")),

                                    DTDChng = (reader.IsDBNull(reader.GetOrdinal("DTDChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DTDChng")),
                                    MTDChng = (reader.IsDBNull(reader.GetOrdinal("MTDChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MTDChng")),
                                    QTDChng = (reader.IsDBNull(reader.GetOrdinal("QTDChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("QTDChng")),
                                    YTDChng = (reader.IsDBNull(reader.GetOrdinal("YTDChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("YTDChng")),
                                    PriorMnthChng = (reader.IsDBNull(reader.GetOrdinal("PriorMnthChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriorMnthChng")),
                                    PriorYearChng = (reader.IsDBNull(reader.GetOrdinal("PriorYearChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriorYearChng")),
                                    Average12Mnth = (reader.IsDBNull(reader.GetOrdinal("Average12Mnth"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Average12Mnth")),
                                    ZScore12Mnth = (reader.IsDBNull(reader.GetOrdinal("ZScore12Mnth"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ZScore12Mnth")),

                                    Range12Mnth = reader["Range12Mnth"] as string,
                                    Range24Mnth = reader["Range24Mnth"] as string,
                                    Range36Mnth = reader["Range36Mnth"] as string,
                                    Range60Mnth = reader["Range60Mnth"] as string,
                                    RangeAll = reader["RangeAll"] as string,

                                    PercentileRank12MnthAsString = reader["Rank12Mnth"] as string,
                                    PercentileRank24MnthAsString = reader["Rank24Mnth"] as string,
                                    PercentileRank36MnthAsString = reader["Rank36Mnth"] as string,
                                    PercentileRank60MnthAsString = reader["Rank60Mnth"] as string,
                                    PercentileRankAllAsString = reader["RankAll"] as string
                                };

                                data.PercentileRank12Mnth = GetPercentileRank(data.PercentileRank12MnthAsString);
                                data.PercentileRank24Mnth = GetPercentileRank(data.PercentileRank24MnthAsString);
                                data.PercentileRank36Mnth = GetPercentileRank(data.PercentileRank36MnthAsString);
                                data.PercentileRank60Mnth = GetPercentileRank(data.PercentileRank60MnthAsString);
                                data.PercentileRankAll = GetPercentileRank(data.PercentileRankAllAsString);

                                data.RollingRtn12Mnth = (reader.IsDBNull(reader.GetOrdinal("Rolling12MnthRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Rolling12MnthRtn"));
                                data.RollingRtn24Mnth = (reader.IsDBNull(reader.GetOrdinal("Rolling24MnthRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Rolling24MnthRtn"));
                                data.RollingRtn36Mnth = (reader.IsDBNull(reader.GetOrdinal("Rolling36MnthRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Rolling36MnthRtn"));
                                data.RollingRtn60Mnth = (reader.IsDBNull(reader.GetOrdinal("Rolling60MnthRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Rolling60MnthRtn"));
                                data.RollingRtnAll = (reader.IsDBNull(reader.GetOrdinal("RollingAllRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RollingAllRtn"));

                                data.SharpeRatio12Mnth = reader["SharpeRatio12Mnth"] as string;
                                data.SharpeRatio24Mnth = reader["SharpeRatio24Mnth"] as string;
                                data.SharpeRatio36Mnth = reader["SharpeRatio36Mnth"] as string;
                                data.SharpeRatio60Mnth = reader["SharpeRatio60Mnth"] as string;
                                data.SharpeRatioAll = reader["SharpeRatioAll"] as string;

                                data.Volatility60Mnth = (reader.IsDBNull(reader.GetOrdinal("Volatility60Mnth"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Volatility60Mnth"));
                                data.VolatilityAll = (reader.IsDBNull(reader.GetOrdinal("VolatilityAll"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("VolatilityAll"));

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

        private double? GetPercentileRank(string input)
        {
            double? percentileRank = null;
            try
            {
                if (!string.IsNullOrEmpty(input))
                {
                    string[] values = input.Split('/');
                    percentileRank = (Convert.ToDouble(values[0]) / Convert.ToDouble(values[1])) * 100.0;
                }
            }
            catch (Exception)
            {
            }
            return percentileRank;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ticker"></param>
        /// <param name="startDate"></param>
        /// <param name="endDate"></param>
        /// <returns></returns>
        public IList<GlobalMarketHistory> GetGlobalMarketHistory(string ticker, DateTime startDate, DateTime endDate)
        {
            IList<GlobalMarketHistory> list = new List<GlobalMarketHistory>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetGlobalMarketIndicatorHistoryQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Market_Indicator_Name", ticker);
                        command.Parameters.AddWithValue("p_StartDate", startDate);
                        command.Parameters.AddWithValue("p_EndDate", endDate);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                GlobalMarketHistory data = new GlobalMarketHistory
                                {
                                    MarketIndicator = ticker,
                                    MarketLevel = (reader.IsDBNull(reader.GetOrdinal("MarketLevel"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarketLevel")),
                                };

                                Nullable<DateTime> effectiveDate = reader.IsDBNull(reader.GetOrdinal("EffectiveDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate"));
                                data.EffectiveDate = effectiveDate;
                                data.EffectiveDateAsString = DateUtils.ConvertDate(effectiveDate, "yyyy-MM-dd");
                                list.Add(data);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing query for ticker: " + ticker);
                throw;
            }

            return list;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="groupName"></param>
        /// <param name="securityType"></param>
        /// <param name="country"></param>
        /// <returns></returns>
        public IList<FundGroupDiscountStatsHist> GetFundDiscountStatsHistory(string groupName, string securityType, string country)
        {
            IList<FundGroupDiscountStatsHist> list = new List<FundGroupDiscountStatsHist>();

            string sql = GetFundDiscountStatsHistoryQuery
                + " where SecurityType = '" + securityType + "' and "
                + " Country = '" + country + "' and "
                + " GroupName = '" + groupName + "' order by EffectiveDate desc";

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundGroupDiscountStatsHist data = new FundGroupDiscountStatsHist
                                {
                                    Mean = (reader.IsDBNull(reader.GetOrdinal("MeanPD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MeanPD")),
                                    Median = (reader.IsDBNull(reader.GetOrdinal("MedianPD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MedianPD")),
                                    Funds = (reader.IsDBNull(reader.GetOrdinal("TotalFunds"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("TotalFunds")),
                                };

                                Nullable<DateTime> effectiveDate = reader.IsDBNull(reader.GetOrdinal("EffectiveDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate"));
                                data.EffectiveDateAsString = DateUtils.ConvertDate(effectiveDate, "yyyy-MM-dd");
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
        /// <param name="securityType"></param>
        /// <param name="country"></param>
        /// <returns></returns>
        public IDictionary<string, FundGroupDiscountStatsTO> GetFundDiscountStatsSummary(string securityType, string country)
        {
            IDictionary<string, FundGroupDiscountStatsTO> dict = new Dictionary<string, FundGroupDiscountStatsTO>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetFundDiscountStatsQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_SecurityType", securityType);
                        command.Parameters.AddWithValue("p_Country", country);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                string id = string.Empty;
                                string dataSecurityType = reader["SecurityType"] as string;
                                string dataCountry = reader["Country"] as string;
                                string dataGroupName = reader["GroupName"] as string;

                                if (dataSecurityType.Equals("Closed End Fund", StringComparison.CurrentCultureIgnoreCase))
                                    id = "CEF";

                                if (dataCountry.Equals("United States", StringComparison.CurrentCultureIgnoreCase))
                                    id += "|" + "US";

                                id += "|" + dataGroupName;

                                if (!dict.TryGetValue(id, out FundGroupDiscountStatsTO data))
                                {

                                    data = new FundGroupDiscountStatsTO();
                                    data.SecurityType = dataSecurityType;
                                    data.Country = dataCountry;
                                    data.GroupName = dataGroupName;
                                    data.GroupLevel1 = reader["GroupLevel1"] as string;
                                    data.GroupLevel2 = reader["GroupLevel2"] as string;
                                    data.GroupLevel3 = reader["GroupLevel3"] as string;
                                    data.ReportDate = reader.IsDBNull(reader.GetOrdinal("ReportDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("ReportDate"));
                                    data.TotalFunds = (reader.IsDBNull(reader.GetOrdinal("TotalFunds"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("TotalFunds"));
                                    data.NumFunds = (reader.IsDBNull(reader.GetOrdinal("NumFunds"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("NumFunds"));
                                    data.Indent = (reader.IsDBNull(reader.GetOrdinal("Indent"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("Indent"));

                                    dict.Add(id, data);
                                }

                                string periodType = reader["PeriodName"] as string;
                                double? medianPD = (reader.IsDBNull(reader.GetOrdinal("MedianPD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MedianPD"));

                                if (periodType.Equals("Current"))
                                {
                                    data.StatsCurrent.PeriodType = periodType;
                                    data.StatsCurrent.MedianPD = medianPD;
                                }
                                else if (periodType.Equals("MTD"))
                                {
                                    data.StatsMTD.PeriodType = periodType;
                                    data.StatsMTD.MedianPD = medianPD;
                                }
                                else if (periodType.Equals("1M"))
                                {
                                    data.Stats1M.PeriodType = periodType;
                                    data.Stats1M.MedianPD = medianPD;
                                }
                                else if (periodType.Equals("3M"))
                                {
                                    data.Stats3M.PeriodType = periodType;
                                    data.Stats3M.MedianPD = medianPD;
                                }
                                else if (periodType.Equals("6M"))
                                {
                                    data.Stats6M.PeriodType = periodType;
                                    data.Stats6M.MedianPD = medianPD;
                                }
                                else if (periodType.Equals("1Yr"))
                                {
                                    data.Stats1Yr.PeriodType = periodType;
                                    data.Stats1Yr.MedianPD = medianPD;
                                }
                                else if (periodType.Equals("2Yr"))
                                {
                                    data.Stats2Yr.PeriodType = periodType;
                                    data.Stats2Yr.MedianPD = medianPD;
                                }
                                else if (periodType.Equals("3Yr"))
                                {
                                    data.Stats3Yr.PeriodType = periodType;
                                    data.Stats3Yr.MedianPD = medianPD;
                                }
                                else if (periodType.Equals("5Yr"))
                                {
                                    data.Stats5Yr.PeriodType = periodType;
                                    data.Stats5Yr.MedianPD = medianPD;
                                }
                                else if (periodType.Equals("10Yr"))
                                {
                                    data.Stats10Yr.PeriodType = periodType;
                                    data.Stats10Yr.MedianPD = medianPD;
                                }
                                else if (periodType.Equals("25Yr"))
                                {
                                    data.Stats25Yr.PeriodType = periodType;
                                    data.Stats25Yr.MedianPD = medianPD;
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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="groupName"></param>
        /// <param name="securityType"></param>
        /// <param name="country"></param>
        /// <param name="startDate"></param>
        /// <param name="endDate"></param>
        /// <returns></returns>
        public IList<FundGroupDiscountStatsHist> GetFundDiscountStatsDetails(string groupName, string securityType, string country, string startDate, string endDate)
        {
            IList<FundGroupDiscountStatsHist> list = new List<FundGroupDiscountStatsHist>();

            string sql = GetFundDiscountStatsHistoryQuery
                + " where GroupName = '" + groupName + "'"
                + " and EffectiveDate >= '" + startDate + "'"
                + " and EffectiveDate <= '" + endDate + "'"
                + " order by EffectiveDate desc";

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundGroupDiscountStatsHist data = new FundGroupDiscountStatsHist
                                {
                                    Mean = (reader.IsDBNull(reader.GetOrdinal("MeanPD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MeanPD")),
                                    Median = (reader.IsDBNull(reader.GetOrdinal("MedianPD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MedianPD")),
                                    Funds = (reader.IsDBNull(reader.GetOrdinal("TotalFunds"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("TotalFunds")),
                                };

                                DateTime? effectiveDate = reader.IsDBNull(reader.GetOrdinal("EffectiveDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate"));
                                data.EffectiveDateAsString = DateUtils.ConvertDate(effectiveDate, "yyyy-MM-dd");
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
        /// <param name="securityType"></param>
        /// <param name="country"></param>
        /// <param name="startDate"></param>
        /// <param name="endDate"></param>
        /// <param name="groupNames"></param>
        /// <returns></returns>
        public IList<FundGroupDiscountDetails> GetFundDiscountDetails(string securityType, string country, string startDate, string endDate, IList<string> groupNames)
        {
            throw new NotImplementedException();
        }

        public IList<SecurityMaster> GetFundDiscountStatsSecurityList(string securityType, string country, string groupName, string period)
        {
            IList<SecurityMaster> list = new List<SecurityMaster>();

            string sql = GetFundDiscountStatsSecurityQuery
                + " where fs.SecurityType = '" + securityType + "'"
                + " and fs.Country = '" + country + "'"
                + " and fs.GroupName = '" + groupName + "'"
                + " and fs.PeriodName = '" + period + "'"
                + " order by s.Ticker";

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                SecurityMaster data = new SecurityMaster
                                {

                                    Ticker = reader["Ticker"] as string,
                                    SecurityDescription = reader["SecurityDescription"] as string,
                                    Country = reader["Country"] as string,
                                    YellowKey = reader["YellowKey"] as string,
                                    Currency = reader["Currency"] as string,
                                    PaymentRank = reader["PaymentRank"] as string,
                                    GeoLevel1 = reader["GeoLevel1"] as string,
                                    GeoLevel2 = reader["GeoLevel2"] as string,
                                    GeoLevel3 = reader["GeoLevel3"] as string,
                                    AssetClassLevel1 = reader["AssetClassLevel1"] as string,
                                    AssetClassLevel2 = reader["AssetClassLevel2"] as string,
                                    AssetClassLevel3 = reader["AssetClassLevel3"] as string,
                                    MarketStatus = reader["MarketStatus"] as string,
                                    InceptionDate = reader.IsDBNull(reader.GetOrdinal("InceptionDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("InceptionDate")),
                                    TerminationDate = reader.IsDBNull(reader.GetOrdinal("TerminationDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("TerminationDate")),
                                };

                                data.InceptionDateAsString = DateUtils.ConvertDate(data.InceptionDate, "yyyy-MM-dd");
                                data.TerminationDateAsString = DateUtils.ConvertDate(data.TerminationDate, "yyyy-MM-dd");
                                list.Add(data);
                            };
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

        public IDictionary<string, GlobalMarketIndicator> GetGlobalMarketIndicators()
        {
            IDictionary<string, GlobalMarketIndicator> dict = new Dictionary<string, GlobalMarketIndicator>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetGlobalMarketIndicatorsQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                GlobalMarketIndicator data = new GlobalMarketIndicator();
                                string marketIndicator = reader["MarketIndicator"] as string;
                                data.DataType = reader["DataType"] as string;
                                data.DataSource = reader["DataSource"] as string;
                                data.label = marketIndicator;
                                data.value = marketIndicator;
                                dict.Add(marketIndicator, data);
                            };
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

        private const string GetFundDiscountStatsQuery = "spGetHistDiscountStatsNew";
        private const string GetFundDiscountStatsByDateQuery = "spGetHistDiscountStatsByDate";
        private const string GetFundDiscountStatsHistoryQuery = "select EffectiveDate, MeanPD, MedianPD, TotalFunds from almitasc_ACTradingBBGData.FundPDGroupStatsDetail";
        private const string GetGlobalMarketStatsQuery = "call almitasc_ACTradingBBGData.spGetGlobalMarketSummary";
        private const string GetGlobalMarketIndicatorsQuery = "select distinct ifnull(conventionalname, marketindicator) as MarketIndicator, DataType, DataSource from GlobalMarketMonitor.globalmarketmonitor_datamaster";
        private const string GetGlobalMarketMonthEndHistQuery = "spGetGlobalMarketMonitorMonthEndHistNew";
        private const string GetGlobalMarketIndicatorHistoryQuery = "spGetGlobalMarketHist";
        private const string GetFundDiscountStatsSecurityQuery = "select s.ticker, s.securitydescription, s.country, s.yellowkey, s.currency, s.paymentrank, s.geolevel1, s.geolevel2, s.geolevel3"
                                            + ", s.assetclasslevel1, s.assetclasslevel2, s.assetclasslevel3, ifnull(sn.marketstatus, s.marketstatus) as marketstatus, s.inceptiondate, s.terminationdate"
                                            + " from almitasc_ACTradingBBGData.FundPDGroupSecurity fs"
                                            + " join almitasc_ACTradingBBGLink.globaltrading_securitymaster s on (fs.ticker = s.ticker)"
                                            + " left join almitasc_ACTradingBBGData.SecurityNAV sn on(s.figi = sn.figi)";
    }
}
