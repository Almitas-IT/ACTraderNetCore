using aACTrader.DAO.Interface;
using aCommons;
using aCommons.Cef;
using aCommons.DTO;
using aCommons.DTO.Fidelity;
using aCommons.DTO.JPM;
using aCommons.Pfd;
using aCommons.Utils;
using aCommons.Web;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace aACTrader.DAO.Repository
{
    public class FundDao : IFundDao
    {
        private readonly ILogger<FundDao> _logger;
        private const string DELIMITER = ",";

        public FundDao(ILogger<FundDao> logger)
        {
            _logger = logger;
            _logger.LogInformation("Initializing FundDao...");
        }

        /// <summary>
        /// Gets Fund Notes
        /// </summarySaveCurrencyExposuresQuery
        /// <returns></returns>
        public IDictionary<string, FundNotes> GetFundNotes()
        {
            IDictionary<string, FundNotes> dict = new Dictionary<string, FundNotes>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetFundNotesQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundNotes data = new FundNotes
                                {
                                    Ticker = reader["Ticker"] as string,
                                    Holdings = reader["Holdings"] as string,
                                    HoldingsComments = reader["HoldingsComments"] as string,
                                    FundComments = reader["FundComments"] as string,
                                    DiscountRange = reader["DiscountRange"] as string,
                                    EstBeta = (reader.IsDBNull(reader.GetOrdinal("EstBeta"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EstBeta")),
                                    EstDuration = (reader.IsDBNull(reader.GetOrdinal("EstDuration"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EstDuration")),
                                    Fees = (reader.IsDBNull(reader.GetOrdinal("Fees"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Fees")),
                                    IncentiveFee = (reader.IsDBNull(reader.GetOrdinal("IncentiveFee"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IncentiveFee")),
                                    HWM = reader["HWM"] as string,
                                    DCM = reader["DCM"] as string,
                                    BuybackShares = reader["BuybackShares"] as string,
                                    Management = reader["Management"] as string,
                                    FXHedging = reader["FXHedging"] as string,
                                    ContinuationVote = reader["ContinuationVote"] as string,
                                    SwapCapacity = reader["SwapCapacity"] as string,
                                    Trader = reader["Trader"] as string,
                                    LastUpdate = reader.IsDBNull(reader.GetOrdinal("LastUpdate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("LastUpdate"))
                                };

                                data.LastUpdateAsString = DateUtils.ConvertDate(data.LastUpdate, "yyyy-MM-dd");
                                if (!dict.TryGetValue(data.Ticker, out FundNotes fundNotesTemp))
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
        /// Save Fund Notes
        /// </summary>
        /// <param name="list"></param>
        public void SaveFundNotes(IList<FundNotes> list)
        {
            try
            {
                StringBuilder sCommand = new StringBuilder("insert into almitasc_ACTradingBBGData.StgFundNotes"
                    + "(Ticker, Holdings, HoldingsComments, FundComments, DiscountRange, EstBeta, EstDuration"
                    + ", Fees, IncentiveFee, HWM, DCM, BuybackShares, Management, FXHedging, ContinuationVote"
                    + ", SwapCapacity, Trader) values ");

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlTransaction trans = connection.BeginTransaction())
                    {
                        try
                        {
                            string sqlDelete = "delete from almitasc_ACTradingBBGData.StgFundNotes";
                            using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                            {
                                _logger.LogInformation("deleting data from almitasc_ACTradingBBGData.StgFundNotes");
                                command.ExecuteNonQuery();
                            }

                            List<string> Rows = new List<string>();
                            StringBuilder sb = new StringBuilder();
                            foreach (FundNotes data in list)
                            {
                                if (!string.IsNullOrEmpty(data.Ticker))
                                    sb.Append(string.Concat("'", data.Ticker, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                if (!string.IsNullOrEmpty(data.Holdings))
                                    sb.Append(string.Concat("'", data.Holdings, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                if (!string.IsNullOrEmpty(data.HoldingsComments))
                                    sb.Append(string.Concat("'", data.HoldingsComments, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                if (!string.IsNullOrEmpty(data.FundComments))
                                    sb.Append(string.Concat("'", data.FundComments, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                if (!string.IsNullOrEmpty(data.DiscountRange))
                                    sb.Append(string.Concat("'", data.DiscountRange, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                if (data.EstBeta.HasValue)
                                    sb.Append(data.EstBeta).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                if (data.EstDuration.HasValue)
                                    sb.Append(data.EstDuration).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                if (data.Fees.HasValue)
                                    sb.Append(data.Fees).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                if (data.IncentiveFee.HasValue)
                                    sb.Append(data.IncentiveFee).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                if (!string.IsNullOrEmpty(data.HWM))
                                    sb.Append(string.Concat("'", data.HWM, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                if (!string.IsNullOrEmpty(data.DCM))
                                    sb.Append(string.Concat("'", data.DCM, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                if (!string.IsNullOrEmpty(data.BuybackShares))
                                    sb.Append(string.Concat("'", data.BuybackShares, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                if (!string.IsNullOrEmpty(data.Management))
                                    sb.Append(string.Concat("'", data.Management, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                if (!string.IsNullOrEmpty(data.FXHedging))
                                    sb.Append(string.Concat("'", data.FXHedging, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                if (!string.IsNullOrEmpty(data.ContinuationVote))
                                    sb.Append(string.Concat("'", data.ContinuationVote, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                if (!string.IsNullOrEmpty(data.SwapCapacity))
                                    sb.Append(string.Concat("'", data.SwapCapacity, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                if (!string.IsNullOrEmpty(data.Trader))
                                    sb.Append(string.Concat("'", data.Trader, "'"));
                                else
                                    sb.Append("null");

                                string row = sb.ToString();
                                Rows.Add(string.Concat("(", row, ")"));
                                sb.Clear();
                            }

                            sCommand.Append(string.Join(",", Rows));
                            sCommand.Append(";");

                            _logger.LogDebug("insert into almitasc_ACTradingBBGData.StgFundNotes");
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), connection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }

                            _logger.LogInformation("Moving data to almitasc_ACTradingBBGData.FundNotes");
                            string sql = "spPopulateFundNotes";
                            using (MySqlCommand command = new MySqlCommand(sql, connection))
                            {
                                command.CommandType = System.Data.CommandType.StoredProcedure;
                                command.ExecuteNonQuery();
                            }
                            trans.Commit();
                        }
                        catch (Exception ex)
                        {
                            trans.Rollback();
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving fund notes into database");
                throw;
            }
        }

        /// <summary>
        /// Gets Fund Performance Returns
        /// </summary>
        /// <returns></returns>
        public IDictionary<string, FundPerformanceReturn> GetFundPerformanceReturns()
        {
            IDictionary<string, FundPerformanceReturn> dict = new Dictionary<string, FundPerformanceReturn>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetFundPerformanceReturnsQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundPerformanceReturn data = new FundPerformanceReturn
                                {
                                    Ticker = reader["Ticker"] as string,
                                    LastUpdate = reader.IsDBNull(reader.GetOrdinal("LastDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("LastDate")),
                                    PriceRtnMTD = (reader.IsDBNull(reader.GetOrdinal("PriceMTDRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceMTDRtn")),
                                    PriceRtnQTD = (reader.IsDBNull(reader.GetOrdinal("PriceQTDRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceQTDRtn")),
                                    PriceRtnYTD = (reader.IsDBNull(reader.GetOrdinal("PriceYTDRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceYTDRtn")),
                                    PriceRtn6Mnths = (reader.IsDBNull(reader.GetOrdinal("Price6MnthsRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price6MnthsRtn")),
                                    PriceRtn12Mnths = (reader.IsDBNull(reader.GetOrdinal("Price12MnthsRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price12MnthsRtn")),
                                    PriceRtn24Mnths = (reader.IsDBNull(reader.GetOrdinal("Price24MnthsRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price24MnthsRtn")),
                                    PriceRtn36Mnths = (reader.IsDBNull(reader.GetOrdinal("Price36MnthsRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price36MnthsRtn")),
                                    PriceRtn60Mnths = (reader.IsDBNull(reader.GetOrdinal("Price60MnthsRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price60MnthsRtn")),
                                    PriceRtn120Mnths = (reader.IsDBNull(reader.GetOrdinal("Price120MnthsRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price120MnthsRtn")),
                                    PriceRtnLife = (reader.IsDBNull(reader.GetOrdinal("PriceLifeRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceLifeRtn")),
                                    NavRtnMTD = (reader.IsDBNull(reader.GetOrdinal("NavMTDRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NavMTDRtn")),
                                    NavRtnQTD = (reader.IsDBNull(reader.GetOrdinal("NavQTDRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NavQTDRtn")),
                                    NavRtnYTD = (reader.IsDBNull(reader.GetOrdinal("NavYTDRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NavYTDRtn")),
                                    NavRtn6Mnths = (reader.IsDBNull(reader.GetOrdinal("Nav6MnthsRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav6MnthsRtn")),
                                    NavRtn12Mnths = (reader.IsDBNull(reader.GetOrdinal("Nav12MnthsRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav12MnthsRtn")),
                                    NavRtn24Mnths = (reader.IsDBNull(reader.GetOrdinal("Nav24MnthsRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav24MnthsRtn")),
                                    NavRtn36Mnths = (reader.IsDBNull(reader.GetOrdinal("Nav36MnthsRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav36MnthsRtn")),
                                    NavRtn60Mnths = (reader.IsDBNull(reader.GetOrdinal("Nav60MnthsRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav60MnthsRtn")),
                                    NavRtn120Mnths = (reader.IsDBNull(reader.GetOrdinal("Nav120MnthsRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav120MnthsRtn")),
                                    NavRtnLife = (reader.IsDBNull(reader.GetOrdinal("NavLifeRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NavLifeRtn")),
                                };

                                data.LastUpdateAsString = DateUtils.ConvertDate(data.LastUpdate, "yyyy-MM-dd");

                                if (!dict.TryGetValue(data.Ticker, out FundPerformanceReturn fndPerformanceReturn))
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
        /// Gets Fund Performance Risk Stats
        /// </summary>
        /// <returns></returns>
        public IDictionary<string, FundPerformanceRisk> GetFundPerformanceRiskStats()
        {
            IDictionary<string, FundPerformanceRisk> dict = new Dictionary<string, FundPerformanceRisk>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetFundPerformanceRiskStatsQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundPerformanceRisk data = new FundPerformanceRisk
                                {
                                    Ticker = reader["Ticker"] as string,
                                    LastUpdate = reader.IsDBNull(reader.GetOrdinal("LastDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("LastDate")),

                                    PriceVolMTD = (reader.IsDBNull(reader.GetOrdinal("PriceMTDVol"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceMTDVol")),
                                    PriceVolQTD = (reader.IsDBNull(reader.GetOrdinal("PriceQTDVol"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceQTDVol")),
                                    PriceVolYTD = (reader.IsDBNull(reader.GetOrdinal("PriceYTDVol"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceYTDVol")),
                                    PriceVol6Mnths = (reader.IsDBNull(reader.GetOrdinal("Price6MnthsVol"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price6MnthsVol")),
                                    PriceVol12Mnths = (reader.IsDBNull(reader.GetOrdinal("Price12MnthsVol"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price12MnthsVol")),
                                    PriceVol24Mnths = (reader.IsDBNull(reader.GetOrdinal("Price24MnthsVol"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price24MnthsVol")),
                                    PriceVol36Mnths = (reader.IsDBNull(reader.GetOrdinal("Price36MnthsVol"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price36MnthsVol")),
                                    PriceVol60Mnths = (reader.IsDBNull(reader.GetOrdinal("Price60MnthsVol"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price60MnthsVol")),
                                    PriceVol120Mnths = (reader.IsDBNull(reader.GetOrdinal("Price120MnthsVol"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price120MnthsVol")),
                                    PriceVolLife = (reader.IsDBNull(reader.GetOrdinal("PriceLifeVol"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceLifeVol")),

                                    NavVolMTD = (reader.IsDBNull(reader.GetOrdinal("NavMTDVol"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NavMTDVol")),
                                    NavVolQTD = (reader.IsDBNull(reader.GetOrdinal("NavQTDVol"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NavQTDVol")),
                                    NavVolYTD = (reader.IsDBNull(reader.GetOrdinal("NavYTDVol"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NavYTDVol")),
                                    NavVol6Mnths = (reader.IsDBNull(reader.GetOrdinal("Nav6MnthsVol"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav6MnthsVol")),
                                    NavVol12Mnths = (reader.IsDBNull(reader.GetOrdinal("Nav12MnthsVol"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav12MnthsVol")),
                                    NavVol24Mnths = (reader.IsDBNull(reader.GetOrdinal("Nav24MnthsVol"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav24MnthsVol")),
                                    NavVol36Mnths = (reader.IsDBNull(reader.GetOrdinal("Nav36MnthsVol"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav36MnthsVol")),
                                    NavVol60Mnths = (reader.IsDBNull(reader.GetOrdinal("Nav60MnthsVol"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav60MnthsVol")),
                                    NavVol120Mnths = (reader.IsDBNull(reader.GetOrdinal("Nav120MnthsVol"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav120MnthsVol")),
                                    NavVolLife = (reader.IsDBNull(reader.GetOrdinal("NavLifeVol"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NavLifeVol")),

                                    PriceSharpeRatioMTD = (reader.IsDBNull(reader.GetOrdinal("PriceMTDSharpeRatio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceMTDSharpeRatio")),
                                    PriceSharpeRatioQTD = (reader.IsDBNull(reader.GetOrdinal("PriceQTDSharpeRatio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceQTDSharpeRatio")),
                                    PriceSharpeRatioYTD = (reader.IsDBNull(reader.GetOrdinal("PriceYTDSharpeRatio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceYTDSharpeRatio")),
                                    PriceSharpeRatio6Mnths = (reader.IsDBNull(reader.GetOrdinal("Price6MnthsSharpeRatio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price6MnthsSharpeRatio")),
                                    PriceSharpeRatio12Mnths = (reader.IsDBNull(reader.GetOrdinal("Price12MnthsSharpeRatio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price12MnthsSharpeRatio")),
                                    PriceSharpeRatio24Mnths = (reader.IsDBNull(reader.GetOrdinal("Price24MnthsSharpeRatio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price24MnthsSharpeRatio")),
                                    PriceSharpeRatio36Mnths = (reader.IsDBNull(reader.GetOrdinal("Price36MnthsSharpeRatio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price36MnthsSharpeRatio")),
                                    PriceSharpeRatio60Mnths = (reader.IsDBNull(reader.GetOrdinal("Price60MnthsSharpeRatio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price60MnthsSharpeRatio")),
                                    PriceSharpeRatio120Mnths = (reader.IsDBNull(reader.GetOrdinal("Price120MnthsSharpeRatio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price120MnthsSharpeRatio")),
                                    PriceSharpeRatioLife = (reader.IsDBNull(reader.GetOrdinal("PriceLifeSharpeRatio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceLifeSharpeRatio")),

                                    NavSharpeRatioMTD = (reader.IsDBNull(reader.GetOrdinal("NavMTDSharpeRatio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NavMTDSharpeRatio")),
                                    NavSharpeRatioQTD = (reader.IsDBNull(reader.GetOrdinal("NavQTDSharpeRatio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NavQTDSharpeRatio")),
                                    NavSharpeRatioYTD = (reader.IsDBNull(reader.GetOrdinal("NavYTDSharpeRatio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NavYTDSharpeRatio")),
                                    NavSharpeRatio6Mnths = (reader.IsDBNull(reader.GetOrdinal("Nav6MnthsSharpeRatio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav6MnthsSharpeRatio")),
                                    NavSharpeRatio12Mnths = (reader.IsDBNull(reader.GetOrdinal("Nav12MnthsSharpeRatio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav12MnthsSharpeRatio")),
                                    NavSharpeRatio24Mnths = (reader.IsDBNull(reader.GetOrdinal("Nav24MnthsSharpeRatio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav24MnthsSharpeRatio")),
                                    NavSharpeRatio36Mnths = (reader.IsDBNull(reader.GetOrdinal("Nav36MnthsSharpeRatio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav36MnthsSharpeRatio")),
                                    NavSharpeRatio60Mnths = (reader.IsDBNull(reader.GetOrdinal("Nav60MnthsSharpeRatio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav60MnthsSharpeRatio")),
                                    NavSharpeRatio120Mnths = (reader.IsDBNull(reader.GetOrdinal("Nav120MnthsSharpeRatio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav120MnthsSharpeRatio")),
                                    NavSharpeRatioLife = (reader.IsDBNull(reader.GetOrdinal("NavLifeSharpeRatio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NavLifeSharpeRatio")),

                                };

                                data.LastUpdateAsString = DateUtils.ConvertDate(data.LastUpdate, "yyyy-MM-dd");
                                if (!dict.TryGetValue(data.Ticker, out FundPerformanceRisk fundPerformanceRisk))
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
        /// Get Fund Performance Ranks
        /// </summary>
        /// <returns></returns>
        public IDictionary<string, IDictionary<string, FundPerformanceRank>> GetFundPerformanceRanks()
        {
            IDictionary<string, IDictionary<string, FundPerformanceRank>> dict = new Dictionary<string, IDictionary<string, FundPerformanceRank>>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetFundPerformanceRanksQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundPerformanceRank data = new FundPerformanceRank
                                {
                                    Ticker = reader["Ticker"] as string,
                                    PeerGroup = reader["PeerGroup"] as string,
                                    PeerGroupHierarchyRank = reader["PerfRank"] as string,

                                    PDMTD = (reader.IsDBNull(reader.GetOrdinal("PDMTD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PDMTD")),
                                    PDQTD = (reader.IsDBNull(reader.GetOrdinal("PDQTD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PDQTD")),
                                    PDYTD = (reader.IsDBNull(reader.GetOrdinal("PDYTD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PDYTD")),
                                    PD6Mnths = (reader.IsDBNull(reader.GetOrdinal("PD6Mnths"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PD6Mnths")),
                                    PD12Mnths = (reader.IsDBNull(reader.GetOrdinal("PD12Mnths"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PD12Mnths")),
                                    PD24Mnths = (reader.IsDBNull(reader.GetOrdinal("PD24Mnths"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PD24Mnths")),
                                    PD36Mnths = (reader.IsDBNull(reader.GetOrdinal("PD36Mnths"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PD36Mnths")),
                                    PD60Mnths = (reader.IsDBNull(reader.GetOrdinal("PD60Mnths"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PD60Mnths")),
                                    PD120Mnths = (reader.IsDBNull(reader.GetOrdinal("PD120Mnths"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PD120Mnths")),
                                    PDLife = (reader.IsDBNull(reader.GetOrdinal("PDLife"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PDLife")),

                                    PDMTDRank = (reader.IsDBNull(reader.GetOrdinal("PDMTDRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PDMTDRank")),
                                    PDQTDRank = (reader.IsDBNull(reader.GetOrdinal("PDQTDRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PDQTDRank")),
                                    PDYTDRank = (reader.IsDBNull(reader.GetOrdinal("PDYTDRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PDYTDRank")),
                                    PD6MnthsRank = (reader.IsDBNull(reader.GetOrdinal("PD6MnthsRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PD6MnthsRank")),
                                    PD12MnthsRank = (reader.IsDBNull(reader.GetOrdinal("PD12MnthsRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PD12MnthsRank")),
                                    PD24MnthsRank = (reader.IsDBNull(reader.GetOrdinal("PD24MnthsRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PD24MnthsRank")),
                                    PD36MnthsRank = (reader.IsDBNull(reader.GetOrdinal("PD36MnthsRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PD36MnthsRank")),
                                    PD60MnthsRank = (reader.IsDBNull(reader.GetOrdinal("PD60MnthsRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PD60MnthsRank")),
                                    PD120MnthsRank = (reader.IsDBNull(reader.GetOrdinal("PD120MnthsRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PD120MnthsRank")),
                                    PDLifeRank = (reader.IsDBNull(reader.GetOrdinal("PDLifeRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PDLifeRank")),

                                    PriceRtnMTDRank = (reader.IsDBNull(reader.GetOrdinal("PriceMTDRtnRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceMTDRtnRank")),
                                    PriceRtnQTDRank = (reader.IsDBNull(reader.GetOrdinal("PriceQTDRtnRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceQTDRtnRank")),
                                    PriceRtnYTDRank = (reader.IsDBNull(reader.GetOrdinal("PriceYTDRtnRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceYTDRtnRank")),
                                    PriceRtn6MnthsRank = (reader.IsDBNull(reader.GetOrdinal("Price6MnthsRtnRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price6MnthsRtnRank")),
                                    PriceRtn12MnthsRank = (reader.IsDBNull(reader.GetOrdinal("Price12MnthsRtnRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price12MnthsRtnRank")),
                                    PriceRtn24MnthsRank = (reader.IsDBNull(reader.GetOrdinal("Price24MnthsRtnRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price24MnthsRtnRank")),
                                    PriceRtn36MnthsRank = (reader.IsDBNull(reader.GetOrdinal("Price36MnthsRtnRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price36MnthsRtnRank")),
                                    PriceRtn60MnthsRank = (reader.IsDBNull(reader.GetOrdinal("Price60MnthsRtnRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price60MnthsRtnRank")),
                                    PriceRtn120MnthsRank = (reader.IsDBNull(reader.GetOrdinal("Price120MnthsRtnRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price120MnthsRtnRank")),
                                    PriceRtnLifeRank = (reader.IsDBNull(reader.GetOrdinal("PriceLifeRtnRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceLifeRtnRank")),

                                    NavRtnMTDRank = (reader.IsDBNull(reader.GetOrdinal("NavMTDRtnRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NavMTDRtnRank")),
                                    NavRtnQTDRank = (reader.IsDBNull(reader.GetOrdinal("NavQTDRtnRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NavQTDRtnRank")),
                                    NavRtnYTDRank = (reader.IsDBNull(reader.GetOrdinal("NavYTDRtnRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NavYTDRtnRank")),
                                    NavRtn6MnthsRank = (reader.IsDBNull(reader.GetOrdinal("Nav6MnthsRtnRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav6MnthsRtnRank")),
                                    NavRtn12MnthsRank = (reader.IsDBNull(reader.GetOrdinal("Nav12MnthsRtnRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav12MnthsRtnRank")),
                                    NavRtn24MnthsRank = (reader.IsDBNull(reader.GetOrdinal("Nav24MnthsRtnRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav24MnthsRtnRank")),
                                    NavRtn36MnthsRank = (reader.IsDBNull(reader.GetOrdinal("Nav36MnthsRtnRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav36MnthsRtnRank")),
                                    NavRtn60MnthsRank = (reader.IsDBNull(reader.GetOrdinal("Nav60MnthsRtnRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav60MnthsRtnRank")),
                                    NavRtn120MnthsRank = (reader.IsDBNull(reader.GetOrdinal("Nav120MnthsRtnRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav120MnthsRtnRank")),
                                    NavRtnLifeRank = (reader.IsDBNull(reader.GetOrdinal("NavLifeRtnRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NavLifeRtnRank")),

                                    PriceVolMTDRank = (reader.IsDBNull(reader.GetOrdinal("PriceMTDVolRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceMTDVolRank")),
                                    PriceVolQTDRank = (reader.IsDBNull(reader.GetOrdinal("PriceQTDVolRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceQTDVolRank")),
                                    PriceVolYTDRank = (reader.IsDBNull(reader.GetOrdinal("PriceYTDVolRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceYTDVolRank")),
                                    PriceVol6MnthsRank = (reader.IsDBNull(reader.GetOrdinal("Price6MnthsVolRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price6MnthsVolRank")),
                                    PriceVol12MnthsRank = (reader.IsDBNull(reader.GetOrdinal("Price12MnthsVolRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price12MnthsVolRank")),
                                    PriceVol24MnthsRank = (reader.IsDBNull(reader.GetOrdinal("Price24MnthsVolRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price24MnthsVolRank")),
                                    PriceVol36MnthsRank = (reader.IsDBNull(reader.GetOrdinal("Price36MnthsVolRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price36MnthsVolRank")),
                                    PriceVol60MnthsRank = (reader.IsDBNull(reader.GetOrdinal("Price60MnthsVolRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price60MnthsVolRank")),
                                    PriceVol120MnthsRank = (reader.IsDBNull(reader.GetOrdinal("Price120MnthsVolRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price120MnthsVolRank")),
                                    PriceVolLifeRank = (reader.IsDBNull(reader.GetOrdinal("PriceLifeVolRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceLifeVolRank")),

                                    NavVolMTDRank = (reader.IsDBNull(reader.GetOrdinal("NavMTDVolRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NavMTDVolRank")),
                                    NavVolQTDRank = (reader.IsDBNull(reader.GetOrdinal("NavQTDVolRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NavQTDVolRank")),
                                    NavVolYTDRank = (reader.IsDBNull(reader.GetOrdinal("NavYTDVolRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NavYTDVolRank")),
                                    NavVol6MnthsRank = (reader.IsDBNull(reader.GetOrdinal("Nav6MnthsVolRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav6MnthsVolRank")),
                                    NavVol12MnthsRank = (reader.IsDBNull(reader.GetOrdinal("Nav12MnthsVolRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav12MnthsVolRank")),
                                    NavVol24MnthsRank = (reader.IsDBNull(reader.GetOrdinal("Nav24MnthsVolRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav24MnthsVolRank")),
                                    NavVol36MnthsRank = (reader.IsDBNull(reader.GetOrdinal("Nav36MnthsVolRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav36MnthsVolRank")),
                                    NavVol60MnthsRank = (reader.IsDBNull(reader.GetOrdinal("Nav60MnthsVolRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav60MnthsVolRank")),
                                    NavVol120MnthsRank = (reader.IsDBNull(reader.GetOrdinal("Nav120MnthsVolRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav120MnthsVolRank")),
                                    NavVolLifeRank = (reader.IsDBNull(reader.GetOrdinal("NavLifeVolRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NavLifeVolRank")),

                                    PriceSharpeRatioMTDRank = (reader.IsDBNull(reader.GetOrdinal("PriceMTDSharpeRatioRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceMTDSharpeRatioRank")),
                                    PriceSharpeRatioQTDRank = (reader.IsDBNull(reader.GetOrdinal("PriceQTDSharpeRatioRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceQTDSharpeRatioRank")),
                                    PriceSharpeRatioYTDRank = (reader.IsDBNull(reader.GetOrdinal("PriceYTDSharpeRatioRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceYTDSharpeRatioRank")),
                                    PriceSharpeRatio6MnthsRank = (reader.IsDBNull(reader.GetOrdinal("Price6MnthsSharpeRatioRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price6MnthsSharpeRatioRank")),
                                    PriceSharpeRatio12MnthsRank = (reader.IsDBNull(reader.GetOrdinal("Price12MnthsSharpeRatioRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price12MnthsSharpeRatioRank")),
                                    PriceSharpeRatio24MnthsRank = (reader.IsDBNull(reader.GetOrdinal("Price24MnthsSharpeRatioRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price24MnthsSharpeRatioRank")),
                                    PriceSharpeRatio36MnthsRank = (reader.IsDBNull(reader.GetOrdinal("Price36MnthsSharpeRatioRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price36MnthsSharpeRatioRank")),
                                    PriceSharpeRatio60MnthsRank = (reader.IsDBNull(reader.GetOrdinal("Price60MnthsSharpeRatioRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price60MnthsSharpeRatioRank")),
                                    PriceSharpeRatio120MnthsRank = (reader.IsDBNull(reader.GetOrdinal("Price120MnthsSharpeRatioRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price120MnthsSharpeRatioRank")),
                                    PriceSharpeRatioLifeRank = (reader.IsDBNull(reader.GetOrdinal("PriceLifeSharpeRatioRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceLifeSharpeRatioRank")),

                                    NavSharpeRatioMTDRank = (reader.IsDBNull(reader.GetOrdinal("NavMTDSharpeRatioRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NavMTDSharpeRatioRank")),
                                    NavSharpeRatioQTDRank = (reader.IsDBNull(reader.GetOrdinal("NavQTDSharpeRatioRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NavQTDSharpeRatioRank")),
                                    NavSharpeRatioYTDRank = (reader.IsDBNull(reader.GetOrdinal("NavYTDSharpeRatioRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NavYTDSharpeRatioRank")),
                                    NavSharpeRatio6MnthsRank = (reader.IsDBNull(reader.GetOrdinal("Nav6MnthsSharpeRatioRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav6MnthsSharpeRatioRank")),
                                    NavSharpeRatio12MnthsRank = (reader.IsDBNull(reader.GetOrdinal("Nav12MnthsSharpeRatioRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav12MnthsSharpeRatioRank")),
                                    NavSharpeRatio24MnthsRank = (reader.IsDBNull(reader.GetOrdinal("Nav24MnthsSharpeRatioRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav24MnthsSharpeRatioRank")),
                                    NavSharpeRatio36MnthsRank = (reader.IsDBNull(reader.GetOrdinal("Nav36MnthsSharpeRatioRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav36MnthsSharpeRatioRank")),
                                    NavSharpeRatio60MnthsRank = (reader.IsDBNull(reader.GetOrdinal("Nav60MnthsSharpeRatioRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav60MnthsSharpeRatioRank")),
                                    NavSharpeRatio120MnthsRank = (reader.IsDBNull(reader.GetOrdinal("Nav120MnthsSharpeRatioRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav120MnthsSharpeRatioRank")),
                                    NavSharpeRatioLifeRank = (reader.IsDBNull(reader.GetOrdinal("NavLifeSharpeRatioRank"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NavLifeSharpeRatioRank")),

                                    LastUpdate = reader.IsDBNull(reader.GetOrdinal("LastDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("LastDate"))
                                };

                                data.LastUpdateAsString = DateUtils.ConvertDate(data.LastUpdate, "yyyy-MM-dd");

                                IDictionary<string, FundPerformanceRank> fundPerformanceStatsDict;
                                if (dict.TryGetValue(data.Ticker, out fundPerformanceStatsDict))
                                {
                                    if (!fundPerformanceStatsDict.TryGetValue(data.PeerGroupHierarchyRank, out FundPerformanceRank fundPerformanceRank))
                                        fundPerformanceStatsDict.Add(data.PeerGroupHierarchyRank, data);
                                }
                                else
                                {
                                    fundPerformanceStatsDict = new Dictionary<string, FundPerformanceRank>
                                        {
                                            { data.PeerGroupHierarchyRank, data }
                                        };
                                    dict.Add(data.Ticker, fundPerformanceStatsDict);
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
        /// Gets Daily PnL Summary
        /// </summary>
        /// <param name="asofDate"></param>
        /// <returns></returns>
        public IList<SecurityPerformance> GetDailyPnLSummary(DateTime asofDate)
        {
            IList<SecurityPerformance> list = new List<SecurityPerformance>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetDailyPnLSummaryQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_AsOfDate", asofDate);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                SecurityPerformance data = new SecurityPerformance
                                {
                                    FundType = reader["FundType"] as string,
                                    EffectiveDate = reader.IsDBNull(reader.GetOrdinal("EffectiveDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate")),

                                    Ticker = reader["Ticker"] as string,
                                    DatePeriod = reader["DatePeriod"] as string,
                                    Position = (reader.IsDBNull(reader.GetOrdinal("Position"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Position")),
                                    BegMV = (reader.IsDBNull(reader.GetOrdinal("BegMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BegMV")),
                                    EndMV = (reader.IsDBNull(reader.GetOrdinal("EndMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EndMV")),
                                    PnL = (reader.IsDBNull(reader.GetOrdinal("PnL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PnL")),
                                    TradingPnL = (reader.IsDBNull(reader.GetOrdinal("TradePnL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TradePnL")),
                                    DailyPnL = (reader.IsDBNull(reader.GetOrdinal("DailyPnL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DailyPnL"))
                                };

                                data.EffectiveDateAsString = DateUtils.ConvertDate(data.EffectiveDate, "yyyy-MM-dd");

                                string id = data.FundType + "|" + data.Ticker;
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
        /// Get Daily PnL Details
        /// </summary>
        /// <param name="asofDate"></param>
        /// <returns></returns>
        public IList<SecurityPerformance> GetDailyPnLDetails(string asofDate)
        {
            IList<SecurityPerformance> list = new List<SecurityPerformance>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    string sql = GetDailyPnLDetailsNewQuery + " where (trade not in ('Closed') or trade is null) and date = '" + asofDate + "'" + " order by p.ticker, p.portfolio";
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                SecurityPerformance data = new SecurityPerformance
                                {
                                    FundType = reader["portfolio"] as string,
                                    EffectiveDate = reader.IsDBNull(reader.GetOrdinal("date")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("date")),
                                    Ticker = reader["ticker"] as string,
                                    YellowKey = reader["ykey"] as string,
                                    Currency = reader["fx"] as string,
                                    Position = (reader.IsDBNull(reader.GetOrdinal("position"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("position")),
                                    ClosePrice = (reader.IsDBNull(reader.GetOrdinal("priceo"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("priceo")),
                                    CloseFXRate = (reader.IsDBNull(reader.GetOrdinal("fxrateo"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("fxrateo")),
                                    TickerId = reader["tkid"] as string,
                                    ProxyId = reader["proxyid"] as string,
                                    Sector = reader["sector"] as string,
                                    BroadSector = reader["broadsector"] as string,
                                    SecurityType = reader["sectype"] as string,
                                    RiskFactor = reader["riskfactor"] as string,
                                    LastPrice = (reader.IsDBNull(reader.GetOrdinal("lastprice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("lastprice")),
                                    LastFXRate = (reader.IsDBNull(reader.GetOrdinal("fxrate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("fxrate")),
                                    TRRLocal = (reader.IsDBNull(reader.GetOrdinal("trrlocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("trrlocal")),
                                    PnLUSD = (reader.IsDBNull(reader.GetOrdinal("pnlusd"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("pnlusd")),
                                    BegMV = (reader.IsDBNull(reader.GetOrdinal("tmvousd"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("tmvousd")),
                                    EndMV = (reader.IsDBNull(reader.GetOrdinal("tmvusd"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("tmvusd")),
                                    TradingPnL = (reader.IsDBNull(reader.GetOrdinal("tradepnl"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("tradepnl")),
                                    DatePeriod = reader["dateperiod"] as string,
                                    TradeType = reader["trade"] as string,
                                    Level1 = reader["Level1"] as string,
                                    Level2 = reader["Level2"] as string,
                                    Level3 = reader["Level3"] as string,
                                };

                                data.EffectiveDateAsString = DateUtils.ConvertDate(data.EffectiveDate, "yyyy-MM-dd");
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
        /// Save REIT Daily Discounts
        /// </summary>
        /// <param name="list"></param>
        public void SaveREITsDailyDiscounts(IList<BDCNavHistory> list)
        {
            try
            {
                StringBuilder sCommand = new StringBuilder("insert into almitasc_ACTradingBBGData.StgReitDailyBVHist"
                    + "(Ticker, Date, PD) values ");

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlTransaction trans = connection.BeginTransaction())
                    {
                        string sqlDelete = "delete from almitasc_ACTradingBBGData.StgReitDailyBVHist";
                        using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                        {
                            _logger.LogInformation("deleting data from almitasc_ACTradingBBGData.StgReitDailyBVHist");
                            command.ExecuteNonQuery();
                        }

                        List<string> Rows = new List<string>();
                        StringBuilder sb = new StringBuilder();
                        foreach (BDCNavHistory data in list)
                        {
                            //Ticker
                            if (!string.IsNullOrEmpty(data.Ticker))
                                sb.Append(string.Concat("'", data.Ticker, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //Effective Date
                            if (!string.IsNullOrEmpty(data.NavDateAsString))
                                sb.Append(string.Concat("'", data.NavDateAsString, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //PD
                            if (data.PD.HasValue)
                                sb.Append(data.PD);
                            else
                                sb.Append("null");

                            string row = sb.ToString();
                            Rows.Add(string.Concat("(", row, ")"));
                            sb.Clear();
                        }

                        sCommand.Append(string.Join(",", Rows));
                        sCommand.Append(";");

                        _logger.LogDebug("insert into almitasc_ACTradingBBGData.StgReitDailyBVHist");
                        using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), connection))
                        {
                            myCmd.CommandType = CommandType.Text;
                            myCmd.ExecuteNonQuery();
                        }

                        _logger.LogInformation("Moving data to Target Table...almitasc_ACTradingBBGData.ReitDailyBVHist");
                        using (MySqlCommand command = new MySqlCommand(SaveREITDailyAnalyticsQuery, connection))
                        {
                            command.CommandType = System.Data.CommandType.StoredProcedure;
                            command.Parameters.AddWithValue("p_DataType", "Discounts");
                            command.ExecuteNonQuery();
                        }
                        trans.Commit();
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving REITs Daily Discounts");
                throw;
            }
        }

        /// <summary>
        /// Save REIT Daily BVs
        /// </summary>
        /// <param name="list"></param>
        public void SaveREITsDailyBVs(IList<BDCNavHistory> list)
        {
            try
            {
                StringBuilder sCommand = new StringBuilder("insert into almitasc_ACTradingBBGData.StgReitDailyBVHist"
                    + "(Ticker, Date, BookValue) values ");

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlTransaction trans = connection.BeginTransaction())
                    {
                        string sqlDelete = "delete from almitasc_ACTradingBBGData.StgReitDailyBVHist";
                        using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                        {
                            _logger.LogInformation("deleting data from almitasc_ACTradingBBGData.StgReitDailyBVHist");
                            command.ExecuteNonQuery();
                        }

                        List<string> Rows = new List<string>();
                        StringBuilder sb = new StringBuilder();
                        foreach (BDCNavHistory data in list)
                        {
                            //Ticker
                            if (!string.IsNullOrEmpty(data.Ticker))
                                sb.Append(string.Concat("'", data.Ticker, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //Effective Date
                            if (!string.IsNullOrEmpty(data.NavDateAsString))
                                sb.Append(string.Concat("'", data.NavDateAsString, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //Book Value
                            if (data.Nav.HasValue)
                                sb.Append(data.Nav);
                            else
                                sb.Append("null");

                            string row = sb.ToString();
                            Rows.Add(string.Concat("(", row, ")"));
                            sb.Clear();
                        }

                        sCommand.Append(string.Join(",", Rows));
                        sCommand.Append(";");

                        _logger.LogDebug("insert into almitasc_ACTradingBBGData.StgReitDailyBVHist");
                        using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), connection))
                        {
                            myCmd.CommandType = CommandType.Text;
                            myCmd.ExecuteNonQuery();
                        }

                        _logger.LogInformation("Moving data to Target Table...almitasc_ACTradingBBGData.ReitDailyBVHist");
                        using (MySqlCommand command = new MySqlCommand(SaveREITDailyAnalyticsQuery, connection))
                        {
                            command.CommandType = System.Data.CommandType.StoredProcedure;
                            command.Parameters.AddWithValue("p_DataType", "BVs");
                            command.ExecuteNonQuery();
                        }
                        trans.Commit();
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving REITs Daily BVs");
                throw;
            }
        }

        /// <summary>
        /// Save REIT Pfd Analytics
        /// </summary>
        /// <param name="list"></param>
        public void SaveREITPfdAnalytics(IList<REITPfdAnalytics> list)
        {
            try
            {
                StringBuilder sCommand = new StringBuilder("insert into almitasc_ACTradingBBGData.globalreit_pfdanalytics"
                    + "(FIGI, PfdTicker, BBGId, SecurityType, EffectiveDate, Cpn, Yield,"
                    + " LastPrice, BidPrice, AskPrice, ClosePrice, CleanPrice,"
                    + " SpreadToWorst, OASSpreadMid, SpreadToFairValueOAS"
                    + ") values ");

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlTransaction trans = connection.BeginTransaction())
                    {
                        string effectiveDate = list[0].EffectiveDateAsString;
                        string sqlDelete = "delete from almitasc_ACTradingBBGData.globalreit_pfdanalytics where EffectiveDate = '" + effectiveDate + "'";
                        using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                        {
                            _logger.LogInformation("deleting data from almitasc_ACTradingBBGData.globalreit_pfdanalytics");
                            command.ExecuteNonQuery();
                        }

                        List<string> Rows = new List<string>();
                        StringBuilder sb = new StringBuilder();
                        foreach (REITPfdAnalytics data in list)
                        {
                            //FIGI
                            if (!string.IsNullOrEmpty(data.FIGI))
                                sb.Append(string.Concat("'", data.FIGI, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //PfdTicker
                            if (!string.IsNullOrEmpty(data.Ticker))
                                sb.Append(string.Concat("'", data.Ticker, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //BBGId
                            if (!string.IsNullOrEmpty(data.BBGId))
                                sb.Append(string.Concat("'", data.BBGId, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //SecurityType
                            if (!string.IsNullOrEmpty(data.SecurityType))
                                sb.Append(string.Concat("'", data.SecurityType, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //Effective Date
                            if (!string.IsNullOrEmpty(data.EffectiveDateAsString))
                                sb.Append(string.Concat("'", data.EffectiveDateAsString, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //Cpn
                            if (data.Coupon.HasValue)
                                sb.Append(data.Coupon).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //Yield
                            if (data.Yield.HasValue)
                                sb.Append(data.Yield).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //LastPrice
                            if (data.LastPrice.HasValue)
                                sb.Append(data.LastPrice).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //BidPrice
                            if (data.BidPrice.HasValue)
                                sb.Append(data.BidPrice).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //AskPrice
                            if (data.AskPrice.HasValue)
                                sb.Append(data.AskPrice).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //ClosePrice
                            if (data.ClosePrice.HasValue)
                                sb.Append(data.ClosePrice).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //CleanPrice
                            if (data.CleanPrice.HasValue)
                                sb.Append(data.CleanPrice).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //SpreadToWorst
                            if (data.SpreadToWorst.HasValue)
                                sb.Append(data.SpreadToWorst).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //OASSpreadMid
                            if (data.OASSpreadMid.HasValue)
                                sb.Append(data.OASSpreadMid).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //SpreadToFairValueOAS
                            if (data.SpreadToFairValueOAS.HasValue)
                                sb.Append(data.SpreadToFairValueOAS);
                            else
                                sb.Append("null");

                            string row = sb.ToString();
                            Rows.Add(string.Concat("(", row, ")"));
                            sb.Clear();
                        }

                        sCommand.Append(string.Join(",", Rows));
                        sCommand.Append(";");

                        _logger.LogDebug("insert into almitasc_ACTradingBBGData.globalreit_pfdanalytics");
                        using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), connection))
                        {
                            myCmd.CommandType = CommandType.Text;
                            myCmd.ExecuteNonQuery();
                        }
                        trans.Commit();
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving REITs Pfd Analytics");
                throw;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="asofDate"></param>
        /// <returns></returns>
        public IList<SecurityPerformance> GetDailyPnLDetailsWithClassifications(string asofDate)
        {
            IList<SecurityPerformance> list = new List<SecurityPerformance>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    string sql = GetDailyPnLDetailsClassificationQuery + " where date = '" + asofDate + "'" + "order by p1.ticker, p1.portfolio";
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                SecurityPerformance data = new SecurityPerformance
                                {
                                    FundType = reader["portfolio"] as string,
                                    EffectiveDate = reader.IsDBNull(reader.GetOrdinal("date")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("date")),
                                    Ticker = reader["ticker"] as string,
                                    YellowKey = reader["ykey"] as string,
                                    Currency = reader["fx"] as string,
                                    Position = (reader.IsDBNull(reader.GetOrdinal("position"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("position")),
                                    ClosePrice = (reader.IsDBNull(reader.GetOrdinal("priceo"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("priceo")),
                                    CloseFXRate = (reader.IsDBNull(reader.GetOrdinal("fxrateo"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("fxrateo")),
                                    TickerId = reader["tkid"] as string,
                                    ProxyId = reader["proxyid"] as string,
                                    Sector = reader["sector"] as string,
                                    BroadSector = reader["broadsector"] as string,
                                    SecurityType = reader["sectype"] as string,
                                    RiskFactor = reader["riskfactor"] as string,
                                    LastPrice = (reader.IsDBNull(reader.GetOrdinal("lastprice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("lastprice")),
                                    LastFXRate = (reader.IsDBNull(reader.GetOrdinal("fxrate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("fxrate")),
                                    TRRLocal = (reader.IsDBNull(reader.GetOrdinal("trrlocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("trrlocal")),
                                    PnLUSD = (reader.IsDBNull(reader.GetOrdinal("pnlusd"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("pnlusd")),
                                    BegMV = (reader.IsDBNull(reader.GetOrdinal("tmvousd"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("tmvousd")),
                                    EndMV = (reader.IsDBNull(reader.GetOrdinal("tmvusd"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("tmvusd")),
                                    TradingPnL = (reader.IsDBNull(reader.GetOrdinal("tradepnl"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("tradepnl")),
                                    DatePeriod = reader["dateperiod"] as string,
                                    TradeType = reader["trade"] as string,
                                    Level1 = reader["Level1"] as string,
                                    Level2 = reader["Level2"] as string,
                                    Level3 = reader["Level3"] as string,
                                    ExchCode = reader["ExchCode"] as string,
                                    Swap = reader["IsSwap"] as string,
                                    CustomLevel1 = reader["CustomLevel1"] as string,
                                    CustomLevel2 = reader["CustomLevel2"] as string,
                                    CustomLevel3 = reader["CustomLevel3"] as string,
                                    CustomLevel4 = reader["CustomLevel4"] as string,
                                };

                                data.EffectiveDateAsString = DateUtils.ConvertDate(data.EffectiveDate, "yyyy-MM-dd");
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
        /// <param name="startDate"></param>
        /// <param name="endDate"></param>
        /// <param name="fundName"></param>
        /// <param name="ticker"></param>
        /// <returns></returns>
        public IList<SecurityPerformance> GetDailyPnLDetailsExt(string startDate, string endDate, string fundName, string ticker)
        {
            IList<SecurityPerformance> list = new List<SecurityPerformance>();
            int rowId = 100;

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    string sql = GetDailyPnLDetailsExtQuery;
                    sql += " where date >= '" + startDate + "' and date <= '" + endDate + "'";
                    if (!string.IsNullOrEmpty(fundName) && !"All".Equals(fundName))
                        sql += " and p1.portfolio = '" + fundName + "'";

                    if (!string.IsNullOrEmpty(ticker))
                        sql += " and p1.ticker like '%" + ticker + "%'";
                    //sql += " and p1.ticker = '" + ticker + "'";

                    sql += " order by p1.ticker, p1.portfolio, p1.date desc";

                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                SecurityPerformance data = new SecurityPerformance
                                {
                                    FundType = reader["portfolio"] as string,
                                    EffectiveDate = reader.IsDBNull(reader.GetOrdinal("date")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("date")),
                                    Ticker = reader["ticker"] as string,
                                    YellowKey = reader["ykey"] as string,
                                    Currency = reader["fx"] as string,
                                    Position = (reader.IsDBNull(reader.GetOrdinal("position"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("position")),
                                    ClosePrice = (reader.IsDBNull(reader.GetOrdinal("priceo"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("priceo")),
                                    CloseFXRate = (reader.IsDBNull(reader.GetOrdinal("fxrateo"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("fxrateo")),
                                    TickerId = reader["tkid"] as string,
                                    ProxyId = reader["proxyid"] as string,
                                    Sector = reader["sector"] as string,
                                    BroadSector = reader["broadsector"] as string,
                                    SecurityType = reader["sectype"] as string,
                                    RiskFactor = reader["riskfactor"] as string,
                                    LastPrice = (reader.IsDBNull(reader.GetOrdinal("lastprice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("lastprice")),
                                    LastFXRate = (reader.IsDBNull(reader.GetOrdinal("fxrate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("fxrate")),
                                    TRRLocal = (reader.IsDBNull(reader.GetOrdinal("trrlocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("trrlocal")),
                                    PnLUSD = (reader.IsDBNull(reader.GetOrdinal("pnlusd"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("pnlusd")),
                                    BegMV = (reader.IsDBNull(reader.GetOrdinal("tmvousd"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("tmvousd")),
                                    EndMV = (reader.IsDBNull(reader.GetOrdinal("tmvusd"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("tmvusd")),
                                    TradingPnL = (reader.IsDBNull(reader.GetOrdinal("tradepnl"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("tradepnl")),
                                    DatePeriod = reader["dateperiod"] as string,
                                    TradeType = reader["trade"] as string,
                                    Level1 = reader["Level1"] as string,
                                    Level2 = reader["Level2"] as string,
                                    Level3 = reader["Level3"] as string,
                                    ExchCode = reader["ExchCode"] as string,
                                    Swap = reader["IsSwap"] as string,
                                    CustomLevel1 = reader["CustomLevel1"] as string,
                                    CustomLevel2 = reader["CustomLevel2"] as string,
                                    CustomLevel3 = reader["CustomLevel3"] as string,
                                    CustomLevel4 = reader["CustomLevel4"] as string,
                                    PriceChng = (reader.IsDBNull(reader.GetOrdinal("PxChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PxChng")),
                                    FxChng = (reader.IsDBNull(reader.GetOrdinal("FxChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FxChng")),
                                    RowId = rowId++
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
        /// <param name="list"></param>
        public void SaveCurrencyExposures(IList<FundCurrExp> list)
        {
            try
            {
                StringBuilder sCommand = new StringBuilder("insert into almitasc_ACTradingBBGData.StgFundCurrencyExp " +
                    "(Ticker, Src, FundCurr, Curr, Exp, GroupName, RowId) values ");

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlTransaction trans = connection.BeginTransaction())
                    {
                        string sqlDelete = "delete from almitasc_ACTradingBBGData.StgFundCurrencyExp";
                        using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                        {
                            _logger.LogInformation("deleting data from almitasc_ACTradingBBGData.StgFundCurrencyExp");
                            command.ExecuteNonQuery();
                        }

                        List<string> Rows = new List<string>();
                        StringBuilder sb = new StringBuilder();
                        foreach (FundCurrExp data in list)
                        {
                            //Ticker
                            if (!string.IsNullOrEmpty(data.Ticker))
                                sb.Append(string.Concat("'", data.Ticker, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //Src
                            if (!string.IsNullOrEmpty(data.Src))
                                sb.Append(string.Concat("'", data.Src, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //FundCurr
                            if (!string.IsNullOrEmpty(data.FundCurr))
                                sb.Append(string.Concat("'", data.FundCurr, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //Curr
                            if (!string.IsNullOrEmpty(data.Curr))
                                sb.Append(string.Concat("'", data.Curr, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //Exp
                            if (data.Exp.HasValue)
                                sb.Append(data.Exp).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //GroupName
                            if (!string.IsNullOrEmpty(data.Grp))
                                sb.Append(string.Concat("'", data.Grp, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //RowId
                            if (data.RowId.HasValue)
                                sb.Append(data.RowId);
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

                        _logger.LogInformation("Moving data to Target Table...almitasc_ACTradingBBGData.FundCurrencyExp");
                        using (MySqlCommand command = new MySqlCommand(SaveCurrencyExposuresQuery, connection))
                        {
                            command.CommandType = System.Data.CommandType.StoredProcedure;
                            command.ExecuteNonQuery();
                        }
                        trans.Commit();
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving Currency Exposures");
                throw;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IDictionary<string, FundCurrExpTO> GetPortCurrencyExposureOverrides()
        {
            IDictionary<string, FundCurrExpTO> dict = new Dictionary<string, FundCurrExpTO>(StringComparer.CurrentCultureIgnoreCase);
            string ticker = string.Empty;
            string grpName = string.Empty;
            string prevGrpName = string.Empty;

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetCurrencyExposuresQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                //Add Group (if it does not exist)
                                grpName = reader["GroupName"] as string;
                                if (!dict.TryGetValue(grpName, out FundCurrExpTO grp))
                                {
                                    grp = new FundCurrExpTO();
                                    grp.Ticker = grpName;
                                    grp.Grp = grpName;
                                    grp.IsGrpRow = 1;
                                    dict.Add(grpName, grp);
                                }

                                //Add Ticker
                                ticker = reader["Ticker"] as string;
                                if (!dict.TryGetValue(ticker, out FundCurrExpTO data))
                                {
                                    data = new FundCurrExpTO();
                                    data.Ticker = ticker;
                                    data.Grp = grpName;
                                    data.IsGrpRow = 0;
                                    dict.Add(ticker, data);
                                }

                                if (data != null)
                                {
                                    data.Curr = reader["FundCurr"] as string;
                                    data.RowId = (reader.IsDBNull(reader.GetOrdinal("RowId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("RowId"));
                                    double? exp = (reader.IsDBNull(reader.GetOrdinal("Exp"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Exp"));

                                    string currency = reader["Curr"] as string;
                                    switch (currency)
                                    {
                                        case "USD":
                                            data.USDExp = exp;
                                            break;
                                        case "CAD":
                                            data.CADExp = exp;
                                            break;
                                        case "GBP":
                                            data.GBPExp = exp;
                                            break;
                                        case "EUR":
                                            data.EURExp = exp;
                                            break;
                                        case "JPY":
                                            data.JPYExp = exp;
                                            break;
                                        case "RON":
                                            data.RONExp = exp;
                                            break;
                                        case "ILS":
                                            data.ILSExp = exp;
                                            break;
                                        case "AUD":
                                            data.AUDExp = exp;
                                            break;
                                        case "INR":
                                            data.INRExp = exp;
                                            break;
                                        case "HKD":
                                            data.HKDExp = exp;
                                            break;
                                        case "CHF":
                                            data.CHFExp = exp;
                                            break;
                                        case "MXN":
                                            data.MXNExp = exp;
                                            break;
                                        case "SGD":
                                            data.SGDExp = exp;
                                            break;
                                        case "CNY":
                                            data.CNYExp = exp;
                                            break;
                                        case "BRL":
                                            data.BRLExp = exp;
                                            break;
                                        case "Others":
                                            data.OtherExp = exp;
                                            break;
                                        case "TOTAL":
                                            data.TotalExp = exp;
                                            break;
                                    }
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
        /// <param name="StartDate"></param>
        /// <param name="EndDate"></param>
        /// <param name="Ticker"></param>
        /// <returns></returns>
        public IList<TaylorCollisonTO> GetTaylorCollisonDetails(string StartDate, string EndDate, string Ticker)
        {
            IList<TaylorCollisonTO> fulllist = new List<TaylorCollisonTO>();
            try
            {
                string sql = GetTaylorCollisonQuery;
                sql += " where EffectiveDate between '" + StartDate + "' and '" + EndDate + "'";
                if (!string.IsNullOrEmpty(Ticker))
                    sql += " and Ticker= '" + Ticker + "'";
                sql += " Order By EffectiveDate, Ticker";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                TaylorCollisonTO listitem = new TaylorCollisonTO()
                                {
                                    Ticker = reader["Ticker"] as string,
                                    FileDate = reader.IsDBNull(reader.GetOrdinal("FileDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    EffectiveDate = reader.IsDBNull(reader.GetOrdinal("EffectiveDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate")),
                                    SecDesc = reader["SecDesc"] as string,
                                    MV = (reader.IsDBNull(reader.GetOrdinal("MV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MV")),
                                    MgrType = reader["MgrType"] as string,
                                    IMAExpiryDate = (reader.IsDBNull(reader.GetOrdinal("IMAExpiryDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("IMAExpiryDate")),
                                    MgrFee = reader["MgrFee"] as string,
                                    PerfFee = reader["PerfFee"] as string,
                                    BuyBack = reader["BuyBack"] as string,
                                    FrankingCredits = (reader.IsDBNull(reader.GetOrdinal("FrankingCredits"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FrankingCredits")),
                                    ProfitReserve = (reader.IsDBNull(reader.GetOrdinal("ProfitReserve"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ProfitReserve")),
                                    DTLNTA = reader["DTLNTA"] as string,
                                    Debt = reader["Debt"] as string,
                                    NTAPreTax = (reader.IsDBNull(reader.GetOrdinal("NTAPreTax"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NTAPreTax")),
                                    NTAPostTax = (reader.IsDBNull(reader.GetOrdinal("NTAPostTax"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NTAPostTax")),
                                    NTAAsOfDate = (reader.IsDBNull(reader.GetOrdinal("NTAAsOfDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("NTAAsOfDate")),
                                    NTAFreq = reader["NTAFreq"] as string,
                                    Price = (reader.IsDBNull(reader.GetOrdinal("Price"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price")),
                                    Yield = (reader.IsDBNull(reader.GetOrdinal("Yield"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Yield")),
                                    PreNTAIndex = (reader.IsDBNull(reader.GetOrdinal("PreNTAIndex"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PreNTAIndex")),
                                    EstVal = (reader.IsDBNull(reader.GetOrdinal("EstVal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EstVal")),
                                    PD = (reader.IsDBNull(reader.GetOrdinal("PD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PD")),

                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing Taylor Collison details. ");
                throw;
            }
            return fulllist;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="startDate"></param>
        /// <param name="endDate"></param>
        /// <param name="fundName"></param>
        /// <param name="ticker"></param>
        /// <returns></returns>
        public IList<JPMSecurityPerfTO> GetJPMPnL(DateTime startDate, DateTime endDate, string fundName, string ticker)
        {
            IList<JPMSecurityPerfTO> list = new List<JPMSecurityPerfTO>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetJPMPnLQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Account", fundName);
                        command.Parameters.AddWithValue("p_Ticker", ticker);
                        command.Parameters.AddWithValue("p_StartDate", startDate);
                        command.Parameters.AddWithValue("p_EndDate", endDate);

                        int rowId = 10;
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                JPMSecurityPerfTO data = new JPMSecurityPerfTO
                                {
                                    AsOfDate = (reader.IsDBNull(reader.GetOrdinal("AsOfDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    AcctName = reader["FundName"] as string,
                                    AssetClass = reader["AssetClass"] as string,
                                    Ticker = reader["Ticker"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    Cusip = reader["Cusip"] as string,
                                    SecId = reader["SecId"] as string,
                                    Curr = reader["Curr"] as string,
                                    SecDesc = reader["SecDesc"] as string,
                                    Qty = (reader.IsDBNull(reader.GetOrdinal("Qty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Qty")),
                                    PriceUSD = (reader.IsDBNull(reader.GetOrdinal("PriceUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceUSD")),
                                    MVUSD = (reader.IsDBNull(reader.GetOrdinal("MVUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MVUSD")),
                                    UnitCostUSD = (reader.IsDBNull(reader.GetOrdinal("UnitCostUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnitCostUSD")),
                                    TotalCostUSD = (reader.IsDBNull(reader.GetOrdinal("TotalCostUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotalCostUSD")),
                                    UnitCost = (reader.IsDBNull(reader.GetOrdinal("UnitCost"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnitCost")),
                                    TotalCost = (reader.IsDBNull(reader.GetOrdinal("TotalCost"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotalCost")),
                                    Price = (reader.IsDBNull(reader.GetOrdinal("Price"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price")),
                                    MV = (reader.IsDBNull(reader.GetOrdinal("MV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MV")),
                                    FxRate = (reader.IsDBNull(reader.GetOrdinal("FxRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FxRate")),
                                    UnrealizedPL = (reader.IsDBNull(reader.GetOrdinal("UnrealizedPL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnrealizedPL")),
                                    MTDTradingPL = (reader.IsDBNull(reader.GetOrdinal("MTDTradingPL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MTDTradingPL")),
                                    YTDTradingPL = (reader.IsDBNull(reader.GetOrdinal("YTDTradingPL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("YTDTradingPL")),
                                    PrevQty = (reader.IsDBNull(reader.GetOrdinal("PrevQty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PrevQty")),
                                    PrevPrice = (reader.IsDBNull(reader.GetOrdinal("PrevPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PrevPrice")),
                                    PrevFxRate = (reader.IsDBNull(reader.GetOrdinal("PrevFxRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PrevFxRate")),
                                    PrevPriceUSD = (reader.IsDBNull(reader.GetOrdinal("PrevPriceUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PrevPriceUSD")),
                                    PrevMVUSD = (reader.IsDBNull(reader.GetOrdinal("PrevMVUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PrevMVUSD")),
                                    PrevUnrealizedPL = (reader.IsDBNull(reader.GetOrdinal("PrevUnrealizedPL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PrevUnrealizedPL")),

                                    PxChng = (reader.IsDBNull(reader.GetOrdinal("PxChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PxChng")),
                                    FxChng = (reader.IsDBNull(reader.GetOrdinal("FxChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FxChng")),
                                    MVChng = (reader.IsDBNull(reader.GetOrdinal("MVChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MVChng")),
                                    PLChng = (reader.IsDBNull(reader.GetOrdinal("PLChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PLChng")),
                                };
                                rowId++;

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
        /// <param name="startDate"></param>
        /// <param name="endDate"></param>
        /// <param name="fundName"></param>
        /// <param name="ticker"></param>
        /// <returns></returns>
        public IList<PositionTypeDetailsTO> GetFidelityPnL(DateTime startDate, DateTime endDate, string fundName, string ticker)
        {
            IList<PositionTypeDetailsTO> list = new List<PositionTypeDetailsTO>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetFidelityPnLQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Account", fundName);
                        command.Parameters.AddWithValue("p_Ticker", ticker);
                        command.Parameters.AddWithValue("p_StartDate", startDate);
                        command.Parameters.AddWithValue("p_EndDate", endDate);

                        int rowId = 10;
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                PositionTypeDetailsTO data = new PositionTypeDetailsTO
                                {
                                    RptDate = (reader.IsDBNull(reader.GetOrdinal("RptDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("RptDate")),
                                    AcctName = reader["FundName"] as string,
                                    Symbol = reader["Symbol"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    Curr = reader["Curr"] as string,
                                    AcctType = reader["AcctType"] as string,
                                    SecType = reader["SecType"] as string,
                                    SecDesc = reader["SecDesc"] as string,
                                    FxRate = (reader.IsDBNull(reader.GetOrdinal("FxRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FxRate")),
                                    Qty = (reader.IsDBNull(reader.GetOrdinal("Qty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Qty")),
                                    UnitCost = (reader.IsDBNull(reader.GetOrdinal("UnitCost"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnitCost")),
                                    UnitCostLocal = (reader.IsDBNull(reader.GetOrdinal("UnitCostLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnitCostLocal")),
                                    ClsPrice = (reader.IsDBNull(reader.GetOrdinal("ClsPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ClsPrice")),
                                    ClsPriceLocal = (reader.IsDBNull(reader.GetOrdinal("ClsPriceLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ClsPriceLocal")),
                                    MV = (reader.IsDBNull(reader.GetOrdinal("MV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MV")),
                                    MVLocal = (reader.IsDBNull(reader.GetOrdinal("MVLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MVLocal")),
                                    Cost = (reader.IsDBNull(reader.GetOrdinal("Cost"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Cost")),
                                    CostLocal = (reader.IsDBNull(reader.GetOrdinal("CostLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CostLocal")),
                                    UnrealizedPL = (reader.IsDBNull(reader.GetOrdinal("UnrealizedPL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnrealizedPL")),
                                    UnrealizedPLLocal = (reader.IsDBNull(reader.GetOrdinal("UnrealizedPLLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnrealizedPLLocal")),
                                    AI = (reader.IsDBNull(reader.GetOrdinal("AI"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AI")),
                                    AILocal = (reader.IsDBNull(reader.GetOrdinal("AILocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AILocal")),
                                    PrevQty = (reader.IsDBNull(reader.GetOrdinal("PrevQty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PrevQty")),
                                    PrevClsPrice = (reader.IsDBNull(reader.GetOrdinal("PrevClsPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PrevClsPrice")),
                                    PrevFxRate = (reader.IsDBNull(reader.GetOrdinal("PrevFxRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PrevFxRate")),
                                    PrevUnitCost = (reader.IsDBNull(reader.GetOrdinal("PrevUnitCost"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PrevUnitCost")),
                                    PrevMV = (reader.IsDBNull(reader.GetOrdinal("PrevMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PrevMV")),
                                    PrevUnrealizedPL = (reader.IsDBNull(reader.GetOrdinal("PrevUnrealizedPL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PrevUnrealizedPL")),
                                    PrevCost = (reader.IsDBNull(reader.GetOrdinal("PrevCost"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PrevCost")),

                                    PxChng = (reader.IsDBNull(reader.GetOrdinal("PxChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PxChng")),
                                    FxChng = (reader.IsDBNull(reader.GetOrdinal("FxChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FxChng")),
                                    MVChng = (reader.IsDBNull(reader.GetOrdinal("MVChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MVChng")),
                                    PLChng = (reader.IsDBNull(reader.GetOrdinal("PLChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PLChng")),
                                };
                                rowId++;

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
        /// <param name="Ticker"></param>
        /// <param name="Curr"></param>
        /// <param name="CurrType"></param>
        /// <param name="Exp"></param>
        /// <returns></returns>
        public string GetExposuresRow(string Ticker, string Curr, string CurrType, double? Exp)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append(string.Concat("'", Ticker, "'")).Append(DELIMITER);
            if (!string.IsNullOrEmpty(Curr))
                sb.Append(string.Concat("'", Curr, "'")).Append(DELIMITER);
            else
                sb.Append("null").Append(DELIMITER);
            sb.Append(string.Concat("'", CurrType, "'")).Append(DELIMITER);
            sb.Append("'User'").Append(DELIMITER);
            sb.Append(Exp).Append(DELIMITER);
            sb.Append("''");//no groupname for now.
            return sb.ToString();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="list"></param>
        public void SavePortCurrencyExposureOverrides(IList<FundCurrExpTO> list)
        {
            try
            {
                StringBuilder sCommand = new StringBuilder("insert into almitasc_ACTradingBBGData.StgFundCurrencyExp"
                    + " (Ticker, FundCurr, Curr, Src, Exp, GroupName) values ");

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    string sqlDelete = "delete from almitasc_ACTradingBBGData.StgFundCurrencyExp";
                    using (MySqlCommand command = new MySqlCommand(sqlDelete, connection))
                        command.ExecuteNonQuery();

                    IList<string> Rows = new List<string>();
                    StringBuilder sb = new StringBuilder();
                    foreach (FundCurrExpTO item in list)
                    {
                        if (!string.IsNullOrEmpty(item.Ticker))
                        {
                            if (item.USDExp.HasValue)
                            {
                                string row = GetExposuresRow(item.Ticker, item.Curr, "USD", item.USDExp);
                                Rows.Add(string.Concat("(", row, ")"));
                            }
                            if (item.CADExp.HasValue)
                            {
                                string row = GetExposuresRow(item.Ticker, item.Curr, "CAD", item.CADExp);
                                Rows.Add(string.Concat("(", row, ")"));
                            }
                            if (item.GBPExp.HasValue)
                            {
                                string row = GetExposuresRow(item.Ticker, item.Curr, "GBP", item.GBPExp);
                                Rows.Add(string.Concat("(", row, ")"));
                            }
                            if (item.EURExp.HasValue)
                            {
                                string row = GetExposuresRow(item.Ticker, item.Curr, "EUR", item.EURExp);
                                Rows.Add(string.Concat("(", row, ")"));
                            }
                            if (item.JPYExp.HasValue)
                            {
                                string row = GetExposuresRow(item.Ticker, item.Curr, "JPY", item.JPYExp);
                                Rows.Add(string.Concat("(", row, ")"));
                            }
                            if (item.RONExp.HasValue)
                            {
                                string row = GetExposuresRow(item.Ticker, item.Curr, "RON", item.RONExp);
                                Rows.Add(string.Concat("(", row, ")"));
                            }
                            if (item.ILSExp.HasValue)
                            {
                                string row = GetExposuresRow(item.Ticker, item.Curr, "ILS", item.ILSExp);
                                Rows.Add(string.Concat("(", row, ")"));
                            }
                            if (item.AUDExp.HasValue)
                            {
                                string row = GetExposuresRow(item.Ticker, item.Curr, "AUD", item.AUDExp);
                                Rows.Add(string.Concat("(", row, ")"));
                            }
                            if (item.INRExp.HasValue)
                            {
                                string row = GetExposuresRow(item.Ticker, item.Curr, "INR", item.INRExp);
                                Rows.Add(string.Concat("(", row, ")"));
                            }
                            if (item.HKDExp.HasValue)
                            {
                                string row = GetExposuresRow(item.Ticker, item.Curr, "HKD", item.HKDExp);
                                Rows.Add(string.Concat("(", row, ")"));
                            }
                            if (item.CHFExp.HasValue)
                            {
                                string row = GetExposuresRow(item.Ticker, item.Curr, "CHF", item.CHFExp);
                                Rows.Add(string.Concat("(", row, ")"));
                            }
                            if (item.MXNExp.HasValue)
                            {
                                string row = GetExposuresRow(item.Ticker, item.Curr, "MXN", item.MXNExp);
                                Rows.Add(string.Concat("(", row, ")"));
                            }
                            if (item.SGDExp.HasValue)
                            {
                                string row = GetExposuresRow(item.Ticker, item.Curr, "SGD", item.SGDExp);
                                Rows.Add(string.Concat("(", row, ")"));
                            }
                            if (item.CNYExp.HasValue)
                            {
                                string row = GetExposuresRow(item.Ticker, item.Curr, "CNY", item.CNYExp);
                                Rows.Add(string.Concat("(", row, ")"));
                            }
                            if (item.BRLExp.HasValue)
                            {
                                string row = GetExposuresRow(item.Ticker, item.Curr, "BRL", item.BRLExp);
                                Rows.Add(string.Concat("(", row, ")"));
                            }
                            if (item.OtherExp.HasValue)
                            {
                                string row = GetExposuresRow(item.Ticker, item.Curr, "Other", item.OtherExp);
                                Rows.Add(string.Concat("(", row, ")"));
                            }
                        }
                    }

                    sCommand.Append(string.Join(",", Rows));
                    sCommand.Append(";");
                    _logger.LogDebug("insert into  almitasc_ACTradingBBGData.StgFundCurrencyExp");
                    using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), connection))
                    {
                        myCmd.CommandType = CommandType.Text;
                        myCmd.ExecuteNonQuery();
                    }

                    _logger.LogInformation("Moving data to almitasc_ACTradingBBGData.FundCurrencyExp");
                    string sql = "almitasc_ACTradingBBGData.spPopulateCurrencyExposures";
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving Currency Exposures into database");
                throw;
            }
        }

        public IList<FundSupplementalDataTO> GetFundSupplementalData(string Ticker)
        {
            IList<FundSupplementalDataTO> list = new List<FundSupplementalDataTO>();

            try
            {
                string sql = GetFundSupplementalDataQuery;
                if (!string.IsNullOrEmpty(Ticker))
                    sql += " where Ticker = '" + Ticker + "'";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundSupplementalDataTO data = new FundSupplementalDataTO
                                {
                                    Ticker = reader["Ticker"] as string,
                                    BoardVotingPeriod = reader["BoardVotingPeriod"] as string,
                                    BoardVotingType = reader["BoardVotingType"] as string,
                                    UNII = reader["UNII"] as string,
                                    DividendCoverage = reader["DividendCoverage"] as string,
                                    LiberumNotes = reader["LiberumNotes"] as string,
                                    FundRedemptionDate = (reader.IsDBNull(reader.GetOrdinal("FundRedemptionDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FundRedemptionDate")),
                                    FundManager = reader["FundManager"] as string,
                                    ManagedInternally = reader["ManagedInternally"] as string,
                                    BaseMgmtFee = reader["BaseMgmtFee"] as string,
                                    IncludesCash = reader["IncludesCash"] as string,
                                    ExcludesCash = reader["ExcludesCash"] as string,
                                    PerfFeeExclIncome = reader["PerfFeeExclIncome"] as string,
                                    CapitalGainsPerfFee = reader["CapitalGainsPerfFee"] as string,
                                    HurdleRate = reader["HurdleRate"] as string,
                                    TotalReturnHurdle = reader["TotalReturnHurdle"] as string,
                                    CatchUp = reader["CatchUp"] as string,
                                    CatchUpProvision = reader["CatchUpProvision"] as string,
                                    CatchUpLowerBound = reader["CatchUpLowerBound"] as string,
                                    CatchUpUpperBound = reader["CatchUpUpperBound"] as string,
                                    MutualFundStructureType = reader["MutualFundStructureType"] as string,
                                    StateOfIncorporation = reader["StateOfIncorporation"] as string,
                                    MCSAA = reader["MCSAA"] as string,
                                    StandstillParty = reader["StandstillParty"] as string,
                                    StandstillDuration = reader["StandstillDuration"] as string,
                                    StandstillStartDate = (reader.IsDBNull(reader.GetOrdinal("StandstillStartDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("StandstillStartDate")),
                                    StandstillEndDate = reader["StandstillEndDate"] as string,
                                    NavUpdateFreq = reader["NavUpdateFreq"] as string,
                                    NavUpdateFreqType = reader["NavUpdateFreqType"] as string,
                                    NavUpdateLag = (reader.IsDBNull(reader.GetOrdinal("NavUpdateLag"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("NavUpdateLag")),
                                    Underwriter = reader["Underwriter"] as string,
                                    PreviousTicker = reader["PreviousTicker"] as string,
                                    PrimaryInvestmentType = reader["PrimaryInvestmentType"] as string,
                                    MgmtFeeOtherProvisions = reader["MgmtFeeOtherProvisions"] as string,
                                    NetInvestmentIncomeRate = reader["NetInvestmentIncomeRate"] as string,
                                    NetCapitalGainsRate = reader["NetCapitalGainsRate"] as string,
                                    Notes = reader["Notes"] as string,
                                    FeeWaiver = reader["FeeWaiver"] as string,
                                    TieredBaseMgmtFee = reader["TieredBaseMgmtFee"] as string,
                                    TieredFeeLeverageLevel = reader["TieredFeeLeverageLevel"] as string,
                                    NavEstErrorCode = (reader.IsDBNull(reader.GetOrdinal("NavEstErrorCode"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("NavEstErrorCode")),
                                    NavEstErrorAvg = reader["NavEstErrorAvg"] as string,
                                    NavEstErrorAvgVol = reader["NavEstErrorAvgVol"] as string,
                                    CorpStructure = reader["CorpStructure"] as string,
                                    ControlShares = reader["ControlShares"] as string,
                                    ContestedElectionOverride = reader["ContestedElectionOverride"] as string,
                                    VotingPrefShares = reader["VotingPrefShares"] as string,
                                    BoardType = reader["BoardType"] as string,
                                    CreateDate = (reader.IsDBNull(reader.GetOrdinal("CreateDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("CreateDate")),
                                };
                                list.Add(data);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing Fund Supplemental Data query");
                throw;
            }
            return list;
        }

        public IList<TenderOfferHistoryTO> GetTenderOfferHistory(string ticker)
        {
            IList<TenderOfferHistoryTO> list = new List<TenderOfferHistoryTO>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetTenderOfferHistoryQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Ticker", ticker == "" ? null : ticker);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                TenderOfferHistoryTO data = new TenderOfferHistoryTO
                                {
                                    Ticker = reader["Ticker"] as string,
                                    FundName = reader["FundName"] as string,
                                    FundSponsor = reader["FundSponsor"] as string,
                                    TenderFrom = reader["TenderFrom"] as string,
                                    TenderCommencedOn = (reader.IsDBNull(reader.GetOrdinal("TenderCommencedOn"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("TenderCommencedOn")),
                                    TenderExpirationDate = (reader.IsDBNull(reader.GetOrdinal("TenderExpirationDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("TenderExpirationDate")),
                                    TenderOfferPrice = reader["TenderOfferPrice"] as string,
                                    TenderIntendtoBuyPct = (reader.IsDBNull(reader.GetOrdinal("TenderIntendtobuyuptoPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TenderIntendtobuyuptoPct")),
                                    TenderedPct = (reader.IsDBNull(reader.GetOrdinal("TenderedPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TenderedPct")),
                                    TenderedPurchasedPct = (reader.IsDBNull(reader.GetOrdinal("TenderPurchasedPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TenderPurchasedPct")),
                                    TenderPurchasePrice = (reader.IsDBNull(reader.GetOrdinal("TenderPurchasePrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TenderPurchasePrice")),
                                };
                                list.Add(data);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing Tender Offer history query");
                throw;
            }
            return list;
        }

        public IList<FundNavEstErrorStatsTO> GetFundNavEstErrStatsDetails(string StartDate, string EndDate, string Country)
        {
            IList<FundNavEstErrorStatsTO> fulllist = new List<FundNavEstErrorStatsTO>();
            try
            {
                string sql = GetFundNavEstErrorsDetailsQuery;
                if (Country != "All")
                    sql += " where sm.Country = '" + Country + "'" + " and err.AsOfDate between '" + StartDate + "' and '" + EndDate + "' order by Ticker";
                else
                    sql += " where err.AsOfDate between '" + StartDate + "' and '" + EndDate + "' order by Ticker";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundNavEstErrorStatsTO listitem = new FundNavEstErrorStatsTO
                                {
                                    Ticker = reader["Ticker"] as string,
                                    AsOfDate = (reader.IsDBNull(reader.GetOrdinal("AsOfDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    MinEffDate = (reader.IsDBNull(reader.GetOrdinal("MinEffDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("MinEffDate")),
                                    MaxEffDate = (reader.IsDBNull(reader.GetOrdinal("MaxEffDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("MaxEffDate")),
                                    MeasureType = reader["MeasureType"] as string,
                                    Stat1W = (reader.IsDBNull(reader.GetOrdinal("Stat1W"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Stat1W")),
                                    Stat2W = (reader.IsDBNull(reader.GetOrdinal("Stat2W"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Stat2W")),
                                    Stat1M = (reader.IsDBNull(reader.GetOrdinal("Stat1M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Stat1M")),
                                    Stat3M = (reader.IsDBNull(reader.GetOrdinal("Stat3M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Stat3M")),
                                    Stat6M = (reader.IsDBNull(reader.GetOrdinal("Stat6M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Stat6M")),
                                    Stat1Y = (reader.IsDBNull(reader.GetOrdinal("Stat1Y"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Stat1Y")),
                                    Stat2Y = (reader.IsDBNull(reader.GetOrdinal("Stat2Y"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Stat2Y")),
                                    CreateDate = (reader.IsDBNull(reader.GetOrdinal("CreateDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("CreateDate")),

                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing Fund Nav Est Errors Stats details. ");
                throw;
            }
            return fulllist;
        }

        public IList<FundNavEstErrorStatsTO> GetFundNavEstErrByTicker(string Ticker)
        {
            IList<FundNavEstErrorStatsTO> fulllist = new List<FundNavEstErrorStatsTO>();
            try
            {
                string sql = GetFundNavEstByTickerQuery;
                sql += " where Ticker = '" + Ticker + "' order by AsOfDate desc";
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundNavEstErrorStatsTO listitem = new FundNavEstErrorStatsTO
                                {
                                    Ticker = reader["Ticker"] as string,
                                    AsOfDate = (reader.IsDBNull(reader.GetOrdinal("AsOfDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    MinEffDate = (reader.IsDBNull(reader.GetOrdinal("MinEffDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("MinEffDate")),
                                    MaxEffDate = (reader.IsDBNull(reader.GetOrdinal("MaxEffDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("MaxEffDate")),
                                    MeasureType = reader["MeasureType"] as string,
                                    Stat1W = (reader.IsDBNull(reader.GetOrdinal("Stat1W"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Stat1W")),
                                    Stat2W = (reader.IsDBNull(reader.GetOrdinal("Stat2W"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Stat2W")),
                                    Stat1M = (reader.IsDBNull(reader.GetOrdinal("Stat1M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Stat1M")),
                                    Stat3M = (reader.IsDBNull(reader.GetOrdinal("Stat3M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Stat3M")),
                                    Stat6M = (reader.IsDBNull(reader.GetOrdinal("Stat6M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Stat6M")),
                                    Stat1Y = (reader.IsDBNull(reader.GetOrdinal("Stat1Y"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Stat1Y")),
                                    Stat2Y = (reader.IsDBNull(reader.GetOrdinal("Stat2Y"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Stat2Y")),
                                    CreateDate = (reader.IsDBNull(reader.GetOrdinal("CreateDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("CreateDate")),

                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing Fund Nav Est Error by Ticker details.");
                throw;
            }
            return fulllist;
        }
        private const string GetFundPerformanceReturnsQuery = "call almitasc_ACTradingBBGData.spGetFundPerformanceReturns";
        private const string GetFundPerformanceRiskStatsQuery = "call almitasc_ACTradingBBGData.spGetFundPerformanceRisk";
        private const string GetFundPerformanceRanksQuery = "call almitasc_ACTradingBBGData.spGetFundPerformanceRanksAll";
        private const string GetDailyPnLSummaryQuery = "spGetDailyPnL";
        private const string GetDailyPnLDetailsQuery = "select * from almitasc_ACTradingPM.portfoliomanager_dailypnl";
        private const string GetDailyPnLDetailsNewQuery = "select p.*, ifnull(s.level1, 'Other') as Level1, s.level2 as Level2, s.level3 as Level3"
            + " from almitasc_ACTradingPM.portfoliomanager_dailypnl p"
            + " left join almitasc_ACTradingBBGData.SecurityMstExt s on (p.ticker = s.ticker)";

        private const string GetDailyPnLDetailsClassificationQuery = "select p1.*, ifnull(s.level1, 'Other') as Level1, s.level2 as Level2, s.level3 as Level3, s.ExchCode,"
            + " ifnull(s.CustomLevel1, 'Other') as CustomLevel1,"
            + " ifnull(s.CustomLevel2, 'Other') as CustomLevel2,"
            + " ifnull(s.CustomLevel3, 'Other') as CustomLevel3,"
            + " s.CustomLevel4"
            + " from (select trim(replace(p.ticker, 'SwapUnderlying#', '')) as PortTicker"
            + " ,(case when p.ticker like 'SwapUnderlying#%' then 'Y' else 'N' end) as IsSwap, p.*"
            + " from almitasc_ACTradingPM.portfoliomanager_dailypnl p) p1"
            + " left join almitasc_ACTradingBBGData.SecurityMstExt s on (p1.PortTicker = s.ticker)";

        private const string GetDailyPnLDetailsExtQuery = "select p1.*, ifnull(s.level1, 'Other') as Level1, s.level2 as Level2, s.level3 as Level3, s.ExchCode,"
            + " ifnull(s.CustomLevel1, 'Other') as CustomLevel1,"
            + " ifnull(s.CustomLevel2, 'Other') as CustomLevel2,"
            + " ifnull(s.CustomLevel3, 'Other') as CustomLevel3,"
            + " s.CustomLevel4,"
            + " (p1.lastprice/p1.priceo)-1 as PxChng,"
            + " (p1.fxrate/p1.fxrateo)-1 as FxChng"
            + " from (select trim(replace(p.ticker, 'SwapUnderlying#', '')) as PortTicker"
            + " ,(case when p.ticker like 'SwapUnderlying#%' then 'Y' else 'N' end) as IsSwap, p.*"
            + " from almitasc_ACTradingPM.portfoliomanager_dailypnl p) p1"
            + " left join almitasc_ACTradingBBGData.SecurityMstExt s on (p1.PortTicker = s.ticker)";

        private const string GetFundNotesQuery = "select * from almitasc_ACTradingBBGData.FundNotes";
        private const string SaveREITDailyAnalyticsQuery = "spSaveREITAnalytics";

        private const string SaveCurrencyExposuresQuery = "spPopulateCurrencyExposures";
        private const string GetCurrencyExposuresQuery = "select * from almitasc_ACTradingBBGData.FundCurrencyExp order by RowId";
        private const string GetTaylorCollisonQuery = "select * from almitasc_ACTradingBBGLink.CEFTaylorCollison";
        private const string GetJPMPnLQuery = "Primebrokerfiles.spGetJPMPnL";
        private const string GetFidelityPnLQuery = "Primebrokerfiles.spGetFidelityPnL";
        private const string GetFundSupplementalDataQuery = "select * from almitasc_ACTradingBBGData.FundSupplementalData";
        private const string GetTenderOfferHistoryQuery = "Reporting.spGetTenderOfferHistory";
        private const string GetFundNavEstByTickerQuery = "select * from almitasc_ACTradingBBGData.FundNavEstErrStats";
        private const string GetFundNavEstErrorsDetailsQuery = "select err.* from almitasc_ACTradingBBGData.FundNavEstErrStats err inner join almitasc_ACTradingBBGLink.globaltrading_securitymaster sm on err. ticker = sm.ticker";
    }
}