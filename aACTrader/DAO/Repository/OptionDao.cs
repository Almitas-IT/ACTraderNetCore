using aACTrader.DAO.Interface;
using aCommons.Derivatives;
using aCommons.Utils;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;

namespace aACTrader.DAO.Repository
{
    public class OptionDao : IOptionDao
    {
        private readonly ILogger<OptionDao> _logger;

        public OptionDao(ILogger<OptionDao> logger)
        {
            _logger = logger;
            _logger.LogInformation("Initializing OptionDao...");
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ticker"></param>
        /// <param name="fundCategory"></param>
        /// <returns></returns>
        public IList<OptionChain> GetOptionChain(string ticker, string fundCategory)
        {
            IList<OptionChain> list = new List<OptionChain>();

            try
            {
                DateTime todaysDate = DateTime.Now;
                string todaysDateAsString = DateUtils.ConvertDate(todaysDate, "yyyy-MM-dd");

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetOptionChainQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Ticker", ticker);
                        command.Parameters.AddWithValue("p_FundCategory", fundCategory);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                OptionChain data = new OptionChain
                                {
                                    Ticker = reader["Ticker"] as string,
                                    CallTicker = reader["CallTicker"] as string,
                                    PutTicker = reader["PutTicker"] as string,

                                    ExpirationDate = reader.IsDBNull(reader.GetOrdinal("ExpirationDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("ExpirationDate")),
                                    DaysToExpiration = (reader.IsDBNull(reader.GetOrdinal("DaysToExpiration"))) ? (Int16?)null : reader.GetInt16(reader.GetOrdinal("DaysToExpiration")),
                                    StrikePrice = (reader.IsDBNull(reader.GetOrdinal("StrikePrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("StrikePrice")),

                                    //Call
                                    CallBidPrice = (reader.IsDBNull(reader.GetOrdinal("CallBidPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CallBidPrice")),
                                    CallAskPrice = (reader.IsDBNull(reader.GetOrdinal("CallAskPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CallAskPrice")),
                                    CallMidPrice = (reader.IsDBNull(reader.GetOrdinal("CallMidPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CallMidPrice")),
                                    CallOpenInterest = (reader.IsDBNull(reader.GetOrdinal("CallOpenInterest"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("CallOpenInterest")),
                                    CallTotalOpenInterest = (reader.IsDBNull(reader.GetOrdinal("CallTotalOpenInterest"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("CallTotalOpenInterest")),

                                    //Put
                                    PutBidPrice = (reader.IsDBNull(reader.GetOrdinal("PutBidPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PutBidPrice")),
                                    PutAskPrice = (reader.IsDBNull(reader.GetOrdinal("PutAskPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PutAskPrice")),
                                    PutMidPrice = (reader.IsDBNull(reader.GetOrdinal("PutMidPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PutMidPrice")),
                                    PutOpenInterest = (reader.IsDBNull(reader.GetOrdinal("PutOpenInterest"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("PutOpenInterest")),
                                    PutTotalOpenInterest = (reader.IsDBNull(reader.GetOrdinal("PutTotalOpenInterest"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("PutTotalOpenInterest")),

                                    CallBidAskSpread = (reader.IsDBNull(reader.GetOrdinal("CallBidAskSpread"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CallBidAskSpread")),
                                    PutBidAskSpread = (reader.IsDBNull(reader.GetOrdinal("PutBidAskSpread"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PutBidAskSpread")),
                                };

                                data.TodaysDateAsString = todaysDateAsString;
                                data.ExpirationDateAsString = DateUtils.ConvertDate(data.ExpirationDate, "yyyy-MM-dd");

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
        /// Gets Option details (Option Type, Strike Price, Expiry Date etc.)
        /// </summary>
        /// <returns></returns>
        public IDictionary<string, OptionDetail> GetOptionDetails()
        {
            IDictionary<string, OptionDetail> dict = new Dictionary<string, OptionDetail>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetOptionDetailsQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                OptionDetail data = new OptionDetail();
                                data.Ticker = reader["Ticker"] as string;
                                data.OptionType = reader["OptionType"] as string;
                                data.UnderlyingTicker = reader["UnderlyingTicker"] as string;
                                data.ExpirationDate = (reader.IsDBNull(reader.GetOrdinal("ExpirationDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("ExpirationDate"));
                                data.StrikePrice = (reader.IsDBNull(reader.GetOrdinal("Strike"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Strike"));
                                data.Multiplier = (reader.IsDBNull(reader.GetOrdinal("Multiplier"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Multiplier"));

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

        private const string GetOptionChainQuery = "spGetOptionChain";
        private const string GetOptionDetailsQuery = "select * from almitasc_ACTradingPM.portfoliomanager_securitymastersupp_options";
    }
}
