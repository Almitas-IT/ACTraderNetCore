using aACTrader.DAO.Interface;
using aCommons.SPACs;
using aCommons.Utils;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;

namespace aACTrader.DAO.Repository
{
    public class SPACDao : ISPACDao
    {
        private readonly ILogger<SPACDao> _logger;

        public SPACDao(ILogger<SPACDao> logger)
        {
            _logger = logger;
            _logger.LogInformation("Initializing SPACDao...");
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IList<SPACPosition> GetSPACPositions()
        {
            IList<SPACPosition> list = new List<SPACPosition>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetSPACPositionsQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                SPACPosition data = new SPACPosition
                                {
                                    BrokerTicker = reader["BrokerTicker"] as string,
                                    Ticker = reader["Ticker"] as string,
                                    Description = reader["SecurityDesc"] as string,
                                    TotalShares = (reader.IsDBNull(reader.GetOrdinal("TotalShares"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("TotalShares")),
                                    OppShares = (reader.IsDBNull(reader.GetOrdinal("OPPShares"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("OPPShares")),
                                    TacShares = (reader.IsDBNull(reader.GetOrdinal("TACShares"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("TACShares")),
                                    DayBought = reader.IsDBNull(reader.GetOrdinal("DayBought")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("DayBought")),
                                    SharesSubscribed = (reader.IsDBNull(reader.GetOrdinal("SharesSubscribed"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("SharesSubscribed")),
                                    SharesSold = (reader.IsDBNull(reader.GetOrdinal("SharesSold"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("SharesSold")),
                                    AvgSubscriptionPrice = (reader.IsDBNull(reader.GetOrdinal("AvgPriceSubscribed"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AvgPriceSubscribed")),
                                    AvgSellPrice = (reader.IsDBNull(reader.GetOrdinal("AvgPriceSold"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AvgPriceSold")),
                                    RealizedGrossProfit = (reader.IsDBNull(reader.GetOrdinal("RealizedGrossProfit"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RealizedGrossProfit")),
                                    RealizedNetProfit = (reader.IsDBNull(reader.GetOrdinal("RealizedNetProfit"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RealizedNetProfit")),
                                };

                                data.DayBoughtAsString = DateUtils.ConvertDate(data.DayBought, "yyyy-MM-dd");
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
        public IList<SPACSecurity> GetSPACSecurities()
        {
            IList<SPACSecurity> list = new List<SPACSecurity>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetSPACSecuritiesQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                SPACSecurity security = new SPACSecurity();
                                security.Ticker = reader["Ticker"] as string;
                                security.BrokerTicker = reader["BrokerTicker"] as string;
                                security.Sedol = reader["Sedol"] as string;
                                security.SecurityType = reader["SecurityType"] as string;
                                security.Description = reader["SecurityDesc"] as string;
                                security.Underwriter = reader["Underwriter"] as string;
                                security.Notes = reader["Notes"] as string;
                                security.FundCategory = reader["FundCategory"] as string;

                                int? sharesSubscribed = (reader.IsDBNull(reader.GetOrdinal("SharesSubscribed"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("SharesSubscribed"));
                                if (sharesSubscribed > 0)
                                {
                                    SPACPosition position = new SPACPosition
                                    {
                                        BrokerTicker = reader["BrokerTicker"] as string,
                                        TotalShares = (reader.IsDBNull(reader.GetOrdinal("TotalShares"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("TotalShares")),
                                        OppShares = (reader.IsDBNull(reader.GetOrdinal("OPPShares"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("OPPShares")),
                                        TacShares = (reader.IsDBNull(reader.GetOrdinal("TACShares"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("TACShares")),
                                        DayBought = reader.IsDBNull(reader.GetOrdinal("DayBought")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("DayBought")),
                                        SharesSubscribed = (reader.IsDBNull(reader.GetOrdinal("SharesSubscribed"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("SharesSubscribed")),
                                        SharesSold = (reader.IsDBNull(reader.GetOrdinal("SharesSold"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("SharesSold")),
                                        AvgSubscriptionPrice = (reader.IsDBNull(reader.GetOrdinal("AvgPriceSubscribed"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AvgPriceSubscribed")),
                                        AvgSellPrice = (reader.IsDBNull(reader.GetOrdinal("AvgPriceSold"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AvgPriceSold")),
                                        RealizedGrossProfit = (reader.IsDBNull(reader.GetOrdinal("RealizedGrossProfit"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RealizedGrossProfit")),
                                        RealizedNetProfit = (reader.IsDBNull(reader.GetOrdinal("RealizedNetProfit"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RealizedNetProfit")),
                                    };

                                    position.DayBoughtAsString = DateUtils.ConvertDate(position.DayBought, "yyyy-MM-dd");
                                    security.Position = position;
                                }

                                list.Add(security);
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

        private const string GetSPACPositionsQuery = "call almitasc_ACTradingBBGData.spGetSpacPositions";
        private const string GetSPACSecuritiesQuery = "call almitasc_ACTradingBBGData.spGetSPACSecurities";
    }
}
