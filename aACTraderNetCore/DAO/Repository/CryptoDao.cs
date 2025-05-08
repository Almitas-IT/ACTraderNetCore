using aACTrader.DAO.Interface;
using aCommons.Crypto;
using aCommons.Utils;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace aACTrader.DAO.Repository
{
    public class CryptoDao : ICryptoDao
    {
        private readonly ILogger<CryptoDao> _logger;
        private const string DELIMITER = ",";

        public CryptoDao(ILogger<CryptoDao> logger)
        {
            _logger = logger;
            _logger.LogInformation("Initializing CryptoDao...");
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IDictionary<string, CryptoSecMst> GetCryptoSecurityDetails()
        {
            IDictionary<string, CryptoSecMst> dict = new Dictionary<string, CryptoSecMst>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetCryptoTickersQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                CryptoSecMst data = new CryptoSecMst
                                {
                                    Ticker = reader["Ticker"] as string,
                                    Curr = reader["Currency"] as string,
                                    RefCryptoCoinId = reader["RefCryptoCoinId"] as string,

                                    MgmtFee = (reader.IsDBNull(reader.GetOrdinal("ManagementFee"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ManagementFee")),
                                    NumCoinsPerUnit = (reader.IsDBNull(reader.GetOrdinal("NumCoinsPerUnit"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NumCoinsPerUnit")),
                                    AsOfDate = reader.IsDBNull(reader.GetOrdinal("AsOfDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate"))
                                };

                                data.AsOfDateAsString = DateUtils.ConvertDate(data.AsOfDate, "yyyy-MM-dd");

                                if (!dict.TryGetValue(data.Ticker, out CryptoSecMst cryptoSecMstTemp))
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
        public void SaveCryptoSecurityDetails(IList<CryptoSecMst> list)
        {
            try
            {
                StringBuilder sCommand = new StringBuilder("insert into almitasc_ACTradingBBGData.StgCryptoSecMst " +
                    "(Action, FIGI, Ticker, ManagementFee, Currency, NumCoinsPerUnit, AsOfDate) values ");

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlTransaction trans = connection.BeginTransaction())
                    {
                        _logger.LogInformation("Deleting data from almitasc_ACTradingBBGData.StgCryptoSecMst");
                        string sqlDelete = "delete from almitasc_ACTradingBBGData.StgCryptoSecMst";
                        using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                        {
                            command.ExecuteNonQuery();
                        }

                        List<string> Rows = new List<string>();
                        StringBuilder sb = new StringBuilder();
                        foreach (CryptoSecMst data in list)
                        {
                            //Action
                            if (!string.IsNullOrEmpty(data.Action))
                                sb.Append(string.Concat("'", data.Action, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //FIGI
                            if (!string.IsNullOrEmpty(data.FIGI))
                                sb.Append(string.Concat("'", data.FIGI, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //Ticker
                            if (!string.IsNullOrEmpty(data.Ticker))
                                sb.Append(string.Concat("'", data.Ticker, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //ManagementFee
                            if (data.MgmtFee.HasValue)
                                sb.Append(data.MgmtFee).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //Currency
                            if (!string.IsNullOrEmpty(data.Curr))
                                sb.Append(string.Concat("'", data.Curr, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //NumCoinsPerUnit
                            if (data.NumCoinsPerUnit.HasValue)
                                sb.Append(data.NumCoinsPerUnit).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //AsOfDate
                            if (!string.IsNullOrEmpty(data.AsOfDateAsString))
                                sb.Append(string.Concat("'", data.AsOfDateAsString, "'"));
                            else
                                sb.Append("null");

                            ////Website
                            //if (!string.IsNullOrEmpty(data.Website))
                            //    sb.Append(string.Concat("'", data.Currency, "'"));
                            //else
                            //    sb.Append("null");

                            string row = sb.ToString();
                            Rows.Add(string.Concat("(", row, ")"));
                            sb.Clear();
                        }

                        sCommand.Append(string.Join(",", Rows));
                        sCommand.Append(";");

                        using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), connection, trans))
                        {
                            myCmd.CommandType = CommandType.Text;
                            myCmd.ExecuteNonQuery();
                        }

                        _logger.LogInformation("Moving data to almitasc_ACTradingBBGData.CryptoSecMst");
                        string sql = "spPopulateCryptoSecMst";
                        using (MySqlCommand command = new MySqlCommand(sql, connection))
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
                _logger.LogError(ex, "Error saving logs into database");
                throw;
            }
        }

        public IList<CryptoSecMst> GetCryptoSecurityList()
        {
            IList<CryptoSecMst> list = new List<CryptoSecMst>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetCryptoTickersQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                CryptoSecMst data = new CryptoSecMst
                                {
                                    Ticker = reader["Ticker"] as string,
                                    FIGI = reader["FIGI"] as string,
                                    Curr = reader["Currency"] as string,
                                    RefCryptoCoinId = reader["RefCryptoCoinId"] as string,
                                    Website = reader["Website"] as string,

                                    MgmtFee = (reader.IsDBNull(reader.GetOrdinal("ManagementFee"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ManagementFee")),
                                    NumCoinsPerUnit = (reader.IsDBNull(reader.GetOrdinal("NumCoinsPerUnit"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NumCoinsPerUnit")),
                                    AsOfDate = reader.IsDBNull(reader.GetOrdinal("AsOfDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate"))
                                };

                                data.AsOfDateAsString = DateUtils.ConvertDate(data.AsOfDate, "yyyy-MM-dd");
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

        private const string GetCryptoTickersQuery = "select FIGI, Ticker, ManagementFee, Currency, NumCoinsPerUnit, (case when AsOfDate = '0000-00-00' then null else AsOfDate end) as AsOfDate, Website, RefCryptoCoinId from almitasc_ACTradingBBGData.CryptoSecMst";
    }
}
