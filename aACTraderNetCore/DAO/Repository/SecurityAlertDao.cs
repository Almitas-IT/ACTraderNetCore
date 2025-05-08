using aACTrader.DAO.Interface;
using aCommons.Alerts;
using aCommons.Compliance;
using aCommons.Utils;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace aACTrader.DAO.Repository
{
    public class SecurityAlertDao : ISecurityAlertDao
    {
        private readonly ILogger<SecurityAlertDao> _logger;
        private const string DELIMITER = ",";

        public SecurityAlertDao(ILogger<SecurityAlertDao> logger)
        {
            _logger = logger;
            _logger.LogInformation("Initializing SecurityAlertDao...");
        }

        public IList<SecurityAlert> GetSecurityAlerts()
        {
            IList<SecurityAlert> list = new List<SecurityAlert>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetSecurityAlertsQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                SecurityAlert data = new SecurityAlert
                                {
                                    Id = (reader.IsDBNull(reader.GetOrdinal("Id"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("Id")),
                                    AlertType = reader["AlertType"] as string,
                                    TransType = reader["TransType"] as string,
                                    Ticker1 = reader["Ticker1"] as string,
                                    Ticker2 = reader["Ticker2"] as string,
                                    AlertOp = reader["AlertOp"] as string,
                                    UserName = reader["UserName"] as string,
                                    TargetField = reader["TargetField"] as string,
                                    TargetChangeType = reader["TargetChangeType"] as string,
                                    TargetChangeSide = reader["TargetChangeSide"] as string,
                                    CountryCode = reader["CountryCode"] as string,
                                    AssetType = reader["AssetType"] as string,
                                    TargetValue = (reader.IsDBNull(reader.GetOrdinal("TargetValue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TargetValue")),
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

        public void SaveSecurityAlerts(IList<SecurityAlert> list)
        {
            try
            {
                StringBuilder sCommand = new StringBuilder("insert into almitasc_ACTradingBBGData.StgSecurityAlertMst " +
                    "(Id, AlertType, TransType, Ticker1, Ticker2, AlertOp, TargetField, TargetValue, UserName) values ");

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlTransaction trans = connection.BeginTransaction())
                    {
                        string sqlDelete = "delete from almitasc_ACTradingBBGData.StgSecurityAlertMst";
                        using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                        {
                            _logger.LogInformation("deleting data from almitasc_ACTradingBBGData.StgSecurityAlertMst");
                            command.ExecuteNonQuery();
                        }

                        List<string> Rows = new List<string>();
                        StringBuilder sb = new StringBuilder();
                        foreach (SecurityAlert data in list)
                        {
                            // Id
                            if (data.Id.HasValue)
                                sb.Append(data.Id).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // AlertType
                            if (!string.IsNullOrEmpty(data.AlertType))
                                sb.Append(string.Concat("'", data.AlertType, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // TransType
                            if (!string.IsNullOrEmpty(data.TransType))
                                sb.Append(string.Concat("'", data.TransType, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // Ticker1
                            if (!string.IsNullOrEmpty(data.Ticker1))
                                sb.Append(string.Concat("'", data.Ticker1, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // Ticker2
                            if (!string.IsNullOrEmpty(data.Ticker2))
                                sb.Append(string.Concat("'", data.Ticker2, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // AlertOp
                            if (!string.IsNullOrEmpty(data.AlertOp))
                                sb.Append(string.Concat("'", data.AlertOp, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // TargetField
                            if (!string.IsNullOrEmpty(data.TargetField))
                                sb.Append(string.Concat("'", data.TargetField, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // TargetValue
                            if (data.TargetValue.HasValue)
                                sb.Append(data.TargetValue).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // UserName
                            if (!string.IsNullOrEmpty(data.UserName))
                                sb.Append(string.Concat("'", data.UserName, "'"));
                            else
                                sb.Append("null");

                            string row = sb.ToString();
                            Rows.Add(string.Concat("(", row, ")"));
                            sb.Clear();
                        }

                        sCommand.Append(string.Join(",", Rows));
                        sCommand.Append(";");

                        _logger.LogDebug("Insert into almitasc_ACTradingBBGData.SecurityAlertMst");
                        using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), connection))
                        {
                            myCmd.CommandType = CommandType.Text;
                            myCmd.ExecuteNonQuery();
                        }

                        _logger.LogInformation("Moving data to almitasc_ACTradingBBGData.SecurityAlertMst");
                        string sql = "spPopulateSecurityAlertMst";
                        using (MySqlCommand command = new MySqlCommand(sql, connection))
                        {
                            command.CommandType = System.Data.CommandType.StoredProcedure;
                            command.ExecuteNonQuery();
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving security prices into database");
                throw;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="userName"></param>
        /// <returns></returns>
        public IList<SecurityAlert> GetSecurityAlerts(string userName)
        {
            IList<SecurityAlert> list = new List<SecurityAlert>();

            try
            {
                string sql = GetSecurityAlertsQuery + " where UserName = '" + userName + "'";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                SecurityAlert data = new SecurityAlert
                                {
                                    Id = (reader.IsDBNull(reader.GetOrdinal("Id"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("Id")),
                                    AlertType = reader["AlertType"] as string,
                                    TransType = reader["TransType"] as string,
                                    Ticker1 = reader["Ticker1"] as string,
                                    Ticker2 = reader["Ticker2"] as string,
                                    AlertOp = reader["AlertOp"] as string,
                                    UserName = reader["UserName"] as string,
                                    TargetField = reader["TargetField"] as string,
                                    TargetValue = (reader.IsDBNull(reader.GetOrdinal("TargetValue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TargetValue")),
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

        public void SaveSecurityOwnershipLimits(IList<SecOwnership> list)
        {
            try
            {
                StringBuilder sCommand = new StringBuilder("INSERT INTO Trading.SecOwnership " +
                    "(RunDate, Ticker, ALMTicker, TotalPos, CashPos, SwapPos, SharesOut, SharesOutOvr, TotalOwnPct, CashOwnPct, SwapOwnPct"
                    + ", SecType, Country, PaymentRank, Notes, RptFlag) values ");

                DateTime runDate = list[0].RunDate.GetValueOrDefault();
                string runDateAsString = DateUtils.ConvertDate(runDate, "yyyy-MM-dd", "0000-00-00");
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlTransaction trans = connection.BeginTransaction())
                    {
                        string sqlDelete = "delete from Trading.SecOwnership where RunDate = '" + runDateAsString + "'";
                        using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                        {
                            _logger.LogInformation(sqlDelete);
                            command.ExecuteNonQuery();
                        }

                        List<string> rows = new List<string>();
                        StringBuilder sb = new StringBuilder();
                        foreach (SecOwnership data in list)
                        {
                            // RunDate
                            sb.Append("'" + runDateAsString + "'").Append(DELIMITER);

                            // Ticker
                            if (!string.IsNullOrEmpty(data.Ticker))
                                sb.Append(string.Concat("'", data.Ticker, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // ALMTicker
                            if (!string.IsNullOrEmpty(data.ALMTicker))
                                sb.Append(string.Concat("'", data.ALMTicker, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // TotalPos
                            if (data.TotalPos.HasValue)
                                sb.Append(data.TotalPos).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // CashPos
                            if (data.CashPos.HasValue)
                                sb.Append(data.CashPos).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // SwapPos
                            if (data.SwapPos.HasValue)
                                sb.Append(data.SwapPos).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // SharesOut
                            if (data.SharesOut.HasValue)
                                sb.Append(data.SharesOut).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // SharesOutOvr
                            if (data.SharesOutOvr.HasValue)
                                sb.Append(data.SharesOutOvr).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // TotalOwnPct
                            if (data.TotalOwnPct.HasValue)
                                sb.Append(data.TotalOwnPct).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // CashOwnPct
                            if (data.CashOwnPct.HasValue)
                                sb.Append(data.CashOwnPct).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // SwapOwnPct
                            if (data.SwapOwnPct.HasValue)
                                sb.Append(data.SwapOwnPct).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // SecType
                            if (!string.IsNullOrEmpty(data.SecType))
                                sb.Append(string.Concat("'", data.SecType, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // Country
                            if (!string.IsNullOrEmpty(data.Country))
                                sb.Append(string.Concat("'", data.Country, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // PaymentRank
                            if (!string.IsNullOrEmpty(data.PaymentRank))
                                sb.Append(string.Concat("'", data.PaymentRank, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // Notes
                            if (!string.IsNullOrEmpty(data.Notes))
                                sb.Append(string.Concat("'", data.Notes, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // RptFlag
                            if (data.RptFlag.HasValue)
                                sb.Append(data.RptFlag);
                            else
                                sb.Append("null");

                            string row = sb.ToString();
                            rows.Add(string.Concat("(", row, ")"));
                            sb.Clear();
                        }

                        sCommand.Append(string.Join(",", rows));
                        sCommand.Append(";");

                        _logger.LogDebug("Insert into Trading.SecOwnership");
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
                _logger.LogError(ex, "Error saving security ownership details into database");
                throw;
            }
        }

        public IList<SecOwnership> GetSecOwnershipDetails(string ticker, string startDate, string endDate)
        {
            IList<SecOwnership> list = new List<SecOwnership>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    string sql = GetSecOwnershipDetailsQuery;
                    sql += " where RunDate >= '" + startDate + "' and RunDate <= '" + endDate + "'";
                    if (!string.IsNullOrEmpty(ticker))
                        sql += " and Ticker = '" + ticker + "'";
                    sql += " order by RunDate desc, RptFlag desc";
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                SecOwnership data = new SecOwnership
                                {
                                    Id = (reader.IsDBNull(reader.GetOrdinal("Id"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("Id")),
                                    RunDate = (reader.IsDBNull(reader.GetOrdinal("RunDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("RunDate")),
                                    Ticker = reader["Ticker"] as string,
                                    ALMTicker = reader["ALMTicker"] as string,
                                    TotalPos = (reader.IsDBNull(reader.GetOrdinal("TotalPos"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotalPos")),
                                    CashPos = (reader.IsDBNull(reader.GetOrdinal("CashPos"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CashPos")),
                                    SwapPos = (reader.IsDBNull(reader.GetOrdinal("SwapPos"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SwapPos")),
                                    SharesOut = (reader.IsDBNull(reader.GetOrdinal("SharesOut"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SharesOut")),
                                    SharesOutOvr = (reader.IsDBNull(reader.GetOrdinal("SharesOutOvr"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SharesOutOvr")),
                                    TotalOwnPct = (reader.IsDBNull(reader.GetOrdinal("TotalOwnPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotalOwnPct")),
                                    CashOwnPct = (reader.IsDBNull(reader.GetOrdinal("CashOwnPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CashOwnPct")),
                                    SwapOwnPct = (reader.IsDBNull(reader.GetOrdinal("SwapOwnPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SwapOwnPct")),
                                    SecType = reader["SecType"] as string,
                                    Country = reader["Country"] as string,
                                    PaymentRank = reader["PaymentRank"] as string,
                                    Notes = reader["Notes"] as string,
                                    LastFilingType = reader["LastFilingType"] as string,
                                    LastFilingDate = (reader.IsDBNull(reader.GetOrdinal("LastFilingDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("LastFilingDate")),
                                    RptFlag = (reader.IsDBNull(reader.GetOrdinal("RptFlag"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("RptFlag")),
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

        public IList<SecFilingDetail> GetSecFilingDetails(string ticker, string startDate, string endDate)
        {
            IList<SecFilingDetail> list = new List<SecFilingDetail>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    string sql = GetSecFilingDetailsQuery;
                    sql += " where LastFilingDate >= '" + startDate + "' and LastFilingDate <= '" + endDate + "'";
                    if (!string.IsNullOrEmpty(ticker))
                        sql += " and Ticker = '" + ticker + "'";
                    sql += " order by Ticker";
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                SecFilingDetail data = new SecFilingDetail
                                {
                                    Id = (reader.IsDBNull(reader.GetOrdinal("Id"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("Id")),
                                    Ticker = reader["Ticker"] as string,
                                    TotalPos = (reader.IsDBNull(reader.GetOrdinal("TotalPos"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotalPos")),
                                    CashPos = (reader.IsDBNull(reader.GetOrdinal("CashPos"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CashPos")),
                                    SwapPos = (reader.IsDBNull(reader.GetOrdinal("SwapPos"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SwapPos")),
                                    SharesOut = (reader.IsDBNull(reader.GetOrdinal("SharesOut"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SharesOut")),
                                    SecOwnPct = (reader.IsDBNull(reader.GetOrdinal("SecOwnWt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SecOwnWt")),
                                    LastFilingDate = (reader.IsDBNull(reader.GetOrdinal("LastFilingDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("LastFilingDate")),
                                    LastFilingType = reader["LastFilingType"] as string,
                                    FilingId = reader["FilingId"] as string,
                                    FilingDest = reader["FilingDest"] as string,
                                    FilingLink = reader["FilingLink"] as string,
                                    UserName = reader["UserName"] as string,
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

        public void SaveSecurityFilingDetails(IList<SecFilingDetail> list)
        {
            try
            {
                StringBuilder sCommand = new StringBuilder("INSERT INTO Trading.StgSecFilingDetail " +
                    "(Ticker, TotalPos, CashPos, SwapPos, SharesOut, SecOwnWt, LastFilingDate, LastFilingType, FilingId"
                    + ", FilingDest, FilingLink, UserName) values ");

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlTransaction trans = connection.BeginTransaction())
                    {
                        string sqlDelete = "delete from Trading.StgSecFilingDetail";
                        using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                        {
                            _logger.LogInformation(sqlDelete);
                            command.ExecuteNonQuery();
                        }

                        List<string> rows = new List<string>();
                        StringBuilder sb = new StringBuilder();
                        foreach (SecFilingDetail data in list)
                        {
                            // Ticker
                            if (!string.IsNullOrEmpty(data.Ticker))
                                sb.Append(string.Concat("'", data.Ticker, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // TotalPos
                            if (data.TotalPos.HasValue)
                                sb.Append(data.TotalPos).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // CashPos
                            if (data.CashPos.HasValue)
                                sb.Append(data.CashPos).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // SwapPos
                            if (data.SwapPos.HasValue)
                                sb.Append(data.SwapPos).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // SharesOut
                            if (data.SharesOut.HasValue)
                                sb.Append(data.SharesOut).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // SecOwnPct
                            if (data.SecOwnPct.HasValue)
                                sb.Append(data.SecOwnPct).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // LastFilingDate
                            sb.Append(string.Concat("'", DateUtils.ConvertDate(data.LastFilingDate, "yyyy-MM-dd", "0000-00-00"), "'")).Append(DELIMITER);

                            // LastFilingType
                            if (!string.IsNullOrEmpty(data.LastFilingType))
                                sb.Append(string.Concat("'", data.LastFilingType, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // FilingId
                            if (!string.IsNullOrEmpty(data.FilingId))
                                sb.Append(string.Concat("'", data.FilingId, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // FilingDest
                            if (!string.IsNullOrEmpty(data.FilingDest))
                                sb.Append(string.Concat("'", data.FilingDest, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // FilingLink
                            if (!string.IsNullOrEmpty(data.FilingLink))
                                sb.Append(string.Concat("'", data.FilingLink, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // UserName
                            if (!string.IsNullOrEmpty(data.UserName))
                                sb.Append(data.UserName);
                            else
                                sb.Append("null");

                            string row = sb.ToString();
                            rows.Add(string.Concat("(", row, ")"));
                            sb.Clear();
                        }

                        sCommand.Append(string.Join(",", rows));
                        sCommand.Append(";");

                        _logger.LogDebug("Insert into Trading.StgSecFilingDetail");
                        using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), connection))
                        {
                            myCmd.CommandType = CommandType.Text;
                            myCmd.ExecuteNonQuery();
                        }

                        _logger.LogInformation("Moving data to Trading.SecFilingDetail");
                        using (MySqlCommand command = new MySqlCommand(SaveSecFilingDetailsQuery, connection))
                        {
                            command.CommandType = CommandType.StoredProcedure;
                            command.ExecuteNonQuery();
                        }

                        trans.Commit();
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving security prices into database");
                throw;
            }
        }

        private const string GetSecurityAlertsQuery = "call almitasc_ACTradingBBGData.spGetSecurityAlertTargets";
        private const string GetSecOwnershipDetailsQuery = "select * from Trading.SecOwnership";
        private const string GetSecFilingDetailsQuery = "select * from Trading.SecFilingDetail";
        private const string SaveSecFilingDetailsQuery = "Trading.spPopulateSecFilingDetails";
    }
}
