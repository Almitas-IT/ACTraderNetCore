using aACTrader.DAO.Interface;
using aCommons.Compliance;
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
    public class ComplianceDao : IComplianceDao
    {
        private readonly ILogger<ComplianceDao> _logger;
        private const string DELIMITER = ",";

        public ComplianceDao(ILogger<ComplianceDao> logger)
        {
            _logger = logger;
            _logger.LogInformation("Initializing ComplianceDao...");
        }

        public IList<RuleCategory> GetRuleCategories()
        {
            throw new System.NotImplementedException();
        }

        public IList<RuleMst> GetRules()
        {
            IList<RuleMst> list = new List<RuleMst>();
            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetRulesMstQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                RuleMst data = new RuleMst
                                {
                                    RuleMstId = (reader.IsDBNull(reader.GetOrdinal("RuleMstId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("RuleMstId")),
                                    RuleName = reader["RuleName"] as string,
                                    RuleDesc = reader["RuleDesc"] as string,
                                    RuleCondition = reader["RuleCondition"] as string,
                                    RuleCategory = reader["RuleCategory"] as string,
                                    ErrorLevel = reader["ErrorLevel"] as string,
                                    CreatedBy = reader["CreatedBy"] as string,
                                    CreateDate = (reader.IsDBNull(reader.GetOrdinal("CreateDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("CreateDate")),
                                    ModifyDate = (reader.IsDBNull(reader.GetOrdinal("ModifyDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("ModifyDate")),
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

        public IDictionary<string, DividendScheduleTO> GetDvdExDates()
        {
            IDictionary<string, DividendScheduleTO> dict = new Dictionary<string, DividendScheduleTO>(StringComparer.CurrentCultureIgnoreCase);
            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetExDvdDatesQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                DividendScheduleTO data = new DividendScheduleTO
                                {
                                    Src = reader["Src"] as string,
                                    Ticker = reader["Ticker"] as string,
                                    ExDvdDate = reader.IsDBNull(reader.GetOrdinal("DvdExDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("DvdExDate")),
                                    DecDate = reader.IsDBNull(reader.GetOrdinal("DvdDecDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("DvdDecDate")),
                                    RecordDate = reader.IsDBNull(reader.GetOrdinal("DvdRecordDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("DvdRecordDate")),
                                    PayDate = reader.IsDBNull(reader.GetOrdinal("DvdPayDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("DvdPayDate")),
                                    DvdAmt = (reader.IsDBNull(reader.GetOrdinal("Amt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Amt")),
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

        public void SaveRuleRunDetails(RuleRunMst data)
        {
            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlTransaction trans = connection.BeginTransaction())
                    {
                        long ruleRunMstId = InsertRuleRunMst(connection, data);
                        InsertRuleRunSummary(connection, data, ruleRunMstId);
                        InsertRuleRunDetails(connection, data, ruleRunMstId);
                        trans.Commit();
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving Rule Run Details into database");
                throw;
            }
        }

        private long InsertRuleRunMst(MySqlConnection connection, RuleRunMst data)
        {
            long ruleRunMstId;
            StringBuilder sCommand = new StringBuilder("INSERT INTO Trading.RuleRunMst (RunDate, Name, Description, SecCount, RuleCount, WarningSecCount, RestrictionSecCount, UserName) values ");

            List<string> rows = new List<string>();
            StringBuilder sb = new StringBuilder();

            // RunDate
            sb.Append("'" + DateUtils.ConvertDate(data.RunDate, "yyyy-MM-dd") + "'").Append(DELIMITER);

            // Name
            if (!string.IsNullOrEmpty(data.Name))
                sb.Append(string.Concat("'", data.Name, "'")).Append(DELIMITER);
            else
                sb.Append("null").Append(DELIMITER);

            // Description
            if (!string.IsNullOrEmpty(data.Desc))
                sb.Append(string.Concat("'", data.Desc, "'")).Append(DELIMITER);
            else
                sb.Append("null").Append(DELIMITER);

            // SecCount
            if (data.SecCount.HasValue)
                sb.Append(data.SecCount).Append(DELIMITER);
            else
                sb.Append("null").Append(DELIMITER);

            // RuleCount
            if (data.RuleCount.HasValue)
                sb.Append(data.RuleCount).Append(DELIMITER);
            else
                sb.Append("null").Append(DELIMITER);

            // WarningSecCount
            if (data.WarningSecCount.HasValue)
                sb.Append(data.WarningSecCount).Append(DELIMITER);
            else
                sb.Append("null").Append(DELIMITER);

            // RestrictionSecCount
            if (data.RestrictionSecCount.HasValue)
                sb.Append(data.RestrictionSecCount).Append(DELIMITER);
            else
                sb.Append("null").Append(DELIMITER);

            // UserName
            if (!string.IsNullOrEmpty(data.UserName))
                sb.Append(string.Concat("'", data.UserName, "'"));
            else
                sb.Append("null");

            string row = sb.ToString();
            rows.Add(string.Concat("(", row, ")"));
            sb.Clear();

            sCommand.Append(string.Join(",", rows));
            sCommand.Append(";");

            _logger.LogDebug("insert into Trading.RuleRunMst");
            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), connection))
            {
                myCmd.CommandType = CommandType.Text;
                myCmd.ExecuteNonQuery();
                ruleRunMstId = myCmd.LastInsertedId;
            }
            return ruleRunMstId;
        }

        private void InsertRuleRunSummary(MySqlConnection connection, RuleRunMst data, long ruleRunMstId)
        {
            StringBuilder sCommand = new StringBuilder("INSERT INTO Trading.RuleRunSummary(RuleRunMstId, Fund, Ticker, ALMTicker, OrdSide, Position, RuleCount, RunDetails, ErrorLevel) values ");

            List<string> rows = new List<string>();
            StringBuilder sb = new StringBuilder();
            foreach (RuleRunSummary summary in data.SummaryDetails)
            {
                if (!string.IsNullOrEmpty(summary.ErrorLevel))
                {
                    // RuleRunMstId
                    sb.Append(ruleRunMstId).Append(DELIMITER);

                    // Fund
                    if (!string.IsNullOrEmpty(summary.Fund))
                        sb.Append(string.Concat("'", summary.Fund, "'")).Append(DELIMITER);
                    else
                        sb.Append("null").Append(DELIMITER);

                    // Ticker
                    if (!string.IsNullOrEmpty(summary.Ticker))
                        sb.Append(string.Concat("'", summary.Ticker, "'")).Append(DELIMITER);
                    else
                        sb.Append("null").Append(DELIMITER);

                    // ALMTicker
                    if (!string.IsNullOrEmpty(summary.ALMTicker))
                        sb.Append(string.Concat("'", summary.ALMTicker, "'")).Append(DELIMITER);
                    else
                        sb.Append("null").Append(DELIMITER);

                    // OrdSide
                    if (!string.IsNullOrEmpty(summary.OrdSide))
                        sb.Append(string.Concat("'", summary.OrdSide, "'")).Append(DELIMITER);
                    else
                        sb.Append("null").Append(DELIMITER);

                    // Position
                    if (summary.Position.HasValue)
                        sb.Append(summary.Position).Append(DELIMITER);
                    else
                        sb.Append("null").Append(DELIMITER);

                    // RuleCount
                    if (summary.RuleCount.HasValue)
                        sb.Append(summary.RuleCount).Append(DELIMITER);
                    else
                        sb.Append("null").Append(DELIMITER);

                    // RunDetails
                    if (!string.IsNullOrEmpty(summary.RunDetails))
                        sb.Append(string.Concat("'", summary.RunDetails, "'")).Append(DELIMITER);
                    else
                        sb.Append("null").Append(DELIMITER);

                    // ErrorLevel
                    if (!string.IsNullOrEmpty(summary.ErrorLevel))
                        sb.Append(string.Concat("'", summary.ErrorLevel, "'"));
                    else
                        sb.Append("null");

                    string row = sb.ToString();
                    rows.Add(string.Concat("(", row, ")"));
                    sb.Clear();
                }
            }

            sCommand.Append(string.Join(",", rows));
            sCommand.Append(";");

            _logger.LogDebug("insert into Trading.RuleRunSummary");
            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), connection))
            {
                myCmd.CommandType = CommandType.Text;
                myCmd.ExecuteNonQuery();
            }
        }

        private void InsertRuleRunDetails(MySqlConnection connection, RuleRunMst data, long ruleRunMstId)
        {
            StringBuilder sCommand = new StringBuilder("INSERT INTO Trading.RuleRunDet (RuleRunMstId, Fund, Ticker, ALMTicker, RuleMstId, RuleCondition, RuleInput, RuleOutput, ErrorLevel) values ");

            List<string> rows = new List<string>();
            StringBuilder sb = new StringBuilder();
            foreach (RuleRunSummary summary in data.SummaryDetails)
            {
                foreach (RuleRunDetail detail in summary.Details)
                {
                    // RuleRunMstId
                    sb.Append(ruleRunMstId).Append(DELIMITER);

                    // Fund
                    if (!string.IsNullOrEmpty(detail.Fund))
                        sb.Append(string.Concat("'", detail.Fund, "'")).Append(DELIMITER);
                    else
                        sb.Append("null").Append(DELIMITER);

                    // Ticker
                    if (!string.IsNullOrEmpty(detail.Ticker))
                        sb.Append(string.Concat("'", detail.Ticker, "'")).Append(DELIMITER);
                    else
                        sb.Append("null").Append(DELIMITER);

                    // ALMTicker
                    if (!string.IsNullOrEmpty(detail.ALMTicker))
                        sb.Append(string.Concat("'", detail.ALMTicker, "'")).Append(DELIMITER);
                    else
                        sb.Append("null").Append(DELIMITER);

                    // RuleMstId
                    if (detail.RuleMstId.HasValue)
                        sb.Append(detail.RuleMstId).Append(DELIMITER);
                    else
                        sb.Append("null").Append(DELIMITER);

                    // RuleCondition
                    if (!string.IsNullOrEmpty(detail.RuleCondition))
                        sb.Append(string.Concat("'", MySqlHelper.EscapeString(detail.RuleCondition), "'")).Append(DELIMITER);
                    else
                        sb.Append("null").Append(DELIMITER);

                    // RuleInput
                    if (!string.IsNullOrEmpty(detail.RuleInput))
                        sb.Append(string.Concat("'", MySqlHelper.EscapeString(detail.RuleInput), "'")).Append(DELIMITER);
                    else
                        sb.Append("null").Append(DELIMITER);

                    // RuleOutput
                    if (!string.IsNullOrEmpty(detail.RuleOutput))
                        sb.Append(string.Concat("'", MySqlHelper.EscapeString(detail.RuleOutput), "'")).Append(DELIMITER);
                    else
                        sb.Append("null").Append(DELIMITER);

                    // ErrorLevel
                    if (!string.IsNullOrEmpty(detail.ErrorLevel))
                        sb.Append(string.Concat("'", detail.ErrorLevel, "'"));
                    else
                        sb.Append("null");

                    string row = sb.ToString();
                    rows.Add(string.Concat("(", row, ")"));
                    sb.Clear();
                }
            }

            sCommand.Append(string.Join(",", rows));
            sCommand.Append(";");

            _logger.LogDebug("insert into Trading.RuleRunDet");
            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), connection))
            {
                myCmd.CommandType = CommandType.Text;
                myCmd.ExecuteNonQuery();
            }
        }

        public IDictionary<string, string> Get1940ActFunds()
        {
            IDictionary<string, string> dict = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);
            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(Get1940ActFundsQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                string ticker = reader["Ticker"] as string;
                                string sec1940ActFlag = reader["Sec1940Act"] as string;
                                if (!dict.ContainsKey(ticker))
                                    dict.Add(ticker, sec1940ActFlag);
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

        public IDictionary<string, RestrictedSecurity> GetRestrictedSecurities()
        {
            IDictionary<string, RestrictedSecurity> dict = new Dictionary<string, RestrictedSecurity>(StringComparer.CurrentCultureIgnoreCase);
            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetRestrictedSecurityListQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                RestrictedSecurity data = new RestrictedSecurity
                                {
                                    Ticker = reader["Ticker"] as string,
                                    Notes = reader["Notes"] as string,
                                    StartDate = reader.IsDBNull(reader.GetOrdinal("StartDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("StartDate")),
                                    EndDate = reader.IsDBNull(reader.GetOrdinal("EndDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EndDate")),
                                    CreateDate = reader.IsDBNull(reader.GetOrdinal("CreateDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("CreateDate")),
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

        public IList<RuleRunMst> GetRuleRunList(DateTime startDate, DateTime endDate)
        {
            IList<RuleRunMst> list = new List<RuleRunMst>();
            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    string sql = GetRuleRunMstQuery;
                    sql += " where RunDate >= '" + DateUtils.ConvertDate(startDate, "yyyy-MM-dd") + "' and RunDate <= '" + DateUtils.ConvertDate(endDate, "yyyy-MM-dd") + "' order by RunDate desc";

                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                RuleRunMst data = new RuleRunMst
                                {
                                    RuleRunMstId = (reader.IsDBNull(reader.GetOrdinal("RuleRunMstId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("RuleRunMstId")),
                                    RunDate = (reader.IsDBNull(reader.GetOrdinal("RunDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("RunDate")),
                                    Name = reader["Name"] as string,
                                    Desc = reader["Description"] as string,
                                    SecCount = (reader.IsDBNull(reader.GetOrdinal("SecCount"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("SecCount")),
                                    RuleCount = (reader.IsDBNull(reader.GetOrdinal("RuleCount"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("RuleCount")),
                                    WarningSecCount = (reader.IsDBNull(reader.GetOrdinal("WarningSecCount"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("WarningSecCount")),
                                    RestrictionSecCount = (reader.IsDBNull(reader.GetOrdinal("RestrictionSecCount"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("RestrictionSecCount")),
                                    UserName = reader["UserName"] as string,
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
                _logger.LogError(ex, "Error executing query");
                throw;
            }
            return list;
        }

        public IList<RuleRunSummary> GetRuleRunSummary(int ruleRunMstId)
        {
            IList<RuleRunSummary> list = new List<RuleRunSummary>();
            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    string sql = GetRuleRunSummaryQuery + " where RuleRunMstId = " + ruleRunMstId;

                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                RuleRunSummary data = new RuleRunSummary
                                {
                                    RuleRunSummaryId = (reader.IsDBNull(reader.GetOrdinal("RuleRunSummaryId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("RuleRunSummaryId")),
                                    RuleRunMstId = (reader.IsDBNull(reader.GetOrdinal("RuleRunMstId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("RuleRunMstId")),
                                    Fund = reader["Fund"] as string,
                                    Ticker = reader["Ticker"] as string,
                                    ALMTicker = reader["ALMTicker"] as string,
                                    OrdSide = reader["OrdSide"] as string,
                                    Position = (reader.IsDBNull(reader.GetOrdinal("Position"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("Position")),
                                    RuleCount = (reader.IsDBNull(reader.GetOrdinal("RuleCount"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("RuleCount")),
                                    RunDetails = reader["RunDetails"] as string,
                                    ErrorLevel = reader["ErrorLevel"] as string,
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
                _logger.LogError(ex, "Error executing query");
                throw;
            }
            return list;
        }

        public IList<RuleRunDetail> GetRuleRunDetails(int ruleRunMstId)
        {
            IList<RuleRunDetail> list = new List<RuleRunDetail>();
            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    string sql = GetRuleRunDetailsQuery + " where RuleRunMstId = " + ruleRunMstId;

                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                RuleRunDetail data = new RuleRunDetail
                                {
                                    RuleRunDetId = (reader.IsDBNull(reader.GetOrdinal("RuleRunDetId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("RuleRunDetId")),
                                    RuleRunMstId = (reader.IsDBNull(reader.GetOrdinal("RuleRunMstId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("RuleRunMstId")),
                                    RuleMstId = (reader.IsDBNull(reader.GetOrdinal("RuleMstId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("RuleMstId")),
                                    Fund = reader["Fund"] as string,
                                    Ticker = reader["Ticker"] as string,
                                    ALMTicker = reader["ALMTicker"] as string,
                                    RuleCondition = reader["RuleCondition"] as string,
                                    RuleInput = reader["RuleInput"] as string,
                                    RuleOutput = reader["RuleOutput"] as string,
                                    ErrorLevel = reader["ErrorLevel"] as string,
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
                _logger.LogError(ex, "Error executing query");
                throw;
            }
            return list;
        }

        public IList<RuleRunDetail> GetRuleRunDetails(int ruleRunMstId, string ticker)
        {
            IList<RuleRunDetail> list = new List<RuleRunDetail>();
            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    string sql = GetRuleRunDetailsQuery + " where RuleRunMstId = " + ruleRunMstId + " and Ticker = '" + ticker + "'";

                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                RuleRunDetail data = new RuleRunDetail
                                {
                                    RuleRunDetId = (reader.IsDBNull(reader.GetOrdinal("RuleRunDetId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("RuleRunDetId")),
                                    RuleRunMstId = (reader.IsDBNull(reader.GetOrdinal("RuleRunMstId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("RuleRunMstId")),
                                    RuleMstId = (reader.IsDBNull(reader.GetOrdinal("RuleMstId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("RuleMstId")),
                                    Fund = reader["Fund"] as string,
                                    Ticker = reader["Ticker"] as string,
                                    ALMTicker = reader["ALMTicker"] as string,
                                    RuleCondition = reader["RuleCondition"] as string,
                                    RuleInput = reader["RuleInput"] as string,
                                    RuleOutput = reader["RuleOutput"] as string,
                                    ErrorLevel = reader["ErrorLevel"] as string,
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
                _logger.LogError(ex, "Error executing query");
                throw;
            }
            return list;
        }

        public string GetRulesMstQuery = "select * from Trading.RuleMst";
        public string GetRuleRunMstQuery = "select * from Trading.RuleRunMst";
        public string GetRuleRunSummaryQuery = "select * from Trading.RuleRunSummary";
        public string GetRuleRunDetailsQuery = "select * from Trading.RuleRunDet";
        public string GetExDvdDatesQuery = "call Trading.spGetExDvdDates";
        public string Get1940ActFundsQuery = "call Trading.spGet1940ActFunds";
        public string GetRestrictedSecurityListQuery = "select * from Trading.RestrictedSecurity";
    }
}
