using aACTrader.DAO.Interface;
using aCommons;
using aCommons.Admin;
using aCommons.DTO;
using aCommons.Utils;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace aACTrader.DAO.Repository
{
    public class AdminDao : IAdminDao
    {
        private readonly ILogger<AdminDao> _logger;
        private const string DELIMITER = ",";

        public AdminDao(ILogger<AdminDao> logger)
        {
            _logger = logger;
            _logger.LogInformation("Initializing AdminDao...");
        }


        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IList<MorningMailData> GetMorningMailData(string asOfDate, string category)
        {
            IList<MorningMailData> MorningMaildatalist = new List<MorningMailData>();

            try
            {
                string sql = GetMorningMailDataQuery + " where AsOfDate = '" + asOfDate + "'";
                if (!category.Equals("All"))
                    sql += " and Category = '" + category + "'";
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                MorningMailData data = new MorningMailData()
                                {
                                    AsOfDate = (reader.IsDBNull(reader.GetOrdinal("AsOfDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    Figi = reader["FIGI"] as String,
                                    Ticker = reader["Ticker"] as String,
                                    Country = reader["Country"] as String,
                                    Category = reader["Category"] as String,
                                    SubCategory = reader["SubCategory"] as String,
                                    NewNav = (reader.IsDBNull(reader.GetOrdinal("NewNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NewNav")),
                                    PrevNav = (reader.IsDBNull(reader.GetOrdinal("PrevNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PrevNav")),
                                    NavChange = (reader.IsDBNull(reader.GetOrdinal("NavChange"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NavChange")),
                                    NewNavDate = (reader.IsDBNull(reader.GetOrdinal("NewNavDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("NewNavDate")),
                                    PrevNavDate = (reader.IsDBNull(reader.GetOrdinal("PrevNavDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("PrevNavDate")),
                                    NewNavSrc = reader["NewNavSrc"] as string,
                                    PrevNavSrc = reader["PrevNavSrc"] as string,
                                    UpdateNavFreq = reader["UpdateNavFreq"] as string,
                                    NewPrice = (reader.IsDBNull(reader.GetOrdinal("NewPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NewPrice")),
                                    PrevPrice = (reader.IsDBNull(reader.GetOrdinal("PrevPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PrevPrice")),
                                    PriceChange = (reader.IsDBNull(reader.GetOrdinal("PriceChange"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceChange")),
                                    PriceSrc = reader["PriceSrc"] as String,
                                    NumFunds = (reader.IsDBNull(reader.GetOrdinal("NumFunds"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("NumFunds")),
                                    NewPos = (reader.IsDBNull(reader.GetOrdinal("NewPos"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("NewPos")),
                                    RptdPos = (reader.IsDBNull(reader.GetOrdinal("RptdPos"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("RptdPos")),
                                    RptdWt = (reader.IsDBNull(reader.GetOrdinal("RptdWt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RptdWt")),
                                    NewRtn = (reader.IsDBNull(reader.GetOrdinal("NewRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NewRtn")),
                                };
                                MorningMaildatalist.Add(data);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing Morning data query");
                throw;
            }
            return MorningMaildatalist;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public ApplicationData GetApplicationDataUpdateFlag()
        {
            ApplicationData data = null;
            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetApplicationDataQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                data = new ApplicationData()
                                {
                                    ApplicationName = reader["ApplicationName"] as string,
                                    DataUpdateFlag = reader["DataUpdateFlag"] as string,
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
        /// 
        /// </summary>
        /// <param name="data"></param>
        public void SaveApplicationDataUpdateFlag(ApplicationData data)
        {
            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(SaveApplicationDataQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.Add(new MySqlParameter("p_ApplicationName", MySqlDbType.VarChar));
                        command.Parameters.Add(new MySqlParameter("p_DataUpdateFlag", MySqlDbType.VarChar));
                        command.Parameters[0].Value = data.ApplicationName;
                        command.Parameters[1].Value = data.DataUpdateFlag;
                        command.ExecuteNonQuery();
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
        /// <param name="logDataList"></param>
        public void SaveLogData(IList<LogData> logDataList)
        {
            try
            {
                StringBuilder sCommand = new StringBuilder("insert into almitasc_ACTradingBBGData.AppLogDet " +
                    "(ServiceName, FunctionName, Ticker, LogMessage, LogLevel, EntryTime) values ");

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlTransaction trans = connection.BeginTransaction())
                    {
                        _logger.LogInformation("Deleting data from almitasc_ACTradingBBGData.AppLogDet");
                        string sqlDelete = "delete from almitasc_ACTradingBBGData.AppLogDet";
                        using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                        {
                            command.ExecuteNonQuery();
                        }

                        List<string> Rows = new List<string>();
                        StringBuilder sb = new StringBuilder();
                        foreach (LogData data in logDataList)
                        {
                            //ServiceName
                            if (!string.IsNullOrEmpty(data.ServiceName))
                                sb.Append(string.Concat("'", data.ServiceName, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //FunctionName
                            if (!string.IsNullOrEmpty(data.FunctionName))
                                sb.Append(string.Concat("'", data.FunctionName, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //Ticker
                            if (!string.IsNullOrEmpty(data.Ticker))
                                sb.Append(string.Concat("'", data.Ticker, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //LogMessage
                            if (!string.IsNullOrEmpty(data.Message))
                                sb.Append(string.Concat("'", data.Message, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //LogLevel
                            if (!string.IsNullOrEmpty(data.LogLevel))
                                sb.Append(string.Concat("'", data.LogLevel, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //EntryTime
                            if (data.EntryTime.HasValue)
                                sb.Append(string.Concat("'", DateUtils.ConvertDate(data.EntryTime, "yyyy-MM-dd HH:mm:ss"), "'"));
                            else
                                sb.Append("null");

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
                        trans.Commit();
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving data");
                throw;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IList<PrimebrokerDetailsTO> GetPrimeBrokerFiles()
        {
            IList<PrimebrokerDetailsTO> list = new List<PrimebrokerDetailsTO>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetPrimeBrokerFilesDetailsQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.Text;


                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                PrimebrokerDetailsTO data = new PrimebrokerDetailsTO
                                {
                                    BrokerName = reader["BrokerName"] as string,
                                    ConnectionType = reader["ConnectionType"] as string,
                                    ProdServer = reader["ProdServer"] as string,
                                    DRServer = reader["DRServer"] as string,
                                    Port = (reader.IsDBNull(reader.GetOrdinal("Port"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("Port")),
                                    UserName = reader["UserName"] as string,
                                    SourceFolder = reader["SourceFolder"] as string,
                                    DestinationFolder = reader["DestinationFolder"] as string,
                                    PrimaryContact = reader["PrimaryContact"] as string,
                                    SecondaryContact = reader["SecondaryContact"] as string,
                                    InboundEncryption = reader["InboundEncryption"] as string,
                                    OutboundEncryption = reader["OutboundEncryption"] as string,
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
                _logger.LogError(ex, "Error executing Prime broker files query");
                throw;
            }
            return list;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IList<ITRepoMstTO> GetITRepoMst()
        {
            IList<ITRepoMstTO> list = new List<ITRepoMstTO>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetITRepoMstQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.Text;
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                ITRepoMstTO data = new ITRepoMstTO
                                {
                                    RepoName = reader["RepoName"] as string,
                                    RepoDesc = reader["RepoDesc"] as string,
                                    Platform = reader["Platform"] as string,
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
                _logger.LogError(ex, "Error executing IT Repo query");
                throw;
            }
            return list;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="StartDate"></param>
        /// <param name="EndDate"></param>
        /// <param name="Broker"></param>
        /// <returns></returns>
        public IList<BrokerFilesStatusDetailsTO> GetBrokerFilesStatusDetails(string StartDate, string EndDate, string Broker)
        {
            IList<BrokerFilesStatusDetailsTO> list = new List<BrokerFilesStatusDetailsTO>();

            string sql = GetBrokerFilesStatusDetailsQuery;
            if (Broker != "All")
                sql += " where BrokerName = '" + Broker + "'" + " and AsOfDate between '" + StartDate + "' and '" + EndDate + "' order by AsOfDate, BrokerName asc";
            else
                sql += " where AsOfDate between '" + StartDate + "' and '" + EndDate + "' order by AsOfDate, BrokerName asc";

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        command.CommandType = System.Data.CommandType.Text;
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                BrokerFilesStatusDetailsTO data = new BrokerFilesStatusDetailsTO
                                {
                                    Id = (reader.IsDBNull(reader.GetOrdinal("Id"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("Id")),
                                    AsOfDate = (reader.IsDBNull(reader.GetOrdinal("AsOfDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    BrokerName = reader["BrokerName"] as string,
                                    FileName = reader["FileName"] as string,
                                    FileNameShort = reader["FileNameShort"] as string,
                                    FileType = reader["FileType"] as string,
                                    FileDate = (reader.IsDBNull(reader.GetOrdinal("FileDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    SheetName = reader["SheetName"] as string,
                                    TableName = reader["TableName"] as string,
                                    FileStatus = reader["FileStatus"] as string,
                                    LoadStatus = reader["LoadStatus"] as string,
                                    LoadError = reader["LoadError"] as string,
                                    Required = reader["Required"] as string,
                                    RowCount = (reader.IsDBNull(reader.GetOrdinal("RowCount"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("RowCount")),
                                };
                                list.Add(data);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing Prime broker files status query");
                throw;
            }
            return list;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="procedureName"></param>
        public void ExecuteStoredProcedure(string procedureName)
        {
            //_logger.LogInformation("Executing Stored Procedure STARTED: " + procedureName);
            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(procedureName, connection))
                    {
                        command.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing query");
            }
            _logger.LogInformation("Executing Stored Procedure DONE: " + procedureName);
        }

        private const string GetApplicationDataQuery = "select * from almitasc_ACTradingBBGData.ApplicationData";
        private const string SaveApplicationDataQuery = "spUpdateApplicationData";
        private const string GetMorningMailDataQuery = "select * from almitasc_ACTradingBBGData.DailyDataValidation";
        private const string GetPrimeBrokerFilesDetailsQuery = "select * from Primebrokerfiles.PrimeBrokerDet";
        private const string GetITRepoMstQuery = "select * from almitasc_ACTradingBBGData.ITRepoMst";
        private const string GetBrokerFilesStatusDetailsQuery = "select * from Primebrokerfiles.FileStatusDet";
    }
}