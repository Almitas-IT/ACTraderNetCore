using aACTrader.DAO.Interface;
using aCommons;
using aCommons.Derivatives;
using aCommons.Security;
using aCommons.Utils;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace aACTrader.DAO.Repository
{
    public class SecurityPriceDao : ISecurityPriceDao
    {
        private readonly ILogger<SecurityPriceDao> _logger;

        private const string DATEFORMAT = "yyyy-MM-dd";
        private const string DELIMITER = ",";
        private static readonly DateTime TodaysDate = DateTime.Now.Date;

        public SecurityPriceDao(ILogger<SecurityPriceDao> logger)
        {
            _logger = logger;
            _logger.LogInformation("Initializing SecurityPriceDao...");
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IList<SecurityPrice> GetLatestPrices()
        {
            IList<SecurityPrice> securityPriceList = new List<SecurityPrice>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetLatestPricesQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                SecurityPrice securityPrice = new SecurityPrice
                                {
                                    Ticker = reader["Ticker"] as string,
                                    ClsPrc = (reader.IsDBNull(reader.GetOrdinal("PrevClosePrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PrevClosePrice")),
                                    LastPrc = (reader.IsDBNull(reader.GetOrdinal("LastPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LastPrice")),
                                    BidPrc = (reader.IsDBNull(reader.GetOrdinal("BidPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BidPrice")),
                                    AskPrc = (reader.IsDBNull(reader.GetOrdinal("AskPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AskPrice")),
                                    MidPrc = (reader.IsDBNull(reader.GetOrdinal("MidPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MidPrice")),
                                    NetPrcChng = (reader.IsDBNull(reader.GetOrdinal("NetChange"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NetChange")),
                                    PrcRtn = (reader.IsDBNull(reader.GetOrdinal("TotalReturn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotalReturn")),
                                    PrcChng = (reader.IsDBNull(reader.GetOrdinal("PriceChange"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceChange")),
                                    Src = reader["Source"] as string,
                                    RTFlag = reader.GetInt16("RealTimeFlag"),
                                    TrdTm = reader["TradeTime"] as string,
                                    Curr = reader["PriceCurrency"] as string,
                                    TrdDt = (reader.IsDBNull(reader.GetOrdinal("TradeDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("TradeDate")),
                                    MktCls = 0 //0 - Open, 1 - Closed
                                };

                                securityPriceList.Add(securityPrice);
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

            return securityPriceList;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IDictionary<string, SecurityPrice> GetSecurityPriceMaster()
        {
            IDictionary<string, SecurityPrice> securityPriceMasterDict = new Dictionary<string, SecurityPrice>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetSecurityPriceMasterQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                SecurityPrice securityPrice = new SecurityPrice
                                {
                                    Ticker = reader["Ticker"] as string,
                                    Src = reader["PriceSource"] as string,
                                    RTFlag = reader.GetInt16("RealTimeFlag")
                                };

                                if (!securityPriceMasterDict.ContainsKey(securityPrice.Ticker))
                                    securityPriceMasterDict.Add(securityPrice.Ticker, securityPrice);
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

            return securityPriceMasterDict;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="filePath"></param>
        /// <param name="tableName"></param>
        public void SavePricesToStgTable(string filePath, string tableName)
        {
            try
            {
                _logger.LogDebug("Before MySqlBulkLoader...");

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    var bl = new MySqlBulkLoader(connection)
                    {
                        TableName = tableName,
                        Timeout = 600,
                        FieldTerminator = ",",
                        LineTerminator = "\r\n",
                        FileName = filePath,
                        NumberOfLinesToSkip = 1,
                        Columns = { "Ticker", "LastPrice", "BidPrice", "AskPrice", "PrevClosePrice", "DividendAmount", "TotalReturn", "Source", "ExDividendAmount", "ExDividendDate", "TradeDate" }
                    };

                    _logger.LogDebug("After MySqlBulkLoader...");

                    int numberOfInsertedRows = bl.Load();

                    _logger.LogDebug("DONE MySqlBulkLoader...");
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
        /// <param name="securityPriceDict"></param>
        public void SavePricesToStgTableNew(IDictionary<string, SecurityPrice> securityPriceDict)
        {
            try
            {
                StringBuilder sCommand = new StringBuilder("insert into almitasc_ACTradingBBGLink.StgRTPriceLatest " +
                    "(Ticker, LastPrice, BidPrice, AskPrice, PrevClosePrice, TotalReturn, NetChange, Source, DividendAmount, TradeDate, TradeTime, PriceChange, MidPrice, LastPriceAct) values ");

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    List<string> Rows = new List<string>();
                    StringBuilder sb = new StringBuilder();
                    foreach (KeyValuePair<string, SecurityPrice> kvp in securityPriceDict)
                    {
                        SecurityPrice data = kvp.Value;

                        //Ticker
                        if (!string.IsNullOrEmpty(data.Ticker))
                            sb.Append(string.Concat("'", data.Ticker, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //LastPrice
                        if (data.LastPrc.HasValue)
                            sb.Append(data.LastPrc).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //BidPrice
                        if (data.BidPrc.HasValue)
                            sb.Append(data.BidPrc).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //AskPrice
                        if (data.AskPrc.HasValue)
                            sb.Append(data.AskPrc).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //PreviousClosePrice
                        if (data.ClsPrc.HasValue)
                            sb.Append(data.ClsPrc).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //DailyReturn
                        if (data.PrcRtn.HasValue)
                            sb.Append(data.PrcRtn).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //NetPriceChange
                        if (data.NetPrcChng.HasValue)
                            sb.Append(data.NetPrcChng).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //Source
                        if (!string.IsNullOrEmpty(data.Src))
                            sb.Append(string.Concat("'", data.Src, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //DividendPayment
                        if (data.DvdAmt.HasValue)
                            sb.Append(data.DvdAmt).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //TradeDate
                        if (data.TrdDt.HasValue)
                            sb.Append(string.Concat("'", DateUtils.ConvertDate(data.TrdDt, "yyyy-MM-dd"), "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //TradeTime
                        if (!string.IsNullOrEmpty(data.TrdTm))
                            sb.Append(string.Concat("'", data.TrdTm, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //PriceChange
                        if (data.PrcChng.HasValue)
                            sb.Append(data.PrcChng).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //MidPrice
                        if (data.MidPrc.HasValue)
                            sb.Append(data.MidPrc).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //LastPriceAct
                        if (data.LastPrcAct.HasValue)
                            sb.Append(data.LastPrcAct);
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
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving security prices into database");
                throw;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void MovePricesToTgtTable()
        {
            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetPopulatePricesQuery, connection))
                    {
                        command.CommandType = CommandType.StoredProcedure;
                        command.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error moving prices to target table");
                throw;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tableName"></param>
        public void TruncateTable(string tableName)
        {
            try
            {
                string sql = "TRUNCATE TABLE " + tableName;

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        //_logger.LogInformation("Truncating table.... " + tableName);
                        command.ExecuteNonQuery();
                        //_logger.LogInformation("Truncated table.... " + tableName);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error truncating table");
                throw;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IDictionary<string, string> GetPriceTickerMap()
        {
            IDictionary<string, string> dict = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetSecurityPriceTickerMapQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                string inputTicker = reader["InputIdentifier"] as string;
                                string sourceTicker = reader["SourceTicker"] as string;
                                if (!dict.ContainsKey(inputTicker))
                                    dict.Add(inputTicker, sourceTicker);
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
        public IList<SecurityPriceDetail> GetSecurityPriceDetails()
        {
            IList<SecurityPriceDetail> securityPriceList = new List<SecurityPriceDetail>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetSecurityPriceDetailsQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                SecurityPriceDetail securityPrice = new SecurityPriceDetail
                                {
                                    Ticker = reader["InputIdentifier"] as string,
                                    SourceTicker = reader["SourceIdentifier"] as string,
                                    PreviousClosePrice = (reader.IsDBNull(reader.GetOrdinal("PrevClosePrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PrevClosePrice")),
                                    LastPrice = (reader.IsDBNull(reader.GetOrdinal("LastPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LastPrice")),
                                    BidPrice = (reader.IsDBNull(reader.GetOrdinal("BidPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BidPrice")),
                                    AskPrice = (reader.IsDBNull(reader.GetOrdinal("AskPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AskPrice")),
                                    //MidPrice = (reader.IsDBNull(reader.GetOrdinal("MidPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MidPrice")),
                                    DividendPayment = (reader.IsDBNull(reader.GetOrdinal("DividendAmount"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DividendAmount")),
                                    NetPriceChange = (reader.IsDBNull(reader.GetOrdinal("NetChange"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NetChange")),
                                    DailyReturn = (reader.IsDBNull(reader.GetOrdinal("TotalReturn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotalReturn")),
                                    Source = reader["PriceSource"] as string,
                                    RealTimeFlag = reader["RealTime"] as string,
                                    TradeTime = reader["TradeTime"] as string,
                                    TradeDate = (reader.IsDBNull(reader.GetOrdinal("TradeDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("TradeDate")),
                                };

                                securityPrice.TradeDateAsString = DateUtils.ConvertDate(securityPrice.TradeDate, "yyyy-MM-dd");
                                securityPriceList.Add(securityPrice);
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

            return securityPriceList;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IDictionary<string, FXRate> GetLatestFXRates()
        {
            IDictionary<string, FXRate> dict = new Dictionary<string, FXRate>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetLatestFXRatesQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FXRate data = new FXRate
                                {
                                    Ticker = reader["Ticker"] as string,
                                    BaseCurrency = reader["BaseCurrency"] as string,
                                    TargetCurrency = reader["TargetCurrency"] as string,
                                    LastPrice = (reader.IsDBNull(reader.GetOrdinal("LastPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LastPrice")),
                                    QuoteFactor = (reader.IsDBNull(reader.GetOrdinal("QuoteFactor"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("QuoteFactor")),
                                    TradeTime = reader["TradeTime"] as string,
                                    TradeDate = (reader.IsDBNull(reader.GetOrdinal("TradeDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("TradeDate"))
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
        /// <param name="fxRateDict"></param>
        public void SaveFXRatesToStg(IDictionary<string, FXRate> fxRateDict)
        {
            try
            {
                StringBuilder sCommand = new StringBuilder("insert into almitasc_ACTradingBBGLink.StgFXRateLive " +
                    "(Ticker, LastPrice, Source, TradeDate, TradeTime) values ");

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    List<string> Rows = new List<string>();
                    StringBuilder sb = new StringBuilder();
                    foreach (KeyValuePair<string, FXRate> kvp in fxRateDict)
                    {
                        FXRate data = kvp.Value;

                        if (!string.IsNullOrEmpty(data.Ticker))
                            sb.Append(string.Concat("'", data.Ticker, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        if (data.LastPrice.HasValue)
                            sb.Append(data.LastPrice).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        if (!string.IsNullOrEmpty(data.Source))
                            sb.Append(string.Concat("'", data.Source, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        if (data.TradeDate.HasValue)
                            sb.Append(string.Concat("'", DateUtils.ConvertDate(data.TradeDate, "yyyy-MM-dd"), "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        if (!string.IsNullOrEmpty(data.TradeTime))
                            sb.Append(string.Concat("'", data.TradeTime, "'"));
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
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving fx rates into database");
                throw;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void MoveFXRatesToTgtTable()
        {
            using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
            {
                connection.Open();

                try
                {
                    using (MySqlCommand command = new MySqlCommand(GetPopulateFXRatesQuery, connection))
                    {
                        try
                        {
                            command.CommandType = CommandType.StoredProcedure;
                            command.ExecuteNonQuery();
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "Error moving fx rates to target table");
                            throw;
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error moving fx rates to target table");
                    throw;
                }
            }
        }

        /// <summary>
        /// Move Data from Staging table to Target table
        /// </summary>
        /// <param name="procedureName"></param>
        public void MoveDataToTargetTable(string procedureName)
        {
            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(procedureName, connection))
                    {
                        command.CommandType = CommandType.StoredProcedure;
                        command.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error moving data to target table");
                throw;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="securityPriceDict"></param>
        public void SaveDailyPrices(IDictionary<string, SecurityPrice> securityPriceDict)
        {
            try
            {
                TruncateTable(StgDailyPriceHistTableName);
                SaveDailyPricesStg(securityPriceDict);
                MoveDataToTargetTable(SaveDailyPricesQuery);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving data");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="securityPriceDict"></param>
        public void SaveDailyPricesStg(IDictionary<string, SecurityPrice> securityPriceDict)
        {
            try
            {
                StringBuilder sCommand = new StringBuilder("insert into almitasc_ACTradingBBGLink.StgDailyPriceHist" +
                    " (Ticker, EffectiveDate, LastPrice, BidPrice, AskPrice, PrevClosePrice, TotalReturn, NetChange, Source, DividendAmount, TradeDate, TradeTime, PriceChange) values ");

                DateTime effectiveDate = TodaysDate;
                string effectiveDateAsString = DateUtils.ConvertDate(effectiveDate, DATEFORMAT, "0000-00-00");

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlTransaction trans = connection.BeginTransaction())
                    {
                        //_logger.LogInformation("Deleting data from almitasc_ACTradingBBGLink.StgDailyPriceHist where EffectiveDate = '" + effectiveDateAsString + "'");
                        //string sqlDelete = "delete from almitasc_ACTradingBBGLink.StgDailyPriceHist where EffectiveDate = '" + effectiveDateAsString + "'";
                        //using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                        //{
                        //    command.ExecuteNonQuery();
                        //}

                        List<string> Rows = new List<string>();
                        StringBuilder sb = new StringBuilder();
                        foreach (KeyValuePair<string, SecurityPrice> kvp in securityPriceDict)
                        {
                            SecurityPrice data = kvp.Value;

                            //Ticker
                            if (!string.IsNullOrEmpty(data.Ticker))
                                sb.Append(string.Concat("'", data.Ticker, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //EffectiveDate
                            sb.Append(string.Concat("'", effectiveDateAsString, "'")).Append(DELIMITER);

                            //LastPrice
                            if (data.LastPrc.HasValue)
                                sb.Append(data.LastPrc).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //BidPrice
                            if (data.BidPrc.HasValue)
                                sb.Append(data.BidPrc).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //AskPrice
                            if (data.AskPrc.HasValue)
                                sb.Append(data.AskPrc).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //PreviousClosePrice
                            if (data.ClsPrc.HasValue)
                                sb.Append(data.ClsPrc).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //DailyReturn
                            if (data.PrcRtn.HasValue)
                                sb.Append(data.PrcRtn).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //NetPriceChange
                            if (data.NetPrcChng.HasValue)
                                sb.Append(data.NetPrcChng).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //Source
                            if (!string.IsNullOrEmpty(data.Src))
                                sb.Append(string.Concat("'", data.Src, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //DividendPayment
                            if (data.DvdAmt.HasValue)
                                sb.Append(data.DvdAmt).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //TradeDate
                            if (data.TrdDt.HasValue)
                                sb.Append(string.Concat("'", DateUtils.ConvertDate(data.TrdDt, "yyyy-MM-dd"), "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //TradeTime
                            if (!string.IsNullOrEmpty(data.TrdTm))
                                sb.Append(string.Concat("'", data.TrdTm, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //PriceChange
                            if (data.PrcChng.HasValue)
                                sb.Append(data.PrcChng);
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

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IDictionary<string, string> GetNVPriceTickerMap()
        {
            IDictionary<string, string> dict = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetNVSecurityPriceTickerMapQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                string inputTicker = reader["Ticker"] as string;
                                string sourceTicker = reader["SourceTicker"] as string;
                                if (!dict.ContainsKey(inputTicker))
                                    dict.Add(inputTicker, sourceTicker);
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
        public void PopulateNeovestOptionSymbols(IList<Option> list)
        {
            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    foreach (Option data in list)
                    {
                        if (!string.IsNullOrEmpty(data.NeovestSymbol))
                        {
                            string sqlUpdate = "update almitasc_ACTradingBBGLink.SecurityPriceMstRT set NVSymbol = '" + data.NeovestSymbol + "' where InputIdentifier = '" + data.BBGSymbol + "'";
                            using (MySqlCommand command = new MySqlCommand(sqlUpdate, connection))
                            {
                                command.ExecuteNonQuery();
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
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IList<Option> GetOptionSymbols()
        {
            IList<Option> list = new List<Option>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetOptionSymbolsQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                Option data = new Option
                                {
                                    BBGSymbol = reader["BBGSymbol"] as string,
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
        public IList<string> GetSecuritiesByExchange()
        {
            IList<string> list = new List<string>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetSecuritiesByExchangeQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                string ticker = reader["Ticker"] as string;
                                if (!list.Contains(ticker))
                                    list.Add(ticker);
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

        public void SaveMonthEndPrices(IList<MonthEndSecurityPrice> list)
        {
            try
            {
                StringBuilder sCommand = new StringBuilder("insert into almitasc_ACTradingBBGLink.MEPriceHist " +
                    "(FundName, EffectiveDate, Ticker, ALMTicker, Cusip, Sedol, RedemptionDate, Qty, MEPrice, AdjPrice, FeePct, FeeAmt, FxRate, MV, NavMV) values ");

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    List<string> Rows = new List<string>();
                    StringBuilder sb = new StringBuilder();
                    foreach (MonthEndSecurityPrice data in list)
                    {
                        //FundName
                        if (!string.IsNullOrEmpty(data.FundName))
                            sb.Append(string.Concat("'", data.FundName, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //EffectiveDate
                        if (!string.IsNullOrEmpty(data.EffectiveDateAsString))
                            sb.Append(string.Concat("'", data.EffectiveDateAsString, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //Ticker
                        if (!string.IsNullOrEmpty(data.Ticker))
                            sb.Append(string.Concat("'", data.Ticker, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //ALMTicker
                        if (!string.IsNullOrEmpty(data.ALMTicker))
                            sb.Append(string.Concat("'", data.ALMTicker, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //Cusip
                        if (!string.IsNullOrEmpty(data.Cusip))
                            sb.Append(string.Concat("'", data.Cusip, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //Sedol
                        if (!string.IsNullOrEmpty(data.Sedol))
                            sb.Append(string.Concat("'", data.Sedol, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //RedemptionDateAsString
                        if (!string.IsNullOrEmpty(data.RedemptionDateAsString))
                            sb.Append(string.Concat("'", data.RedemptionDateAsString, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //Qty
                        if (data.Qty.HasValue)
                            sb.Append(data.Qty).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //MEPrice
                        if (data.MonthEndPrice.HasValue)
                            sb.Append(data.MonthEndPrice).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //AdjPrice
                        if (data.AdjPrice.HasValue)
                            sb.Append(data.AdjPrice).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //FeePct
                        if (data.FeePct.HasValue)
                            sb.Append(data.FeePct).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //FeeAmt
                        if (data.FeeAmt.HasValue)
                            sb.Append(data.FeeAmt).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //FxRate
                        if (data.FxRate.HasValue)
                            sb.Append(data.FxRate).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //MV
                        if (data.MV.HasValue)
                            sb.Append(data.MV).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //NavMV
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

                    using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), connection))
                    {
                        myCmd.CommandType = CommandType.Text;
                        myCmd.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving security prices into database");
                throw;
            }
        }

        public IDictionary<string, SharesImbalance> GetSharesImbalanceList()
        {
            IDictionary<string, SharesImbalance> dict = new Dictionary<string, SharesImbalance>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetSharesImbalanceQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                SharesImbalance data = new SharesImbalance
                                {
                                    Ticker = reader["Ticker"] as string,
                                    BBGTicker = reader["BBGTicker"] as string,
                                    ALMTicker = reader["ALMTicker"] as string,
                                    Paired = (reader.IsDBNull(reader.GetOrdinal("SharesPaired"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("SharesPaired")),
                                    Imbalance = (reader.IsDBNull(reader.GetOrdinal("SharesImbalance"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("SharesImbalance")),
                                    ImbalanceSide = reader["ImbalanceSide"] as string,
                                    IsLiveUpdate = reader["IsLiveUpdate"] as string,
                                    DfTime = reader["DfTime"] as string,
                                    CurrentRefPrice = (reader.IsDBNull(reader.GetOrdinal("CurrRefPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CurrRefPrice")),
                                    FarIndPrice = (reader.IsDBNull(reader.GetOrdinal("FarIndPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FarIndPrice")),
                                    NearIndPrice = (reader.IsDBNull(reader.GetOrdinal("NearIndPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NearIndPrice")),
                                    NumIndPrice = (reader.IsDBNull(reader.GetOrdinal("NumIndPrice"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("NumIndPrice")),
                                    PriceVar = (reader.IsDBNull(reader.GetOrdinal("PriceVar"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceVar")),
                                    Avg20Vol = (reader.IsDBNull(reader.GetOrdinal("Avg20Vol"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Avg20Vol")),
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

        public void SaveSharesImbalanceList(IDictionary<string, SharesImbalance> dict)
        {
            try
            {
                StringBuilder sCommand = new StringBuilder("insert into almitasc_ACTradingBBGLink.StgSharesImbalanceMst"
                    + "(Ticker, BBGTicker, ALMTicker, SharesPaired, SharesImbalance, ImbalanceSide, CurrRefPrice, FarIndPrice, NearIndPrice"
                    + ", NumIndPrice, PriceVar, IsLiveUpdate, DfTime, Avg20Vol) values ");

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlTransaction trans = connection.BeginTransaction())
                    {
                        string sqlDelete = "delete from almitasc_ACTradingBBGLink.StgSharesImbalanceMst";
                        using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                        {
                            _logger.LogInformation("deleting data from almitasc_ACTradingBBGLink.StgSharesImbalanceMst");
                            command.ExecuteNonQuery();
                        }

                        List<string> Rows = new List<string>();
                        StringBuilder sb = new StringBuilder();
                        foreach (KeyValuePair<string, SharesImbalance> kvp in dict)
                        {
                            SharesImbalance data = kvp.Value;

                            // Ticker
                            if (!string.IsNullOrEmpty(data.Ticker))
                                sb.Append(string.Concat("'", data.Ticker, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // BBGTicker
                            if (!string.IsNullOrEmpty(data.BBGTicker))
                                sb.Append(string.Concat("'", data.BBGTicker, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // ALMTicker
                            if (!string.IsNullOrEmpty(data.ALMTicker))
                                sb.Append(string.Concat("'", data.ALMTicker, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // SharesPaired
                            if (data.Paired.HasValue)
                                sb.Append(data.Paired).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // SharesImbalance
                            if (data.Imbalance.HasValue)
                                sb.Append(data.Imbalance).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // ImbalanceSide
                            if (!string.IsNullOrEmpty(data.ImbalanceSide))
                                sb.Append(string.Concat("'", data.ImbalanceSide, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // CurrRefPrice
                            if (data.CurrentRefPrice.HasValue)
                                sb.Append(data.CurrentRefPrice).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // FarIndPrice
                            if (data.FarIndPrice.HasValue)
                                sb.Append(data.FarIndPrice).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // NearIndPrice
                            if (data.NearIndPrice.HasValue)
                                sb.Append(data.NearIndPrice).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // NumIndPrice
                            if (data.NumIndPrice.HasValue)
                                sb.Append(data.NumIndPrice).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // PriceVar
                            if (data.PriceVar.HasValue)
                                sb.Append(data.PriceVar).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // IsLiveUpdate
                            if (!string.IsNullOrEmpty(data.IsLiveUpdate))
                                sb.Append(string.Concat("'", data.IsLiveUpdate, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // DfTime
                            if (!string.IsNullOrEmpty(data.DfTime))
                                sb.Append(string.Concat("'", data.DfTime, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // Avg20Vol
                            if (data.Avg20Vol.HasValue)
                                sb.Append(data.Avg20Vol);
                            else
                                sb.Append("null");

                            string row = sb.ToString();
                            Rows.Add(string.Concat("(", row, ")"));
                            sb.Clear();
                        }

                        sCommand.Append(string.Join(",", Rows));
                        sCommand.Append(";");

                        _logger.LogDebug("insert into almitasc_ACTradingBBGLink.StgSharesImbalanceMst");
                        using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), connection))
                        {
                            myCmd.CommandType = CommandType.Text;
                            myCmd.ExecuteNonQuery();
                        }

                        _logger.LogInformation("Moving data to almitasc_ACTradingBBGLink.SharesImbalanceMst");
                        string sql = "almitasc_ACTradingBBGLink.spPopulateSharesImbalanceData";
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
                _logger.LogError(ex, "Error saving Security Imbalance data into database");
                throw;
            }
        }

        public void CalculateFXReturns()
        {
            using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
            {
                connection.Open();
                try
                {
                    using (MySqlCommand command = new MySqlCommand(CalculateFXReturnsQuery, connection))
                    {
                        try
                        {
                            command.CommandType = CommandType.StoredProcedure;
                            command.ExecuteNonQuery();
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "Error calculating fx returns");
                            throw;
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error calculating fx returns");
                    throw;
                }
            }
        }

        private const string LATEST_QUERY = "call almitasc_ACTradingBBGLink.spGetLatestPrices";
        private const string NV_PRICE_TICKER_MAP_QUERY = "select distinct replace(ifnull(PriceLookupIdentifier, InputIdentifier), ' Equity', '') as Ticker,"
            + " ifnull(PriceLookupIdentifier, InputIdentifier) as SourceTicker"
            + " from almitasc_ACTradingBBGLink.SecurityPriceMstRT"
            + " where InputIdentifier is not null and length(InputIdentifier) > 0";
        private const string PRICE_TICKER_MAP_QUERY = "select distinct InputIdentifier, ifnull(PriceLookupIdentifier, InputIdentifier) as SourceTicker"
            + " from almitasc_ACTradingBBGLink.SecurityPriceMstRT"
            + " where InputIdentifier is not null and length(InputIdentifier) > 0";

        private const string GetLatestPricesQuery = "call almitasc_ACTradingBBGLink.spGetLatestPrices";
        private const string GetSecurityPriceMasterQuery = "select ifnull(PriceLookupIdentifier, InputIdentifier) as Ticker, (case when ifnull(s.RealTime,'N') = 'N' then 0 else 1 end) as RealTimeFlag, s.* from almitasc_ACTradingBBGLink.SecurityPriceMstRT s where ifnull(s.RealTime,'N') = 'Y'";
        private const string GetSecurityPriceTickerMapQuery = "select distinct InputIdentifier, ifnull(PriceLookupIdentifier, InputIdentifier) as SourceTicker from almitasc_ACTradingBBGLink.SecurityPriceMstRT where InputIdentifier is not null and length(InputIdentifier) > 0";
        private const string GetNVSecurityPriceTickerMapQuery = "select distinct replace(ifnull(PriceLookupIdentifier, InputIdentifier), ' Equity', '') as Ticker, ifnull(PriceLookupIdentifier, InputIdentifier) as SourceTicker from almitasc_ACTradingBBGLink.SecurityPriceMstRT where InputIdentifier is not null and length(InputIdentifier) > 0";
        private const string GetSecurityPriceDetailsQuery = "select s.InputIdentifier, s.PriceLookupIdentifier as SourceIdentifier, s.PriceExchangeId, s.PriceSource, s.RealTime, r.LastPrice, r.BidPrice, r.AskPrice, r.PrevClosePrice, r.DividendAmount, r.NetChange, r.TotalReturn, r.TradeDate, r.TradeTime from almitasc_ACTradingBBGLink.SecurityPriceMstRT s left join almitasc_ACTradingBBGLink.RTPriceLatest r on (s.PriceLookupIdentifier = r.Ticker) order by s.InputIdentifier";
        private const string GetLatestFXRatesQuery = "select l.Ticker, left(m.Ticker, 3) as BaseCurrency, right(m.Ticker, 3) as TargetCurrency, m.QuoteFactor, l.LastPrice, l.TradeDate, l.TradeTime from almitasc_ACTradingBBGLink.FXRateLive l left join almitasc_ACTradingBBGLink.FXRateMst m on (l.Ticker = m.Ticker)";
        private const string GetPopulateFXRatesQuery = "spPopulateLiveFXRates";
        private const string GetPopulatePricesQuery = "spPopulateNeovestPrices";
        private const string GetOptionSymbolsQuery = "select InputIdentifier as BBGSymbol, NVSymbol from almitasc_ACTradingBBGLink.SecurityPriceMstRT where trim(InputIdentifier) like '% US %' or trim(InputIdentifier) like '% CN %'";
        private const string GetSecuritiesByExchangeQuery = "call almitasc_ACTradingBBGData.spGetSecuritiesByExchange";
        private const string GetSharesImbalanceQuery = "select * from almitasc_ACTradingBBGLink.SharesImbalanceMst";
        private const string StgDailyPriceHistTableName = "almitasc_ACTradingBBGLink.StgDailyPriceHist";
        private const string SaveDailyPricesQuery = "almitasc_ACTradingBBGLink.spPopulateDailyPrices";
        private const string CalculateFXReturnsQuery = "spCalcFXReturns";
    }
}
