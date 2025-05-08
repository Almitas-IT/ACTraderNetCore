using aACTrader.DAO.Interface;
using aCommons.Trading;
using aCommons.Utils;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace aACTrader.DAO.Repository
{
    public class EMSXDao : IEMSXDao
    {
        private readonly ILogger<EMSXDao> _logger;
        private readonly IConfiguration _configuration;

        private const string DELIMITER = ",";

        public EMSXDao(ILogger<EMSXDao> logger, IConfiguration configuration)
        {
            _logger = logger;
            this._configuration = configuration;
            _logger.LogInformation("Initializing EMSXDao...");
        }

        public IList<NewOrder> GetBatchOrders()
        {
            IList<NewOrder> list = new List<NewOrder>();

            try
            {
                string sql = GetBatchOrdersQuery;
                if (_configuration["ConnectionStrings:ENV"].Equals("DEV"))
                    sql = GetBatchOrdersDevQuery;

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetBatchOrdersQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                NewOrder data = new NewOrder();
                                data.ActionType = reader["ActionType"] as string;
                                data.Id = reader["Id"] as string;
                                data.MainOrderId = reader["MainOrderId"] as string;
                                data.OrderId = reader["OrderId"] as string;
                                data.Symbol = reader["Ticker"] as string;
                                data.NeovestSymbol = reader["NeovestSymbol"] as string;
                                data.Sedol = reader["Sedol"] as string;
                                data.OrderDate = reader.IsDBNull(reader.GetOrdinal("OrderDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("OrderDate"));
                                data.OrderSide = reader["OrderSide"] as string;
                                data.OrderType = reader["OrderType"] as string;
                                data.OrderPrice = (reader.IsDBNull(reader.GetOrdinal("OrderPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OrderPrice"));
                                data.NewOrderPrice = (reader.IsDBNull(reader.GetOrdinal("NewOrderPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NewOrderPrice"));
                                data.OrderLimitPrice = (reader.IsDBNull(reader.GetOrdinal("OrderLimitPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OrderLimitPrice"));
                                data.OrderStopPrice = (reader.IsDBNull(reader.GetOrdinal("OrderStopPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OrderStopPrice"));
                                data.OrderQty = (reader.IsDBNull(reader.GetOrdinal("OrderQuantity"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OrderQuantity"));
                                data.NewOrderQty = (reader.IsDBNull(reader.GetOrdinal("NewOrderQuantity"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NewOrderQuantity"));
                                data.OrderExpiration = (reader.IsDBNull(reader.GetOrdinal("OrderExpiration"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("OrderExpiration"));
                                data.OrderExpire = reader["OrderExpire"] as string;
                                data.AccountId = (reader.IsDBNull(reader.GetOrdinal("AccountId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("AccountId"));
                                data.AccountName = reader["AccountName"] as string;
                                data.OrderExchangeId = (reader.IsDBNull(reader.GetOrdinal("ExchangeId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("ExchangeId"));
                                data.Destination = reader["Destination"] as string;
                                data.BrokerStrategy = reader["BrokerStrategy"] as string;
                                data.UserName = reader["Trader"] as string;
                                data.Environment = reader["Environment"] as string;
                                data.Comments = reader["Comments"] as string;
                                data.AlgoParameters = reader["AlgoParameters"] as string;
                                data.Locate = reader["Locate"] as string;
                                //
                                data.OptionPosition = reader["OptionPosition"] as string;
                                data.OptionEquityPosition = reader["OptionEquityPosition"] as string;
                                data.OptionFill = reader["OptionFill"] as string;
                                //
                                data.RefIndex = reader["RefIndex"] as string;
                                data.RefIndexPrice = (reader.IsDBNull(reader.GetOrdinal("RefIndexPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RefIndexPrice"));
                                data.RefIndexPriceBeta = (reader.IsDBNull(reader.GetOrdinal("RefIndexBeta"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RefIndexBeta"));
                                data.RefIndexPriceBetaShiftInd = (reader.IsDBNull(reader.GetOrdinal("RefIndexBetaInd"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("RefIndexBetaInd"));
                                data.RefIndexBetaAdjType = reader["RefIndexPriceBetaAdj"] as string;
                                data.RefIndexPriceCap = (reader.IsDBNull(reader.GetOrdinal("RefIndexPriceCap"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RefIndexPriceCap"));
                                data.RefIndexPriceCapShiftInd = (reader.IsDBNull(reader.GetOrdinal("RefIndexPriceInd"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("RefIndexPriceInd"));
                                data.RefIndexMaxPrice = (reader.IsDBNull(reader.GetOrdinal("RefIndexMaxPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RefIndexMaxPrice"));
                                data.RefIndexPriceType = reader["RefIndexPriceType"] as string;
                                //
                                data.DiscountTarget = (reader.IsDBNull(reader.GetOrdinal("DiscountTarget"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DiscountTarget"));
                                data.AutoUpdate = reader["AutoUpdate"] as string;
                                data.AutoUpdateThreshold = (reader.IsDBNull(reader.GetOrdinal("AutoUpdateThreshold"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AutoUpdateThreshold"));
                                data.MarketPriceThreshold = (reader.IsDBNull(reader.GetOrdinal("MarketPriceThreshold"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarketPriceThreshold"));
                                data.MarketPriceField = reader["MarketPriceField"] as string;
                                data.EstNavType = reader["EstNavType"] as string;
                                // Pair Trades
                                data.ParentId = reader["ParentId"] as string;
                                data.PairRatio = (reader.IsDBNull(reader.GetOrdinal("PairRatio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PairRatio"));
                                data.PairSpread = (reader.IsDBNull(reader.GetOrdinal("PairSpread"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PairSpread"));

                                if (!string.IsNullOrEmpty(data.ParentId))
                                    data.IsPairTrade = "Y";
                                else
                                    data.IsPairTrade = "N";

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

        public IList<NewOrder> GetSimBatchOrders()
        {
            IList<NewOrder> list = new List<NewOrder>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetBatchOrdersQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                NewOrder data = new NewOrder();
                                data.ActionType = reader["ActionType"] as string;
                                data.Id = reader["Id"] as string;
                                data.MainOrderId = reader["MainOrderId"] as string;
                                data.OrderId = reader["OrderId"] as string;
                                data.Symbol = reader["Ticker"] as string;
                                data.NeovestSymbol = reader["NeovestSymbol"] as string;
                                data.Sedol = reader["Sedol"] as string;
                                data.OrderDate = reader.IsDBNull(reader.GetOrdinal("OrderDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("OrderDate"));
                                data.OrderSide = reader["OrderSide"] as string;
                                data.OrderType = reader["OrderType"] as string;
                                data.OrderPrice = (reader.IsDBNull(reader.GetOrdinal("OrderPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OrderPrice"));
                                data.NewOrderPrice = (reader.IsDBNull(reader.GetOrdinal("NewOrderPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NewOrderPrice"));
                                data.OrderLimitPrice = (reader.IsDBNull(reader.GetOrdinal("OrderLimitPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OrderLimitPrice"));
                                data.OrderStopPrice = (reader.IsDBNull(reader.GetOrdinal("OrderStopPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OrderStopPrice"));
                                data.OrderQty = (reader.IsDBNull(reader.GetOrdinal("OrderQuantity"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OrderQuantity"));
                                data.NewOrderQty = (reader.IsDBNull(reader.GetOrdinal("NewOrderQuantity"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NewOrderQuantity"));
                                data.OrderExpiration = (reader.IsDBNull(reader.GetOrdinal("OrderExpiration"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("OrderExpiration"));
                                data.OrderExpire = reader["OrderExpire"] as string;
                                data.AccountId = (reader.IsDBNull(reader.GetOrdinal("AccountId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("AccountId"));
                                data.AccountName = reader["AccountName"] as string;
                                data.OrderExchangeId = (reader.IsDBNull(reader.GetOrdinal("ExchangeId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("ExchangeId"));
                                data.Destination = reader["Destination"] as string;
                                data.BrokerStrategy = reader["BrokerStrategy"] as string;
                                data.UserName = reader["Trader"] as string;
                                data.Environment = reader["Environment"] as string;
                                data.Comments = reader["Comments"] as string;
                                data.AlgoParameters = reader["AlgoParameters"] as string;
                                data.Locate = reader["Locate"] as string;
                                //
                                data.OptionPosition = reader["OptionPosition"] as string;
                                data.OptionEquityPosition = reader["OptionEquityPosition"] as string;
                                data.OptionFill = reader["OptionFill"] as string;
                                //
                                data.RefIndex = reader["RefIndex"] as string;
                                data.RefIndexPrice = (reader.IsDBNull(reader.GetOrdinal("RefIndexPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RefIndexPrice"));
                                data.RefIndexPriceBeta = (reader.IsDBNull(reader.GetOrdinal("RefIndexBeta"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RefIndexBeta"));
                                data.RefIndexPriceBetaShiftInd = (reader.IsDBNull(reader.GetOrdinal("RefIndexBetaInd"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("RefIndexBetaInd"));
                                data.RefIndexBetaAdjType = reader["RefIndexPriceBetaAdj"] as string;
                                data.RefIndexPriceCap = (reader.IsDBNull(reader.GetOrdinal("RefIndexPriceCap"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RefIndexPriceCap"));
                                data.RefIndexPriceCapShiftInd = (reader.IsDBNull(reader.GetOrdinal("RefIndexPriceInd"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("RefIndexPriceInd"));
                                data.RefIndexMaxPrice = (reader.IsDBNull(reader.GetOrdinal("RefIndexMaxPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RefIndexMaxPrice"));
                                data.RefIndexPriceType = reader["RefIndexPriceType"] as string;
                                //
                                data.DiscountTarget = (reader.IsDBNull(reader.GetOrdinal("DiscountTarget"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DiscountTarget"));
                                data.AutoUpdate = reader["AutoUpdate"] as string;
                                data.AutoUpdateThreshold = (reader.IsDBNull(reader.GetOrdinal("AutoUpdateThreshold"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AutoUpdateThreshold"));
                                data.MarketPriceThreshold = (reader.IsDBNull(reader.GetOrdinal("MarketPriceThreshold"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarketPriceThreshold"));
                                data.MarketPriceField = reader["MarketPriceField"] as string;
                                data.EstNavType = reader["EstNavType"] as string;
                                // Pair Trades
                                data.ParentId = reader["ParentId"] as string;
                                data.PairRatio = (reader.IsDBNull(reader.GetOrdinal("PairRatio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PairRatio"));
                                data.PairSpread = (reader.IsDBNull(reader.GetOrdinal("PairSpread"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PairSpread"));

                                if (!string.IsNullOrEmpty(data.ParentId))
                                    data.IsPairTrade = "Y";
                                else
                                    data.IsPairTrade = "N";

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

        public IList<BatchOrderTemplate> GetBatchTemplate(string templateName)
        {
            IList<BatchOrderTemplate> list = new List<BatchOrderTemplate>();

            try
            {
                string sql = GetBatchTemplateQuery + " where TemplateName = '" + templateName + "' order by RowId asc";
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                BatchOrderTemplate data = new BatchOrderTemplate();
                                data.TemplateName = reader["TemplateName"] as string;
                                data.Symbol = reader["Symbol"] as string;
                                data.OrderSide = reader["OrderSide"] as string;
                                data.OrderType = reader["OrderType"] as string;
                                data.AccountNumber = reader["AccountNumber"] as string;
                                data.Destination = reader["Destination"] as string;
                                data.BrokerStrategy = reader["BrokerStrategy"] as string;
                                data.OrderExpire = reader["OrderExpire"] as string;
                                data.Qty = (reader.IsDBNull(reader.GetOrdinal("Qty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Qty"));
                                data.TotalTradeValue = (reader.IsDBNull(reader.GetOrdinal("TotalTradeValue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotalTradeValue"));
                                data.PriceTarget = (reader.IsDBNull(reader.GetOrdinal("PriceTarget"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceTarget"));
                                data.DiscountTarget = (reader.IsDBNull(reader.GetOrdinal("DiscountTarget"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DiscountTarget"));
                                data.DiscountTargetAdj = (reader.IsDBNull(reader.GetOrdinal("DiscountTargetAdj"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DiscountTargetAdj"));
                                data.ChooseNav = reader["ChooseNav"] as string;
                                data.EstNavType = reader["EstNavType"] as string;
                                data.NavOvr = (reader.IsDBNull(reader.GetOrdinal("NavOvr"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NavOvr"));
                                data.Locate = reader["Locate"] as string;
                                data.APExpire = reader["APExpire"] as string;
                                data.APStartDate = reader["APStartDate"] as string;
                                data.APEndDate = reader["APEndDate"] as string;
                                data.APUrgency = reader["APUrgency"] as string;
                                data.APMaxPctVol = (reader.IsDBNull(reader.GetOrdinal("APMaxPctVol"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("APMaxPctVol"));
                                data.APWouldPrice = (reader.IsDBNull(reader.GetOrdinal("APWouldPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("APWouldPrice"));
                                data.APMinDarkFillSize = (reader.IsDBNull(reader.GetOrdinal("APMinDarkFillSize"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("APMinDarkFillSize"));
                                data.APDisplayMode = reader["APDisplayMode"] as string;
                                data.APDisplaySize = (reader.IsDBNull(reader.GetOrdinal("APDisplaySize"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("APDisplaySize"));
                                data.APLiquiditySeek = reader["APLiquiditySeek"] as string;
                                data.APSeekPrice = (reader.IsDBNull(reader.GetOrdinal("APSeekPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("APSeekPrice"));
                                data.APSeekMinQty = (reader.IsDBNull(reader.GetOrdinal("APSeekMinQty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("APSeekMinQty"));
                                data.APPctOfVolume = (reader.IsDBNull(reader.GetOrdinal("APPctOfVolume"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("APPctOfVolume"));

                                data.RefIndex = reader["RefIndex"] as string;
                                data.RefIndexPriceType = reader["RefIndexPriceType"] as string;
                                data.RefIndexBetaAdjType = reader["RefIndexPriceBetaAdj"] as string;
                                data.RefIndexPriceBeta = (reader.IsDBNull(reader.GetOrdinal("RefIndexPriceBeta"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RefIndexPriceBeta"));
                                data.RefIndexPriceCap = (reader.IsDBNull(reader.GetOrdinal("RefIndexPriceCap"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RefIndexPriceCap"));
                                data.RefIndexPriceBetaShiftInd = (reader.IsDBNull(reader.GetOrdinal("RefIndexPriceBetaShiftInd"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("RefIndexPriceBetaShiftInd"));
                                data.RefIndexPriceCapShiftInd = (reader.IsDBNull(reader.GetOrdinal("RefIndexPriceCapShiftInd"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("RefIndexPriceCapShiftInd"));
                                data.RefIndexMaxPrice = (reader.IsDBNull(reader.GetOrdinal("RefIndexMaxPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RefIndexMaxPrice"));

                                data.OptionPosition = reader["OptionPosition"] as string;
                                data.OptionEquityPosition = reader["OptionEquityPosition"] as string;
                                data.OptionFill = reader["OptionFill"] as string;

                                data.APTradingStyle = reader["APTradingStyle"] as string;
                                data.APTradingSession = reader["APTradingSession"] as string;
                                data.APSORPreference = reader["APSORPreference"] as string;
                                data.APSORSessionPreference = reader["APSORSessionPreference"] as string;
                                data.APStyle = reader["APStyle"] as string;

                                data.ActionFlag = reader["ActionFlag"] as string;
                                data.AutoUpdateFlag = reader["AutoUpdateFlag"] as string;
                                data.AutoUpdatePriceThreshold = (reader.IsDBNull(reader.GetOrdinal("AutoUpdatePriceThreshold"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AutoUpdatePriceThreshold"));
                                data.MarketPriceThreshold = (reader.IsDBNull(reader.GetOrdinal("MarketPriceThreshold"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarketPriceThreshold"));
                                data.MarketPriceField = reader["MarketPriceField"] as string;

                                if (data.RefIndexPriceBetaShiftInd == 0)
                                    data.RefIndexPriceBetaShiftIndAsString = "Both";
                                else if (data.RefIndexPriceBetaShiftInd == 1)
                                    data.RefIndexPriceBetaShiftIndAsString = "Up";
                                else if (data.RefIndexPriceBetaShiftInd == -1)
                                    data.RefIndexPriceBetaShiftIndAsString = "Down";

                                if (data.RefIndexPriceCapShiftInd == 0)
                                    data.RefIndexPriceCapShiftIndAsString = "Both";
                                else if (data.RefIndexPriceCapShiftInd == 1)
                                    data.RefIndexPriceCapShiftIndAsString = "Up";
                                else if (data.RefIndexPriceCapShiftInd == -1)
                                    data.RefIndexPriceCapShiftIndAsString = "Down";

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

        public IList<string> GetBatchTemplates()
        {
            IList<string> list = new List<string>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetBatchTemplatesQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                string templateName = reader["TemplateName"] as string;
                                list.Add(templateName);
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
        /// Gets Batch Order Templates for User
        /// </summary>
        /// <param name="userName"></param>
        /// <param name="includePairTradeTemplate"></param>
        /// <returns></returns>
        public IList<string> GetBatchTemplatesForUser(string userName, string includePairTradeTemplate)
        {
            IList<string> list = new List<string>();

            try
            {
                string sql = GetBatchTemplatesForUserQuery;
                if (string.IsNullOrEmpty(includePairTradeTemplate))
                    sql += " where IsPairTrade is null";
                else
                    sql += " where IsPairTrade = 'Y'";

                if (!string.IsNullOrEmpty(userName))
                    if (!userName.Equals("All", StringComparison.CurrentCultureIgnoreCase))
                        sql += " and (UserName is null or UserName in ('admin', '" + userName + "'))";
                sql += " order by TemplateName";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                string templateName = reader["TemplateName"] as string;
                                list.Add(templateName);
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

        public void SaveBatchOrders(IList<NewOrder> orderList)
        {
            StringBuilder sCommand = new StringBuilder("INSERT INTO Trading.StgBatchDetail"
                + " (ActionType, Id, MainOrderId, OrderId,"
                + " Ticker, Sedol, OrderSide, OrderType, OrderPrice, NewOrderPrice,"
                + " OrderLimitPrice, OrderStopPrice, OrderQuantity, NewOrderQuantity, OrderExpiration,"
                + " AccountId, AccountName, ExchangeId, Destination, BrokerStrategy, Trader, Environment,"
                + " Comments, AlgoParameters, Locate, OptionPosition, OptionEquityPosition, OptionFill,"
                + " RefIndex, RefIndexPrice, RefIndexBeta, RefIndexBetaInd, RefIndexPriceBetaAdj,"
                + " RefIndexPriceCap, RefIndexPriceInd, RefIndexMaxPrice, DiscountTarget,"
                + " AutoUpdate, AutoUpdateThreshold, RefIndexPriceType, OrderExpire, NeovestSymbol,"
                + " ParentId, PairRatio, PairSpread, MarketPriceThreshold, OrigOrderPrice, MarketPriceField,"
                + " EstNavType)"
                + " values ");

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    if (orderList != null && orderList.Count > 0)
                    {
                        using (MySqlTransaction trans = connection.BeginTransaction())
                        {
                            _logger.LogInformation("Deleting data from Trading.StgBatchDetail");
                            string sqlDelete = "delete from Trading.StgBatchDetail";
                            using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                            {
                                command.ExecuteNonQuery();
                            }

                            List<string> Rows = new List<string>();
                            StringBuilder sb = new StringBuilder();
                            foreach (NewOrder order in orderList)
                            {
                                // ActionType
                                if (!string.IsNullOrEmpty(order.ActionType))
                                    sb.Append(string.Concat("'", order.ActionType, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Id
                                if (!string.IsNullOrEmpty(order.Id))
                                    sb.Append(string.Concat("'", order.Id, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // MainOrderId
                                if (!string.IsNullOrEmpty(order.MainOrderId))
                                    sb.Append(string.Concat("'", order.MainOrderId, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // OrderId
                                if (!string.IsNullOrEmpty(order.OrderId))
                                    sb.Append(string.Concat("'", order.OrderId, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Ticker
                                if (!string.IsNullOrEmpty(order.Symbol))
                                    sb.Append(string.Concat("'", order.Symbol, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Sedol
                                if (!string.IsNullOrEmpty(order.Sedol))
                                    sb.Append(string.Concat("'", order.Sedol, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // OrderSide
                                if (!string.IsNullOrEmpty(order.OrderSide))
                                    sb.Append(string.Concat("'", order.OrderSide, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // OrderType
                                if (!string.IsNullOrEmpty(order.OrderType))
                                    sb.Append(string.Concat("'", order.OrderType, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // OrderPrice
                                if (order.OrderPrice.HasValue)
                                    sb.Append(order.OrderPrice).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // NewOrderPrice
                                if (order.NewOrderPrice.HasValue)
                                    sb.Append(order.NewOrderPrice).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // OrderLimitPrice
                                if (order.OrderLimitPrice.HasValue)
                                    sb.Append(order.OrderLimitPrice).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // OrderStopPrice
                                if (order.OrderStopPrice.HasValue)
                                    sb.Append(order.OrderStopPrice).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // OrderQuantity
                                if (order.OrderQty.HasValue)
                                    sb.Append(order.OrderQty).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // NewOrderQuantity
                                if (order.NewOrderQty.HasValue)
                                    sb.Append(order.NewOrderQty).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // OrderExpiration
                                if (order.OrderExpiration.HasValue)
                                    sb.Append(order.OrderExpiration).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // AccountId
                                if (order.AccountId.HasValue)
                                    sb.Append(order.AccountId).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // AccountName
                                if (!string.IsNullOrEmpty(order.AccountName))
                                    sb.Append(string.Concat("'", order.AccountName, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // ExchangeId
                                if (order.OrderExchangeId.HasValue)
                                    sb.Append(order.OrderExchangeId).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Destination
                                if (!string.IsNullOrEmpty(order.Destination))
                                    sb.Append(string.Concat("'", order.Destination, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // BrokerStrategy
                                if (!string.IsNullOrEmpty(order.BrokerStrategy))
                                    sb.Append(string.Concat("'", order.BrokerStrategy, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Trader
                                if (!string.IsNullOrEmpty(order.UserName))
                                    sb.Append(string.Concat("'", order.UserName, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Environment
                                if (!string.IsNullOrEmpty(order.Environment))
                                    sb.Append(string.Concat("'", order.Environment, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Comments
                                if (!string.IsNullOrEmpty(order.Comments))
                                    sb.Append(string.Concat("'", order.Comments, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // AlgoParameters
                                if (!string.IsNullOrEmpty(order.AlgoParameters))
                                    sb.Append(string.Concat("'", order.AlgoParameters, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Locate
                                if (!string.IsNullOrEmpty(order.Locate))
                                    sb.Append(string.Concat("'", order.Locate, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // OptionPosition
                                if (!string.IsNullOrEmpty(order.OptionPosition))
                                    sb.Append(string.Concat("'", order.OptionPosition, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // OptionEquityPosition
                                if (!string.IsNullOrEmpty(order.OptionEquityPosition))
                                    sb.Append(string.Concat("'", order.OptionEquityPosition, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // OptionFill
                                if (!string.IsNullOrEmpty(order.OptionFill))
                                    sb.Append(string.Concat("'", order.OptionFill, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // RefIndex
                                if (!string.IsNullOrEmpty(order.RefIndex))
                                    sb.Append(string.Concat("'", order.RefIndex, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // RefIndexPrice
                                if (order.RefIndexPrice.HasValue)
                                    sb.Append(order.RefIndexPrice).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // RefIndexBeta
                                if (order.RefIndexPriceBeta.HasValue)
                                    sb.Append(order.RefIndexPriceBeta).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // RefIndexBetaInd
                                if (order.RefIndexPriceBetaShiftInd.HasValue)
                                    sb.Append(order.RefIndexPriceBetaShiftInd).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // RefIndexPriceBetaAdj
                                if (!string.IsNullOrEmpty(order.RefIndexBetaAdjType))
                                    sb.Append(string.Concat("'", order.RefIndexBetaAdjType, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // RefIndexPriceCap
                                if (order.RefIndexPriceCap.HasValue)
                                    sb.Append(order.RefIndexPriceCap).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // RefIndexPriceInd
                                if (order.RefIndexPriceCapShiftInd.HasValue)
                                    sb.Append(order.RefIndexPriceCapShiftInd).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // RefIndexMaxPrice
                                if (order.RefIndexMaxPrice.HasValue)
                                    sb.Append(order.RefIndexMaxPrice).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // DiscountTarget
                                if (order.DiscountTarget.HasValue)
                                    sb.Append(order.DiscountTarget).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // AutoUpdate
                                if (!string.IsNullOrEmpty(order.AutoUpdate))
                                    sb.Append(string.Concat("'", order.AutoUpdate, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // AutoUpdateThreshold
                                if (order.AutoUpdateThreshold.HasValue)
                                    sb.Append(order.AutoUpdateThreshold).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // RefIndexPriceType
                                if (!string.IsNullOrEmpty(order.RefIndexPriceType))
                                    sb.Append(string.Concat("'", order.RefIndexPriceType, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // OrderExpire
                                if (!string.IsNullOrEmpty(order.OrderExpire))
                                    sb.Append(string.Concat("'", order.OrderExpire, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // NeovestSymbol
                                if (!string.IsNullOrEmpty(order.NeovestSymbol))
                                    sb.Append(string.Concat("'", order.NeovestSymbol, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // ParentId
                                if (!string.IsNullOrEmpty(order.ParentId))
                                    sb.Append(string.Concat("'", order.ParentId, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // PairRatio
                                if (order.PairRatio.HasValue)
                                    sb.Append(order.PairRatio).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // PairSpread
                                if (order.PairSpread.HasValue)
                                    sb.Append(order.PairSpread).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // MarketPriceThreshold
                                if (order.MarketPriceThreshold.HasValue)
                                    sb.Append(order.MarketPriceThreshold).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // OrigOrderPrice
                                if (order.OrigOrderPrice.HasValue)
                                    sb.Append(order.OrigOrderPrice).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // MarketPriceField
                                if (!string.IsNullOrEmpty(order.MarketPriceField))
                                    sb.Append(string.Concat("'", order.MarketPriceField, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // EstNavType
                                if (!string.IsNullOrEmpty(order.EstNavType))
                                    sb.Append(string.Concat("'", order.EstNavType, "'"));
                                else
                                    sb.Append("null");

                                string row = sb.ToString();
                                Rows.Add(string.Concat("(", row, ")"));
                                sb.Clear();
                            }

                            sCommand.Append(string.Join(",", Rows));
                            sCommand.Append(";");

                            _logger.LogInformation("insert into Trading.StgBatchDetail");
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), connection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }

                            _logger.LogInformation("Moving data to Trading.BatchDetail");
                            using (MySqlCommand command = new MySqlCommand(SaveBatchOrdersQuery, connection))
                            {
                                command.CommandType = CommandType.StoredProcedure;
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

        public void SaveBatchTemplate(IList<BatchOrderTemplate> list)
        {
            StringBuilder sCommand = new StringBuilder("INSERT INTO Trading.BatchTemplate"
                + " (TemplateName, Symbol, OrderSide, OrderType, AccountNumber, Destination, BrokerStrategy,"
                + " Qty, TotalTradeValue, PriceTarget, DiscountTarget, ChooseNav, NavOvr,"
                + " APExpire, APStartDate, APEndDate, APUrgency, APMaxPctVol, APWouldPrice, APMinDarkFillSize,"
                + " APDisplayMode, APDisplaySize, APLiquiditySeek, APSeekPrice, APSeekMinQty, APPctOfVolume, Locate,"
                + " RefIndex, RefIndexPriceBeta, RefIndexPriceCap, RefIndexPriceBetaShiftInd, RefIndexPriceCapShiftInd,"
                + " RefIndexPriceBetaAdj, DiscountTargetAdj, RefIndexMaxPrice, OptionPosition, OptionEquityPosition, OptionFill,"
                + " RefIndexPriceType, OrderExpire, SampleTemplate,"
                + " APTradingStyle, APTradingSession, APSORPreference, APSORSessionPreference, RefIndexPrice, RowId,"
                + " IsPairTrade, PairRatio, PairSpread,"
                + " ActionFlag, AutoUpdateFlag, AutoUpdatePriceThreshold, MarketPriceThreshold, MarketPriceField, APStyle, EstNavType"
                + " ) values ");

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    if (list != null && list.Count > 0)
                    {
                        string templateName = list[0].TemplateName;
                        using (MySqlTransaction trans = connection.BeginTransaction())
                        {
                            _logger.LogInformation("Deleting data from Trading.BatchTemplate where TemplateName = '" + templateName + "'");
                            string sqlDelete = "delete from Trading.BatchTemplate where TemplateName = '" + templateName + "'";
                            using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                            {
                                command.ExecuteNonQuery();
                            }

                            List<string> Rows = new List<string>();
                            StringBuilder sb = new StringBuilder();
                            foreach (BatchOrderTemplate data in list)
                            {
                                // TemplateName
                                if (!string.IsNullOrEmpty(data.TemplateName))
                                    sb.Append(string.Concat("'", data.TemplateName, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Symbol
                                if (!string.IsNullOrEmpty(data.Symbol))
                                    sb.Append(string.Concat("'", data.Symbol, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // OrderSide
                                if (!string.IsNullOrEmpty(data.OrderSide))
                                    sb.Append(string.Concat("'", data.OrderSide, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // OrderType
                                if (!string.IsNullOrEmpty(data.OrderType))
                                    sb.Append(string.Concat("'", data.OrderType, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // AccountNumber
                                if (!string.IsNullOrEmpty(data.AccountNumber))
                                    sb.Append(string.Concat("'", data.AccountNumber, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Destination
                                if (!string.IsNullOrEmpty(data.Destination))
                                    sb.Append(string.Concat("'", data.Destination, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // BrokerStrategy
                                if (!string.IsNullOrEmpty(data.BrokerStrategy))
                                    sb.Append(string.Concat("'", data.BrokerStrategy, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Qty
                                if (data.Qty.HasValue)
                                    sb.Append(data.Qty).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // TotalTradeValue
                                if (data.TotalTradeValue.HasValue)
                                    sb.Append(data.TotalTradeValue).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // PriceTarget
                                if (data.PriceTarget.HasValue)
                                    sb.Append(data.PriceTarget).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // DiscountTarget
                                if (data.DiscountTarget.HasValue)
                                    sb.Append(data.DiscountTarget).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // ChooseNav
                                if (!string.IsNullOrEmpty(data.ChooseNav))
                                    sb.Append(string.Concat("'", data.ChooseNav, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // NavOvr
                                if (data.NavOvr.HasValue)
                                    sb.Append(data.NavOvr).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // APExpire
                                if (!string.IsNullOrEmpty(data.APExpire))
                                    sb.Append(string.Concat("'", data.APExpire, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // APStartDate
                                if (!string.IsNullOrEmpty(data.APStartDate))
                                    sb.Append(string.Concat("'", data.APStartDate, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // APEndDate
                                if (!string.IsNullOrEmpty(data.APEndDate))
                                    sb.Append(string.Concat("'", data.APEndDate, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // APUrgency
                                if (!string.IsNullOrEmpty(data.APUrgency))
                                    sb.Append(string.Concat("'", data.APUrgency, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // APMaxPctVol
                                if (data.APMaxPctVol.HasValue)
                                    sb.Append(data.APMaxPctVol).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // APWouldPrice
                                if (data.APWouldPrice.HasValue)
                                    sb.Append(data.APWouldPrice).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // APMinDarkFillSize
                                if (data.APMinDarkFillSize.HasValue)
                                    sb.Append(data.APMinDarkFillSize).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // APDisplayMode
                                if (!string.IsNullOrEmpty(data.APDisplayMode))
                                    sb.Append(string.Concat("'", data.APDisplayMode, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // APDisplaySize
                                if (data.APDisplaySize.HasValue)
                                    sb.Append(data.APDisplaySize).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // APLiquiditySeek
                                if (!string.IsNullOrEmpty(data.APLiquiditySeek))
                                    sb.Append(string.Concat("'", data.APLiquiditySeek, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // APSeekPrice
                                if (data.APSeekPrice.HasValue)
                                    sb.Append(data.APSeekPrice).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // APSeekMinQty
                                if (data.APSeekMinQty.HasValue)
                                    sb.Append(data.APSeekMinQty).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // APPctOfVolume
                                if (data.APPctOfVolume.HasValue)
                                    sb.Append(data.APPctOfVolume).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Locate
                                if (!string.IsNullOrEmpty(data.Locate))
                                    sb.Append(string.Concat("'", data.Locate, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // RefIndex
                                if (!string.IsNullOrEmpty(data.RefIndex))
                                    sb.Append(string.Concat("'", data.RefIndex, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // RefIndexPriceBeta
                                if (data.RefIndexPriceBeta.HasValue)
                                    sb.Append(data.RefIndexPriceBeta).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // RefIndexPriceCap
                                if (data.RefIndexPriceCap.HasValue)
                                    sb.Append(data.RefIndexPriceCap).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                if (!string.IsNullOrEmpty(data.RefIndex) &&
                                    !string.IsNullOrEmpty(data.RefIndexPriceBetaShiftIndAsString))
                                {
                                    if (data.RefIndexPriceBetaShiftIndAsString.Equals("Both", StringComparison.CurrentCultureIgnoreCase))
                                        data.RefIndexPriceBetaShiftInd = 0;
                                    if (data.RefIndexPriceBetaShiftIndAsString.Equals("Up", StringComparison.CurrentCultureIgnoreCase))
                                        data.RefIndexPriceBetaShiftInd = 1;
                                    if (data.RefIndexPriceBetaShiftIndAsString.Equals("Down", StringComparison.CurrentCultureIgnoreCase))
                                        data.RefIndexPriceBetaShiftInd = -1;
                                }
                                else if (!string.IsNullOrEmpty(data.RefIndex))
                                {
                                    data.RefIndexPriceBetaShiftInd = 0;
                                }

                                if (!string.IsNullOrEmpty(data.RefIndexPriceCapShiftIndAsString))
                                {
                                    if (data.RefIndexPriceCapShiftIndAsString.Equals("Both", StringComparison.CurrentCultureIgnoreCase))
                                        data.RefIndexPriceCapShiftInd = 0;
                                    if (data.RefIndexPriceCapShiftIndAsString.Equals("Up", StringComparison.CurrentCultureIgnoreCase))
                                        data.RefIndexPriceCapShiftInd = 1;
                                    if (data.RefIndexPriceCapShiftIndAsString.Equals("Down", StringComparison.CurrentCultureIgnoreCase))
                                        data.RefIndexPriceCapShiftInd = -1;
                                }

                                // RefIndexPriceBetaShiftInd
                                if (data.RefIndexPriceBetaShiftInd.HasValue)
                                    sb.Append(data.RefIndexPriceBetaShiftInd).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // RefIndexPriceCapShiftInd
                                if (data.RefIndexPriceCapShiftInd.HasValue)
                                    sb.Append(data.RefIndexPriceCapShiftInd).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // RefIndexPriceBetaAdj
                                if (!string.IsNullOrEmpty(data.RefIndexBetaAdjType))
                                    sb.Append(string.Concat("'", data.RefIndexBetaAdjType, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // DiscountTargetAdj
                                if (data.DiscountTargetAdj.HasValue)
                                    sb.Append(data.DiscountTargetAdj).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // RefIndexMaxPrice
                                if (data.RefIndexMaxPrice.HasValue)
                                    sb.Append(data.RefIndexMaxPrice).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // OptionPosition
                                if (!string.IsNullOrEmpty(data.OptionPosition))
                                    sb.Append(string.Concat("'", data.OptionPosition, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // OptionEquityPosition
                                if (!string.IsNullOrEmpty(data.OptionEquityPosition))
                                    sb.Append(string.Concat("'", data.OptionEquityPosition, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // OptionFill
                                if (!string.IsNullOrEmpty(data.OptionFill))
                                    sb.Append(string.Concat("'", data.OptionFill, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // RefIndexPriceType
                                if (!string.IsNullOrEmpty(data.RefIndexPriceType))
                                    sb.Append(string.Concat("'", data.RefIndexPriceType, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // OrderExpire
                                if (!string.IsNullOrEmpty(data.OrderExpire))
                                    sb.Append(string.Concat("'", data.OrderExpire, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // SampleTemplate
                                if (!string.IsNullOrEmpty(data.SampleTemplate))
                                    sb.Append(string.Concat("'", data.SampleTemplate, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // APTradingStyle
                                if (!string.IsNullOrEmpty(data.APTradingStyle))
                                    sb.Append(string.Concat("'", data.APTradingStyle, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // APTradingSession
                                if (!string.IsNullOrEmpty(data.APTradingSession))
                                    sb.Append(string.Concat("'", data.APTradingSession, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // APSORPreference
                                if (!string.IsNullOrEmpty(data.APSORPreference))
                                    sb.Append(string.Concat("'", data.APSORPreference, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // APSORSessionPreference
                                if (!string.IsNullOrEmpty(data.APSORSessionPreference))
                                    sb.Append(string.Concat("'", data.APSORSessionPreference, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // RefIndexPrice
                                if (data.RefIndexPrice.HasValue)
                                    sb.Append(data.RefIndexPrice).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // RowId
                                if (data.RowId.HasValue)
                                    sb.Append(data.RowId).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // IsPairTrade
                                if (!string.IsNullOrEmpty(data.IsPairTrade))
                                    sb.Append(string.Concat("'", data.IsPairTrade, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // PairRatio
                                if (data.PairRatio.HasValue)
                                    sb.Append(data.PairRatio).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // PairSpread
                                if (data.PairSpread.HasValue)
                                    sb.Append(data.PairSpread).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // ActionFlag
                                if (!string.IsNullOrEmpty(data.ActionFlag))
                                    sb.Append(string.Concat("'", data.ActionFlag, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // AutoUpdateFlag
                                if (!string.IsNullOrEmpty(data.AutoUpdateFlag))
                                    sb.Append(string.Concat("'", data.AutoUpdateFlag, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // AutoUpdatePriceThreshold
                                if (data.AutoUpdatePriceThreshold.HasValue)
                                    sb.Append(data.AutoUpdatePriceThreshold).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // MarketPriceThreshold
                                if (data.MarketPriceThreshold.HasValue)
                                    sb.Append(data.MarketPriceThreshold).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // MarketPriceField
                                if (!string.IsNullOrEmpty(data.MarketPriceField))
                                    sb.Append(string.Concat("'", data.MarketPriceField, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // APStyle
                                if (!string.IsNullOrEmpty(data.APStyle))
                                    sb.Append(string.Concat("'", data.APStyle, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // EstNavType
                                if (!string.IsNullOrEmpty(data.EstNavType))
                                    sb.Append(string.Concat("'", data.EstNavType, "'"));
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
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving data");
            }
        }

        /// <summary>
        /// Truncate Table
        /// </summary>
        /// <param name="tableName"></param>
        public void TruncateTable(string tableName)
        {
            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand("TRUNCATE TABLE " + tableName, connection))
                        command.ExecuteNonQuery();
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
        /// <param name="dict"></param>
        public void SaveRouteUpdates(IDictionary<Int32, EMSXRouteStatus> dict)
        {
            StringBuilder sCommand = new StringBuilder("INSERT INTO Trading.StgEMSXRouteStatus"
                + " (Environment, OrdSeq, OrdRefId, Broker, Strategy, OrdType, Status, RouteId, RouteAsOfDate, RouteCreateDate, RouteCreateTime, RouteLastUpdTime,"
                + " RoutePrice, LimitPrice, StopPrice, AvgPrice, DayAvgPrice, Trader, Amount, StartAmount, WorkingAmount, SettleAmount, BalRemain,"
                + " PctRemain, DayFill, FillId, FillAmount, LastFillDate, LastFillTime, LastPrice, LastShares, LastCapacity, Principal,"
                + " ExchDest, ExecBroker, BrokerComm, AccountId, ApiSeqNum, CorrelationId, TraderNotes, Notes, OTCFlag, Urgency,"
                + " ReasonCode, ReasonDesc, SettleDate, StrategyStyle, Symbol, Sedol, Isin, OrdSide,"
                + " RefIndex, RefIndexBeta, RefIndexBetaInd, RefIndexPriceBetaAdj, RefIndexLastPrice, RefIndexLivePrice, RefIndexPriceCap,"
                + " RefIndexPriceInd, RefIndexPriceChng, RefIndexPriceChngFinal, RefIndexMaxPrice, RefIndexPriceType, TargetPrice,"
                + " DiscountTarget, DiscountTargetLastNav, DiscountToLastPrice, DiscountToLivePrice, EstimatedNav, EstimatedDiscount,"
                + " AutoUpdate, AutoUpdateThreshold, MarketPriceThreshold, MarketPriceField, LastLivePrice, BidPrice, AskPrice,"
                + " EstNavType, AlgoParameters, OrigPrice, OrigOrdPrice"
                + " ) values ");

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    if (dict != null && dict.Count > 0)
                    {
                        using (MySqlTransaction trans = connection.BeginTransaction())
                        {
                            //_logger.LogInformation("Deleting data from Trading.StgEMSXRouteStatus");
                            string sqlDelete = "delete from Trading.StgEMSXRouteStatus";
                            using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                            {
                                command.ExecuteNonQuery();
                            }

                            List<string> Rows = new List<string>();
                            StringBuilder sb = new StringBuilder();
                            foreach (EMSXRouteStatus data in dict.Values)
                            {
                                // Environment
                                if (!string.IsNullOrEmpty(data.Env))
                                    sb.Append(string.Concat("'", data.Env, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // OrdSeq
                                if (data.OrdSeq.HasValue)
                                    sb.Append(data.OrdSeq).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // OrdRefId
                                if (!string.IsNullOrEmpty(data.OrdRefId))
                                    sb.Append(string.Concat("'", data.OrdRefId, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Broker
                                if (!string.IsNullOrEmpty(data.Bkr))
                                    sb.Append(string.Concat("'", data.Bkr, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Strategy
                                if (!string.IsNullOrEmpty(data.StratType))
                                    sb.Append(string.Concat("'", data.StratType, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // OrdType
                                if (!string.IsNullOrEmpty(data.OrdType))
                                    sb.Append(string.Concat("'", data.OrdType, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Status
                                if (!string.IsNullOrEmpty(data.Status))
                                    sb.Append(string.Concat("'", data.Status, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // RouteId
                                if (data.RouteId.HasValue)
                                    sb.Append(data.RouteId).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // RouteAsOfDate
                                if (data.RouteAsOfDate.HasValue)
                                    sb.Append(string.Concat("'", DateUtils.ConvertDate(data.RouteAsOfDate, "yyyy-MM-dd"), "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // RouteCreateDate
                                if (data.RouteCreateDate.HasValue)
                                    sb.Append(string.Concat("'", DateUtils.ConvertDate(data.RouteCreateDate, "yyyy-MM-dd"), "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // RouteCreateTime
                                if (!string.IsNullOrEmpty(data.RouteCreateTm))
                                    sb.Append(string.Concat("'", data.RouteCreateTm, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // RouteLastUpdTime
                                if (!string.IsNullOrEmpty(data.RouteLastUpdTm))
                                    sb.Append(string.Concat("'", data.RouteLastUpdTm, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // RoutePrice
                                if (data.RoutePrc.HasValue)
                                    sb.Append(data.RoutePrc).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // LimitPrice
                                if (data.LimitPrc.HasValue)
                                    sb.Append(data.LimitPrc).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // StopPrice
                                if (data.StopPrc.HasValue)
                                    sb.Append(data.StopPrc).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // AvgPrice
                                if (data.AvgPrc.HasValue)
                                    sb.Append(data.AvgPrc).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // DayAvgPrice
                                if (data.DayAvgPrc.HasValue)
                                    sb.Append(data.DayAvgPrc).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Trader
                                if (!string.IsNullOrEmpty(data.Trader))
                                    sb.Append(string.Concat("'", data.Trader, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Amount
                                if (data.Amt.HasValue)
                                    sb.Append(data.Amt).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // StartAmount
                                if (data.StartAmt.HasValue)
                                    sb.Append(data.StartAmt).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // WorkingAmount
                                if (data.Working.HasValue)
                                    sb.Append(data.Working).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // SettleAmount
                                if (data.SettleAmt.HasValue)
                                    sb.Append(data.SettleAmt).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // BalRemain
                                if (data.RemBal.HasValue)
                                    sb.Append(data.RemBal).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // PctRemain
                                if (data.PctRemain.HasValue)
                                    sb.Append(data.PctRemain).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // DayFill
                                if (data.DayFill.HasValue)
                                    sb.Append(data.DayFill).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // FillId
                                if (data.FillId.HasValue)
                                    sb.Append(data.FillId).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // FillAmount
                                if (data.Filled.HasValue)
                                    sb.Append(data.Filled).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // LastFillDate
                                if (data.LastFillDate.HasValue)
                                    sb.Append(string.Concat("'", DateUtils.ConvertDate(data.LastFillDate, "yyyy-MM-dd"), "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // LastFillTime
                                if (!string.IsNullOrEmpty(data.LastFillTm))
                                    sb.Append(string.Concat("'", data.LastFillTm, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // LastPrice
                                if (data.LastPrc.HasValue)
                                    sb.Append(data.LastPrc).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // LastShares
                                if (data.LastShares.HasValue)
                                    sb.Append(data.LastShares).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // LastCapacity
                                if (!string.IsNullOrEmpty(data.LastCapacity))
                                    sb.Append(string.Concat("'", data.LastCapacity, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Principal
                                if (data.Principal.HasValue)
                                    sb.Append(data.Principal).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // ExchDest
                                if (!string.IsNullOrEmpty(data.ExchDest))
                                    sb.Append(string.Concat("'", data.ExchDest, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // ExecBroker
                                if (!string.IsNullOrEmpty(data.ExecBkr))
                                    sb.Append(string.Concat("'", data.ExecBkr, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // BrokerComm
                                if (data.BkrComm.HasValue)
                                    sb.Append(data.BkrComm).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // AccountId
                                if (!string.IsNullOrEmpty(data.Acct))
                                    sb.Append(string.Concat("'", data.Acct, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // ApiSeqNum
                                if (data.ApiSeqNum.HasValue)
                                    sb.Append(data.ApiSeqNum).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // CorrelationId
                                if (data.CorrelationId.HasValue)
                                    sb.Append(data.CorrelationId).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // TraderNotes
                                if (!string.IsNullOrEmpty(data.TraderNotes))
                                    sb.Append(string.Concat("'", data.TraderNotes, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Notes
                                if (!string.IsNullOrEmpty(data.Notes))
                                    sb.Append(string.Concat("'", data.Notes, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // OTCFlag
                                if (!string.IsNullOrEmpty(data.OtcFlag))
                                    sb.Append(string.Concat("'", data.OtcFlag, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Urgency
                                if (data.UrgencyLvl.HasValue)
                                    sb.Append(data.UrgencyLvl).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // ReasonCode
                                if (!string.IsNullOrEmpty(data.ReasonCd))
                                    sb.Append(string.Concat("'", data.ReasonCd, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // ReasonDesc
                                if (!string.IsNullOrEmpty(data.ReasonDesc))
                                    sb.Append(string.Concat("'", data.ReasonDesc, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // SettleDate
                                if (data.SettleDate.HasValue)
                                    sb.Append(string.Concat("'", DateUtils.ConvertDate(data.SettleDate, "yyyy-MM-dd"), "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // StrategyStyle
                                if (!string.IsNullOrEmpty(data.StratStyle))
                                    sb.Append(string.Concat("'", data.StratStyle, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Symbol
                                if (!string.IsNullOrEmpty(data.Ticker))
                                    sb.Append(string.Concat("'", data.Ticker, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Sedol
                                if (!string.IsNullOrEmpty(data.Sedol))
                                    sb.Append(string.Concat("'", data.Sedol, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Isin
                                if (!string.IsNullOrEmpty(data.Isin))
                                    sb.Append(string.Concat("'", data.Isin, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // OrdSide
                                if (!string.IsNullOrEmpty(data.OrdSide))
                                    sb.Append(string.Concat("'", data.OrdSide, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // RefIndex
                                if (!string.IsNullOrEmpty(data.RI))
                                    sb.Append(string.Concat("'", data.RI, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // RefIndexBeta
                                if (data.RIBeta.HasValue)
                                    sb.Append(data.RIBeta).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // RefIndexBetaInd
                                if (data.RIPrBetaShiftInd.HasValue)
                                    sb.Append(data.RIPrBetaShiftInd).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // RefIndexPriceBetaAdj
                                if (!string.IsNullOrEmpty(data.RIBetaAdjTyp))
                                    sb.Append(string.Concat("'", data.RIBetaAdjTyp, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // RefIndexLastPrice
                                if (data.RILastPr.HasValue)
                                    sb.Append(data.RILastPr).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // RefIndexLivePrice
                                if (data.RILivePr.HasValue)
                                    sb.Append(data.RILivePr).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // RefIndexPriceCap
                                if (data.RIPrCap.HasValue)
                                    sb.Append(data.RIPrCap).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // RefIndexPriceInd
                                if (data.RIPrCapShiftInd.HasValue)
                                    sb.Append(data.RIPrCapShiftInd).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // RefIndexPriceChng
                                if (data.RIPrChng.HasValue)
                                    sb.Append(data.RIPrChng).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // RefIndexPriceChngFinal
                                if (data.RIPrChngFinal.HasValue)
                                    sb.Append(data.RIPrChngFinal).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // RefIndexMaxPrice
                                if (data.RIMaxPr.HasValue)
                                    sb.Append(data.RIMaxPr).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // RefIndexPriceType
                                if (!string.IsNullOrEmpty(data.RIPrTyp))
                                    sb.Append(string.Concat("'", data.RIPrTyp, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // TargetPrice
                                if (data.RITgtPr.HasValue)
                                    sb.Append(data.RITgtPr).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // DiscountTarget
                                if (data.DscntTgt.HasValue)
                                    sb.Append(data.DscntTgt).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // DiscountTargetLastNav
                                if (data.DscntTgtLastNav.HasValue)
                                    sb.Append(data.DscntTgtLastNav).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // DiscountToLastPrice
                                if (data.DscntToLastPr.HasValue)
                                    sb.Append(data.DscntToLastPr).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // DiscountToLivePrice
                                if (data.DscntToLivePr.HasValue)
                                    sb.Append(data.DscntToLivePr).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // EstimatedNav
                                if (data.EstNav.HasValue)
                                    sb.Append(data.EstNav).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // EstimatedDiscount
                                if (data.DscntToLivePr.HasValue)
                                    sb.Append(data.DscntToLivePr).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // AutoUpdate
                                if (!string.IsNullOrEmpty(data.AutoUpdate))
                                    sb.Append(string.Concat("'", data.AutoUpdate, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // AutoUpdateThreshold
                                if (data.AutoUpdateThld.HasValue)
                                    sb.Append(data.AutoUpdateThld).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // MarketPriceThreshold
                                if (data.MktPrThld.HasValue)
                                    sb.Append(data.MktPrThld).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // MarketPriceField
                                if (!string.IsNullOrEmpty(data.MktPrFld))
                                    sb.Append(string.Concat("'", data.MktPrFld, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // LastLivePrice
                                if (data.LastPr.HasValue)
                                    sb.Append(data.LastPr).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // BidPrice
                                if (data.BidPr.HasValue)
                                    sb.Append(data.BidPr).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // AskPrice
                                if (data.AskPr.HasValue)
                                    sb.Append(data.AskPr).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // EstNavType
                                if (!string.IsNullOrEmpty(data.EstNavTyp))
                                    sb.Append(string.Concat("'", data.EstNavTyp, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // AlgoParameters
                                if (!string.IsNullOrEmpty(data.AlgoParams))
                                    sb.Append(string.Concat("'", data.AlgoParams, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // OrigPrice
                                if (data.OrigPrc.HasValue)
                                    sb.Append(data.OrigPrc).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // OrigOrdPrice
                                if (data.OrdOrigPr.HasValue)
                                    sb.Append(data.OrdOrigPr);
                                else
                                    sb.Append("null");

                                string row = sb.ToString();
                                Rows.Add(string.Concat("(", row, ")"));
                                sb.Clear();
                            }

                            sCommand.Append(string.Join(",", Rows));
                            sCommand.Append(";");

                            //_logger.LogInformation("Inserting data into Trading.StgEMSXRouteStatus");
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), connection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }

                            //_logger.LogInformation("Moving data to Trading.EMSXRouteStatus");
                            using (MySqlCommand command = new MySqlCommand(SaveRouteStatusQuery, connection))
                            {
                                command.CommandType = CommandType.StoredProcedure;
                                command.ExecuteNonQuery();
                            }

                            trans.Commit();
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving data");
            }
        }

        public void SaveOrderUpdates(IDictionary<Int32, EMSXOrderStatus> dict)
        {
            StringBuilder sCommand = new StringBuilder("INSERT INTO Trading.StgEMSXOrderStatus"
                + " (Environment, OrdRefId, Symbol, Broker, Strategy, OrdSide, OrdType, OrdDate, OrdTime,"
                + " Status, AvgPrice, DayAvgPrice, LimitPrice, StopPrice, ArrivalPrice, WorkPrice,"
                + " Trader, Amount, StartAmount, WorkingAmount, BalRemain, PctRemain,"
                + " FillId, FillAmount, Principal, Isin, Sedol, PortNum, PortName, PortMgr, Exch, ExchDest,"
                + " ExecBroker, BrokerComm, AccountId, RouteId, ApiSeqNum, OrdSeq, CorrelationId,"
                + " TraderNotes, Notes, SettleDate, OrdTmStamp, OrigPrice"
                + " ) values ");

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    if (dict != null && dict.Count > 0)
                    {
                        using (MySqlTransaction trans = connection.BeginTransaction())
                        {
                            //_logger.LogInformation("Deleting data from Trading.StgEMSXOrderStatus");
                            string sqlDelete = "delete from Trading.StgEMSXOrderStatus";
                            using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                            {
                                command.ExecuteNonQuery();
                            }

                            List<string> Rows = new List<string>();
                            StringBuilder sb = new StringBuilder();
                            foreach (EMSXOrderStatus data in dict.Values)
                            {
                                // Environment
                                if (!string.IsNullOrEmpty(data.Env))
                                    sb.Append(string.Concat("'", data.Env, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // OrdRefId
                                if (!string.IsNullOrEmpty(data.OrdRefId))
                                    sb.Append(string.Concat("'", data.OrdRefId, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Symbol/Ticker
                                if (!string.IsNullOrEmpty(data.Ticker))
                                    sb.Append(string.Concat("'", data.Ticker, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Broker
                                if (!string.IsNullOrEmpty(data.Bkr))
                                    sb.Append(string.Concat("'", data.Bkr, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Strategy
                                if (!string.IsNullOrEmpty(data.StratType))
                                    sb.Append(string.Concat("'", data.StratType, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // OrdSide
                                if (!string.IsNullOrEmpty(data.OrdSide))
                                    sb.Append(string.Concat("'", data.OrdSide, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // OrdType
                                if (!string.IsNullOrEmpty(data.OrdType))
                                    sb.Append(string.Concat("'", data.OrdType, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // OrdDate
                                if (data.OrdDate.HasValue)
                                    sb.Append(string.Concat("'", DateUtils.ConvertDate(data.OrdDate, "yyyy-MM-dd"), "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // OrdTime
                                if (!string.IsNullOrEmpty(data.OrdTm))
                                    sb.Append(string.Concat("'", data.OrdTm, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Status
                                if (!string.IsNullOrEmpty(data.Status))
                                    sb.Append(string.Concat("'", data.Status, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // AvgPrice
                                if (data.AvgPrc.HasValue)
                                    sb.Append(data.AvgPrc).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // DayAvgPrice
                                if (data.DayAvgPrc.HasValue)
                                    sb.Append(data.DayAvgPrc).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // LimitPrice
                                if (data.LimitPrc.HasValue)
                                    sb.Append(data.LimitPrc).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // StopPrice
                                if (data.StopPrc.HasValue)
                                    sb.Append(data.StopPrc).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // ArrivalPrice
                                if (data.ArrivalPrc.HasValue)
                                    sb.Append(data.ArrivalPrc).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // WorkPrice
                                if (data.WorkPrc.HasValue)
                                    sb.Append(data.WorkPrc).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Trader
                                if (!string.IsNullOrEmpty(data.Trader))
                                    sb.Append(string.Concat("'", data.Trader, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Amount
                                if (data.Amt.HasValue)
                                    sb.Append(data.Amt).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // StartAmount
                                if (data.StartAmt.HasValue)
                                    sb.Append(data.StartAmt).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // WorkingAmount
                                if (data.Working.HasValue)
                                    sb.Append(data.Working).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // BalRemain
                                if (data.RemBal.HasValue)
                                    sb.Append(data.RemBal).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // PctRemain
                                if (data.PctRemain.HasValue)
                                    sb.Append(data.PctRemain).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // FillId
                                if (data.FillId.HasValue)
                                    sb.Append(data.FillId).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // FillAmount
                                if (data.Filled.HasValue)
                                    sb.Append(data.Filled).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Principal
                                if (data.Principal.HasValue)
                                    sb.Append(data.Principal).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Isin
                                if (!string.IsNullOrEmpty(data.Isin))
                                    sb.Append(string.Concat("'", data.Isin, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Sedol
                                if (!string.IsNullOrEmpty(data.Sedol))
                                    sb.Append(string.Concat("'", data.Sedol, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // PortNum
                                if (data.PortNum.HasValue)
                                    sb.Append(data.PortNum).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // PortName
                                if (!string.IsNullOrEmpty(data.PortName))
                                    sb.Append(string.Concat("'", data.PortName, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // PortMgr
                                if (!string.IsNullOrEmpty(data.PortMgr))
                                    sb.Append(string.Concat("'", data.PortMgr, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Exch
                                if (!string.IsNullOrEmpty(data.Exch))
                                    sb.Append(string.Concat("'", data.Exch, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // ExchDest
                                if (!string.IsNullOrEmpty(data.ExchDest))
                                    sb.Append(string.Concat("'", data.ExchDest, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // ExecBroker
                                sb.Append("null").Append(DELIMITER);

                                // BrokerComm
                                if (data.BkrComm.HasValue)
                                    sb.Append(data.BkrComm).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // AccountId
                                if (!string.IsNullOrEmpty(data.Acct))
                                    sb.Append(string.Concat("'", data.Acct, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // RouteId
                                if (data.RouteId.HasValue)
                                    sb.Append(data.RouteId).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // ApiSeqNum
                                if (data.ApiSeqNum.HasValue)
                                    sb.Append(data.ApiSeqNum).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // OrdSeq
                                if (data.OrdSeq.HasValue)
                                    sb.Append(data.OrdSeq).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // CorrelationId
                                if (data.CorrelationId.HasValue)
                                    sb.Append(data.CorrelationId).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // TraderNotes
                                if (!string.IsNullOrEmpty(data.TraderNotes))
                                    sb.Append(string.Concat("'", data.TraderNotes, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Notes
                                if (!string.IsNullOrEmpty(data.Notes))
                                    sb.Append(string.Concat("'", data.Notes, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // SettleDate
                                if (data.SettleDate.HasValue)
                                    sb.Append(string.Concat("'", DateUtils.ConvertDate(data.SettleDate, "yyyy-MM-dd"), "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // OrdTmStamp
                                if (!string.IsNullOrEmpty(data.TimeStamp))
                                    sb.Append(string.Concat("'", data.TimeStamp, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // OrigPrice
                                if (data.OrigPrc.HasValue)
                                    sb.Append(data.OrigPrc);
                                else
                                    sb.Append("null");

                                string row = sb.ToString();
                                Rows.Add(string.Concat("(", row, ")"));
                                sb.Clear();
                            }

                            sCommand.Append(string.Join(",", Rows));
                            sCommand.Append(";");

                            //_logger.LogInformation("Inserting data into Trading.StgEMSXOrderStatus");
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), connection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }

                            //_logger.LogInformation("Moving data to Trading.EMSXOrderStatus");
                            using (MySqlCommand command = new MySqlCommand(SaveOrderStatusQuery, connection))
                            {
                                command.CommandType = CommandType.StoredProcedure;
                                command.ExecuteNonQuery();
                            }

                            trans.Commit();
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving data");
            }
        }

        public void SaveOrderFills(IDictionary<Int32, IDictionary<Int32, EMSXOrderFill>> dict)
        {
            StringBuilder sCommand = new StringBuilder("INSERT INTO Trading.StgEMSXOrderFill"
                + " (Environment, OrdSeq, OrdRefId, Status, DayFill, FillId, FillAmount, LastFillDate, LastFillTime,"
                + " LastPrice, LastShares, LastCapacity, PctRemain, BalRemain, StartAmount, WorkingAmount,"
                + " AvgPrice, DayAvgPrice, RoutePrice, OrdDate"
                + " ) values ");

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    if (dict != null && dict.Count > 0)
                    {
                        using (MySqlTransaction trans = connection.BeginTransaction())
                        {
                            //_logger.LogInformation("Deleting data from Trading.StgEMSXOrderFill");
                            string sqlDelete = "delete from Trading.StgEMSXOrderFill";
                            using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                            {
                                command.ExecuteNonQuery();
                            }

                            List<string> Rows = new List<string>();
                            StringBuilder sb = new StringBuilder();
                            foreach (IDictionary<Int32, EMSXOrderFill> fillDict in dict.Values)
                            {
                                foreach (EMSXOrderFill data in fillDict.Values)
                                {
                                    // Environment
                                    if (!string.IsNullOrEmpty(data.Env))
                                        sb.Append(string.Concat("'", data.Env, "'")).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    // OrdSeq
                                    if (data.OrdSeq.HasValue)
                                        sb.Append(data.OrdSeq).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    // OrdRefId
                                    if (!string.IsNullOrEmpty(data.OrdRefId))
                                        sb.Append(string.Concat("'", data.OrdRefId, "'")).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    // Status
                                    if (!string.IsNullOrEmpty(data.Status))
                                        sb.Append(string.Concat("'", data.Status, "'")).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    // DayFill
                                    if (data.DayFill.HasValue)
                                        sb.Append(data.DayFill).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    // FillId
                                    if (data.FillId.HasValue)
                                        sb.Append(data.FillId).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    // FillAmount
                                    if (data.Filled.HasValue)
                                        sb.Append(data.Filled).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    // LastFillDate
                                    if (data.LastFillDate.HasValue)
                                        sb.Append(string.Concat("'", DateUtils.ConvertDate(data.LastFillDate, "yyyy-MM-dd"), "'")).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    // LastFillTime
                                    if (!string.IsNullOrEmpty(data.LastFillTm))
                                        sb.Append(string.Concat("'", data.LastFillTm, "'")).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    // LastPrice
                                    if (data.LastPrc.HasValue)
                                        sb.Append(data.LastPrc).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    // LastShares
                                    if (data.LastShares.HasValue)
                                        sb.Append(data.LastShares).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    // LastCapacity
                                    if (!string.IsNullOrEmpty(data.LastCapacity))
                                        sb.Append(string.Concat("'", data.LastCapacity, "'")).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    // PctRemain
                                    if (data.PctRemain.HasValue)
                                        sb.Append(data.PctRemain).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    // BalRemain
                                    if (data.RemBal.HasValue)
                                        sb.Append(data.RemBal).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    // StartAmount
                                    sb.Append("null").Append(DELIMITER);

                                    // WorkingAmount
                                    if (data.Working.HasValue)
                                        sb.Append(data.Working).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    // AvgPrice
                                    if (data.AvgPrc.HasValue)
                                        sb.Append(data.AvgPrc).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    // DayAvgPrice
                                    if (data.DayAvgPrc.HasValue)
                                        sb.Append(data.DayAvgPrc).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    // RoutePrice
                                    if (data.RoutePrc.HasValue)
                                        sb.Append(data.RoutePrc).Append(DELIMITER);
                                    else
                                        sb.Append("null").Append(DELIMITER);

                                    // OrdDate
                                    if (data.OrdDate.HasValue)
                                        sb.Append(string.Concat("'", DateUtils.ConvertDate(data.OrdDate, "yyyy-MM-dd"), "'"));
                                    else
                                        sb.Append("null");

                                    string row = sb.ToString();
                                    Rows.Add(string.Concat("(", row, ")"));
                                    sb.Clear();
                                }
                            }

                            sCommand.Append(string.Join(",", Rows));
                            sCommand.Append(";");

                            //_logger.LogInformation("Inserting data into Trading.StgEMSXOrderFill");
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), connection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }

                            //_logger.LogInformation("Moving data to Trading.EMSXOrderFill");
                            using (MySqlCommand command = new MySqlCommand(SaveOrderFillQuery, connection))
                            {
                                command.CommandType = CommandType.StoredProcedure;
                                command.ExecuteNonQuery();
                            }

                            trans.Commit();
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving data");
            }
        }

        public IDictionary<int, EMSXOrderStatus> GetOrderStatus()
        {
            IDictionary<int, EMSXOrderStatus> dict = new Dictionary<int, EMSXOrderStatus>();

            try
            {
                string sql = GetOrderStatusQuery;
                if (_configuration["ConnectionStrings:ENV"].Equals("DEV"))
                    sql = GetOrderStatusDevQuery;

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                EMSXOrderStatus data = new EMSXOrderStatus();
                                data.Env = reader["Environment"] as string;
                                data.OrdSeq = reader.GetInt32(reader.GetOrdinal("OrdSeq"));
                                data.OrdRefId = reader["OrdRefId"] as string;
                                data.Ticker = reader["Symbol"] as string;
                                data.Bkr = reader["Broker"] as string;
                                data.StratType = reader["Strategy"] as string;
                                data.OrdSide = reader["OrdSide"] as string;
                                data.OrdType = reader["OrdType"] as string;
                                data.OrdDate = reader.IsDBNull(reader.GetOrdinal("OrdDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("OrdDate"));
                                data.OrdTm = reader["OrdTime"] as string;
                                data.Status = reader["Status"] as string;
                                data.AvgPrc = (reader.IsDBNull(reader.GetOrdinal("AvgPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AvgPrice"));
                                data.DayAvgPrc = (reader.IsDBNull(reader.GetOrdinal("DayAvgPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DayAvgPrice"));
                                data.LimitPrc = (reader.IsDBNull(reader.GetOrdinal("LimitPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LimitPrice"));
                                data.StopPrc = (reader.IsDBNull(reader.GetOrdinal("StopPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("StopPrice"));
                                data.ArrivalPrc = (reader.IsDBNull(reader.GetOrdinal("ArrivalPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ArrivalPrice"));
                                data.WorkPrc = (reader.IsDBNull(reader.GetOrdinal("WorkPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("WorkPrice"));
                                data.Trader = reader["Trader"] as string;
                                data.Amt = (reader.IsDBNull(reader.GetOrdinal("Amount"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("Amount"));
                                data.StartAmt = (reader.IsDBNull(reader.GetOrdinal("StartAmount"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("StartAmount"));
                                data.Working = (reader.IsDBNull(reader.GetOrdinal("WorkingAmount"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("WorkingAmount"));
                                data.RemBal = (reader.IsDBNull(reader.GetOrdinal("BalRemain"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BalRemain"));
                                data.PctRemain = (reader.IsDBNull(reader.GetOrdinal("PctRemain"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PctRemain"));
                                data.FillId = (reader.IsDBNull(reader.GetOrdinal("FillId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("FillId"));
                                data.Filled = (reader.IsDBNull(reader.GetOrdinal("FillAmount"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("FillAmount"));
                                data.Principal = (reader.IsDBNull(reader.GetOrdinal("Principal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Principal"));
                                data.Isin = reader["Isin"] as string;
                                data.Sedol = reader["Sedol"] as string;
                                data.PortNum = (reader.IsDBNull(reader.GetOrdinal("PortNum"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("PortNum"));
                                data.PortName = reader["PortName"] as string;
                                data.PortMgr = reader["PortMgr"] as string;
                                data.Exch = reader["Exch"] as string;
                                data.ExchDest = reader["ExchDest"] as string;
                                data.BkrComm = (reader.IsDBNull(reader.GetOrdinal("BrokerComm"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BrokerComm"));
                                data.Acct = reader["AccountId"] as string;
                                data.RouteId = (reader.IsDBNull(reader.GetOrdinal("RouteId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("RouteId"));
                                data.ApiSeqNum = (reader.IsDBNull(reader.GetOrdinal("ApiSeqNum"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("ApiSeqNum"));
                                data.CorrelationId = (reader.IsDBNull(reader.GetOrdinal("CorrelationId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("CorrelationId"));
                                data.TraderNotes = reader["TraderNotes"] as string;
                                data.Notes = reader["Notes"] as string;
                                data.SettleDate = reader.IsDBNull(reader.GetOrdinal("SettleDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("SettleDate"));
                                data.TimeStamp = reader["OrdTmStamp"] as string;
                                data.OrigPrc = (reader.IsDBNull(reader.GetOrdinal("OrigPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OrigPrice"));

                                dict.Add(data.OrdSeq.GetValueOrDefault(), data);
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

        public IDictionary<int, EMSXRouteStatus> GetRouteStatus()
        {
            IDictionary<int, EMSXRouteStatus> dict = new Dictionary<int, EMSXRouteStatus>();

            try
            {
                string sql = GetRouteStatusQuery;
                if (_configuration["ConnectionStrings:ENV"].Equals("DEV"))
                    sql = GetRouteStatusDevQuery;

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                EMSXRouteStatus data = new EMSXRouteStatus
                                {
                                    Env = reader["Environment"] as string,
                                    Ticker = reader["Symbol"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    Isin = reader["Isin"] as string,
                                    OrdSide = reader["OrdSide"] as string,
                                    OrdSeq = reader.GetInt32(reader.GetOrdinal("OrdSeq")),
                                    OrdRefId = reader["OrdRefId"] as string,
                                    Bkr = reader["Broker"] as string,
                                    StratType = reader["Strategy"] as string,
                                    OrdType = reader["OrdType"] as string,
                                    Status = reader["Status"] as string,
                                    RouteId = (reader.IsDBNull(reader.GetOrdinal("RouteId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("RouteId")),
                                    RouteAsOfDate = reader.IsDBNull(reader.GetOrdinal("RouteAsOfDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("RouteAsOfDate")),
                                    RouteCreateDate = reader.IsDBNull(reader.GetOrdinal("RouteCreateDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("RouteCreateDate")),
                                    RouteCreateTm = reader["RouteCreateTime"] as string,
                                    RouteLastUpdTm = reader["RouteLastUpdTime"] as string,
                                    RoutePrc = (reader.IsDBNull(reader.GetOrdinal("RoutePrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RoutePrice")),
                                    LimitPrc = (reader.IsDBNull(reader.GetOrdinal("LimitPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LimitPrice")),
                                    StopPrc = (reader.IsDBNull(reader.GetOrdinal("StopPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("StopPrice")),
                                    AvgPrc = (reader.IsDBNull(reader.GetOrdinal("AvgPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AvgPrice")),
                                    DayAvgPrc = (reader.IsDBNull(reader.GetOrdinal("DayAvgPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DayAvgPrice")),
                                    Trader = reader["Trader"] as string,
                                    Amt = (reader.IsDBNull(reader.GetOrdinal("Amount"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("Amount")),
                                    StartAmt = (reader.IsDBNull(reader.GetOrdinal("StartAmount"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("StartAmount")),
                                    Working = (reader.IsDBNull(reader.GetOrdinal("WorkingAmount"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("WorkingAmount")),
                                    RemBal = (reader.IsDBNull(reader.GetOrdinal("BalRemain"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BalRemain")),
                                    PctRemain = (reader.IsDBNull(reader.GetOrdinal("PctRemain"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PctRemain")),
                                    DayFill = (reader.IsDBNull(reader.GetOrdinal("DayFill"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("DayFill")),
                                    FillId = (reader.IsDBNull(reader.GetOrdinal("FillId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("FillId")),
                                    Filled = (reader.IsDBNull(reader.GetOrdinal("FillAmount"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("FillAmount")),
                                    LastFillDate = reader.IsDBNull(reader.GetOrdinal("LastFillDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("LastFillDate")),
                                    LastFillTm = reader["LastFillTime"] as string,
                                    LastPrc = (reader.IsDBNull(reader.GetOrdinal("LastPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LastPrice")),
                                    LastShares = (reader.IsDBNull(reader.GetOrdinal("LastShares"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("LastShares")),
                                    LastCapacity = reader["LastCapacity"] as string,
                                    Principal = (reader.IsDBNull(reader.GetOrdinal("Principal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Principal")),
                                    ExchDest = reader["ExchDest"] as string,
                                    ExecBkr = reader["ExecBroker"] as string,
                                    BkrComm = (reader.IsDBNull(reader.GetOrdinal("BrokerComm"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BrokerComm")),
                                    Acct = reader["AccountId"] as string,
                                    ApiSeqNum = (reader.IsDBNull(reader.GetOrdinal("ApiSeqNum"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("ApiSeqNum")),
                                    CorrelationId = (reader.IsDBNull(reader.GetOrdinal("CorrelationId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("CorrelationId")),
                                    TraderNotes = reader["TraderNotes"] as string,
                                    Notes = reader["Notes"] as string,
                                    OtcFlag = reader["OTCFlag"] as string,
                                    UrgencyLvl = (reader.IsDBNull(reader.GetOrdinal("Urgency"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("Urgency")),
                                    ReasonCd = reader["ReasonCode"] as string,
                                    ReasonDesc = reader["ReasonDesc"] as string,
                                    SettleDate = reader.IsDBNull(reader.GetOrdinal("SettleDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("SettleDate")),
                                    StratStyle = reader["StrategyStyle"] as string,
                                    OrigPrc = (reader.IsDBNull(reader.GetOrdinal("OrigPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OrigPrice")),
                                    OrdOrigPr = (reader.IsDBNull(reader.GetOrdinal("OrigOrdPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OrigOrdPrice")),

                                    //Ref Index Target Fields
                                    RI = reader["RefIndex"] as string,
                                    RIBeta = (reader.IsDBNull(reader.GetOrdinal("RefIndexBeta"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RefIndexBeta")),
                                    RIPrBetaShiftInd = (reader.IsDBNull(reader.GetOrdinal("RefIndexBetaInd"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("RefIndexBetaInd")),
                                    RIBetaAdjTyp = reader["RefIndexPriceBetaAdj"] as string,
                                    RILastPr = (reader.IsDBNull(reader.GetOrdinal("RefIndexLastPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RefIndexLastPrice")),
                                    RILivePr = (reader.IsDBNull(reader.GetOrdinal("RefIndexLivePrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RefIndexLivePrice")),
                                    RIPrCap = (reader.IsDBNull(reader.GetOrdinal("RefIndexPriceCap"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RefIndexPriceCap")),
                                    RIPrCapShiftInd = (reader.IsDBNull(reader.GetOrdinal("RefIndexPriceInd"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("RefIndexPriceInd")),
                                    RIPrTyp = reader["RefIndexPriceType"] as string,
                                    RITgtPr = (reader.IsDBNull(reader.GetOrdinal("TargetPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TargetPrice")),
                                    RIMaxPr = (reader.IsDBNull(reader.GetOrdinal("RefIndexMaxPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RefIndexMaxPrice")),
                                    RIPrChng = (reader.IsDBNull(reader.GetOrdinal("RefIndexPriceChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RefIndexPriceChng")),
                                    RIPrChngFinal = (reader.IsDBNull(reader.GetOrdinal("RefIndexPriceChngFinal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RefIndexPriceChngFinal")),

                                    //Discount Target Fields
                                    DscntTgt = (reader.IsDBNull(reader.GetOrdinal("DiscountTarget"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DiscountTarget")),
                                    DscntTgtLastNav = (reader.IsDBNull(reader.GetOrdinal("DiscountTargetLastNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DiscountTargetLastNav")),
                                    DscntToLastPr = (reader.IsDBNull(reader.GetOrdinal("DiscountToLastPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DiscountToLastPrice")),
                                    DscntToLivePr = (reader.IsDBNull(reader.GetOrdinal("DiscountToLivePrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DiscountToLivePrice")),
                                    EstNav = (reader.IsDBNull(reader.GetOrdinal("EstimatedNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EstimatedNav")),
                                    EstNavTyp = reader["EstNavType"] as string,

                                    //Auto Update & Market Price Threshold Settings
                                    AutoUpdate = reader["AutoUpdate"] as string,
                                    AutoUpdateThld = (reader.IsDBNull(reader.GetOrdinal("AutoUpdateThreshold"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AutoUpdateThreshold")),
                                    MktPrThld = (reader.IsDBNull(reader.GetOrdinal("MarketPriceThreshold"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarketPriceThreshold")),
                                    MktPrSprd = (reader.IsDBNull(reader.GetOrdinal("MarketPriceSpread"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarketPriceSpread")),
                                    MktPrFld = reader["MarketPriceField"] as string,

                                    LastPr = (reader.IsDBNull(reader.GetOrdinal("LastPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LastPrice")),
                                    BidPr = (reader.IsDBNull(reader.GetOrdinal("BidPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BidPrice")),
                                    AskPr = (reader.IsDBNull(reader.GetOrdinal("AskPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AskPrice")),
                                    AlgoParams = reader["AlgoParameters"] as string,
                                };
                                dict.Add(data.OrdSeq.GetValueOrDefault(), data);
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

        public IDictionary<Int32, IDictionary<Int32, EMSXOrderFill>> GetOrderFills()
        {
            IDictionary<Int32, IDictionary<Int32, EMSXOrderFill>> dict = new Dictionary<Int32, IDictionary<Int32, EMSXOrderFill>>();

            try
            {
                string sql = GetOrderFillsQuery;
                if (_configuration["ConnectionStrings:ENV"].Equals("DEV"))
                    sql = GetOrderFillsDevQuery;

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                EMSXOrderFill data = new EMSXOrderFill();
                                data.Env = reader["Environment"] as string;
                                data.OrdSeq = reader.GetInt32(reader.GetOrdinal("OrdSeq"));
                                data.OrdRefId = reader["OrdRefId"] as string;
                                data.Status = reader["Status"] as string;
                                data.DayFill = (reader.IsDBNull(reader.GetOrdinal("DayFill"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("DayFill"));
                                data.FillId = (reader.IsDBNull(reader.GetOrdinal("FillId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("FillId"));
                                data.Filled = (reader.IsDBNull(reader.GetOrdinal("FillAmount"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("FillAmount"));
                                data.LastFillDate = reader.IsDBNull(reader.GetOrdinal("LastFillDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("LastFillDate"));
                                data.LastFillTm = reader["LastFillTime"] as string;
                                data.LastPrc = (reader.IsDBNull(reader.GetOrdinal("LastPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LastPrice"));
                                data.LastShares = (reader.IsDBNull(reader.GetOrdinal("LastShares"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("LastShares"));
                                data.LastCapacity = reader["LastCapacity"] as string;
                                data.RemBal = (reader.IsDBNull(reader.GetOrdinal("BalRemain"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BalRemain"));
                                data.PctRemain = (reader.IsDBNull(reader.GetOrdinal("PctRemain"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PctRemain"));
                                data.StartAmt = (reader.IsDBNull(reader.GetOrdinal("StartAmount"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("StartAmount"));
                                data.Working = (reader.IsDBNull(reader.GetOrdinal("WorkingAmount"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("WorkingAmount"));
                                data.AvgPrc = (reader.IsDBNull(reader.GetOrdinal("AvgPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AvgPrice"));
                                data.DayAvgPrc = (reader.IsDBNull(reader.GetOrdinal("DayAvgPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DayAvgPrice"));
                                data.RoutePrc = (reader.IsDBNull(reader.GetOrdinal("RoutePrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RoutePrice"));

                                if (dict.TryGetValue(data.OrdSeq.GetValueOrDefault(), out IDictionary<Int32, EMSXOrderFill> fillDict))
                                {
                                    if (!fillDict.TryGetValue(data.FillId.GetValueOrDefault(), out EMSXOrderFill orderFill))
                                        fillDict.Add(data.FillId.GetValueOrDefault(), data);
                                }
                                else
                                {
                                    fillDict = new Dictionary<Int32, EMSXOrderFill>();
                                    fillDict.Add(data.FillId.GetValueOrDefault(), data);
                                    dict.Add(data.OrdSeq.GetValueOrDefault(), fillDict);
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

        public IList<EMSXOrderStatus> GetEMSXOrderStatus(string Env, string Trader)
        {
            IList<EMSXOrderStatus> list = new List<EMSXOrderStatus>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    string sql = "select * from Trading.EMSXOrderStatus";

                    if (Env != "All" || Trader != "All")
                        sql = sql + " where " + (Env != "All" ? (" Environment = '" + Env + "'") : "") + ((Env != "All" && Trader != "All") ? " and " : "") + (Trader != "All" ? (" Trader = '" + Trader + "'") : "");

                    sql = sql + ";";


                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                EMSXOrderStatus data = new EMSXOrderStatus
                                {
                                    Env = reader["Environment"] as string,
                                    OrdSeq = reader.GetInt32(reader.GetOrdinal("OrdSeq")),
                                    OrdRefId = reader["OrdRefId"] as string,
                                    Ticker = reader["Symbol"] as string,
                                    Bkr = reader["Broker"] as string,
                                    StratType = reader["Strategy"] as string,
                                    OrdSide = reader["OrdSide"] as string,
                                    OrdType = reader["OrdType"] as string,
                                    OrdDate = reader.IsDBNull(reader.GetOrdinal("OrdDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("OrdDate")),
                                    OrdTm = reader["OrdTime"] as string,
                                    Status = reader["Status"] as string,
                                    AvgPrc = (reader.IsDBNull(reader.GetOrdinal("AvgPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AvgPrice")),
                                    DayAvgPrc = (reader.IsDBNull(reader.GetOrdinal("DayAvgPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DayAvgPrice")),
                                    LimitPrc = (reader.IsDBNull(reader.GetOrdinal("LimitPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LimitPrice")),
                                    StopPrc = (reader.IsDBNull(reader.GetOrdinal("StopPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("StopPrice")),
                                    ArrivalPrc = (reader.IsDBNull(reader.GetOrdinal("ArrivalPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ArrivalPrice")),
                                    WorkPrc = (reader.IsDBNull(reader.GetOrdinal("WorkPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("WorkPrice")),
                                    Trader = reader["Trader"] as string,
                                    Amt = (reader.IsDBNull(reader.GetOrdinal("Amount"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("Amount")),
                                    StartAmt = (reader.IsDBNull(reader.GetOrdinal("StartAmount"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("StartAmount")),
                                    Working = (reader.IsDBNull(reader.GetOrdinal("WorkingAmount"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("WorkingAmount")),
                                    RemBal = (reader.IsDBNull(reader.GetOrdinal("BalRemain"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BalRemain")),
                                    PctRemain = (reader.IsDBNull(reader.GetOrdinal("PctRemain"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PctRemain")),
                                    FillId = (reader.IsDBNull(reader.GetOrdinal("FillId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("FillId")),
                                    Filled = (reader.IsDBNull(reader.GetOrdinal("FillAmount"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("FillAmount")),
                                    Principal = (reader.IsDBNull(reader.GetOrdinal("Principal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Principal")),
                                    Isin = reader["Isin"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    PortNum = (reader.IsDBNull(reader.GetOrdinal("PortNum"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("PortNum")),
                                    PortName = reader["PortName"] as string,
                                    PortMgr = reader["PortMgr"] as string,
                                    Exch = reader["Exch"] as string,
                                    ExchDest = reader["ExchDest"] as string,
                                    BkrComm = (reader.IsDBNull(reader.GetOrdinal("BrokerComm"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BrokerComm")),
                                    Acct = reader["AccountId"] as string,
                                    RouteId = (reader.IsDBNull(reader.GetOrdinal("RouteId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("RouteId")),
                                    ApiSeqNum = (reader.IsDBNull(reader.GetOrdinal("ApiSeqNum"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("ApiSeqNum")),
                                    CorrelationId = (reader.IsDBNull(reader.GetOrdinal("CorrelationId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("CorrelationId")),
                                    TraderNotes = reader["TraderNotes"] as string,
                                    Notes = reader["Notes"] as string,
                                    SettleDate = reader.IsDBNull(reader.GetOrdinal("SettleDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("SettleDate")),
                                    TimeStamp = reader["OrdTmStamp"] as string,
                                };

                                list.Add(data);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing EMSX Order status query");
                throw;
            }
            return list;
        }

        public IList<EMSXRouteStatus> GetEMSXRouteStatus(string Env, string Trader)
        {
            IList<EMSXRouteStatus> list = new List<EMSXRouteStatus>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    string sql = "select * from Trading.EMSXRouteStatus";

                    if (Env != "All" || Trader != "All")
                        sql = sql + " where " + (Env != "All" ? (" Environment = '" + Env + "'") : "") + ((Env != "All" && Trader != "All") ? " and " : "") + (Trader != "All" ? (" Trader = '" + Trader + "'") : "");

                    sql = sql + ";";


                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                EMSXRouteStatus data = new EMSXRouteStatus
                                {
                                    Env = reader["Environment"] as string,
                                    OrdSeq = reader.GetInt32(reader.GetOrdinal("OrdSeq")),
                                    OrdRefId = reader["OrdRefId"] as string,
                                    Bkr = reader["Broker"] as string,
                                    StratType = reader["Strategy"] as string,
                                    OrdType = reader["OrdType"] as string,
                                    Status = reader["Status"] as string,
                                    RouteId = (reader.IsDBNull(reader.GetOrdinal("RouteId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("RouteId")),
                                    RouteAsOfDate = reader.IsDBNull(reader.GetOrdinal("RouteAsOfDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("RouteAsOfDate")),
                                    RouteCreateDate = reader.IsDBNull(reader.GetOrdinal("RouteCreateDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("RouteCreateDate")),
                                    RouteCreateTm = reader["RouteCreateTime"] as string,
                                    RouteLastUpdTm = reader["RouteLastUpdTime"] as string,
                                    RoutePrc = (reader.IsDBNull(reader.GetOrdinal("RoutePrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RoutePrice")),
                                    LimitPrc = (reader.IsDBNull(reader.GetOrdinal("LimitPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LimitPrice")),
                                    StopPrc = (reader.IsDBNull(reader.GetOrdinal("StopPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("StopPrice")),
                                    AvgPrc = (reader.IsDBNull(reader.GetOrdinal("AvgPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AvgPrice")),
                                    DayAvgPrc = (reader.IsDBNull(reader.GetOrdinal("DayAvgPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DayAvgPrice")),
                                    Trader = reader["Trader"] as string,
                                    Amt = (reader.IsDBNull(reader.GetOrdinal("Amount"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("Amount")),
                                    StartAmt = (reader.IsDBNull(reader.GetOrdinal("StartAmount"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("StartAmount")),
                                    Working = (reader.IsDBNull(reader.GetOrdinal("WorkingAmount"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("WorkingAmount")),
                                    RemBal = (reader.IsDBNull(reader.GetOrdinal("BalRemain"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BalRemain")),
                                    PctRemain = (reader.IsDBNull(reader.GetOrdinal("PctRemain"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PctRemain")),
                                    DayFill = (reader.IsDBNull(reader.GetOrdinal("DayFill"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("DayFill")),
                                    FillId = (reader.IsDBNull(reader.GetOrdinal("FillId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("FillId")),
                                    Filled = (reader.IsDBNull(reader.GetOrdinal("FillAmount"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("FillAmount")),
                                    LastFillDate = reader.IsDBNull(reader.GetOrdinal("LastFillDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("LastFillDate")),
                                    LastFillTm = reader["LastFillTime"] as string,
                                    LastPrc = (reader.IsDBNull(reader.GetOrdinal("LastPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LastPrice")),
                                    LastShares = (reader.IsDBNull(reader.GetOrdinal("LastShares"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("LastShares")),
                                    LastCapacity = reader["LastCapacity"] as string,
                                    Principal = (reader.IsDBNull(reader.GetOrdinal("Principal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Principal")),
                                    ExchDest = reader["ExchDest"] as string,
                                    ExecBkr = reader["ExecBroker"] as string,
                                    BkrComm = (reader.IsDBNull(reader.GetOrdinal("BrokerComm"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BrokerComm")),
                                    Acct = reader["AccountId"] as string,
                                    ApiSeqNum = (reader.IsDBNull(reader.GetOrdinal("ApiSeqNum"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("ApiSeqNum")),
                                    CorrelationId = (reader.IsDBNull(reader.GetOrdinal("CorrelationId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("CorrelationId")),
                                    TraderNotes = reader["TraderNotes"] as string,
                                    Notes = reader["Notes"] as string,
                                    OtcFlag = reader["OTCFlag"] as string,
                                    UrgencyLvl = (reader.IsDBNull(reader.GetOrdinal("Urgency"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("Urgency")),
                                    ReasonCd = reader["ReasonCode"] as string,
                                    ReasonDesc = reader["ReasonDesc"] as string,
                                    SettleDate = reader.IsDBNull(reader.GetOrdinal("SettleDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("SettleDate")),
                                    StratStyle = reader["StrategyStyle"] as string,
                                };

                                list.Add(data);
                            }

                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing EMSX Order status query");
                throw;
            }

            return list;
        }

        public IList<EMSXOrderFill> GetEMSXOrderFills(string Env)
        {
            IList<EMSXOrderFill> list = new List<EMSXOrderFill>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    string sql = "select * from Trading.EMSXOrderFill";
                    if (Env != "All")
                        sql = sql + " where Environment = '" + Env + "'";
                    sql = sql + ";";


                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                EMSXOrderFill data = new EMSXOrderFill
                                {
                                    Env = reader["Environment"] as string,
                                    OrdSeq = reader.GetInt32(reader.GetOrdinal("OrdSeq")),
                                    OrdRefId = reader["OrdRefId"] as string,
                                    Status = reader["Status"] as string,
                                    DayFill = (reader.IsDBNull(reader.GetOrdinal("DayFill"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("DayFill")),
                                    FillId = (reader.IsDBNull(reader.GetOrdinal("FillId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("FillId")),
                                    Filled = (reader.IsDBNull(reader.GetOrdinal("FillAmount"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("FillAmount")),
                                    LastFillDate = reader.IsDBNull(reader.GetOrdinal("LastFillDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("LastFillDate")),
                                    LastFillTm = reader["LastFillTime"] as string,
                                    LastPrc = (reader.IsDBNull(reader.GetOrdinal("LastPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LastPrice")),
                                    LastShares = (reader.IsDBNull(reader.GetOrdinal("LastShares"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("LastShares")),
                                    LastCapacity = reader["LastCapacity"] as string,
                                    RemBal = (reader.IsDBNull(reader.GetOrdinal("BalRemain"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BalRemain")),
                                    PctRemain = (reader.IsDBNull(reader.GetOrdinal("PctRemain"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PctRemain")),
                                    StartAmt = (reader.IsDBNull(reader.GetOrdinal("StartAmount"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("StartAmount")),
                                    Working = (reader.IsDBNull(reader.GetOrdinal("WorkingAmount"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("WorkingAmount")),
                                    AvgPrc = (reader.IsDBNull(reader.GetOrdinal("AvgPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AvgPrice")),
                                    DayAvgPrc = (reader.IsDBNull(reader.GetOrdinal("DayAvgPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DayAvgPrice")),
                                    RoutePrc = (reader.IsDBNull(reader.GetOrdinal("RoutePrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RoutePrice")),
                                };

                                list.Add(data);
                            }

                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing EMSX Order fills query");
                throw;
            }

            return list;
        }

        public void SaveRouteHistUpdates(IDictionary<string, EMSXRouteStatus> dict)
        {
            StringBuilder sCommand = new StringBuilder("INSERT INTO Trading.StgEMSXRouteStatusHist"
                + " (Environment, OrdSeq, ApiSeqNum, Symbol, Sedol, Isin, OrdSide, OrdRefId, Broker,"
                + " Strategy, OrdType, Status, RouteId, RouteAsOfDate, RouteCreateDate, RouteCreateTime,"
                + " RouteLastUpdTime, RoutePrice, LimitPrice, StopPrice, AvgPrice, DayAvgPrice, Trader,"
                + " Amount, StartAmount, WorkingAmount, SettleAmount, BalRemain, PctRemain, DayFill,"
                + " FillId, FillAmount, LastFillDate, LastFillTime, LastPrice, LastShares, LastCapacity,"
                + " Principal, ExchDest, ExecBroker, BrokerComm, AccountId, CorrelationId, TraderNotes,"
                + " Notes, OTCFlag, Urgency, ReasonCode, ReasonDesc, SettleDate, StrategyStyle"
                + " ) values ");

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    if (dict != null && dict.Count > 0)
                    {
                        using (MySqlTransaction trans = connection.BeginTransaction())
                        {
                            //_logger.LogInformation("Deleting data from Trading.StgEMSXRouteStatusHist");
                            string sqlDelete = "delete from Trading.StgEMSXRouteStatusHist";
                            using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                            {
                                command.ExecuteNonQuery();
                            }

                            List<string> Rows = new List<string>();
                            StringBuilder sb = new StringBuilder();
                            foreach (EMSXRouteStatus data in dict.Values)
                            {
                                // Environment
                                if (!string.IsNullOrEmpty(data.Env))
                                    sb.Append(string.Concat("'", data.Env, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // OrdSeq
                                if (data.OrdSeq.HasValue)
                                    sb.Append(data.OrdSeq).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // ApiSeqNum
                                if (data.ApiSeqNum.HasValue)
                                    sb.Append(data.ApiSeqNum).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Symbol
                                if (!string.IsNullOrEmpty(data.Ticker))
                                    sb.Append(string.Concat("'", data.Ticker, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Sedol
                                if (!string.IsNullOrEmpty(data.Sedol))
                                    sb.Append(string.Concat("'", data.Sedol, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Isin
                                if (!string.IsNullOrEmpty(data.Isin))
                                    sb.Append(string.Concat("'", data.Isin, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // OrdSide
                                if (!string.IsNullOrEmpty(data.OrdSide))
                                    sb.Append(string.Concat("'", data.OrdSide, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // OrdRefId
                                if (!string.IsNullOrEmpty(data.OrdRefId))
                                    sb.Append(string.Concat("'", data.OrdRefId, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Broker
                                if (!string.IsNullOrEmpty(data.Bkr))
                                    sb.Append(string.Concat("'", data.Bkr, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Strategy
                                if (!string.IsNullOrEmpty(data.StratType))
                                    sb.Append(string.Concat("'", data.StratType, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // OrdType
                                if (!string.IsNullOrEmpty(data.OrdType))
                                    sb.Append(string.Concat("'", data.OrdType, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Status
                                if (!string.IsNullOrEmpty(data.Status))
                                    sb.Append(string.Concat("'", data.Status, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // RouteId
                                if (data.RouteId.HasValue)
                                    sb.Append(data.RouteId).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // RouteAsOfDate
                                if (data.RouteAsOfDate.HasValue)
                                    sb.Append(string.Concat("'", DateUtils.ConvertDate(data.RouteAsOfDate, "yyyy-MM-dd"), "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // RouteCreateDate
                                if (data.RouteCreateDate.HasValue)
                                    sb.Append(string.Concat("'", DateUtils.ConvertDate(data.RouteCreateDate, "yyyy-MM-dd"), "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // RouteCreateTime
                                if (!string.IsNullOrEmpty(data.RouteCreateTm))
                                    sb.Append(string.Concat("'", data.RouteCreateTm, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // RouteLastUpdTime
                                if (!string.IsNullOrEmpty(data.RouteLastUpdTm))
                                    sb.Append(string.Concat("'", data.RouteLastUpdTm, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // RoutePrice
                                if (data.RoutePrc.HasValue)
                                    sb.Append(data.RoutePrc).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // LimitPrice
                                if (data.LimitPrc.HasValue)
                                    sb.Append(data.LimitPrc).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // StopPrice
                                if (data.StopPrc.HasValue)
                                    sb.Append(data.StopPrc).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // AvgPrice
                                if (data.AvgPrc.HasValue)
                                    sb.Append(data.AvgPrc).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // DayAvgPrice
                                if (data.DayAvgPrc.HasValue)
                                    sb.Append(data.DayAvgPrc).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Trader
                                if (!string.IsNullOrEmpty(data.Trader))
                                    sb.Append(string.Concat("'", data.Trader, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Amount
                                if (data.Amt.HasValue)
                                    sb.Append(data.Amt).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // StartAmount
                                if (data.StartAmt.HasValue)
                                    sb.Append(data.StartAmt).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // WorkingAmount
                                if (data.Working.HasValue)
                                    sb.Append(data.Working).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // SettleAmount
                                if (data.SettleAmt.HasValue)
                                    sb.Append(data.SettleAmt).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // BalRemain
                                if (data.RemBal.HasValue)
                                    sb.Append(data.RemBal).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // PctRemain
                                if (data.PctRemain.HasValue)
                                    sb.Append(data.PctRemain).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // DayFill
                                if (data.DayFill.HasValue)
                                    sb.Append(data.DayFill).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // FillId
                                if (data.FillId.HasValue)
                                    sb.Append(data.FillId).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // FillAmount
                                if (data.Filled.HasValue)
                                    sb.Append(data.Filled).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // LastFillDate
                                if (data.LastFillDate.HasValue)
                                    sb.Append(string.Concat("'", DateUtils.ConvertDate(data.LastFillDate, "yyyy-MM-dd"), "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // LastFillTime
                                if (!string.IsNullOrEmpty(data.LastFillTm))
                                    sb.Append(string.Concat("'", data.LastFillTm, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // LastPrice
                                if (data.LastPrc.HasValue)
                                    sb.Append(data.LastPrc).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // LastShares
                                if (data.LastShares.HasValue)
                                    sb.Append(data.LastShares).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // LastCapacity
                                if (!string.IsNullOrEmpty(data.LastCapacity))
                                    sb.Append(string.Concat("'", data.LastCapacity, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Principal
                                if (data.Principal.HasValue)
                                    sb.Append(data.Principal).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // ExchDest
                                if (!string.IsNullOrEmpty(data.ExchDest))
                                    sb.Append(string.Concat("'", data.ExchDest, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // ExecBroker
                                if (!string.IsNullOrEmpty(data.ExecBkr))
                                    sb.Append(string.Concat("'", data.ExecBkr, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // BrokerComm
                                if (data.BkrComm.HasValue)
                                    sb.Append(data.BkrComm).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // AccountId
                                if (!string.IsNullOrEmpty(data.Acct))
                                    sb.Append(string.Concat("'", data.Acct, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // CorrelationId
                                if (data.CorrelationId.HasValue)
                                    sb.Append(data.CorrelationId).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // TraderNotes
                                if (!string.IsNullOrEmpty(data.TraderNotes))
                                    sb.Append(string.Concat("'", data.TraderNotes, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Notes
                                if (!string.IsNullOrEmpty(data.Notes))
                                    sb.Append(string.Concat("'", data.Notes, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // OTCFlag
                                if (!string.IsNullOrEmpty(data.OtcFlag))
                                    sb.Append(string.Concat("'", data.OtcFlag, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Urgency
                                if (data.UrgencyLvl.HasValue)
                                    sb.Append(data.UrgencyLvl).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // ReasonCode
                                if (!string.IsNullOrEmpty(data.ReasonCd))
                                    sb.Append(string.Concat("'", data.ReasonCd, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // ReasonDesc
                                if (!string.IsNullOrEmpty(data.ReasonDesc))
                                    sb.Append(string.Concat("'", data.ReasonDesc, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // SettleDate
                                if (data.SettleDate.HasValue)
                                    sb.Append(string.Concat("'", DateUtils.ConvertDate(data.SettleDate, "yyyy-MM-dd"), "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // StrategyStyle
                                if (!string.IsNullOrEmpty(data.StratStyle))
                                    sb.Append(string.Concat("'", data.StratStyle, "'"));
                                else
                                    sb.Append("null");

                                string row = sb.ToString();
                                Rows.Add(string.Concat("(", row, ")"));
                                sb.Clear();
                            }

                            sCommand.Append(string.Join(",", Rows));
                            sCommand.Append(";");

                            //_logger.LogInformation("Inserting data into Trading.StgEMSXRouteStatusHist");
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), connection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }

                            //_logger.LogInformation("Moving data to Trading.EMSXRouteStatusHist");
                            using (MySqlCommand command = new MySqlCommand(SaveRouteStatusHistQuery, connection))
                            {
                                command.CommandType = CommandType.StoredProcedure;
                                command.ExecuteNonQuery();
                            }

                            trans.Commit();
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving data");
            }
        }

        public IList<EMSXRouteStatus> GetEMSXTradeHistory(string StartDate, string EndDate)
        {
            IList<EMSXRouteStatus> fulllist = new List<EMSXRouteStatus>();
            try
            {
                string sql = getTradeHistoryQuery;
                sql += " where RouteAsOfDate between '" + StartDate + "' and '" + EndDate + "'";
                sql += " order by RouteAsOfDate desc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                EMSXRouteStatus listitem = new EMSXRouteStatus()
                                {
                                    Symbol = reader["Symbol"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    Isin = reader["Isin"] as string,
                                    OrdSide = reader["OrdSide"] as string,
                                    OrdSeq = reader.GetInt32(reader.GetOrdinal("OrdSeq")),
                                    OrdRefId = reader["OrdRefId"] as string,
                                    Bkr = reader["Broker"] as string,
                                    StratType = reader["Strategy"] as string,
                                    OrdType = reader["OrdType"] as string,
                                    Status = reader["Status"] as string,
                                    RouteId = (reader.IsDBNull(reader.GetOrdinal("RouteId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("RouteId")),
                                    RouteAsOfDate = reader.IsDBNull(reader.GetOrdinal("RouteAsOfDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("RouteAsOfDate")),
                                    RouteCreateDate = reader.IsDBNull(reader.GetOrdinal("RouteCreateDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("RouteCreateDate")),
                                    RouteCreateTm = reader["RouteCreateTime"] as string,
                                    RouteLastUpdTm = reader["RouteLastUpdTime"] as string,
                                    RoutePrc = (reader.IsDBNull(reader.GetOrdinal("RoutePrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RoutePrice")),
                                    LimitPrc = (reader.IsDBNull(reader.GetOrdinal("LimitPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LimitPrice")),
                                    StopPrc = (reader.IsDBNull(reader.GetOrdinal("StopPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("StopPrice")),
                                    AvgPrc = (reader.IsDBNull(reader.GetOrdinal("AvgPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AvgPrice")),
                                    DayAvgPrc = (reader.IsDBNull(reader.GetOrdinal("DayAvgPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DayAvgPrice")),
                                    Trader = reader["Trader"] as string,
                                    Amt = (reader.IsDBNull(reader.GetOrdinal("Amount"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("Amount")),
                                    StartAmt = (reader.IsDBNull(reader.GetOrdinal("StartAmount"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("StartAmount")),
                                    Working = (reader.IsDBNull(reader.GetOrdinal("WorkingAmount"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("WorkingAmount")),
                                    SettleAmt = (reader.IsDBNull(reader.GetOrdinal("SettleAmount"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("SettleAmount")),

                                    RemBal = (reader.IsDBNull(reader.GetOrdinal("BalRemain"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BalRemain")),
                                    PctRemain = (reader.IsDBNull(reader.GetOrdinal("PctRemain"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PctRemain")),
                                    DayFill = (reader.IsDBNull(reader.GetOrdinal("DayFill"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("DayFill")),
                                    FillId = (reader.IsDBNull(reader.GetOrdinal("FillId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("FillId")),
                                    Filled = (reader.IsDBNull(reader.GetOrdinal("FillAmount"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("FillAmount")),
                                    LastFillDate = reader.IsDBNull(reader.GetOrdinal("LastFillDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("LastFillDate")),
                                    LastFillTm = reader["LastFillTime"] as string,
                                    LastPrc = (reader.IsDBNull(reader.GetOrdinal("LastPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LastPrice")),
                                    LastShares = (reader.IsDBNull(reader.GetOrdinal("LastShares"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("LastShares")),
                                    LastCapacity = reader["LastCapacity"] as string,
                                    Principal = (reader.IsDBNull(reader.GetOrdinal("Principal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Principal")),
                                    ExchDest = reader["ExchDest"] as string,
                                    ExecBkr = reader["ExecBroker"] as string,
                                    BkrComm = (reader.IsDBNull(reader.GetOrdinal("BrokerComm"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BrokerComm")),
                                    Acct = reader["AccountId"] as string,
                                    ApiSeqNum = (reader.IsDBNull(reader.GetOrdinal("ApiSeqNum"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("ApiSeqNum")),
                                    CorrelationId = (reader.IsDBNull(reader.GetOrdinal("CorrelationId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("CorrelationId")),
                                    TraderNotes = reader["TraderNotes"] as string,
                                    Notes = reader["Notes"] as string,
                                    OtcFlag = reader["OTCFlag"] as string,
                                    UrgencyLvl = (reader.IsDBNull(reader.GetOrdinal("Urgency"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("Urgency")),
                                    ReasonCd = reader["ReasonCode"] as string,
                                    ReasonDesc = reader["ReasonDesc"] as string,
                                    SettleDate = reader.IsDBNull(reader.GetOrdinal("SettleDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("SettleDate")),
                                    StratStyle = reader["StrategyStyle"] as string,

                                    RI = reader["RefIndex"] as string,
                                    RIBeta = (reader.IsDBNull(reader.GetOrdinal("RefIndexBeta"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RefIndexBeta")),
                                    RIBetaInd = (reader.IsDBNull(reader.GetOrdinal("RefIndexBetaInd"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("RefIndexBetaInd")),
                                    RIBetaAdjTyp = reader["RefIndexPriceBetaAdj"] as string,
                                    RILastPr = (reader.IsDBNull(reader.GetOrdinal("RefIndexLastPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RefIndexLastPrice")),
                                    RILivePr = (reader.IsDBNull(reader.GetOrdinal("RefIndexLivePrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RefIndexLivePrice")),
                                    RIPrCap = (reader.IsDBNull(reader.GetOrdinal("RefIndexPriceCap"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RefIndexPriceCap")),
                                    RIPriceInd = (reader.IsDBNull(reader.GetOrdinal("RefIndexPriceInd"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("RefIndexPriceInd")),
                                    RIPrChng = (reader.IsDBNull(reader.GetOrdinal("RefIndexPriceChng"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RefIndexPriceChng")),
                                    RIPrChngFinal = (reader.IsDBNull(reader.GetOrdinal("RefIndexPriceChngFinal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RefIndexPriceChngFinal")),
                                    RIMaxPr = (reader.IsDBNull(reader.GetOrdinal("RefIndexMaxPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RefIndexMaxPrice")),
                                    RIPrTyp = reader["RefIndexPriceBetaAdj"] as string,
                                    TargetPrc = (reader.IsDBNull(reader.GetOrdinal("TargetPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TargetPrice")),
                                    DscntTgt = (reader.IsDBNull(reader.GetOrdinal("DiscountTarget"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DiscountTarget")),
                                    DscntTgtLastNav = (reader.IsDBNull(reader.GetOrdinal("DiscountTargetLastNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DiscountTargetLastNav")),
                                    DscntToLastPr = (reader.IsDBNull(reader.GetOrdinal("DiscountToLastPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DiscountToLastPrice")),
                                    DscntToLivePr = (reader.IsDBNull(reader.GetOrdinal("DiscountToLivePrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DiscountToLivePrice")),

                                    EstNav = (reader.IsDBNull(reader.GetOrdinal("EstimatedNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EstimatedNav")),
                                    EstDscnt = (reader.IsDBNull(reader.GetOrdinal("EstimatedDiscount"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EstimatedDiscount")),

                                    AutoUpdate = reader["AutoUpdate"] as string,
                                    AutoUpdateThld = (reader.IsDBNull(reader.GetOrdinal("AutoUpdateThreshold"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AutoUpdateThreshold")),
                                    MktPrThld = (reader.IsDBNull(reader.GetOrdinal("MarketPriceThreshold"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MarketPriceThreshold")),
                                    MktPrFld = reader["MarketPriceField"] as string,
                                    LastLivePr = (reader.IsDBNull(reader.GetOrdinal("LastLivePrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LastLivePrice")),
                                    BidPr = (reader.IsDBNull(reader.GetOrdinal("BidPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BidPrice")),
                                    AskPr = (reader.IsDBNull(reader.GetOrdinal("AskPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AskPrice")),
                                    EstNavTyp = reader["EstNavType"] as string,
                                    AlgoParams = reader["AlgoParameters"] as string,
                                };
                                fulllist.Add(listitem);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing EMSX Trade history details. ");
                throw;
            }
            return fulllist;
        }

        public IDictionary<string, EMSXRouteStatus> GetRouteStatusHist()
        {
            IDictionary<string, EMSXRouteStatus> dict = new Dictionary<string, EMSXRouteStatus>();

            try
            {
                string sql = GetRouteStatusHistQuery;
                if (_configuration["ConnectionStrings:ENV"].Equals("DEV"))
                    sql = GetRouteStatusHistDevQuery;

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetRouteStatusHistQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                EMSXRouteStatus data = new EMSXRouteStatus();
                                data.Env = reader["Environment"] as string;
                                data.OrdSeq = reader.GetInt32(reader.GetOrdinal("OrdSeq"));
                                data.OrdRefId = reader["OrdRefId"] as string;
                                data.Bkr = reader["Broker"] as string;
                                data.StratType = reader["Strategy"] as string;
                                data.OrdType = reader["OrdType"] as string;
                                data.Status = reader["Status"] as string;
                                data.RouteId = (reader.IsDBNull(reader.GetOrdinal("RouteId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("RouteId"));
                                data.RouteAsOfDate = reader.IsDBNull(reader.GetOrdinal("RouteAsOfDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("RouteAsOfDate"));
                                data.RouteCreateDate = reader.IsDBNull(reader.GetOrdinal("RouteCreateDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("RouteCreateDate"));
                                data.RouteCreateTm = reader["RouteCreateTime"] as string;
                                data.RouteLastUpdTm = reader["RouteLastUpdTime"] as string;
                                data.RoutePrc = (reader.IsDBNull(reader.GetOrdinal("RoutePrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RoutePrice"));
                                data.LimitPrc = (reader.IsDBNull(reader.GetOrdinal("LimitPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LimitPrice"));
                                data.StopPrc = (reader.IsDBNull(reader.GetOrdinal("StopPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("StopPrice"));
                                data.AvgPrc = (reader.IsDBNull(reader.GetOrdinal("AvgPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AvgPrice"));
                                data.DayAvgPrc = (reader.IsDBNull(reader.GetOrdinal("DayAvgPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DayAvgPrice"));
                                //data.Trader = reader["Trader"] as string;
                                data.Amt = (reader.IsDBNull(reader.GetOrdinal("Amount"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("Amount"));
                                //data.StartAmt = (reader.IsDBNull(reader.GetOrdinal("StartAmount"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("StartAmount"));
                                data.Working = (reader.IsDBNull(reader.GetOrdinal("WorkingAmount"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("WorkingAmount"));
                                data.RemBal = (reader.IsDBNull(reader.GetOrdinal("BalRemain"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BalRemain"));
                                data.PctRemain = (reader.IsDBNull(reader.GetOrdinal("PctRemain"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PctRemain"));
                                data.DayFill = (reader.IsDBNull(reader.GetOrdinal("DayFill"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("DayFill"));
                                data.FillId = (reader.IsDBNull(reader.GetOrdinal("FillId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("FillId"));
                                data.Filled = (reader.IsDBNull(reader.GetOrdinal("FillAmount"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("FillAmount"));
                                data.LastFillDate = reader.IsDBNull(reader.GetOrdinal("LastFillDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("LastFillDate"));
                                data.LastFillTm = reader["LastFillTime"] as string;
                                data.LastPrc = (reader.IsDBNull(reader.GetOrdinal("LastPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LastPrice"));
                                data.LastShares = (reader.IsDBNull(reader.GetOrdinal("LastShares"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("LastShares"));
                                data.LastCapacity = reader["LastCapacity"] as string;
                                data.Principal = (reader.IsDBNull(reader.GetOrdinal("Principal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Principal"));
                                data.ExchDest = reader["ExchDest"] as string;
                                data.ExecBkr = reader["ExecBroker"] as string;
                                data.BkrComm = (reader.IsDBNull(reader.GetOrdinal("BrokerComm"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BrokerComm"));
                                data.Acct = reader["AccountId"] as string;
                                data.ApiSeqNum = (reader.IsDBNull(reader.GetOrdinal("ApiSeqNum"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("ApiSeqNum"));
                                data.CorrelationId = (reader.IsDBNull(reader.GetOrdinal("CorrelationId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("CorrelationId"));
                                data.TraderNotes = reader["TraderNotes"] as string;
                                data.Notes = reader["Notes"] as string;
                                data.OtcFlag = reader["OTCFlag"] as string;
                                data.UrgencyLvl = (reader.IsDBNull(reader.GetOrdinal("Urgency"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("Urgency"));
                                data.ReasonCd = reader["ReasonCode"] as string;
                                data.ReasonDesc = reader["ReasonDesc"] as string;
                                data.SettleDate = reader.IsDBNull(reader.GetOrdinal("SettleDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("SettleDate"));
                                data.StratStyle = reader["StrategyStyle"] as string;

                                string id = data.OrdSeq.GetValueOrDefault() + "|" + data.ApiSeqNum.GetValueOrDefault();
                                dict.Add(id, data);
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

        public IList<EMSXOrderError> GetOrderErrors()
        {
            IList<EMSXOrderError> list = new List<EMSXOrderError>();
            try
            {
                string sql = GetOrderErrorsQuery;
                if (_configuration["ConnectionStrings:ENV"].Equals("DEV"))
                    sql = GetOrderErrorsDevQuery;

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                EMSXOrderError data = new EMSXOrderError()
                                {
                                    Env = reader["Environment"] as string,
                                    CorrelationId = (reader.IsDBNull(reader.GetOrdinal("CorrelationId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("CorrelationId")),
                                    ErrorCode = (reader.IsDBNull(reader.GetOrdinal("ErrorCode"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("ErrorCode")),
                                    ErrorMessage = reader["ErrorMessage"] as string,
                                    Notes = reader["Notes"] as string,
                                    ErrorType = reader["ErrorType"] as string,
                                    OrdDate = reader.IsDBNull(reader.GetOrdinal("OrdDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("OrdDate"))
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

        public void SaveOrderErrors(IList<EMSXOrderError> list)
        {
            try
            {
                StringBuilder sCommand = new StringBuilder("insert into Trading.StgEMSXOrderError " +
                    "(Environment, CorrelationId, OrdDate, ErrorCode, ErrorMessage, Notes, ErrorType) values ");

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlTransaction trans = connection.BeginTransaction())
                    {
                        _logger.LogInformation("Deleting data from Trading.StgEMSXOrderError");
                        string sqlDelete = "delete from Trading.StgEMSXOrderError";
                        using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                        {
                            command.ExecuteNonQuery();
                        }

                        List<string> Rows = new List<string>();
                        StringBuilder sb = new StringBuilder();
                        foreach (EMSXOrderError data in list)
                        {
                            //Environment
                            if (!string.IsNullOrEmpty(data.Env))
                                sb.Append(string.Concat("'", data.Env, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //CorrelationId
                            if (data.CorrelationId.HasValue)
                                sb.Append(data.CorrelationId).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //OrdDate
                            if (data.OrdDate.HasValue)
                                sb.Append(string.Concat("'", DateUtils.ConvertDate(data.OrdDate, "yyyy-MM-dd"), "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //ErrorCode
                            if (data.ErrorCode.HasValue)
                                sb.Append(data.ErrorCode).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //ErrorMessage
                            if (!string.IsNullOrEmpty(data.ErrorMessage))
                                sb.Append(string.Concat("'", data.ErrorMessage, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //Notes
                            if (!string.IsNullOrEmpty(data.Notes))
                                sb.Append(string.Concat("'", data.Notes, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            //ErrorType
                            if (!string.IsNullOrEmpty(data.ErrorType))
                                sb.Append(string.Concat("'", data.ErrorType, "'"));
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

                        _logger.LogInformation("Moving data to Trading.EMSXOrderError");
                        using (MySqlCommand command = new MySqlCommand(SaveOrderErrorsQuery, connection))
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
                _logger.LogError(ex, "Error saving EMSX Order Errors into database");
                throw;
            }
        }

        private const string BatchOrdersStgTableName = "Trading.StgBatchDetail";
        private const string SaveBatchOrdersQuery = "Trading.spPopulateBatchDetails";
        private const string GetBatchOrdersQuery = "select * from Trading.BatchDetail where date_format(OrderDate, '%Y-%m-%d') = date_format(CONVERT_TZ(current_timestamp(),'+00:00','-8:00'), '%Y-%m-%d')";

        private const string GetOrderStatusQuery = "select * from Trading.EMSXOrderStatus where date_format(OrdDate, '%Y-%m-%d') = date_format(CONVERT_TZ(current_timestamp(),'+00:00','-8:00'), '%Y-%m-%d')";
        private const string GetRouteStatusQuery = "select * from Trading.EMSXRouteStatus where date_format(RouteCreateDate, '%Y-%m-%d') = date_format(CONVERT_TZ(current_timestamp(),'+00:00','-8:00'), '%Y-%m-%d')";
        private const string GetRouteStatusHistQuery = "select * from Trading.EMSXRouteStatusHist where date_format(RouteCreateDate, '%Y-%m-%d') = date_format(CONVERT_TZ(current_timestamp(),'+00:00','-8:00'), '%Y-%m-%d')";
        private const string getTradeHistoryQuery = "select * from Trading.EMSXRouteStatus";
        private const string GetOrderFillsQuery = "select * from Trading.EMSXOrderFill where date_format(OrdDate, '%Y-%m-%d') = date_format(CONVERT_TZ(current_timestamp(),'+00:00','-8:00'), '%Y-%m-%d')";
        private const string GetOrderErrorsQuery = "select * from Trading.EMSXOrderError where date_format(OrdDate, '%Y-%m-%d') = date_format(CONVERT_TZ(current_timestamp(),'+00:00','-8:00'), '%Y-%m-%d')";

        private const string GetOrderStatusDevQuery = "select * from Trading.EMSXOrderStatus where date_format(OrdDate, '%Y-%m-%d') = '2025-03-03'";
        private const string GetRouteStatusDevQuery = "select * from Trading.EMSXRouteStatus where RouteCreateDate = '2025-03-03'";
        private const string GetOrderFillsDevQuery = "select * from Trading.EMSXOrderFill where date_format(OrdDate, '%Y-%m-%d') = '2025-03-03'";
        private const string GetBatchOrdersDevQuery = "select * from Trading.BatchDetail where date_format(OrdDate, '%Y-%m-%d') = '2025-03-03'";
        private const string GetOrderErrorsDevQuery = "select * from Trading.EMSXOrderError where date_format(OrdDate, '%Y-%m-%d') = '2025-03-03'";
        private const string GetRouteStatusHistDevQuery = "select * from Trading.EMSXRouteStatusHist where date_format(RouteCreateDate, '%Y-%m-%d') = '2025-03-03'";

        private const string SaveOrderStatusQuery = "Trading.spPopulateEMSXOrderStatus";
        private const string SaveRouteStatusQuery = "Trading.spPopulateEMSXRouteStatus";
        private const string SaveRouteStatusHistQuery = "Trading.spPopulateEMSXRouteStatusHistory";
        private const string SaveOrderFillQuery = "Trading.spPopulateEMSXOrderFills";
        private const string SaveOrderErrorsQuery = "Trading.spPopulateEMSXOrderErrors";

        private const string GetBatchTemplateQuery = "select * from Trading.BatchTemplate";
        private const string GetBatchTemplatesQuery = "select distinct TemplateName from Trading.BatchTemplate where SampleTemplate is null order by TemplateName";
        private const string GetBatchTemplatesForUserQuery = "select distinct TemplateName from Trading.BatchTemplate";
    }
}