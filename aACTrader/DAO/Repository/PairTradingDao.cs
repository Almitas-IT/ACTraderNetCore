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
    public class PairTradingDao : IPairTradingDao
    {
        private readonly ILogger<PairTradingDao> _logger;
        private readonly IConfiguration _configuration;

        private const string DELIMITER = ",";

        public PairTradingDao(ILogger<PairTradingDao> logger, IConfiguration configuration)
        {
            _logger = logger;
            this._configuration = configuration;
            _logger.LogInformation("Initializing PairTradingDao...");
        }

        public IList<string> GetTemplates()
        {
            throw new NotImplementedException();
        }

        public IList<PairOrderTemplate> GetTemplate(string templateName)
        {
            IList<PairOrderTemplate> list = new List<PairOrderTemplate>();
            PairOrderTemplate data = new PairOrderTemplate();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    string sql = GetTemplateQuery + " where TemplateName = '" + templateName + "'";

                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                PairOrderDetail detail = new PairOrderDetail();
                                detail.TemplateName = reader["TemplateName"] as string;
                                detail.Symbol = reader["Symbol"] as string;
                                detail.OrderSide = reader["OrderSide"] as string;
                                detail.OrderType = reader["OrderType"] as string;
                                detail.AccountNumber = reader["AccountNumber"] as string;
                                detail.Destination = reader["Destination"] as string;
                                detail.BrokerStrategy = reader["BrokerStrategy"] as string;
                                detail.OrderQty = (reader.IsDBNull(reader.GetOrdinal("OrderQty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OrderQty"));
                                detail.Locate = reader["Locate"] as string;
                                detail.APExpire = reader["APExpire"] as string;
                                detail.APStartDate = reader["APStartDate"] as string;
                                detail.APEndDate = reader["APEndDate"] as string;
                                detail.APUrgency = reader["APUrgency"] as string;
                                detail.APMaxPctVol = (reader.IsDBNull(reader.GetOrdinal("APMaxPctVol"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("APMaxPctVol"));
                                detail.APWouldPrice = (reader.IsDBNull(reader.GetOrdinal("APWouldPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("APWouldPrice"));
                                detail.APMinDarkFillSize = (reader.IsDBNull(reader.GetOrdinal("APMinDarkFillSize"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("APMinDarkFillSize"));
                                detail.APDisplayMode = reader["APDisplayMode"] as string;
                                detail.APDisplaySize = (reader.IsDBNull(reader.GetOrdinal("APDisplaySize"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("APDisplaySize"));
                                detail.APLiquiditySeek = reader["APLiquiditySeek"] as string;
                                detail.APSeekPrice = (reader.IsDBNull(reader.GetOrdinal("APSeekPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("APSeekPrice"));
                                detail.APSeekMinQty = (reader.IsDBNull(reader.GetOrdinal("APSeekMinQty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("APSeekMinQty"));
                                detail.APPctOfVolume = (reader.IsDBNull(reader.GetOrdinal("APPctOfVolume"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("APPctOfVolume"));
                                detail.APTradingStyle = reader["APTradingStyle"] as string;
                                detail.APTradingSession = reader["APTradingSession"] as string;
                                detail.APSORPreference = reader["APSORPreference"] as string;
                                detail.APSORSessionPreference = reader["APSORSessionPreference"] as string;
                                detail.APStyle = reader["APStyle"] as string;

                                detail.PairStrategy = reader["PairStrategy"] as string;
                                detail.PairRatioSetup = reader["PairRatioSetup"] as string;
                                detail.PairInitiateLeg = reader["PairInitiateLeg"] as string;
                                detail.PairSpreadOper = reader["PairSpreadOper"] as string;
                                detail.PairRatio = (reader.IsDBNull(reader.GetOrdinal("PairRatio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PairRatio"));
                                detail.UnhedgedQty = (reader.IsDBNull(reader.GetOrdinal("PairUnhedgedQty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PairUnhedgedQty"));
                                detail.TickSize = (reader.IsDBNull(reader.GetOrdinal("PairPriceTickSize"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PairPriceTickSize"));
                                detail.PriceIter = (reader.IsDBNull(reader.GetOrdinal("PairPriceIter"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("PairPriceIter"));

                                if (detail.OrderSide.Equals("B", StringComparison.CurrentCultureIgnoreCase) ||
                                    detail.OrderSide.Equals("Buy", StringComparison.CurrentCultureIgnoreCase))
                                {
                                    data.BuyOrder = detail;
                                }
                                else
                                {
                                    data.SellOrder = detail;
                                }
                            }
                            list.Add(data);
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

        public void SaveTemplate(IList<PairOrderDetail> list)
        {
            StringBuilder sCommand = new StringBuilder("INSERT INTO almitasc_ACTradingBBGData.PairOrderTemplate"
                + " (TemplateName, Symbol, OrderSide, OrderType, OrderExpire,"
                + " AccountNumber, Destination, BrokerStrategy, OrderQty,"
                + " APExpire, APStartDate, APEndDate, APUrgency, APMaxPctVol, APWouldPrice,"
                + " APMinDarkFillSize, APDisplayMode, APDisplaySize, APLiquiditySeek, APSeekPrice,"
                + " APSeekMinQty, APPctOfVolume, APTradingStyle, APTradingSession, APSORPreference,"
                + " APSORSessionPreference, APStyle, Locate, PairRatio, PairSpread, UserName,"
                + " PairStrategy, PairRatioSetup, PairInitiateLeg, PairSpreadOper, PairExecutionStyle,"
                + " PairUnhedgedQty, PairPriceTickSize, PairPriceIter"
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
                            _logger.LogInformation("Deleting data from almitasc_ACTradingBBGData.PairOrderTemplate where TemplateName = '" + templateName + "'");
                            string sqlDelete = "delete from almitasc_ACTradingBBGData.PairOrderTemplate where TemplateName = '" + templateName + "'";
                            using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                            {
                                command.ExecuteNonQuery();
                            }

                            List<string> Rows = new List<string>();
                            StringBuilder sb = new StringBuilder();

                            foreach (PairOrderDetail data in list)
                            {
                                PopulatePairLeg(templateName, data, sb);
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

        private void PopulatePairLeg(string templateName, PairOrderDetail data, StringBuilder sb)
        {
            // TemplateName
            if (!string.IsNullOrEmpty(templateName))
                sb.Append(string.Concat("'", templateName, "'")).Append(DELIMITER);
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

            // OrderExpire
            if (!string.IsNullOrEmpty(data.OrderExpire))
                sb.Append(string.Concat("'", data.OrderExpire, "'")).Append(DELIMITER);
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

            // OrderQty
            if (data.OrderQty.HasValue)
                sb.Append(data.OrderQty).Append(DELIMITER);
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

            // APStyle
            if (!string.IsNullOrEmpty(data.APStyle))
                sb.Append(string.Concat("'", data.APStyle, "'")).Append(DELIMITER);
            else
                sb.Append("null").Append(DELIMITER);

            // Locate
            if (!string.IsNullOrEmpty(data.Locate))
                sb.Append(string.Concat("'", data.Locate, "'")).Append(DELIMITER);
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

            // UserName
            if (!string.IsNullOrEmpty(data.UserName))
                sb.Append(string.Concat("'", data.UserName, "'")).Append(DELIMITER);
            else
                sb.Append("null").Append(DELIMITER);

            // PairStrategy
            if (!string.IsNullOrEmpty(data.PairStrategy))
                sb.Append(string.Concat("'", data.PairStrategy, "'")).Append(DELIMITER);
            else
                sb.Append("null").Append(DELIMITER);

            // PairRatioSetup
            if (!string.IsNullOrEmpty(data.PairRatioSetup))
                sb.Append(string.Concat("'", data.PairRatioSetup, "'")).Append(DELIMITER);
            else
                sb.Append("null").Append(DELIMITER);

            // PairInitiateLeg
            if (!string.IsNullOrEmpty(data.PairInitiateLeg))
                sb.Append(string.Concat("'", data.PairInitiateLeg, "'")).Append(DELIMITER);
            else
                sb.Append("null").Append(DELIMITER);

            // PairSpreadOper
            if (!string.IsNullOrEmpty(data.PairSpreadOper))
                sb.Append(string.Concat("'", data.PairSpreadOper, "'")).Append(DELIMITER);
            else
                sb.Append("null").Append(DELIMITER);

            // PairExecutionStyle
            if (!string.IsNullOrEmpty(data.PairExecutionStyle))
                sb.Append(string.Concat("'", data.PairExecutionStyle, "'")).Append(DELIMITER);
            else
                sb.Append("null").Append(DELIMITER);

            // PairUnhedgedQty
            if (data.UnhedgedQty.HasValue)
                sb.Append(data.UnhedgedQty).Append(DELIMITER);
            else
                sb.Append("null").Append(DELIMITER);

            // PairPriceTickSize
            if (data.TickSize.HasValue)
                sb.Append(data.TickSize).Append(DELIMITER);
            else
                sb.Append("null").Append(DELIMITER);

            // PairPriceIter
            if (data.PriceIter.HasValue)
                sb.Append(data.PriceIter);
            else
                sb.Append("null");
        }

        public IList<string> GetTemplatesForUser(string userName)
        {
            IList<string> list = new List<string>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    string sql = GetTemplatesForUserQuery;

                    if (!string.IsNullOrEmpty(userName))
                        if (!userName.Equals("All", StringComparison.CurrentCultureIgnoreCase))
                            sql += " where (UserName is null or UserName in ('admin', '" + userName + "'))";

                    sql += " order by TemplateName";

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

        public IDictionary<string, PairOrder> GetPairOrders()
        {
            IDictionary<string, PairOrder> dict = new Dictionary<string, PairOrder>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                string sql = string.Empty;

                if (_configuration["ConnectionStrings:ENV"].Equals("PROD"))
                    sql = GetPairOrdersQuery + " and p.Environment = 'PRODUCTION'";
                else
                    sql = GetPairOrdersQuery + " and p.Environment = 'SIMULATION'";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetPairOrdersQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                string parentId = reader["ParentId"] as string;
                                string orderSide = reader["OrderSide"] as string;

                                PairOrder pairOrder;
                                if (!dict.TryGetValue(parentId, out pairOrder))
                                {
                                    pairOrder = new PairOrder();
                                    pairOrder.ParentId = parentId;
                                    dict.Add(parentId, pairOrder);
                                }

                                PairOrderDetail data = new PairOrderDetail();
                                data.ActionType = reader["ActionType"] as string;
                                data.Id = reader["Id"] as string;
                                data.MainOrderId = reader["MainOrderId"] as string;
                                data.OrderId = reader["OrderId"] as string;
                                data.ParentId = reader["ParentId"] as string;
                                data.Symbol = reader["Ticker"] as string;
                                data.OrderDate = reader.IsDBNull(reader.GetOrdinal("OrderDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("OrderDate"));
                                data.OrderSide = reader["OrderSide"] as string;
                                data.OrderType = reader["OrderType"] as string;
                                data.OrderPrice = (reader.IsDBNull(reader.GetOrdinal("OrderPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OrderPrice"));
                                data.NewOrderPrice = (reader.IsDBNull(reader.GetOrdinal("NewOrderPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NewOrderPrice"));
                                data.OrderLimitPrice = (reader.IsDBNull(reader.GetOrdinal("OrderLimitPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OrderLimitPrice"));
                                data.OrderStopPrice = (reader.IsDBNull(reader.GetOrdinal("OrderStopPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OrderStopPrice"));
                                data.OrderQty = (reader.IsDBNull(reader.GetOrdinal("OrderQuantity"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OrderQuantity"));
                                data.OrderExpire = reader["OrderExpire"] as string;
                                data.AccountId = (reader.IsDBNull(reader.GetOrdinal("AccountId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("AccountId"));
                                data.AccountNumber = reader["AccountName"] as string;
                                data.OrderExchangeId = (reader.IsDBNull(reader.GetOrdinal("ExchangeId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("ExchangeId"));
                                data.Destination = reader["Destination"] as string;
                                data.BrokerStrategy = reader["BrokerStrategy"] as string;
                                data.UserName = reader["Trader"] as string;
                                data.Environment = reader["Environment"] as string;
                                data.AlgoParameters = reader["AlgoParameters"] as string;
                                data.Locate = reader["Locate"] as string;

                                // Pair Trades
                                data.ParentId = reader["ParentId"] as string;
                                data.PairRatio = (reader.IsDBNull(reader.GetOrdinal("PairRatio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PairRatio"));
                                data.PairSpread = (reader.IsDBNull(reader.GetOrdinal("PairSpread"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PairSpread"));
                                data.PairStrategy = reader["PairStrategy"] as string;
                                data.PairRatioSetup = reader["PairRatioSetup"] as string;
                                data.PairInitiateLeg = reader["PairInitiateLeg"] as string;
                                data.PairSpreadOper = reader["PairSpreadOper"] as string;
                                data.PairExecutionStyle = reader["PairExecutionStyle"] as string;
                                data.UnhedgedQty = (reader.IsDBNull(reader.GetOrdinal("PairUnhedgedQty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PairUnhedgedQty"));
                                data.TickSize = (reader.IsDBNull(reader.GetOrdinal("PairPriceTickSize"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PairPriceTickSize"));
                                data.PriceIter = (reader.IsDBNull(reader.GetOrdinal("PairPriceIter"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("PairPriceIter"));

                                //Pair Trades Ext
                                data.OrderDetail.MainParentId = reader["PE_MainParentId"] as string;
                                data.OrderDetail.ParentId = reader["PE_ParentId"] as string;
                                data.OrderDetail.OrderRefId = reader["PE_ReferenceId"] as string;
                                data.OrderDetail.OrderStatus = reader["PE_OrderStatus"] as string;
                                data.OrderDetail.MainOrderId = reader["PE_MainOrderId"] as string;
                                data.OrderDetail.OrderId = reader["PE_OrderId"] as string;
                                data.OrderDetail.OrderType = reader["PE_OrderType"] as string;
                                data.OrderDetail.OrderPriceType = reader["PE_OrderPriceType"] as string;
                                data.OrderDetail.DerivedOrderPrice = (reader.IsDBNull(reader.GetOrdinal("PE_OrderPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PE_OrderPrice"));
                                data.OrderDetail.RatioLast = (reader.IsDBNull(reader.GetOrdinal("RatioLast"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RatioLast"));
                                data.OrderDetail.RatioPassive = (reader.IsDBNull(reader.GetOrdinal("RatioPassive"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RatioPassive"));
                                data.OrderDetail.RatioMarket = (reader.IsDBNull(reader.GetOrdinal("RatioMarket"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RatioMarket"));
                                data.OrderDetail.RatioFilled = (reader.IsDBNull(reader.GetOrdinal("RatioFilled"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RatioFilled"));
                                data.OrderDetail.Distance = (reader.IsDBNull(reader.GetOrdinal("Distance"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Distance"));
                                data.OrderDetail.LastPrc = (reader.IsDBNull(reader.GetOrdinal("LastPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LastPrice"));
                                data.OrderDetail.BidPrc = (reader.IsDBNull(reader.GetOrdinal("BidPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BidPrice"));
                                data.OrderDetail.AskPrc = (reader.IsDBNull(reader.GetOrdinal("AskPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AskPrice"));
                                data.OrderDetail.Vol = (reader.IsDBNull(reader.GetOrdinal("Volume"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Volume"));
                                data.OrderDetail.BidSz = (reader.IsDBNull(reader.GetOrdinal("BidSize"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BidSize"));
                                data.OrderDetail.AskSz = (reader.IsDBNull(reader.GetOrdinal("AskSize"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AskSize"));
                                data.OrderDetail.TradedQty = (reader.IsDBNull(reader.GetOrdinal("PE_TradedQty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PE_TradedQty"));

                                string tradable = reader["Tradable"] as string;
                                if (!string.IsNullOrEmpty(tradable))
                                {
                                    if ("YES".Equals(tradable))
                                        data.OrderDetail.Tradable = true;
                                    else if ("NO".Equals(tradable))
                                        data.OrderDetail.Tradable = true;
                                }

                                if (data.OrderSide.Equals("B", StringComparison.CurrentCultureIgnoreCase)
                                    || data.OrderSide.Equals("BC", StringComparison.CurrentCultureIgnoreCase))
                                    pairOrder.BuyOrder = data;
                                else if (data.OrderSide.Equals("S", StringComparison.CurrentCultureIgnoreCase)
                                    || data.OrderSide.Equals("SS", StringComparison.CurrentCultureIgnoreCase))
                                    pairOrder.SellOrder = data;
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
        /// <param name="orderList"></param>
        public void SavePairOrders(IDictionary<string, PairOrder> orderDict)
        {
            try
            {
                TruncateTable(PairOrdersStgTableName);
                SavePairOrdersStg(orderDict);
                MoveDataToTargetTable(SavePairOrdersQuery);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving data");
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
        /// Truncate Table
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
                        command.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error truncating table");
                throw;
            }
        }

        public void SavePairOrdersStg(IDictionary<string, PairOrder> orderDict)
        {
            StringBuilder sCommand = new StringBuilder("INSERT INTO almitasc_ACTradingBBGData.StgPairOrderDetail"
                + "(ActionType, Id, MainOrderId, OrderId, Ticker, NeovestSymbol,"
                + " Sedol, OrderDate, OrderSide, OrderType, OrderPrice, NewOrderPrice,"
                + " OrigOrderPrice, OrderLimitPrice, OrderStopPrice, OrderQuantity, NewOrderQuantity,"
                + " OrderExpiration, OrderExpire, AccountId, AccountName, ExchangeId, Destination,"
                + " BrokerStrategy, Trader, Environment, Comments, AlgoParameters, Locate,"
                + " OptionPosition, OptionEquityPosition, OptionFill,"
                + " RefIndex, RefIndexPrice, RefIndexBeta, RefIndexBetaInd, RefIndexPriceBetaAdj,"
                + " RefIndexPriceCap, RefIndexPriceInd, RefIndexMaxPrice, RefIndexPriceType,"
                + " DiscountTarget, AutoUpdate, AutoUpdateThreshold, MarketPriceThreshold,"
                + " MarketPriceField, ParentId, PairRatio, PairSpread,"
                + " PairStrategy, PairRatioSetup, PairInitiateLeg, PairSpreadOper, PairExecutionStyle,"
                + " PairUnhedgedQty, PairPriceTickSize, PairPriceIter"
                + " ) values ");

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlTransaction trans = connection.BeginTransaction())
                    {

                        List<string> Rows = new List<string>();
                        StringBuilder sb = new StringBuilder();

                        foreach (KeyValuePair<string, PairOrder> data in orderDict)
                        {
                            PairOrder pairOrder = data.Value;
                            PopulatePairOrderLeg(pairOrder.BuyOrder, sb);
                            string row = sb.ToString();
                            Rows.Add(string.Concat("(", row, ")"));
                            sb.Clear();

                            PopulatePairOrderLeg(pairOrder.SellOrder, sb);
                            row = sb.ToString();
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
                _logger.LogError(ex, "Error saving data");
            }
        }

        public void SavePairOrderDetailsStg(IDictionary<string, PairOrder> orderDict)
        {
            StringBuilder sCommand = new StringBuilder("INSERT INTO almitasc_ACTradingBBGData.StgPairOrderDetailExt"
                + "(MainParentId, ParentId, ReferenceId, MainOrderId, OrderId,"
                + " Ticker, OrderSide, OrderDate, OrderQty, TradedQty,"
                + " OrderStatus, PairRatio, RatioLast, RatioPassive, RatioMarket, RatioFilled,"
                + " Distance, Tradable, LastPrice, BidPrice, AskPrice, OrderVolLimit,"
                + " Volume, BidSize, AskSize,"
                + " OrderType, OrderPriceType, OrderPrice"
                + " ) values ");

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlTransaction trans = connection.BeginTransaction())
                    {

                        List<string> Rows = new List<string>();
                        StringBuilder sb = new StringBuilder();

                        foreach (KeyValuePair<string, PairOrder> data in orderDict)
                        {
                            PairOrder pairOrder = data.Value;

                            PopulatePairOrderDetailLeg(pairOrder.BuyOrder, sb);
                            string row = sb.ToString();
                            Rows.Add(string.Concat("(", row, ")"));
                            sb.Clear();

                            PopulatePairOrderDetailLeg(pairOrder.SellOrder, sb);
                            row = sb.ToString();
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
                _logger.LogError(ex, "Error saving data");
            }
        }

        private void PopulatePairOrderLeg(PairOrderDetail data, StringBuilder sb)
        {
            // ActionType
            if (!string.IsNullOrEmpty(data.ActionType))
                sb.Append(string.Concat("'", data.ActionType, "'")).Append(DELIMITER);
            else
                sb.Append("null").Append(DELIMITER);

            // Id
            if (!string.IsNullOrEmpty(data.Id))
                sb.Append(string.Concat("'", data.Id, "'")).Append(DELIMITER);
            else
                sb.Append("null").Append(DELIMITER);

            // MainOrderId
            if (!string.IsNullOrEmpty(data.MainOrderId))
                sb.Append(string.Concat("'", data.MainOrderId, "'")).Append(DELIMITER);
            else
                sb.Append("null").Append(DELIMITER);

            // OrderId
            if (!string.IsNullOrEmpty(data.OrderId))
                sb.Append(string.Concat("'", data.OrderId, "'")).Append(DELIMITER);
            else
                sb.Append("null").Append(DELIMITER);

            // Ticker
            if (!string.IsNullOrEmpty(data.Symbol))
                sb.Append(string.Concat("'", data.Symbol, "'")).Append(DELIMITER);
            else
                sb.Append("null").Append(DELIMITER);

            // NeovestSymbol
            sb.Append("null").Append(DELIMITER);

            // Sedol
            sb.Append("null").Append(DELIMITER);

            // OrderDate
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

            // OrderPrice
            if (data.OrderPrice.HasValue)
                sb.Append(data.OrderPrice).Append(DELIMITER);
            else
                sb.Append("null").Append(DELIMITER);

            // NewOrderPrice
            if (data.NewOrderPrice.HasValue)
                sb.Append(data.NewOrderPrice).Append(DELIMITER);
            else
                sb.Append("null").Append(DELIMITER);

            // OrigOrderPrice
            sb.Append("null").Append(DELIMITER);

            // OrderLimitPrice
            if (data.OrderLimitPrice.HasValue)
                sb.Append(data.OrderLimitPrice).Append(DELIMITER);
            else
                sb.Append("null").Append(DELIMITER);

            // OrderStopPrice
            if (data.OrderStopPrice.HasValue)
                sb.Append(data.OrderStopPrice).Append(DELIMITER);
            else
                sb.Append("null").Append(DELIMITER);

            // OrderQuantity
            if (data.OrderQty.HasValue)
                sb.Append(data.OrderQty).Append(DELIMITER);
            else
                sb.Append("null").Append(DELIMITER);

            // NewOrderQuantity
            sb.Append("null").Append(DELIMITER);

            // OrderExpiration
            sb.Append("null").Append(DELIMITER);

            // OrderExpire
            sb.Append("null").Append(DELIMITER);

            // AccountId
            if (data.AccountId.HasValue)
                sb.Append(data.AccountId).Append(DELIMITER);
            else
                sb.Append("null").Append(DELIMITER);

            // AccountName
            if (!string.IsNullOrEmpty(data.AccountNumber))
                sb.Append(string.Concat("'", data.AccountNumber, "'")).Append(DELIMITER);
            else
                sb.Append("null").Append(DELIMITER);

            // ExchangeId
            if (data.OrderExchangeId.HasValue)
                sb.Append(data.OrderExchangeId).Append(DELIMITER);
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

            // Trader
            if (!string.IsNullOrEmpty(data.UserName))
                sb.Append(string.Concat("'", data.UserName, "'")).Append(DELIMITER);
            else
                sb.Append("null").Append(DELIMITER);

            // Environment
            if (!string.IsNullOrEmpty(data.Environment))
                sb.Append(string.Concat("'", data.Environment, "'")).Append(DELIMITER);
            else
                sb.Append("null").Append(DELIMITER);

            // Comments
            sb.Append("null").Append(DELIMITER);

            // AlgoParameters
            if (!string.IsNullOrEmpty(data.AlgoParameters))
                sb.Append(string.Concat("'", data.AlgoParameters, "'")).Append(DELIMITER);
            else
                sb.Append("null").Append(DELIMITER);

            // Locate
            if (!string.IsNullOrEmpty(data.Locate))
                sb.Append(string.Concat("'", data.Locate, "'")).Append(DELIMITER);
            else
                sb.Append("null").Append(DELIMITER);

            // OptionPosition
            sb.Append("null").Append(DELIMITER);

            // OptionEquityPosition
            sb.Append("null").Append(DELIMITER);

            // OptionFill
            sb.Append("null").Append(DELIMITER);

            // RefIndex
            sb.Append("null").Append(DELIMITER);

            // RefIndexPrice
            sb.Append("null").Append(DELIMITER);

            // RefIndexBeta
            sb.Append("null").Append(DELIMITER);

            // RefIndexBetaInd
            sb.Append("null").Append(DELIMITER);

            // RefIndexPriceBetaAdj
            sb.Append("null").Append(DELIMITER);

            // RefIndexPriceCap
            sb.Append("null").Append(DELIMITER);

            // RefIndexPriceInd
            sb.Append("null").Append(DELIMITER);

            // RefIndexMaxPrice
            sb.Append("null").Append(DELIMITER);

            // RefIndexPriceType
            sb.Append("null").Append(DELIMITER);

            // DiscountTarget
            sb.Append("null").Append(DELIMITER);

            // AutoUpdate
            if (!string.IsNullOrEmpty(data.AutoUpdate))
                sb.Append(string.Concat("'", data.AutoUpdate, "'")).Append(DELIMITER);
            else
                sb.Append("null").Append(DELIMITER);

            // AutoUpdateThreshold
            if (data.AutoUpdateThreshold.HasValue)
                sb.Append(data.AutoUpdateThreshold).Append(DELIMITER);
            else
                sb.Append("null").Append(DELIMITER);

            // MarketPriceThreshold
            if (data.MarketPriceThreshold.HasValue)
                sb.Append(data.MarketPriceThreshold).Append(DELIMITER);
            else
                sb.Append("null").Append(DELIMITER);

            // MarketPriceField
            sb.Append("null").Append(DELIMITER);

            // ParentId
            if (!string.IsNullOrEmpty(data.ParentId))
                sb.Append(string.Concat("'", data.ParentId, "'")).Append(DELIMITER);
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

            // PairStrategy
            if (!string.IsNullOrEmpty(data.PairStrategy))
                sb.Append(string.Concat("'", data.PairStrategy, "'")).Append(DELIMITER);
            else
                sb.Append("null").Append(DELIMITER);

            // PairRatioSetup
            if (!string.IsNullOrEmpty(data.PairRatioSetup))
                sb.Append(string.Concat("'", data.PairRatioSetup, "'")).Append(DELIMITER);
            else
                sb.Append("null").Append(DELIMITER);

            // PairInitiateLeg
            if (!string.IsNullOrEmpty(data.PairInitiateLeg))
                sb.Append(string.Concat("'", data.PairInitiateLeg, "'")).Append(DELIMITER);
            else
                sb.Append("null").Append(DELIMITER);

            // PairSpreadOper
            if (!string.IsNullOrEmpty(data.PairSpreadOper))
                sb.Append(string.Concat("'", data.PairSpreadOper, "'")).Append(DELIMITER);
            else
                sb.Append("null").Append(DELIMITER);

            // PairExecutionStyle
            if (!string.IsNullOrEmpty(data.PairExecutionStyle))
                sb.Append(string.Concat("'", data.PairExecutionStyle, "'")).Append(DELIMITER);
            else
                sb.Append("null").Append(DELIMITER);

            // PairUnhedgedQty
            if (data.UnhedgedQty.HasValue)
                sb.Append(data.UnhedgedQty).Append(DELIMITER);
            else
                sb.Append("null").Append(DELIMITER);

            // PairPriceTickSize
            if (data.TickSize.HasValue)
                sb.Append(data.TickSize).Append(DELIMITER);
            else
                sb.Append("null").Append(DELIMITER);

            // PairPriceIter
            if (data.PriceIter.HasValue)
                sb.Append(data.PriceIter);
            else
                sb.Append("null");
        }

        private void PopulatePairOrderDetailLeg(PairOrderDetail data, StringBuilder sb)
        {
            PairOrderDetailExt orderDetailExt = data.OrderDetail;

            if (orderDetailExt != null)
            {
                // MainParentId
                if (!string.IsNullOrEmpty(orderDetailExt.MainParentId))
                    sb.Append(string.Concat("'", orderDetailExt.MainParentId, "'")).Append(DELIMITER);
                else
                    sb.Append("null").Append(DELIMITER);

                // ParentId
                if (!string.IsNullOrEmpty(orderDetailExt.ParentId))
                    sb.Append(string.Concat("'", orderDetailExt.ParentId, "'")).Append(DELIMITER);
                else
                    sb.Append("null").Append(DELIMITER);

                // ReferenceId
                if (!string.IsNullOrEmpty(orderDetailExt.OrderRefId))
                    sb.Append(string.Concat("'", orderDetailExt.OrderRefId, "'")).Append(DELIMITER);
                else
                    sb.Append("null").Append(DELIMITER);

                // MainOrderId
                if (!string.IsNullOrEmpty(orderDetailExt.MainOrderId))
                    sb.Append(string.Concat("'", orderDetailExt.MainOrderId, "'")).Append(DELIMITER);
                else
                    sb.Append("null").Append(DELIMITER);

                // OrderId
                if (!string.IsNullOrEmpty(orderDetailExt.OrderId))
                    sb.Append(string.Concat("'", orderDetailExt.OrderId, "'")).Append(DELIMITER);
                else
                    sb.Append("null").Append(DELIMITER);

                // Ticker
                if (!string.IsNullOrEmpty(data.Symbol))
                    sb.Append(string.Concat("'", data.Symbol, "'")).Append(DELIMITER);
                else
                    sb.Append("null").Append(DELIMITER);

                // OrderSide
                if (!string.IsNullOrEmpty(data.OrderSide))
                    sb.Append(string.Concat("'", data.OrderSide, "'")).Append(DELIMITER);
                else
                    sb.Append("null").Append(DELIMITER);

                // OrderDate
                if (data.OrderDate.HasValue)
                    sb.Append(string.Concat("'", DateUtils.ConvertDate(data.OrderDate, "yyyy-MM-dd"), "'")).Append(DELIMITER);
                else
                    sb.Append("null").Append(DELIMITER);

                // OrderQuantity
                if (data.OrderQty.HasValue)
                    sb.Append(data.OrderQty).Append(DELIMITER);
                else
                    sb.Append("null").Append(DELIMITER);

                // TradedQty
                if (orderDetailExt.TradedQty.HasValue)
                    sb.Append(orderDetailExt.TradedQty).Append(DELIMITER);
                else
                    sb.Append("null").Append(DELIMITER);

                // OrderStatus
                if (!string.IsNullOrEmpty(orderDetailExt.OrderStatus))
                    sb.Append(string.Concat("'", orderDetailExt.OrderStatus, "'")).Append(DELIMITER);
                else
                    sb.Append("null").Append(DELIMITER);

                // PairRatio
                if (data.PairRatio.HasValue)
                    sb.Append(data.PairRatio).Append(DELIMITER);
                else
                    sb.Append("null").Append(DELIMITER);

                // RatioLast
                if (orderDetailExt.RatioLast.HasValue)
                    sb.Append(orderDetailExt.RatioLast).Append(DELIMITER);
                else
                    sb.Append("null").Append(DELIMITER);

                // RatioPassive
                if (orderDetailExt.RatioPassive.HasValue)
                    sb.Append(orderDetailExt.RatioPassive).Append(DELIMITER);
                else
                    sb.Append("null").Append(DELIMITER);

                // RatioMarket
                if (orderDetailExt.RatioMarket.HasValue)
                    sb.Append(orderDetailExt.RatioMarket).Append(DELIMITER);
                else
                    sb.Append("null").Append(DELIMITER);

                // RatioFilled
                if (orderDetailExt.RatioFilled.HasValue)
                    sb.Append(orderDetailExt.RatioFilled).Append(DELIMITER);
                else
                    sb.Append("null").Append(DELIMITER);

                // Distance
                if (orderDetailExt.Distance.HasValue)
                    sb.Append(orderDetailExt.Distance).Append(DELIMITER);
                else
                    sb.Append("null").Append(DELIMITER);

                // Tradable
                if (orderDetailExt.Tradable)
                    sb.Append(string.Concat("'YES'")).Append(DELIMITER);
                else
                    sb.Append(string.Concat("'NO'")).Append(DELIMITER);

                // LastPrice
                if (orderDetailExt.LastPrc.HasValue)
                    sb.Append(orderDetailExt.LastPrc).Append(DELIMITER);
                else
                    sb.Append("null").Append(DELIMITER);

                // BidPrice
                if (orderDetailExt.BidPrc.HasValue)
                    sb.Append(orderDetailExt.BidPrc).Append(DELIMITER);
                else
                    sb.Append("null").Append(DELIMITER);

                // AskPrice
                if (orderDetailExt.AskPrc.HasValue)
                    sb.Append(orderDetailExt.AskPrc).Append(DELIMITER);
                else
                    sb.Append("null").Append(DELIMITER);

                // OrderVolLimit
                if (data.VolumeLimit.HasValue)
                    sb.Append(data.VolumeLimit).Append(DELIMITER);
                else
                    sb.Append("null").Append(DELIMITER);

                // Volume
                if (orderDetailExt.Vol.HasValue)
                    sb.Append(orderDetailExt.Vol).Append(DELIMITER);
                else
                    sb.Append("null").Append(DELIMITER);

                // BidSize
                if (orderDetailExt.BidSz.HasValue)
                    sb.Append(orderDetailExt.BidSz).Append(DELIMITER);
                else
                    sb.Append("null").Append(DELIMITER);

                // AskSize
                if (orderDetailExt.BidSz.HasValue)
                    sb.Append(orderDetailExt.BidSz).Append(DELIMITER);
                else
                    sb.Append("null").Append(DELIMITER);

                // OrderType
                if (!string.IsNullOrEmpty(orderDetailExt.OrderType))
                    sb.Append(string.Concat("'", orderDetailExt.OrderType, "'")).Append(DELIMITER);
                else
                    sb.Append("null").Append(DELIMITER);

                // OrderPriceType
                if (!string.IsNullOrEmpty(orderDetailExt.OrderPriceType))
                    sb.Append(string.Concat("'", orderDetailExt.OrderPriceType, "'")).Append(DELIMITER);
                else
                    sb.Append("null").Append(DELIMITER);

                // OrderPrice
                if (orderDetailExt.BidSz.HasValue)
                    sb.Append(orderDetailExt.BidSz);
                else
                    sb.Append("null");
            }
        }

        public void SavePairOrderDetails(IDictionary<string, PairOrder> orderDict)
        {
            try
            {
                TruncateTable(PairOrderDetailsStgTableName);
                SavePairOrderDetailsStg(orderDict);
                MoveDataToTargetTable(SavePairOrderDetailsQuery);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving data");
            }
        }

        private const string GetPairOrdersQuery = "select"
            + " p.ActionType, p.Id, p.MainOrderId, p.OrderId, p.Ticker, p.NeovestSymbol, p.Sedol,"
            + " p.OrderDate, p.OrderSide, p.OrderType, p.OrderPrice, p.NewOrderPrice,"
            + " p.OrigOrderPrice, p.OrderLimitPrice, p.OrderStopPrice, p.OrderQuantity, p.NewOrderQuantity,"
            + " p.OrderExpiration, p.OrderExpire, p.AccountId, p.AccountName, p.ExchangeId,"
            + " p.Destination, p.BrokerStrategy, p.Trader, p.Environment, p.Comments, p.AlgoParameters,"
            + " p.Locate, p.OptionPosition, p.OptionEquityPosition, p.OptionFill,"
            + " p.RefIndex, p.RefIndexPrice, p.RefIndexBeta, p.RefIndexBetaInd, p.RefIndexPriceBetaAdj,"
            + " p.RefIndexPriceCap, p.RefIndexPriceInd, p.RefIndexMaxPrice, p.RefIndexPriceType, p.DiscountTarget,"
            + " p.AutoUpdate, p.AutoUpdateThreshold, p.MarketPriceThreshold, p.MarketPriceField, p.ParentId,"
            + " p.PairRatio, p.PairSpread, p.PairStrategy, p.PairRatioSetup, p.PairInitiateLeg, p.PairSpreadOper, p.PairExecutionStyle,"
            + " p.PairUnhedgedQty, p.PairPriceTickSize, p.PairPriceIter,"
            + " pe.MainParentId as PE_MainParentId, pe.ParentId as PE_ParentId, pe.ReferenceId as PE_ReferenceId,"
            + " pe.OrderQty as PE_OrderQty, pe.TradedQty as PE_TradedQty, pe.OrderStatus as PE_OrderStatus,"
            + " pe.MainOrderId as PE_MainOrderId, pe.OrderId as PE_OrderId,"
            + " pe.RatioLast, pe.RatioPassive, pe.RatioMarket, pe.RatioFilled, pe.Distance, pe.Tradable,"
            + " pe.LastPrice, pe.BidPrice, pe.AskPrice, pe.OrderVolLimit, pe.Volume, pe.BidSize, pe.AskSize,"
            + " pe.OrderType as PE_OrderType, pe.OrderPriceType as PE_OrderPriceType, pe.OrderPrice as PE_OrderPrice"
            + " from almitasc_ACTradingBBGData.PairOrderDetail p"
            + " left join almitasc_ACTradingBBGData.PairOrderDetailExt pe on(p.Id = pe.ParentId)"
            + " where date_format(p.OrderDate, '%Y-%m-%d') = date_format(CONVERT_TZ(current_timestamp(),'+00:00','-8:00'), '%Y-%m-%d')";

        private const string GetTemplateQuery = "select * from almitasc_ACTradingBBGData.PairOrderTemplate";
        private const string GetTemplatesForUserQuery = "select distinct TemplateName from almitasc_ACTradingBBGData.PairOrderTemplate";
        private const string PairOrdersStgTableName = "almitasc_ACTradingBBGData.StgPairOrderDetail";
        private const string PairOrderDetailsStgTableName = "almitasc_ACTradingBBGData.StgPairOrderDetailExt";
        private const string SavePairOrdersQuery = "spPopulatePairTrades";
        private const string SavePairOrderDetailsQuery = "spPopulatePairTradesExt";
    }
}
