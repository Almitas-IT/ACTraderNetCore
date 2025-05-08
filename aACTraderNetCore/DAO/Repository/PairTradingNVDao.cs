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
    public class PairTradingNVDao : IPairTradingNVDao
    {
        private readonly ILogger<PairTradingNVDao> _logger;
        private readonly IConfiguration _configuration;

        private const string DELIMITER = ",";

        public PairTradingNVDao(ILogger<PairTradingNVDao> logger, IConfiguration configuration)
        {
            _logger = logger;
            this._configuration = configuration;
            _logger.LogInformation("Initializing PairTradingNVDao...");
        }

        public IList<PairOrderTemplateNV> GetTemplate(string templateName)
        {
            IList<PairOrderTemplateNV> list = new List<PairOrderTemplateNV>();

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
                                PairOrderTemplateNV detail = new PairOrderTemplateNV();
                                detail.TemplateName = reader["TemplateName"] as string;
                                detail.Leg1Symbol = reader["Leg1Symbol"] as string;
                                detail.Leg1OrderSide = reader["Leg1OrderSide"] as string;
                                detail.Leg1OrderType = reader["Leg1OrderType"] as string;
                                detail.Leg1OrderQty = (reader.IsDBNull(reader.GetOrdinal("Leg1OrderQty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Leg1OrderQty"));
                                detail.Leg1OrderPrice = (reader.IsDBNull(reader.GetOrdinal("Leg1OrderPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Leg1OrderPrice"));

                                detail.Leg2Symbol = reader["Leg2Symbol"] as string;
                                detail.Leg2OrderSide = reader["Leg2OrderSide"] as string;
                                detail.Leg2OrderType = reader["Leg2OrderType"] as string;
                                detail.Leg2OrderQty = (reader.IsDBNull(reader.GetOrdinal("Leg2OrderQty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Leg2OrderQty"));
                                detail.Leg2OrderPrice = (reader.IsDBNull(reader.GetOrdinal("Leg2OrderPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Leg2OrderPrice"));

                                detail.Dest = reader["Destination"] as string;
                                detail.BkrStrategy = reader["BrokerStrategy"] as string;
                                detail.Locate = reader["Locate"] as string;

                                detail.APStartDate = reader["APStartDate"] as string;
                                detail.APEndDate = reader["APEndDate"] as string;
                                detail.APExecutionStyle = reader["APExecutionStyle"] as string;
                                detail.APLeadingLeg = reader["APLeadingLeg"] as string;
                                detail.APManageLegging = reader["APManageLegging"] as string;
                                detail.APCurrencyNeutral = reader["APCurrencyNeutral"] as string;
                                detail.APMaxPctVolume = (reader.IsDBNull(reader.GetOrdinal("APMaxPctVolume"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("APMaxPctVolume"));
                                detail.APMaxClipShares = (reader.IsDBNull(reader.GetOrdinal("APMaxClipShares"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("APMaxClipShares"));
                                detail.APMinClipShares = (reader.IsDBNull(reader.GetOrdinal("APMinClipShares"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("APMinClipShares"));
                                detail.APSpreadLimit = (reader.IsDBNull(reader.GetOrdinal("APSpreadLimit"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("APSpreadLimit"));
                                detail.APRatio = (reader.IsDBNull(reader.GetOrdinal("APRatio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("APRatio"));
                                detail.APTradingSession = reader["APTradingSession"] as string;
                                detail.APVolumeLimit = (reader.IsDBNull(reader.GetOrdinal("APVolumeLimit"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("APVolumeLimit"));
                                detail.APBalance = reader["APBalance"] as string;
                                detail.APSpreadOperator = reader["APSpreadOperator"] as string;
                                detail.APSpreadCurrency = reader["APSpreadCurrency"] as string;
                                detail.APCash = (reader.IsDBNull(reader.GetOrdinal("APCash"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("APCash"));
                                detail.APRefPriceType = reader["APRefPriceType"] as string;
                                detail.APNotionalPricing = reader["APNotionalPricing"] as string;
                                detail.ActionFlag = reader["ActionFlag"] as string;
                                detail.AutoUpdateFlag = reader["AutoUpdateFlag"] as string;
                                detail.UserName = reader["UserName"] as string;
                                detail.RowId = (reader.IsDBNull(reader.GetOrdinal("RowId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("RowId"));
                                list.Add(detail);
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

        public IList<string> GetTemplates()
        {
            throw new System.NotImplementedException();
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

        public void SaveTemplate(IList<PairOrderTemplateNV> list)
        {
            StringBuilder sCommand = new StringBuilder("INSERT INTO almitasc_ACTradingBBGData.PairOrderTemplateNV"
                + " (TemplateName, Leg1Symbol, Leg1OrderSide, Leg1OrderType, Leg1OrderQty, Leg1OrderPrice,"
                + " Leg2Symbol, Leg2OrderSide, Leg2OrderType, Leg2OrderQty, Leg2OrderPrice,"
                + " Destination, BrokerStrategy, APStartDate, APEndDate, APExecutionStyle, APLeadingLeg,"
                + " APManageLegging, APCurrencyNeutral, APMaxPctVolume, APMaxClipShares, APMinClipShares,"
                + " Locate, APSpreadLimit, APRatio, APTradingSession, APVolumeLimit, APBalance,"
                + " APSpreadOperator, APSpreadCurrency, APCash, APRefPriceType, APNotionalPricing,"
                + " ActionFlag, AutoUpdateFlag, RowId, UserName"
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
                            _logger.LogInformation("Deleting data from almitasc_ACTradingBBGData.PairOrderTemplateNV where TemplateName = '" + templateName + "'");
                            string sqlDelete = "delete from almitasc_ACTradingBBGData.PairOrderTemplateNV where TemplateName = '" + templateName + "'";
                            using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                            {
                                command.ExecuteNonQuery();
                            }

                            List<string> Rows = new List<string>();
                            StringBuilder sb = new StringBuilder();

                            foreach (PairOrderTemplateNV data in list)
                            {
                                // TemplateName
                                if (!string.IsNullOrEmpty(templateName))
                                    sb.Append(string.Concat("'", templateName, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Leg1Symbol
                                if (!string.IsNullOrEmpty(data.Leg1Symbol))
                                    sb.Append(string.Concat("'", data.Leg1Symbol, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Leg1OrderSide
                                if (!string.IsNullOrEmpty(data.Leg1OrderSide))
                                    sb.Append(string.Concat("'", data.Leg1OrderSide, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Leg1OrderType
                                if (!string.IsNullOrEmpty(data.Leg1OrderType))
                                    sb.Append(string.Concat("'", data.Leg1OrderType, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Leg1OrderQty
                                if (data.Leg1OrderQty.HasValue)
                                    sb.Append(data.Leg1OrderQty).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Leg1OrderPrice
                                if (data.Leg1OrderPrice.HasValue)
                                    sb.Append(data.Leg1OrderPrice).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Leg2Symbol
                                if (!string.IsNullOrEmpty(data.Leg2Symbol))
                                    sb.Append(string.Concat("'", data.Leg2Symbol, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Leg2OrderSide
                                if (!string.IsNullOrEmpty(data.Leg2OrderSide))
                                    sb.Append(string.Concat("'", data.Leg2OrderSide, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Leg2OrderType
                                if (!string.IsNullOrEmpty(data.Leg2OrderType))
                                    sb.Append(string.Concat("'", data.Leg2OrderType, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Leg2OrderQty
                                if (data.Leg2OrderQty.HasValue)
                                    sb.Append(data.Leg2OrderQty).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Leg2OrderPrice
                                if (data.Leg2OrderPrice.HasValue)
                                    sb.Append(data.Leg2OrderPrice).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Destination
                                if (!string.IsNullOrEmpty(data.Dest))
                                    sb.Append(string.Concat("'", data.Dest, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // BrokerStrategy
                                if (!string.IsNullOrEmpty(data.BkrStrategy))
                                    sb.Append(string.Concat("'", data.BkrStrategy, "'")).Append(DELIMITER);
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

                                // APExecutionStyle
                                if (!string.IsNullOrEmpty(data.APExecutionStyle))
                                    sb.Append(string.Concat("'", data.APExecutionStyle, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // APLeadingLeg
                                if (!string.IsNullOrEmpty(data.APLeadingLeg))
                                    sb.Append(string.Concat("'", data.APLeadingLeg, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // APManageLegging
                                if (!string.IsNullOrEmpty(data.APManageLegging))
                                    sb.Append(string.Concat("'", data.APManageLegging, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // APCurrencyNeutral
                                if (!string.IsNullOrEmpty(data.APCurrencyNeutral))
                                    sb.Append(string.Concat("'", data.APCurrencyNeutral, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // APMaxPctVolume
                                if (data.APMaxPctVolume.HasValue)
                                    sb.Append(data.APMaxPctVolume).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // APMaxClipShares
                                if (data.APMaxClipShares.HasValue)
                                    sb.Append(data.APMaxClipShares).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // APMinClipShares
                                if (data.APMinClipShares.HasValue)
                                    sb.Append(data.APMinClipShares).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Locate
                                if (!string.IsNullOrEmpty(data.Locate))
                                    sb.Append(string.Concat("'", data.Locate, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // APSpreadLimit
                                if (data.APSpreadLimit.HasValue)
                                    sb.Append(data.APSpreadLimit).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // APRatio
                                if (data.APRatio.HasValue)
                                    sb.Append(data.APRatio).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // APTradingSession
                                if (!string.IsNullOrEmpty(data.APTradingSession))
                                    sb.Append(string.Concat("'", data.APTradingSession, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // APVolumeLimit
                                if (data.APVolumeLimit.HasValue)
                                    sb.Append(data.APVolumeLimit).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // APBalance
                                if (!string.IsNullOrEmpty(data.APBalance))
                                    sb.Append(string.Concat("'", data.APBalance, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // APSpreadOperator
                                if (!string.IsNullOrEmpty(data.APSpreadOperator))
                                    sb.Append(string.Concat("'", data.APSpreadOperator, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // APSpreadCurrency
                                if (!string.IsNullOrEmpty(data.APSpreadCurrency))
                                    sb.Append(string.Concat("'", data.APSpreadCurrency, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // APCash
                                if (data.APCash.HasValue)
                                    sb.Append(data.APCash).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // APRefIndexType
                                if (!string.IsNullOrEmpty(data.APRefPriceType))
                                    sb.Append(string.Concat("'", data.APRefPriceType, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // APNotionalPricing
                                if (!string.IsNullOrEmpty(data.APNotionalPricing))
                                    sb.Append(string.Concat("'", data.APNotionalPricing, "'")).Append(DELIMITER);
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

                                // RowId
                                if (data.RowId.HasValue)
                                    sb.Append(data.RowId).Append(DELIMITER);
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

        public IDictionary<string, NewPairOrder> GetPairOrders()
        {
            IDictionary<string, NewPairOrder> dict = new Dictionary<string, NewPairOrder>(StringComparer.CurrentCultureIgnoreCase);

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

                                NewPairOrder pairOrder;
                                if (!dict.TryGetValue(parentId, out pairOrder))
                                {
                                    pairOrder = new NewPairOrder();
                                    pairOrder.ParentId = parentId;
                                    dict.Add(parentId, pairOrder);
                                }

                                NewOrder data = new NewOrder();
                                data.ActionType = reader["ActionType"] as string;
                                data.Id = reader["Id"] as string;
                                data.Symbol = reader["Ticker"] as string;
                                data.OrderDate = reader.IsDBNull(reader.GetOrdinal("OrderDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("OrderDate"));
                                data.OrderSide = reader["OrderSide"] as string;
                                data.OrderType = reader["OrderType"] as string;
                                data.OrderPrice = (reader.IsDBNull(reader.GetOrdinal("OrderPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OrderPrice"));
                                data.OrigOrderPrice = (reader.IsDBNull(reader.GetOrdinal("OrigOrderPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OrigOrderPrice"));
                                data.OrderQty = (reader.IsDBNull(reader.GetOrdinal("OrderQuantity"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OrderQuantity"));
                                data.AccountId = (reader.IsDBNull(reader.GetOrdinal("AccountId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("AccountId"));
                                data.AccountName = reader["AccountName"] as string;
                                data.OrderExchangeId = (reader.IsDBNull(reader.GetOrdinal("ExchangeId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("ExchangeId"));
                                data.Destination = reader["Destination"] as string;
                                data.BrokerStrategy = reader["BrokerStrategy"] as string;
                                data.UserName = reader["Trader"] as string;
                                data.Environment = reader["Environment"] as string;
                                data.AlgoParameters = reader["AlgoParameters"] as string;
                                data.Locate = reader["Locate"] as string;
                                data.AutoUpdate = reader["AutoUpdate"] as string;
                                data.ParentId = reader["ParentId"] as string;
                                data.PairRatio = (reader.IsDBNull(reader.GetOrdinal("PairRatio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PairRatio"));
                                data.PairSpread = (reader.IsDBNull(reader.GetOrdinal("PairSpread"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PairSpread"));

                                if (data.OrderSide.Equals("B", StringComparison.CurrentCultureIgnoreCase)
                                    || data.OrderSide.Equals("BC", StringComparison.CurrentCultureIgnoreCase)
                                    || data.OrderSide.Equals("BUY", StringComparison.CurrentCultureIgnoreCase))
                                    pairOrder.BuyOrder = data;
                                else if (data.OrderSide.Equals("S", StringComparison.CurrentCultureIgnoreCase)
                                    || data.OrderSide.Equals("SS", StringComparison.CurrentCultureIgnoreCase)
                                    || data.OrderSide.Equals("SELL SHORT", StringComparison.CurrentCultureIgnoreCase)
                                    )
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

        public void SavePairOrders(IDictionary<string, NewPairOrder> orderDict)
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

        public void SavePairOrdersStg(IDictionary<string, NewPairOrder> orderDict)
        {
            StringBuilder sCommand = new StringBuilder("INSERT INTO almitasc_ACTradingBBGData.StgNVPairOrderDetail"
                + " (ActionType, Id, Ticker, OrderDate, OrderSide, OrderType, OrderPrice,"
                + " OrigOrderPrice, OrderQuantity, AccountId, AccountName, ExchangeId, Destination,"
                + " BrokerStrategy, Trader, Environment, AlgoParameters, Locate, AutoUpdate,"
                + " ParentId, PairRatio, PairSpread"
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

                        foreach (KeyValuePair<string, NewPairOrder> data in orderDict)
                        {
                            NewPairOrder pairOrder = data.Value;
                            PopulatePairLeg(pairOrder.BuyOrder, sb);
                            string row = sb.ToString();
                            Rows.Add(string.Concat("(", row, ")"));
                            sb.Clear();

                            PopulatePairLeg(pairOrder.SellOrder, sb);
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

        private void PopulatePairLeg(NewOrder data, StringBuilder sb)
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

            // Ticker
            if (!string.IsNullOrEmpty(data.Symbol))
                sb.Append(string.Concat("'", data.Symbol, "'")).Append(DELIMITER);
            else
                sb.Append("null").Append(DELIMITER);

            // OrderDate
            if (data.OrderDate.HasValue)
                sb.Append(string.Concat("'", DateUtils.ConvertDate(data.OrderDate, "yyyy-MM-dd"), "'")).Append(DELIMITER);
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

            // OrderPrice
            if (data.OrderPrice.HasValue)
                sb.Append(data.OrderPrice).Append(DELIMITER);
            else
                sb.Append("null").Append(DELIMITER);

            // OrigOrderPrice
            if (data.OrigOrderPrice.HasValue)
                sb.Append(data.OrigOrderPrice).Append(DELIMITER);
            else
                sb.Append("null").Append(DELIMITER);

            // OrderQuantity
            if (data.OrderQty.HasValue)
                sb.Append(data.OrderQty).Append(DELIMITER);
            else
                sb.Append("null").Append(DELIMITER);

            // AccountId
            if (data.AccountId.HasValue)
                sb.Append(data.AccountId).Append(DELIMITER);
            else
                sb.Append("null").Append(DELIMITER);

            // AccountName
            if (!string.IsNullOrEmpty(data.AccountName))
                sb.Append(string.Concat("'", data.AccountName, "'")).Append(DELIMITER);
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

            // AutoUpdate
            if (!string.IsNullOrEmpty(data.AutoUpdate))
                sb.Append(string.Concat("'", data.AutoUpdate, "'")).Append(DELIMITER);
            else
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
                sb.Append(data.PairSpread);
            else
                sb.Append("null");
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

        private const string GetTemplateQuery = "select * from almitasc_ACTradingBBGData.PairOrderTemplateNV";
        private const string GetTemplatesForUserQuery = "select distinct TemplateName from almitasc_ACTradingBBGData.PairOrderTemplateNV";
        private const string PairOrdersStgTableName = "almitasc_ACTradingBBGData.StgNVPairOrderDetail";
        private const string SavePairOrdersQuery = "spPopulateNVPairTrades";

        private const string GetPairOrdersQuery = "select * from almitasc_ACTradingBBGData.NVPairOrderDetail"
            + " where date_format(OrderDate, '%Y-%m-%d') = date_format(CONVERT_TZ(current_timestamp(),'+00:00','-8:00'), '%Y-%m-%d')";
    }
}
