using aACTrader.DAO.Interface;
using aCommons;
using aCommons.Cef;
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
    public class BaseDao : IBaseDao
    {
        private readonly ILogger<BaseDao> _logger;
        private const string DELIMITER = ",";

        public BaseDao(ILogger<BaseDao> logger)
        {
            _logger = logger;
            _logger.LogInformation("Initializing BaseDao...");
        }

        /// <summary>
        /// Gets underlying holdings of a fund for a selected source (Bloomberg or User provided)
        /// </summary>
        /// <param name="ticker"></param>
        /// <param name="source"></param>
        /// <returns></returns>
        public IList<FundHolding> GetFundHoldings(string ticker, string source)
        {
            IList<FundHolding> list = new List<FundHolding>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetFundHoldingsQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Ticker", ticker);
                        command.Parameters.AddWithValue("p_Source", source);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundHolding fundHoldings = new FundHolding
                                {
                                    FundTicker = reader["fund_ticker"] as string,
                                    FundName = reader["fund_name"] as string,
                                    AssetType = reader["asset_type"] as string,
                                    SecuritySector = reader["security_sector"] as string,
                                    SecurityType = reader["security_type"] as string,
                                    Ticker = reader["ticker"] as string,
                                    Cusip = reader["cusip"] as string,
                                    SecurityName = reader["name"] as string,
                                    PortDate = reader.IsDBNull(reader.GetOrdinal("port_date")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("port_date")),
                                    MaturityDate = reader.IsDBNull(reader.GetOrdinal("maturity")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("maturity")),
                                    Coupon = (reader.IsDBNull(reader.GetOrdinal("coupon"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("coupon")),
                                    Position = (reader.IsDBNull(reader.GetOrdinal("position"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("position")),
                                    Weight = (reader.IsDBNull(reader.GetOrdinal("weight"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("weight")),
                                    MV = (reader.IsDBNull(reader.GetOrdinal("market_value"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("market_value")),
                                    Price = (reader.IsDBNull(reader.GetOrdinal("price"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("price")),
                                    Currency = reader["currency"] as string,
                                    FXRate = (reader.IsDBNull(reader.GetOrdinal("fxrate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("fxrate")),
                                    FXRateBase = (reader.IsDBNull(reader.GetOrdinal("fxrate2base"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("fxrate2base")),
                                    Country = reader["country"] as string,
                                    BroadIndustry = reader["broadindustry"] as string,
                                    Industry = reader["industry"] as string,
                                    SubIndustry = reader["subindustry"] as string,

                                    YearsToMaturity = (reader.IsDBNull(reader.GetOrdinal("years_to_maturity"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("years_to_maturity")),
                                    YieldToWorst = (reader.IsDBNull(reader.GetOrdinal("yield_to_worst"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("yield_to_worst")),
                                    YieldToMaturity = (reader.IsDBNull(reader.GetOrdinal("yield_to_maturity"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("yield_to_maturity")),
                                    OAS = (reader.IsDBNull(reader.GetOrdinal("oas"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("oas")),
                                    OASDuration = (reader.IsDBNull(reader.GetOrdinal("option_adj_duration"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("option_adj_duration")),
                                    OASConvexity = (reader.IsDBNull(reader.GetOrdinal("option_adj_convexity"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("option_adj_convexity")),
                                    DV01 = (reader.IsDBNull(reader.GetOrdinal("dv01"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("dv01")),
                                    SpreadDuration = (reader.IsDBNull(reader.GetOrdinal("spread_duration"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("spread_duration")),

                                    BBGRating = reader["bbgrating"] as string,
                                    SPRating = reader["sprating"] as string,
                                    MoodysRating = reader["moodysrating"] as string,
                                    DataFile = reader["datafile"] as string
                                };

                                fundHoldings.PortDateAsString = DateUtils.ConvertDate(fundHoldings.PortDate, "yyyy-MM-dd");
                                fundHoldings.MaturityDateAsString = DateUtils.ConvertDate(fundHoldings.MaturityDate, "yyyy-MM-dd");

                                list.Add(fundHoldings);
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
        /// Gets security master information for a ticker
        /// </summary>
        /// <param name="ticker"></param>
        /// <param name="status"></param>
        /// <returns></returns>
        public IList<SecurityMaster> GetSecurityMaster(string ticker, string status)
        {
            IList<SecurityMaster> list = new List<SecurityMaster>();

            string almitasId = string.Empty;

            try
            {
                string sql = GetSecurityMasterQuery + " where 1=1";

                if (!string.IsNullOrEmpty(ticker))
                    sql += " and ticker like ('" + ticker + "%')";

                if (!string.IsNullOrEmpty(status))
                    sql += " and market_status = '" + status + "'";

                sql += " order by ticker asc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                SecurityMaster data = new SecurityMaster();
                                almitasId = reader["AlmitasId"] as string;
                                data.AlmitasId = reader["AlmitasId"] as string;
                                data.FIGI = reader["FIGI"] as string;
                                data.SecId = reader["SecId"] as string;
                                data.Ticker = reader["Ticker"] as string;
                                data.ISIN = reader["ISIN"] as string;
                                data.YellowKey = reader["YellowKey"] as string;
                                data.SecurityDescription = reader["SecurityDescription"] as string;
                                data.Country = reader["Country"] as string;
                                data.Currency = reader["Currency"] as string;
                                data.SecurityType = reader["SecurityType"] as string;
                                data.PaymentRank = reader["PaymentRank"] as string;
                                data.CEFInstrumentType = reader["CefInstrumentType"] as string;
                                data.GeoLevel1 = reader["GeoLevel1"] as string;
                                data.GeoLevel2 = reader["GeoLevel2"] as string;
                                data.GeoLevel3 = reader["GeoLevel3"] as string;
                                data.AssetClassLevel1 = reader["AssetClassLevel1"] as string;
                                data.AssetClassLevel2 = reader["AssetClassLevel2"] as string;
                                data.AssetClassLevel3 = reader["AssetClassLevel3"] as string;
                                data.MarketStatus = reader["MarketStatus"] as string;
                                data.TradingStatus = reader["TradingStatus"] as string;
                                data.ParentCompany = reader["ParentCompany"] as string;
                                data.Security1940Act = reader["Sec1940Act"] as string;
                                data.FundCategory = reader["FundCategory"] as string;

                                data.InceptionDate = reader.IsDBNull(reader.GetOrdinal("InceptionDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("InceptionDate"));
                                data.TerminationDate = reader.IsDBNull(reader.GetOrdinal("TerminationDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("TerminationDate"));
                                data.InceptionDateAsString = DateUtils.ConvertDate(data.InceptionDate, "yyyy-MM-dd");
                                data.TerminationDateAsString = DateUtils.ConvertDate(data.TerminationDate, "yyyy-MM-dd");

                                data.ExpenseRatio = (reader.IsDBNull(reader.GetOrdinal("ExpenseRatio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExpenseRatio"));
                                data.Leverage = (reader.IsDBNull(reader.GetOrdinal("Leverage"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Leverage"));

                                list.Add(data);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing query: " + almitasId);
                throw;
            }

            return list;
        }

        /// <summary>
        /// Saves custom view
        /// </summary>
        /// <param name="customViewList"></param>
        public void SaveCustomView(IList<CustomView> customViewList)
        {
            string sql = "insert into almitasc_ACTradingBBGData.CustomViewMst (ViewName, SortOrder, "
                + " FieldName, FieldOperator, FieldValue1, FieldValue2, FieldCondition, UserName) "
                + " values (@ViewName, @SortOrder, @FieldName, @FieldOperator, @FieldValue1, @FieldValue2, @FieldCondition, @UserName)";

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    if (customViewList != null && customViewList.Count > 0)
                    {
                        using (MySqlTransaction trans = connection.BeginTransaction())
                        {
                            string viewName = customViewList[0].ViewName;
                            string sqlDelete = "delete from almitasc_ACTradingBBGData.CustomViewMst where ViewName = '" + viewName + "'";
                            using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                            {
                                _logger.LogInformation("Deleting view from almitasc_ACTradingBBGData.CustomViewMst");
                                command.ExecuteNonQuery();
                            }

                            using (MySqlCommand command = new MySqlCommand(sql, connection))
                            {
                                command.Parameters.Add(new MySqlParameter("@ViewName", MySqlDbType.VarChar));
                                command.Parameters.Add(new MySqlParameter("@SortOrder", MySqlDbType.Int32));
                                command.Parameters.Add(new MySqlParameter("@FieldName", MySqlDbType.VarChar));
                                command.Parameters.Add(new MySqlParameter("@FieldOperator", MySqlDbType.VarChar));
                                command.Parameters.Add(new MySqlParameter("@FieldValue1", MySqlDbType.VarChar));
                                command.Parameters.Add(new MySqlParameter("@FieldValue2", MySqlDbType.VarChar));
                                command.Parameters.Add(new MySqlParameter("@FieldCondition", MySqlDbType.VarChar));
                                command.Parameters.Add(new MySqlParameter("@UserName", MySqlDbType.VarChar));

                                foreach (CustomView customView in customViewList)
                                {
                                    command.Parameters[0].Value = customView.ViewName;
                                    command.Parameters[1].Value = customView.SortOrder;
                                    command.Parameters[2].Value = customView.FieldName;
                                    command.Parameters[3].Value = customView.FieldOperator;
                                    command.Parameters[4].Value = customView.FieldValue1;
                                    command.Parameters[5].Value = customView.FieldValue2;
                                    command.Parameters[6].Value = customView.FieldCondition;
                                    command.Parameters[7].Value = customView.UserName;

                                    command.ExecuteNonQuery();
                                }
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
        /// Gets custom view details for specified view
        /// </summary>
        /// <param name="viewName"></param>
        /// <returns></returns>
        public IList<CustomView> GetCustomViewDetails(string viewName)
        {
            IList<CustomView> list = new List<CustomView>();

            try
            {
                string sql = GetCustomViewQuery + " where ViewName = '" + viewName + "' order by SortOrder";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                CustomView data = new CustomView
                                {
                                    ViewName = reader["ViewName"] as string,
                                    SortOrder = reader.GetInt32(reader.GetOrdinal("SortOrder")),
                                    FieldName = reader["FieldName"] as string,
                                    FieldOperator = reader["FieldOperator"] as string,
                                    FieldValue1 = reader["FieldValue1"] as string,
                                    FieldValue2 = reader["FieldValue2"] as string,
                                    FieldCondition = reader["FieldCondition"] as string
                                };

                                data.FieldValue1AsDouble = DataConversionUtils.ConvertToDouble(data.FieldValue1);
                                data.FieldValue2AsDouble = DataConversionUtils.ConvertToDouble(data.FieldValue2);

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
        /// Gets list of custom views
        /// </summary>
        /// <returns></returns>
        public IList<CustomView> GetCustomViews()
        {
            IList<CustomView> list = new List<CustomView>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetCustomViewsQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                CustomView customView = new CustomView
                                {
                                    ViewName = reader["ViewName"] as string,
                                    UserName = reader["UserName"] as string,
                                    CreateDate = reader.IsDBNull(reader.GetOrdinal("CreateDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("CreateDate"))
                                };

                                customView.CreateDateAsString = DateUtils.ConvertDate(customView.CreateDate, "yyyy-MM-dd");
                                list.Add(customView);
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
        /// Gets BDC Nav history
        /// </summary>
        /// <param name="ticker"></param>
        /// <returns></returns>
        public IList<BDCNavHistory> GetBDCNavHistory(string ticker)
        {
            IList<BDCNavHistory> list = new List<BDCNavHistory>();

            try
            {
                string sql = GetBDCNavHistoryQuery + " where 1=1";

                if (!string.IsNullOrEmpty(ticker))
                    sql += " and s.ticker like ('" + ticker + "%')";

                sql += " order by s.ticker, b.date desc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                BDCNavHistory data = new BDCNavHistory
                                {
                                    FIGI = reader["FIGI"] as string,
                                    Ticker = reader["Ticker"] as string,
                                    Nav = (reader.IsDBNull(reader.GetOrdinal("Nav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Nav")),
                                    NavDate = reader.IsDBNull(reader.GetOrdinal("NavDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("NavDate")),
                                    UserName = reader["UserName"] as string,
                                    LastUpdated = reader.IsDBNull(reader.GetOrdinal("CreateDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("CreateDate"))
                                };

                                data.NavDateAsString = DateUtils.ConvertDate(data.NavDate, "yyyy-MM-dd");
                                data.LastUpdatedAsString = DateUtils.ConvertDate(data.LastUpdated, "yyyy-MM-dd");
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
        /// Gets REIT Book Value history
        /// </summary>
        /// <param name="ticker"></param>
        /// <returns></returns>
        public IList<ReitBookValueHistory> GetReitBookValueHistory(string ticker)
        {
            IList<ReitBookValueHistory> list = new List<ReitBookValueHistory>();

            try
            {
                string sql = GetReitBookValueHistoryQuery + " where 1=1";

                if (!string.IsNullOrEmpty(ticker))
                    sql += " and s.ticker like ('" + ticker + "%')";

                sql += " order by s.ticker, b.date desc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                ReitBookValueHistory data = new ReitBookValueHistory
                                {
                                    FIGI = reader["FIGI"] as string,
                                    Ticker = reader["Ticker"] as string,
                                    BookValue = (reader.IsDBNull(reader.GetOrdinal("BookValue"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BookValue")),
                                    BookValueDate = reader.IsDBNull(reader.GetOrdinal("BookValueDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("BookValueDate")),
                                    UserName = reader["UserName"] as string,
                                    LastUpdated = reader.IsDBNull(reader.GetOrdinal("CreateDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("CreateDate"))
                                };

                                data.BookValueDateAsString = DateUtils.ConvertDate(data.BookValueDate, "yyyy-MM-dd");
                                data.LastUpdatedAsString = DateUtils.ConvertDate(data.LastUpdated, "yyyy-MM-dd");

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
        /// Saves BDC Nav history
        /// </summary>
        /// <param name="bdcNavHistory"></param>
        public void SaveBDCNavHistory(IList<BDCNavHistory> bdcNavHistory)
        {
            string sql = "insert into almitasc_ACTradingBBGData.StgBDCNavHistory (ActionType, FIGI, Ticker, NavDate, Nav, UserName) "
                + " values (@ActionType, @FIGI, @Ticker, @NavDate, @Nav, @UserName)";

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    if (bdcNavHistory != null && bdcNavHistory.Count > 0)
                    {
                        string userName = bdcNavHistory[0].UserName;

                        using (MySqlTransaction trans = connection.BeginTransaction())
                        {
                            _logger.LogInformation("Deleting data from almitasc_ACTradingBBGData.StgBDCNavHistory for UserName = '" + userName + "'");
                            string sqlDelete = "delete from almitasc_ACTradingBBGData.StgBDCNavHistory where UserName = '" + userName + "'";
                            using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                            {
                                command.ExecuteNonQuery();
                            }

                            _logger.LogInformation("Saving data to almitasc_ACTradingBBGData.StgBDCNavHistory for UserName = '" + userName + "'");
                            using (MySqlCommand command = new MySqlCommand(sql, connection))
                            {
                                command.Parameters.Add(new MySqlParameter("@ActionType", MySqlDbType.VarChar));
                                command.Parameters.Add(new MySqlParameter("@FIGI", MySqlDbType.VarChar));
                                command.Parameters.Add(new MySqlParameter("@Ticker", MySqlDbType.VarChar));
                                command.Parameters.Add(new MySqlParameter("@NavDate", MySqlDbType.DateTime));
                                command.Parameters.Add(new MySqlParameter("@Nav", MySqlDbType.Decimal));
                                command.Parameters.Add(new MySqlParameter("@UserName", MySqlDbType.VarChar));

                                foreach (BDCNavHistory data in bdcNavHistory)
                                {
                                    data.NavDate = DateUtils.ConvertToDate(data.NavDateAsString, "yyyy-MM-dd");

                                    command.Parameters[0].Value = data.ActionType;
                                    command.Parameters[1].Value = data.FIGI;
                                    command.Parameters[2].Value = data.Ticker;
                                    command.Parameters[3].Value = data.NavDate;
                                    command.Parameters[4].Value = data.Nav;
                                    command.Parameters[5].Value = data.UserName;

                                    command.ExecuteNonQuery();
                                }
                            }

                            _logger.LogInformation("Moving data to almitasc_ACTradingBBGData.globalbdc_navhistory for UserName = '" + userName + "'");
                            sql = "spPopulateBDCNavs";
                            using (MySqlCommand command = new MySqlCommand(sql, connection))
                            {
                                command.CommandType = System.Data.CommandType.StoredProcedure;
                                command.Parameters.AddWithValue("p_UserName", userName);

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

        /// <summary>
        /// Saves REIT Book Value history
        /// </summary>
        /// <param name="reitBVHistory"></param>
        public void SaveReitBookValueHistory(IList<ReitBookValueHistory> reitBVHistory)
        {
            string sql = "insert into almitasc_ACTradingBBGData.StgReitBookValueHistory (ActionType, FIGI, Ticker, BookValueDate, BookValue, UserName) "
                + " values (@ActionType, @FIGI, @Ticker, @BookValueDate, @BookValue, @UserName)";

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    if (reitBVHistory != null && reitBVHistory.Count > 0)
                    {
                        string userName = reitBVHistory[0].UserName;

                        using (MySqlTransaction trans = connection.BeginTransaction())
                        {
                            _logger.LogInformation("Deleting data from almitasc_ACTradingBBGData.StgReitBookValueHistory for UserName = '" + userName + "'");
                            string sqlDelete = "delete from almitasc_ACTradingBBGData.StgReitBookValueHistory where UserName = '" + userName + "'";
                            using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                            {
                                command.ExecuteNonQuery();
                            }

                            _logger.LogInformation("Saving data to almitasc_ACTradingBBGData.StgReitBookValueHistory for UserName = '" + userName + "'");
                            using (MySqlCommand command = new MySqlCommand(sql, connection))
                            {
                                command.Parameters.Add(new MySqlParameter("@ActionType", MySqlDbType.VarChar));
                                command.Parameters.Add(new MySqlParameter("@FIGI", MySqlDbType.VarChar));
                                command.Parameters.Add(new MySqlParameter("@Ticker", MySqlDbType.VarChar));
                                command.Parameters.Add(new MySqlParameter("@BookValueDate", MySqlDbType.DateTime));
                                command.Parameters.Add(new MySqlParameter("@BookValue", MySqlDbType.Decimal));
                                command.Parameters.Add(new MySqlParameter("@UserName", MySqlDbType.VarChar));

                                foreach (ReitBookValueHistory data in reitBVHistory)
                                {
                                    data.BookValueDate = DateUtils.ConvertToDate(data.BookValueDateAsString, "yyyy-MM-dd");

                                    command.Parameters[0].Value = data.ActionType;
                                    command.Parameters[1].Value = data.FIGI;
                                    command.Parameters[2].Value = data.Ticker;
                                    command.Parameters[3].Value = data.BookValueDate;
                                    command.Parameters[4].Value = data.BookValue;
                                    command.Parameters[5].Value = data.UserName;

                                    command.ExecuteNonQuery();
                                }
                            }

                            _logger.LogInformation("Moving data to almitasc_ACTradingBBGData.globalreit_bookvaluehistory for UserName = '" + userName + "'");
                            sql = "spPopulateReitBookValues";
                            using (MySqlCommand command = new MySqlCommand(sql, connection))
                            {
                                command.CommandType = System.Data.CommandType.StoredProcedure;
                                command.Parameters.AddWithValue("p_UserName", userName);

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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ticker"></param>
        /// <returns></returns>
        public IList<FundCurrencyHedge> GetFundCurrencyHedges(string ticker)
        {
            IList<FundCurrencyHedge> list = new List<FundCurrencyHedge>();

            try
            {
                string sql = GetFundCurrencyHedgeQuery + " where 1=1";

                if (!string.IsNullOrEmpty(ticker))
                    sql += " and ticker like ('" + ticker + "%')";

                sql += " order by ticker asc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundCurrencyHedge fundCurrencyHedge = new FundCurrencyHedge
                                {
                                    Ticker = reader["Ticker"] as string,
                                    CurrencyTicker = reader["CurrencyTicker"] as string,
                                    LongShortPosIndicator = reader["LongShortPosIndicator"] as string,
                                    LongCurrency = reader["LongCurrency"] as string,
                                    ShortCurrency = reader["ShortCurrency"] as string,
                                    LongPosition = (reader.IsDBNull(reader.GetOrdinal("LongPosition"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LongPosition")),
                                    ShortPosition = (reader.IsDBNull(reader.GetOrdinal("ShortPosition"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ShortPosition")),
                                    GrossAssets = (reader.IsDBNull(reader.GetOrdinal("GrossAssets"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("GrossAssets")),
                                    PctHedged = (reader.IsDBNull(reader.GetOrdinal("PctHedged"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PctHedged"))
                                };

                                list.Add(fundCurrencyHedge);
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
        /// Saves Currency Hedges in a fund
        /// </summary>
        /// <param name="currencyHedgeList"></param>
        public void SaveFundCurrencyHedges(IList<FundCurrencyHedge> currencyHedgeList)
        {
            string sql = "insert into almitasc_ACTradingBBGData.StgFundFXHedge (ActionType, Ticker, LongCurrency, LongPosition, ShortCurrency, ShortPosition, GrossAssets, PctHedged, CurrencyTicker, LongShortPosIndicator, UserName) "
                + " values (@ActionType, @Ticker, @LongCurrency, @LongPosition, @ShortCurrency, @ShortPosition, @GrossAssets, @PctHedged, @CurrencyTicker, @LongShortPosIndicator, @UserName)";

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    if (currencyHedgeList != null && currencyHedgeList.Count > 0)
                    {
                        string userName = currencyHedgeList[0].UserName;

                        using (MySqlTransaction trans = connection.BeginTransaction())
                        {
                            _logger.LogInformation("Deleting data from almitasc_ACTradingBBGData.StgFundFXHedge for UserName = '" + userName + "'");
                            string sqlDelete = "delete from almitasc_ACTradingBBGData.StgFundFXHedge where UserName = '" + userName + "'";
                            using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                            {
                                command.ExecuteNonQuery();
                            }

                            _logger.LogInformation("Saving data to almitasc_ACTradingBBGData.StgFundFXHedge for UserName = '" + userName + "'");
                            using (MySqlCommand command = new MySqlCommand(sql, connection))
                            {
                                command.Parameters.Add(new MySqlParameter("@ActionType", MySqlDbType.VarChar));
                                command.Parameters.Add(new MySqlParameter("@Ticker", MySqlDbType.VarChar));
                                command.Parameters.Add(new MySqlParameter("@LongCurrency", MySqlDbType.VarChar));
                                command.Parameters.Add(new MySqlParameter("@LongPosition", MySqlDbType.Decimal));
                                command.Parameters.Add(new MySqlParameter("@ShortCurrency", MySqlDbType.VarChar));
                                command.Parameters.Add(new MySqlParameter("@ShortPosition", MySqlDbType.Decimal));
                                command.Parameters.Add(new MySqlParameter("@GrossAssets", MySqlDbType.Decimal));
                                command.Parameters.Add(new MySqlParameter("@PctHedged", MySqlDbType.Decimal));
                                command.Parameters.Add(new MySqlParameter("@CurrencyTicker", MySqlDbType.VarChar));
                                command.Parameters.Add(new MySqlParameter("@LongShortPosIndicator", MySqlDbType.VarChar));
                                command.Parameters.Add(new MySqlParameter("@UserName", MySqlDbType.VarChar));

                                foreach (FundCurrencyHedge data in currencyHedgeList)
                                {
                                    command.Parameters[0].Value = data.ActionType;
                                    command.Parameters[1].Value = data.Ticker;
                                    command.Parameters[2].Value = data.LongCurrency;
                                    command.Parameters[3].Value = data.LongPosition;
                                    command.Parameters[4].Value = data.ShortCurrency;
                                    command.Parameters[5].Value = data.ShortPosition;
                                    command.Parameters[6].Value = data.GrossAssets;
                                    command.Parameters[7].Value = data.PctHedged;
                                    command.Parameters[8].Value = data.CurrencyTicker;
                                    command.Parameters[9].Value = data.LongShortPosIndicator;
                                    command.Parameters[10].Value = data.UserName;

                                    command.ExecuteNonQuery();
                                }
                            }

                            _logger.LogInformation("Moving data to almitasc_ACTradingBBGData.globalcef_fundmaster_fxhedge for UserName = '" + userName + "'");
                            sql = "almitasc_ACTradingBBGData.spPopulateFundFXHedge";
                            using (MySqlCommand command = new MySqlCommand(sql, connection))
                            {
                                command.CommandType = System.Data.CommandType.StoredProcedure;
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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="category"></param>
        /// <param name="startDate"></param>
        /// <param name="endDate"></param>
        /// <returns></returns>
        public IList<FundAlert> GetFundAlerts(string category, string startDate, string endDate)
        {
            IList<FundAlert> list = new List<FundAlert>();

            try
            {
                string sql = GetFundAlertQuery + " where 1=1";

                if (!string.IsNullOrEmpty(category) && !("All".Equals(category, StringComparison.CurrentCultureIgnoreCase)))
                    sql += " and alertcategory = '" + category + "'";

                if (!string.IsNullOrEmpty(startDate))
                    sql += " and effectiveDate >= '" + startDate + "'";

                if (!string.IsNullOrEmpty(endDate))
                    sql += " and effectiveDate <= '" + endDate + "'";

                sql += " order by effectivedate desc, alertcategory asc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundAlert data = new FundAlert
                                {
                                    Ticker = reader["Ticker"] as string,
                                    AlertCategory = reader["AlertCategory"] as string,
                                    AlertType = reader["AlertType"] as string,
                                    AlertDetail = reader["AlertDetail"] as string,
                                    EffectiveDate = (reader.IsDBNull(reader.GetOrdinal("EffectiveDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate"))
                                };

                                data.EffectiveDateAsString = DateUtils.ConvertDate(data.EffectiveDate, "yyyy-MM-dd");

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
        /// Saves Fund Alerts
        /// </summary>
        public void PopulateFundAlerts()
        {
            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetPopulateFundAlertQuery, connection))
                    {
                        command.ExecuteNonQuery();
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
        /// Gets Fund Alert Targets
        /// </summary>
        /// <returns></returns>
        public IList<FundAlertTarget> GetFundAlertTargets()
        {
            IList<FundAlertTarget> list = new List<FundAlertTarget>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetFundAlertTargetsQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundAlertTarget data = new FundAlertTarget
                                {
                                    AlertType = reader["AlertType"] as string,
                                    Ticker = reader["Ticker"] as string,
                                    UserName = reader["UserName"] as string,
                                    BuyTarget = (reader.IsDBNull(reader.GetOrdinal("BuyTarget"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BuyTarget")),
                                    SellTarget = (reader.IsDBNull(reader.GetOrdinal("SellTarget"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SellTarget"))
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
        /// Saves Fund Alert Targets
        /// </summary>
        /// <param name="fundAlertTargetList"></param>
        public void SaveFundAlertTargets(IList<FundAlertTarget> fundAlertTargetList)
        {
            string sql = "insert into almitasc_ACTradingBBGData.StgFundAlertTarget (AlertType, Ticker, BuyTarget, SellTarget, UserName) "
                + " values (@AlertType, @Ticker, @BuyTarget, @SellTarget, @UserName)";

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    if (fundAlertTargetList != null && fundAlertTargetList.Count > 0)
                    {
                        string userName = fundAlertTargetList[0].UserName;

                        using (MySqlTransaction trans = connection.BeginTransaction())
                        {
                            _logger.LogInformation("Deleting data from almitasc_ACTradingBBGData.StgFundAlertTarget");
                            string sqlDelete = "delete from almitasc_ACTradingBBGData.StgFundAlertTarget";
                            using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                            {
                                command.ExecuteNonQuery();
                            }

                            _logger.LogInformation("Saving data to almitasc_ACTradingBBGData.StgFundAlertTarget for UserName = '" + userName + "'");
                            using (MySqlCommand command = new MySqlCommand(sql, connection))
                            {
                                command.Parameters.Add(new MySqlParameter("@AlertType", MySqlDbType.VarChar));
                                command.Parameters.Add(new MySqlParameter("@Ticker", MySqlDbType.VarChar));
                                command.Parameters.Add(new MySqlParameter("@BuyTarget", MySqlDbType.Decimal));
                                command.Parameters.Add(new MySqlParameter("@SellTarget", MySqlDbType.Decimal));
                                command.Parameters.Add(new MySqlParameter("@UserName", MySqlDbType.VarChar));

                                foreach (FundAlertTarget data in fundAlertTargetList)
                                {
                                    command.Parameters[0].Value = data.AlertType;
                                    command.Parameters[1].Value = data.Ticker;
                                    command.Parameters[2].Value = data.BuyTarget;
                                    command.Parameters[3].Value = data.SellTarget;
                                    command.Parameters[4].Value = data.UserName;

                                    command.ExecuteNonQuery();
                                }
                            }

                            _logger.LogInformation("Moving data to almitasc_ACTradingBBGData.FundAlertTarget for UserName = '" + userName + "'");
                            sql = "spPopulateFundAlertTarget";
                            using (MySqlCommand command = new MySqlCommand(sql, connection))
                            {
                                command.CommandType = System.Data.CommandType.StoredProcedure;
                                command.Parameters.AddWithValue("p_UserName", userName);

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

        /// <summary>
        /// Gets TaxLots for a ticker, account and broker
        /// </summary>
        /// <param name="ticker"></param>
        /// <param name="account"></param>
        /// <param name="broker"></param>
        /// <returns></returns>
        public IList<TradeTaxLot> GetTaxLots(string ticker, string account, string broker, string AgeRangeFrom, string AgeRangeTo)
        {
            IList<TradeTaxLot> list = new List<TradeTaxLot>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetTaxLotsQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Ticker", ticker);
                        command.Parameters.AddWithValue("p_Account", account);
                        command.Parameters.AddWithValue("p_Broker", broker);
                        command.Parameters.AddWithValue("p_AgeRangeFrom", AgeRangeFrom);
                        command.Parameters.AddWithValue("p_AgeRangeTo", AgeRangeTo);

                        int rowId = 10;
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                TradeTaxLot data = new TradeTaxLot
                                {
                                    AsOfDate = (reader.IsDBNull(reader.GetOrdinal("AsOfDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("AsOfDate")),
                                    FileDate = (reader.IsDBNull(reader.GetOrdinal("FileDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate")),
                                    Ticker = reader["ALMTicker"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    ISIN = reader["ISIN"] as string,
                                    Cusip = reader["Cusip"] as string,
                                    TaxLotCurr = reader["Curr"] as string,
                                    Name = reader["Name"] as string,
                                    Bkr = reader["Broker"] as string,
                                    Acct = reader["Account"] as string,
                                    Qty = (reader.IsDBNull(reader.GetOrdinal("Position"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Position")),
                                    OpenDate = (reader.IsDBNull(reader.GetOrdinal("OpenDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("OpenDate")),
                                    Age = (reader.IsDBNull(reader.GetOrdinal("Age"))) ? (Int16?)null : reader.GetInt16(reader.GetOrdinal("Age")),
                                    UnitCostLcl = (reader.IsDBNull(reader.GetOrdinal("UnitCostLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnitCostLocal")),
                                    UnitCostUSD = (reader.IsDBNull(reader.GetOrdinal("UnitCostUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnitCostUSD")),
                                    PrcLcl = (reader.IsDBNull(reader.GetOrdinal("PriceLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceLocal")),
                                    PrcUSD = (reader.IsDBNull(reader.GetOrdinal("PriceUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceUSD")),
                                    MVLcl = (reader.IsDBNull(reader.GetOrdinal("MVLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MVLocal")),
                                    MVUSD = (reader.IsDBNull(reader.GetOrdinal("MVUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MVUSD")),
                                    TotalCostLcl = (reader.IsDBNull(reader.GetOrdinal("TotalCostLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotalCostLocal")),
                                    TotalCostUSD = (reader.IsDBNull(reader.GetOrdinal("TotalCostUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotalCostUSD")),
                                    FXRate = (reader.IsDBNull(reader.GetOrdinal("FxRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FxRate")),
                                    PnLLcl = (reader.IsDBNull(reader.GetOrdinal("UnrealizedPLLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnrealizedPLLocal")),
                                    PnLUSD = (reader.IsDBNull(reader.GetOrdinal("UnrealizedPL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnrealizedPL")),
                                    PnLPct = (reader.IsDBNull(reader.GetOrdinal("PnLPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PnLPct")),
                                    CurPVLeg1 = (reader.IsDBNull(reader.GetOrdinal("CurPVLeg1"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CurPVLeg1")),
                                    CurPVLeg2 = (reader.IsDBNull(reader.GetOrdinal("CurPVLeg2"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CurPVLeg2")),
                                    MTM = (reader.IsDBNull(reader.GetOrdinal("MTM"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MTM")),
                                    MTMUSD = (reader.IsDBNull(reader.GetOrdinal("MTMUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MTMUSD")),
                                    AcctType = reader["AcctType"] as string,
                                    AssetCls = reader["AssetClass"] as string,
                                    Term = reader["Term"] as string,
                                    SecCurr = reader["SecCurr"] as string,
                                    TrdDtPrc = (reader.IsDBNull(reader.GetOrdinal("TrdDtPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TrdDtPrice")),
                                    Nav = (reader.IsDBNull(reader.GetOrdinal("PubNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PubNav")),
                                    NavInterp = (reader.IsDBNull(reader.GetOrdinal("PubNavInterp"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PubNavInterp")),
                                    PD = (reader.IsDBNull(reader.GetOrdinal("PubPD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PubPD")),
                                    PDInterp = (reader.IsDBNull(reader.GetOrdinal("PubPDInterp"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PubPDInterp")),
                                    PDTrd = (reader.IsDBNull(reader.GetOrdinal("TrdPD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TrdPD")),
                                    MultBkrFlag = (reader.IsDBNull(reader.GetOrdinal("MultBrokerFlag"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("MultBrokerFlag")),
                                    SortId = rowId,
                                    LotId = (reader.IsDBNull(reader.GetOrdinal("LotId"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("LotId")),
                                    LastTradedDate = (reader.IsDBNull(reader.GetOrdinal("LastTradedDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("LastTradedDate")),
                                    IsSwap = reader["IsSwap"] as string,
                                    TradeType = reader["TradeType"] as string,
                                    LongShortInd = reader["LongShortInd"] as string,
                                    LastPrc = (reader.IsDBNull(reader.GetOrdinal("LastPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LastPrice")),
                                };
                                rowId++;

                                data.OpenDateAsString = DateUtils.ConvertDate(data.OpenDate, "yyyy-MM-dd");
                                data.FileDateAsString = DateUtils.ConvertDate(data.FileDate, "yyyy-MM-dd");

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
        /// Gets TaxLot Summary
        /// </summary>
        /// <param name="account"></param>
        /// <param name="broker"></param>
        /// <returns></returns>
        public IList<TradeTaxLotSummary> GetTaxLotSummary(string account, string broker)
        {
            IList<TradeTaxLotSummary> list = new List<TradeTaxLotSummary>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetTaxLotSummaryQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Account", account);
                        command.Parameters.AddWithValue("p_Broker", broker);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                TradeTaxLotSummary data = new TradeTaxLotSummary
                                {
                                    Ticker = reader["SecTicker"] as string,
                                    YellowKey = reader["YellowKey"] as string,
                                    TotalPosition = (reader.IsDBNull(reader.GetOrdinal("TotalPosition"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotalPosition")),

                                    //long term -- position
                                    LongTermJefferies = (reader.IsDBNull(reader.GetOrdinal("LongTermJefferies"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LongTermJefferies")),
                                    LongTermJPM = (reader.IsDBNull(reader.GetOrdinal("LongTermJPM"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LongTermJPM")),
                                    LongTermIB = (reader.IsDBNull(reader.GetOrdinal("LongTermIB"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LongTermIB")),
                                    LongTermFido = (reader.IsDBNull(reader.GetOrdinal("LongTermFido"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LongTermFido")),

                                    //long term -- avg age
                                    LongTermAvgAgeJefferies = (reader.IsDBNull(reader.GetOrdinal("LongTermAvgAgeJefferies"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LongTermAvgAgeJefferies")),
                                    LongTermAvgAgeJPM = (reader.IsDBNull(reader.GetOrdinal("LongTermAvgAgeJPM"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LongTermAvgAgeJPM")),
                                    LongTermAvgAgeIB = (reader.IsDBNull(reader.GetOrdinal("LongTermAvgAgeIB"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LongTermAvgAgeIB")),
                                    LongTermAvgAgeFido = (reader.IsDBNull(reader.GetOrdinal("LongTermAvgAgeFido"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LongTermAvgAgeFido")),

                                    //short 1 day to long term -- position
                                    Short1DayJefferies = (reader.IsDBNull(reader.GetOrdinal("Short1DayJefferies"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Short1DayJefferies")),
                                    Short1DayJPM = (reader.IsDBNull(reader.GetOrdinal("Short1DayJPM"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Short1DayJPM")),
                                    Short1DayIB = (reader.IsDBNull(reader.GetOrdinal("Short1DayIB"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Short1DayIB")),
                                    Short1DayFido = (reader.IsDBNull(reader.GetOrdinal("Short1DayFido"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Short1DayFido")),

                                    //short 1 day to long term -- avg age
                                    Short1DayAvgAgeJefferies = (reader.IsDBNull(reader.GetOrdinal("Short1DayAvgAgeJefferies"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Short1DayAvgAgeJefferies")),
                                    Short1DayAvgAgeJPM = (reader.IsDBNull(reader.GetOrdinal("Short1DayAvgAgeJPM"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Short1DayAvgAgeJPM")),
                                    Short1DayAvgAgeIB = (reader.IsDBNull(reader.GetOrdinal("Short1DayAvgAgeIB"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Short1DayAvgAgeIB")),
                                    Short1DayAvgAgeFido = (reader.IsDBNull(reader.GetOrdinal("Short1DayAvgAgeFido"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Short1DayAvgAgeFido")),

                                    //short 1 week to long term -- position
                                    Short1WeekJefferies = (reader.IsDBNull(reader.GetOrdinal("Short1WeekJefferies"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Short1WeekJefferies")),
                                    Short1WeekJPM = (reader.IsDBNull(reader.GetOrdinal("Short1WeekJPM"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Short1WeekJPM")),
                                    Short1WeekIB = (reader.IsDBNull(reader.GetOrdinal("Short1WeekIB"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Short1WeekIB")),
                                    Short1WeekFido = (reader.IsDBNull(reader.GetOrdinal("Short1WeekFido"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Short1WeekFido")),

                                    //short 1 week to long term -- avg age
                                    Short1WeekAvgAgeJefferies = (reader.IsDBNull(reader.GetOrdinal("Short1WeekAvgAgeJefferies"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Short1WeekAvgAgeJefferies")),
                                    Short1WeekAvgAgeJPM = (reader.IsDBNull(reader.GetOrdinal("Short1WeekAvgAgeJPM"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Short1WeekAvgAgeJPM")),
                                    Short1WeekAvgAgeIB = (reader.IsDBNull(reader.GetOrdinal("Short1WeekAvgAgeIB"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Short1WeekAvgAgeIB")),
                                    Short1WeekAvgAgeFido = (reader.IsDBNull(reader.GetOrdinal("Short1WeekAvgAgeFido"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Short1WeekAvgAgeFido")),

                                    //short 1 month to long term -- position
                                    Short1MonthJefferies = (reader.IsDBNull(reader.GetOrdinal("Short1MonthJefferies"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Short1MonthJefferies")),
                                    Short1MonthJPM = (reader.IsDBNull(reader.GetOrdinal("Short1MonthJPM"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Short1MonthJPM")),
                                    Short1MonthIB = (reader.IsDBNull(reader.GetOrdinal("Short1MonthIB"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Short1MonthIB")),
                                    Short1MonthFido = (reader.IsDBNull(reader.GetOrdinal("Short1MonthFido"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Short1MonthFido")),

                                    //short 1 month to long term -- avg age
                                    Short1MonthAvgAgeJefferies = (reader.IsDBNull(reader.GetOrdinal("Short1MonthAvgAgeJefferies"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Short1MonthAvgAgeJefferies")),
                                    Short1MonthAvgAgeJPM = (reader.IsDBNull(reader.GetOrdinal("Short1MonthAvgAgeJPM"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Short1MonthAvgAgeJPM")),
                                    Short1MonthAvgAgeIB = (reader.IsDBNull(reader.GetOrdinal("Short1MonthAvgAgeIB"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Short1MonthAvgAgeIB")),
                                    Short1MonthAvgAgeFido = (reader.IsDBNull(reader.GetOrdinal("Short1MonthAvgAgeFido"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Short1MonthAvgAgeFido")),

                                    //short over 1 month -- position
                                    ShortOver1MonthJefferies = (reader.IsDBNull(reader.GetOrdinal("ShortOver1MonthJefferies"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ShortOver1MonthJefferies")),
                                    ShortOver1MonthJPM = (reader.IsDBNull(reader.GetOrdinal("ShortOver1MonthJPM"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ShortOver1MonthJPM")),
                                    ShortOver1MonthIB = (reader.IsDBNull(reader.GetOrdinal("ShortOver1MonthIB"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ShortOver1MonthIB")),
                                    ShortOver1MonthFido = (reader.IsDBNull(reader.GetOrdinal("ShortOver1MonthFido"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ShortOver1MonthFido")),

                                    //short over 1 month -- avg age
                                    ShortOver1MonthAvgAgeJefferies = (reader.IsDBNull(reader.GetOrdinal("ShortOver1MonthAvgAgeJefferies"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ShortOver1MonthAvgAgeJefferies")),
                                    ShortOver1MonthAvgAgeJPM = (reader.IsDBNull(reader.GetOrdinal("ShortOver1MonthAvgAgeJPM"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ShortOver1MonthAvgAgeJPM")),
                                    ShortOver1MonthAvgAgeIB = (reader.IsDBNull(reader.GetOrdinal("ShortOver1MonthAvgAgeIB"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ShortOver1MonthAvgAgeIB")),
                                    ShortOver1MonthAvgAgeFido = (reader.IsDBNull(reader.GetOrdinal("ShortOver1MonthAvgAgeFido"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ShortOver1MonthAvgAgeFido")),

                                    ShortTermGain = (reader.IsDBNull(reader.GetOrdinal("ShortTermGain"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ShortTermGain")),
                                    ShortTermLoss = (reader.IsDBNull(reader.GetOrdinal("ShortTermLoss"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ShortTermLoss")),
                                    LongTermGain = (reader.IsDBNull(reader.GetOrdinal("LongTermGain"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LongTermGain")),
                                    LongTermLoss = (reader.IsDBNull(reader.GetOrdinal("LongTermLoss"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LongTermLoss")),

                                    TotalPnL = (reader.IsDBNull(reader.GetOrdinal("TotalPnL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotalPnL")),
                                    FileDate = (reader.IsDBNull(reader.GetOrdinal("FileDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FileDate"))
                                };

                                data.FileDateAsString = DateUtils.ConvertDate(data.FileDate, "yyyy-MM-dd");

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
        /// Saves Fund Redemption Details
        /// </summary>
        /// <param name="fundRedemptionList"></param>
        public void SaveFundRedemptions(IList<aCommons.Cef.FundRedemption> fundRedemptionList)
        {
            string sql = "insert into almitasc_ACTradingBBGData.StgFundRedemptionRules"
                + "(RedemptionType, Ticker, Structure, FirstRedemptionDate, RedemptionFrequency, LastRedemptionDate, "
                + "RedemptionDaysFromMonthEnd, RedemptionNoticeDateType, NoticeDaysFromRedemption, PaymentDelayDateType, "
                + "PaymentDelay, FundLiquidityTransactionCost, FundRedemptionFixedFee, PerShareCommissionOnPurchase, "
                + "PreferredShareRedemptionValueIncludedInNAV, IsPreferredShareRedemptionValueIncludedInNAV, "
                + "PreferredInterestOnRedemptionDate, SEDOL, PreferredShareTicker, "
                + "SplitSharesRedeemedSeparately, NumPreferredSharesPerCommonSplitTrust, FundRedemptionFeePct, "
                + "AddFundRedemptionFeePct, PreferredTakenOutRedemptionPrice, StampTax, OvrNextRedemptionNoticeDate, OvrNextRedemptionPaymentDate)"
                + " values "
                + "(@RedemptionType, @Ticker, @Structure, @FirstRedemptionDate, @RedemptionFrequency, @LastRedemptionDate, "
                + "@RedemptionDaysFromMonthEnd, @RedemptionNoticeDateType, @NoticeDaysFromRedemption, @PaymentDelayDateType, "
                + "@PaymentDelay, @FundLiquidityTransactionCost, @FundRedemptionFixedFee, @PerShareCommissionOnPurchase, "
                + "@PreferredShareRedemptionValueIncludedInNAV, @IsPreferredShareRedemptionValueIncludedInNAV, "
                + "@PreferredInterestOnRedemptionDate, @SEDOL, @PreferredShareTicker, "
                + "@SplitSharesRedeemedSeparately, @NumPreferredSharesPerCommonSplitTrust, @FundRedemptionFeePct, "
                + "@AddFundRedemptionFeePct, @PreferredTakenOutRedemptionPrice, @StampTax, @OvrNextRedemptionNoticeDate, @OvrNextRedemptionPaymentDate)";

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    if (fundRedemptionList != null && fundRedemptionList.Count > 0)
                    {
                        using (MySqlTransaction trans = connection.BeginTransaction())
                        {
                            _logger.LogInformation("Deleting data from almitasc_ACTradingBBGData.StgFundRedemptionRules");
                            string sqlDelete = "delete from almitasc_ACTradingBBGData.StgFundRedemptionRules";
                            using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                            {
                                command.ExecuteNonQuery();
                            }

                            _logger.LogInformation("Saving data to almitasc_ACTradingBBGData.StgFundRedemptionRules");
                            using (MySqlCommand command = new MySqlCommand(sql, connection))
                            {
                                command.Parameters.Add(new MySqlParameter("@RedemptionType", MySqlDbType.VarChar));
                                command.Parameters.Add(new MySqlParameter("@Ticker", MySqlDbType.VarChar));
                                command.Parameters.Add(new MySqlParameter("@Structure", MySqlDbType.VarChar));
                                command.Parameters.Add(new MySqlParameter("@FirstRedemptionDate", MySqlDbType.Date));
                                command.Parameters.Add(new MySqlParameter("@RedemptionFrequency", MySqlDbType.VarChar));
                                command.Parameters.Add(new MySqlParameter("@LastRedemptionDate", MySqlDbType.Date));
                                command.Parameters.Add(new MySqlParameter("@RedemptionDaysFromMonthEnd", MySqlDbType.Int32));
                                command.Parameters.Add(new MySqlParameter("@RedemptionNoticeDateType", MySqlDbType.VarChar));
                                command.Parameters.Add(new MySqlParameter("@NoticeDaysFromRedemption", MySqlDbType.Int32));
                                command.Parameters.Add(new MySqlParameter("@PaymentDelayDateType", MySqlDbType.VarChar));
                                command.Parameters.Add(new MySqlParameter("@PaymentDelay", MySqlDbType.Int32));
                                command.Parameters.Add(new MySqlParameter("@FundLiquidityTransactionCost", MySqlDbType.Decimal));
                                command.Parameters.Add(new MySqlParameter("@FundRedemptionFixedFee", MySqlDbType.Decimal));
                                command.Parameters.Add(new MySqlParameter("@PerShareCommissionOnPurchase", MySqlDbType.Decimal));
                                command.Parameters.Add(new MySqlParameter("@PreferredShareRedemptionValueIncludedInNAV", MySqlDbType.Decimal));
                                command.Parameters.Add(new MySqlParameter("@IsPreferredShareRedemptionValueIncludedInNAV", MySqlDbType.VarChar));
                                command.Parameters.Add(new MySqlParameter("@PreferredInterestOnRedemptionDate", MySqlDbType.Decimal));
                                command.Parameters.Add(new MySqlParameter("@SEDOL", MySqlDbType.VarChar));
                                command.Parameters.Add(new MySqlParameter("@PreferredShareTicker", MySqlDbType.VarChar));
                                command.Parameters.Add(new MySqlParameter("@SplitSharesRedeemedSeparately", MySqlDbType.VarChar));
                                command.Parameters.Add(new MySqlParameter("@NumPreferredSharesPerCommonSplitTrust", MySqlDbType.Decimal));
                                command.Parameters.Add(new MySqlParameter("@FundRedemptionFeePct", MySqlDbType.Decimal));
                                command.Parameters.Add(new MySqlParameter("@AddFundRedemptionFeePct", MySqlDbType.Decimal));
                                command.Parameters.Add(new MySqlParameter("@PreferredTakenOutRedemptionPrice", MySqlDbType.VarChar));
                                command.Parameters.Add(new MySqlParameter("@StampTax", MySqlDbType.Decimal));
                                command.Parameters.Add(new MySqlParameter("@OvrNextRedemptionNoticeDate", MySqlDbType.Date));
                                command.Parameters.Add(new MySqlParameter("@OvrNextRedemptionPaymentDate", MySqlDbType.Date));

                                foreach (aCommons.Cef.FundRedemption data in fundRedemptionList)
                                {
                                    //convert redemption dates from string to date
                                    if (!string.IsNullOrEmpty(data.FirstRedemptionDateAsString))
                                        data.FirstRedemptionDate = DateUtils.ConvertToDate(data.FirstRedemptionDateAsString);

                                    if (!string.IsNullOrEmpty(data.LastRedemptionDateAsString))
                                        data.LastRedemptionDate = DateUtils.ConvertToDate(data.LastRedemptionDateAsString);

                                    if (!string.IsNullOrEmpty(data.OvrNextRedemptionNoticeDateAsString))
                                        data.OvrNextRedemptionNoticeDate = DateUtils.ConvertToDate(data.OvrNextRedemptionNoticeDateAsString);

                                    if (!string.IsNullOrEmpty(data.OvrNextRedemptionPaymentDateAsString))
                                        data.OvrNextRedemptionPaymentDate = DateUtils.ConvertToDate(data.OvrNextRedemptionPaymentDateAsString);

                                    command.Parameters[0].Value = data.RedemptionType;
                                    command.Parameters[1].Value = data.Ticker;
                                    command.Parameters[2].Value = data.Structure;
                                    command.Parameters[3].Value = data.FirstRedemptionDate;
                                    command.Parameters[4].Value = data.RedemptionFrequency;
                                    command.Parameters[5].Value = data.LastRedemptionDate;
                                    command.Parameters[6].Value = data.RedemptionDaysFromMonthEnd;
                                    command.Parameters[7].Value = data.RedemptionNoticeDateType;
                                    command.Parameters[8].Value = data.RedemptionNoticeDays;
                                    command.Parameters[9].Value = data.PaymentDelayDateType;
                                    command.Parameters[10].Value = data.PaymentDelay;
                                    command.Parameters[11].Value = data.FundLiquidityTransactionCost;
                                    command.Parameters[12].Value = data.RedemptionFixedFee;
                                    command.Parameters[13].Value = data.PerShareCommission;
                                    command.Parameters[14].Value = data.PreferredShareRedemptionValue;
                                    command.Parameters[15].Value = data.IsPreferredShareRedemptionValueIncludedInNav;
                                    command.Parameters[16].Value = data.PreferredInterestOnRedemptionDate;
                                    command.Parameters[17].Value = data.PreferredShareSedol;
                                    command.Parameters[18].Value = data.PreferredShareTicker;
                                    command.Parameters[19].Value = data.SplitSharesRedeemedSeparately;
                                    command.Parameters[20].Value = data.NumPreferredSharesPerCommonSplitTrust;
                                    command.Parameters[21].Value = data.RedemptionFeePct;
                                    command.Parameters[22].Value = data.AddlRedemptionFeePct;
                                    command.Parameters[23].Value = data.IsPreferredTakenOutAtRedemptionPrice;
                                    command.Parameters[24].Value = data.StampTax;
                                    command.Parameters[25].Value = data.OvrNextRedemptionNoticeDate;
                                    command.Parameters[26].Value = data.OvrNextRedemptionPaymentDate;

                                    command.ExecuteNonQuery();
                                }
                            }

                            _logger.LogInformation("Moving data to almitasc_ACTradingBBGData.spPopulateFundRedemptionRules");
                            sql = "spPopulateFundRedemptionRules";
                            using (MySqlCommand command = new MySqlCommand(sql, connection))
                            {
                                command.CommandType = System.Data.CommandType.StoredProcedure;
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

        /// <summary>
        /// Gets Fund Nav History
        /// </summary>
        /// <param name="ticker"></param>
        /// <param name="startDate"></param>
        /// <param name="endDate"></param>
        /// <returns></returns>
        public IList<FundNavHistory> GetFundNavHistory(string ticker, DateTime startDate, DateTime endDate)
        {
            IList<FundNavHistory> list = new List<FundNavHistory>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetFundNavHistoryQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Ticker", ticker);
                        command.Parameters.AddWithValue("p_StartDate", startDate);
                        command.Parameters.AddWithValue("p_EndDate", endDate);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundNavHistory data = new FundNavHistory
                                {
                                    FIGI = reader["FIGI"] as string,
                                    Ticker = reader["Ticker"] as string,
                                    Currency = reader["Currency"] as string,
                                    EffectiveDate = reader.IsDBNull(reader.GetOrdinal("EffectiveDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate")),

                                    BBGNav = (reader.IsDBNull(reader.GetOrdinal("BBGNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BBGNav")),
                                    BBGPrice = (reader.IsDBNull(reader.GetOrdinal("BBGPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BBGPrice")),
                                    BBGPD = (reader.IsDBNull(reader.GetOrdinal("BBGPD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BBGPD")),

                                    MSNav = (reader.IsDBNull(reader.GetOrdinal("MSNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MSNav")),
                                    MSPrice = (reader.IsDBNull(reader.GetOrdinal("MSPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MSPrice")),
                                    MSPD = (reader.IsDBNull(reader.GetOrdinal("MSPD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MSPD")),

                                    ALMNav = (reader.IsDBNull(reader.GetOrdinal("ALMNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ALMNav")),
                                    ALMPrice = (reader.IsDBNull(reader.GetOrdinal("ALMPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ALMPrice")),
                                    ALMPD = (reader.IsDBNull(reader.GetOrdinal("ALMPD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ALMPD")),

                                    NavYield = (reader.IsDBNull(reader.GetOrdinal("NavYield"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NavYield")),
                                    EarningsCoverageRatioPct = (reader.IsDBNull(reader.GetOrdinal("EarningCoverageRatioPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EarningCoverageRatioPct")),
                                    UNIINavPct = (reader.IsDBNull(reader.GetOrdinal("UNIINavPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UNIINavPct"))
                                };

                                data.EffectiveDateAsString = DateUtils.ConvertDate(data.EffectiveDate, "yyyy-MM-dd");

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
        /// Gets Fund Earnings History (source - Morningstar)
        /// </summary>
        /// <param name="ticker"></param>
        /// <param name="profilePeriod"></param>
        /// <returns></returns>
        public IList<FundEarningHist> GetFundEarningsHistory(string ticker, string profilePeriod)
        {
            IList<FundEarningHist> list = new List<FundEarningHist>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetFundEarningsHistoryQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Ticker", ticker);
                        command.Parameters.AddWithValue("p_Profile_Period", profilePeriod);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundEarningHist data = new FundEarningHist
                                {
                                    SecId = reader["SecId"] as string,
                                    Ticker = reader["Ticker"] as string,
                                    EarningsProfileFrequency = reader["EarningsProfileFreq"] as string,
                                    EarningsProfileDate = reader.IsDBNull(reader.GetOrdinal("EarningsProfileDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EarningsProfileDate")),
                                    EarningsProfilePeriod = (reader.IsDBNull(reader.GetOrdinal("EarningsProfilePeriod"))) ? (Int16?)null : reader.GetInt16(reader.GetOrdinal("EarningsProfilePeriod")),

                                    TotalInvIncome = (reader.IsDBNull(reader.GetOrdinal("TotalInvIncome"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotalInvIncome")),
                                    IncPfdShareholders = (reader.IsDBNull(reader.GetOrdinal("IncPfdShareholders"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IncPfdShareholders")),
                                    InterestPaid = (reader.IsDBNull(reader.GetOrdinal("InterestPaid"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("InterestPaid")),
                                    OperatingExpenses = (reader.IsDBNull(reader.GetOrdinal("OperatingExpenses"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OperatingExpenses")),
                                    NetInvIncome = (reader.IsDBNull(reader.GetOrdinal("NetInvIncome"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NetInvIncome")),
                                    NetIncPostPfdShareDistribution = (reader.IsDBNull(reader.GetOrdinal("NetIncPostPfdShareDistribution"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NetIncPostPfdShareDistribution")),
                                    RealisedCapitalGains = (reader.IsDBNull(reader.GetOrdinal("RealisedCapitalGains"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RealisedCapitalGains")),
                                    UnrealisedCapitalGains = (reader.IsDBNull(reader.GetOrdinal("UnrealisedCapitalGains"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnrealisedCapitalGains")),
                                    NetInvIncomePerShare = (reader.IsDBNull(reader.GetOrdinal("NetInvIncomePerShare"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NetInvIncomePerShare")),
                                    NetInvIncomePostPfdShareDistribution = (reader.IsDBNull(reader.GetOrdinal("NetInvIncomePostPfdShareDistribution"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NetInvIncomePostPfdShareDistribution")),
                                    NetInvIncomePerMonthPerShare = (reader.IsDBNull(reader.GetOrdinal("NetInvIncomePerMonthPerShare"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NetInvIncomePerMonthPerShare")),
                                    NetInvIncomePostPfdPerMonthPerShare = (reader.IsDBNull(reader.GetOrdinal("NetInvIncomePostPfdPerMonthPerShare"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NetInvIncomePostPfdPerMonthPerShare")),
                                    EarningsRateReportingPeriod = (reader.IsDBNull(reader.GetOrdinal("EarningsRateReportingPeriod"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EarningsRateReportingPeriod")),
                                    DistributionCoverageReportingPeriod = (reader.IsDBNull(reader.GetOrdinal("DistributionCoverageReportingPeriod"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DistributionCoverageReportingPeriod")),
                                    EarningsRate = (reader.IsDBNull(reader.GetOrdinal("EarningsRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EarningsRate")),
                                    DistributionCoverage = (reader.IsDBNull(reader.GetOrdinal("DistributionCoverage"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DistributionCoverage")),
                                    RealisedCapitalGainsPerShare = (reader.IsDBNull(reader.GetOrdinal("RealisedCapitalGainsPerShare"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RealisedCapitalGainsPerShare")),
                                    UnrealisedCapitalGainsPerShare = (reader.IsDBNull(reader.GetOrdinal("UnrealisedCapitalGainsPerShare"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnrealisedCapitalGainsPerShare")),
                                    UNII = (reader.IsDBNull(reader.GetOrdinal("UNII"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UNII")),
                                    UNIIPerShare = (reader.IsDBNull(reader.GetOrdinal("UNIIPerShare"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UNIIPerShare")),
                                    LifeDvdReservesReportingPeriod = (reader.IsDBNull(reader.GetOrdinal("LifeDvdReservesReportingPeriod"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LifeDvdReservesReportingPeriod")),
                                    UNIIDividendCoverage = (reader.IsDBNull(reader.GetOrdinal("UNIIDividendCoverage"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UNIIDividendCoverage")),
                                    DividendShortfall = (reader.IsDBNull(reader.GetOrdinal("DividendShortfall"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DividendShortfall")),
                                    NetUnrealzedCapGainPctNav = (reader.IsDBNull(reader.GetOrdinal("NetUnrealzedCapGainPctNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NetUnrealzedCapGainPctNav")),
                                    DividendShortfallReportingPeriod = (reader.IsDBNull(reader.GetOrdinal("DividendShortfallReportingPeriod"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DividendShortfallReportingPeriod")),
                                    LifeDividendReserves = (reader.IsDBNull(reader.GetOrdinal("LifeDividendReserves"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LifeDividendReserves")),
                                    CapitalGainUnrealized = (reader.IsDBNull(reader.GetOrdinal("CapitalGainUnrealized"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CapitalGainUnrealized")),
                                    CapitalGainRealized = (reader.IsDBNull(reader.GetOrdinal("CapitalGainRealized"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CapitalGainRealized")),
                                    NetAssetsShareClassMonthly = (reader.IsDBNull(reader.GetOrdinal("NetAssetsShareClassMonthly"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NetAssetsShareClassMonthly")),
                                    GrossAssetsExPar = (reader.IsDBNull(reader.GetOrdinal("GrossAssetsExPar"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("GrossAssetsExPar")),
                                    SharesOutstanding = (reader.IsDBNull(reader.GetOrdinal("SharesOutstanding"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SharesOutstanding")),
                                    TotalLeverageRatioHistorical = (reader.IsDBNull(reader.GetOrdinal("TotalLeverageRatioHistorical"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotalLeverageRatioHistorical"))
                                };

                                data.EarningsProfileDateAsString = DateUtils.ConvertDate(data.EarningsProfileDate, "yyyy-MM-dd");

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
        /// Saves Fund Holdings
        /// </summary>
        /// <param name="fundHoldings"></param>
        /// <param name="flag"></param>
        public void SaveFundHoldings(IList<FundHolding> fundHoldings, string flag)
        {
            StringBuilder sCommand = new StringBuilder("insert into almitasc_ACTradingBBGData.stg_globalcef_port"
                + " (fund_ticker, port_date, fund_name, asset_type, security_sector, security_type, ticker, full_ticker,"
                + " cusip, name, maturity, coupon, position, weight, market_value, price, currency, fxrate,"
                + " fxrate2base, country, broadindustry, industry, subindustry, years_to_maturity, yield_to_worst,"
                + " yield_to_maturity, oas, option_adj_duration, option_adj_convexity, dv01, spread_duration,"
                + " bbgrating, sprating, moodysrating, datafile, username, total_rtn) values ");

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    if (fundHoldings != null && fundHoldings.Count > 0)
                    {
                        string fundTicker = fundHoldings[0].FundTicker;

                        using (MySqlTransaction trans = connection.BeginTransaction())
                        {
                            _logger.LogInformation("Deleting data from almitasc_ACTradingBBGData.stg_globalcef_port");
                            string sqlDelete = "delete from almitasc_ACTradingBBGData.stg_globalcef_port";
                            using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                            {
                                command.ExecuteNonQuery();
                            }

                            List<string> Rows = new List<string>();
                            StringBuilder sb = new StringBuilder();
                            foreach (FundHolding data in fundHoldings)
                            {
                                //Fund Ticker
                                if (!string.IsNullOrEmpty(data.FundTicker))
                                    sb.Append(string.Concat("'", data.FundTicker, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                //Port Date
                                sb.Append(string.Concat("'", data.PortDateAsString, "'")).Append(DELIMITER);

                                //Fund Name
                                if (!string.IsNullOrEmpty(data.FundName))
                                    sb.Append(string.Concat("'", MySqlHelper.EscapeString(data.FundName), "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                //Asset Type
                                if (!string.IsNullOrEmpty(data.AssetType))
                                    sb.Append(string.Concat("'", data.AssetType, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                //Security Sector
                                if (!string.IsNullOrEmpty(data.SecuritySector))
                                    sb.Append(string.Concat("'", data.SecuritySector, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                //Security Type
                                if (!string.IsNullOrEmpty(data.SecurityType))
                                    sb.Append(string.Concat("'", data.SecurityType, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                //Ticker
                                if (!string.IsNullOrEmpty(data.Ticker))
                                    sb.Append(string.Concat("'", data.Ticker, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                //Full Ticker
                                if (!string.IsNullOrEmpty(data.FullTicker))
                                    sb.Append(string.Concat("'", data.FullTicker, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                //Cusip
                                if (!string.IsNullOrEmpty(data.Cusip))
                                    sb.Append(string.Concat("'", data.Cusip, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                //Name
                                if (!string.IsNullOrEmpty(data.SecurityName))
                                    sb.Append(string.Concat("'", MySqlHelper.EscapeString(data.SecurityName), "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                //Maturity Date
                                if (!string.IsNullOrEmpty(data.MaturityDateAsString))
                                    sb.Append(string.Concat("'", data.MaturityDateAsString, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                //Coupon
                                if (data.Coupon.HasValue)
                                    sb.Append(data.Coupon).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                //Position
                                if (data.Position.HasValue)
                                    sb.Append(data.Position).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                //Weight
                                if (data.Weight.HasValue)
                                    sb.Append(data.Weight).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                //Market Value
                                if (data.MV.HasValue)
                                    sb.Append(data.MV).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                //Price
                                if (data.Price.HasValue)
                                    sb.Append(data.Price).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                //Currency
                                if (!string.IsNullOrEmpty(data.Currency))
                                    sb.Append(string.Concat("'", data.Currency, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                //FX Rate
                                if (data.FXRate.HasValue)
                                    sb.Append(data.FXRate).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                //FX Rate Base
                                if (data.FXRateBase.HasValue)
                                    sb.Append(data.FXRateBase).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                //Country
                                if (!string.IsNullOrEmpty(data.Country))
                                    sb.Append(string.Concat("'", data.Country, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                //Broad Industry
                                if (!string.IsNullOrEmpty(data.BroadIndustry))
                                    sb.Append(string.Concat("'", data.BroadIndustry, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                //Industry
                                if (!string.IsNullOrEmpty(data.Industry))
                                    sb.Append(string.Concat("'", data.Industry, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                //Sub Industry
                                if (!string.IsNullOrEmpty(data.SubIndustry))
                                    sb.Append(string.Concat("'", data.SubIndustry, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                //Years To Maturity
                                if (data.YearsToMaturity.HasValue)
                                    sb.Append(data.YearsToMaturity).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                //Yield To Worst
                                if (data.YieldToWorst.HasValue)
                                    sb.Append(data.YieldToWorst).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                //Yield To Maturity
                                if (data.YieldToMaturity.HasValue)
                                    sb.Append(data.YieldToMaturity).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                //OAS
                                if (data.OAS.HasValue)
                                    sb.Append(data.OAS).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                //OAS Duration
                                if (data.OASDuration.HasValue)
                                    sb.Append(data.OASDuration).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                //OAS Convexity
                                if (data.OASConvexity.HasValue)
                                    sb.Append(data.OASConvexity).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                //DV01
                                if (data.DV01.HasValue)
                                    sb.Append(data.DV01).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                //Spread Duration
                                if (data.SpreadDuration.HasValue)
                                    sb.Append(data.SpreadDuration).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                //BBG Rating
                                if (!string.IsNullOrEmpty(data.BBGRating))
                                    sb.Append(string.Concat("'", data.BBGRating, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                //S&P Rating
                                if (!string.IsNullOrEmpty(data.SPRating))
                                    sb.Append(string.Concat("'", data.SPRating, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                //Moodys Rating
                                if (!string.IsNullOrEmpty(data.MoodysRating))
                                    sb.Append(string.Concat("'", data.MoodysRating, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                //Data File
                                if (!string.IsNullOrEmpty(data.DataFile))
                                    sb.Append(string.Concat("'", data.DataFile, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                //User Name
                                if (!string.IsNullOrEmpty(data.UserName))
                                    sb.Append(string.Concat("'", data.UserName, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                //Total Rtn
                                if (data.TotalRtn.HasValue)
                                    sb.Append(data.TotalRtn);
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

                            try
                            {
                                _logger.LogInformation("Moving data to almitasc_ACTradingBBGData.globalcef_port");
                                string sql = "spPopulateFundHoldings";
                                using (MySqlCommand command = new MySqlCommand(sql, connection))
                                {
                                    command.CommandType = System.Data.CommandType.StoredProcedure;
                                    command.ExecuteNonQuery();
                                }
                            }
                            catch (Exception e)
                            {
                                _logger.LogError(e, "Error moving data to target table");
                            }
                            trans.Commit();
                            _logger.LogInformation("Moved data to almitasc_ACTradingBBGData.globalcef_port");
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
        /// Saves User Fund Holdings
        /// </summary>
        /// <param name="fundHoldings"></param>
        /// <param name="flag"></param>
        public void SaveUserFundHoldings(IList<FundHolding> fundHoldings, string flag)
        {
            string sql = "insert into almitasc_ACTradingBBGData.stg_usercef_port"
                + " (fund_ticker, port_date, fund_name, asset_type, security_sector, security_type, ticker,"
                + " cusip, name, maturity, coupon, position, weight, market_value, price, currency, fxrate,"
                + " fxrate2base, country, username, beta, total_rtn) "
                + " values"
                + " (@fund_ticker, @port_date, @fund_name, @asset_type, @security_sector,"
                + " @security_type, @ticker, @cusip, @name, @maturity, @coupon, @position, @weight, @market_value,"
                + " @price, @currency, @fxrate, @fxrate2base, @country, @username, @beta, @total_rtn)";

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    if (fundHoldings != null && fundHoldings.Count > 0)
                    {
                        string fundTicker = fundHoldings[0].FundTicker;

                        using (MySqlTransaction trans = connection.BeginTransaction())
                        {
                            _logger.LogInformation("Deleting data from almitasc_ACTradingBBGData.stg_usercef_port");
                            string sqlDelete = "delete from almitasc_ACTradingBBGData.stg_usercef_port";
                            using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                            {
                                command.ExecuteNonQuery();
                            }

                            if (flag.Equals("Update", StringComparison.CurrentCultureIgnoreCase))
                            {
                                _logger.LogInformation("Deleting data from almitasc_ACTradingBBGData.stg_usercef_port for fund_ticker = '" + fundTicker + "'");
                                sqlDelete = "delete from almitasc_ACTradingBBGData.stg_usercef_port where fund_ticker = '" + fundTicker + "'";
                                using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                                {
                                    command.ExecuteNonQuery();
                                }
                            }

                            using (MySqlCommand command = new MySqlCommand(sql, connection))
                            {
                                command.Parameters.Add(new MySqlParameter("@fund_ticker", MySqlDbType.VarChar));
                                command.Parameters.Add(new MySqlParameter("@port_date", MySqlDbType.Date));
                                command.Parameters.Add(new MySqlParameter("@fund_name", MySqlDbType.VarChar));
                                command.Parameters.Add(new MySqlParameter("@asset_type", MySqlDbType.VarChar));
                                command.Parameters.Add(new MySqlParameter("@security_sector", MySqlDbType.VarChar));
                                command.Parameters.Add(new MySqlParameter("@security_type", MySqlDbType.VarChar));
                                command.Parameters.Add(new MySqlParameter("@ticker", MySqlDbType.VarChar));
                                command.Parameters.Add(new MySqlParameter("@cusip", MySqlDbType.VarChar));
                                command.Parameters.Add(new MySqlParameter("@name", MySqlDbType.VarChar));
                                command.Parameters.Add(new MySqlParameter("@maturity", MySqlDbType.Date));
                                command.Parameters.Add(new MySqlParameter("@coupon", MySqlDbType.Decimal));
                                command.Parameters.Add(new MySqlParameter("@position", MySqlDbType.Decimal));
                                command.Parameters.Add(new MySqlParameter("@weight", MySqlDbType.Decimal));
                                command.Parameters.Add(new MySqlParameter("@market_value", MySqlDbType.Decimal));
                                command.Parameters.Add(new MySqlParameter("@price", MySqlDbType.Decimal));
                                command.Parameters.Add(new MySqlParameter("@currency", MySqlDbType.VarChar));
                                command.Parameters.Add(new MySqlParameter("@fxrate", MySqlDbType.Decimal));
                                command.Parameters.Add(new MySqlParameter("@fxrate2base", MySqlDbType.Decimal));
                                command.Parameters.Add(new MySqlParameter("@country", MySqlDbType.VarChar));
                                command.Parameters.Add(new MySqlParameter("@UserName", MySqlDbType.VarChar));
                                command.Parameters.Add(new MySqlParameter("@beta", MySqlDbType.Decimal));
                                command.Parameters.Add(new MySqlParameter("@total_rtn", MySqlDbType.Decimal));

                                foreach (FundHolding data in fundHoldings)
                                {
                                    data.PortDate = DateUtils.ConvertToDate(data.PortDateAsString, "yyyy-MM-dd");
                                    data.MaturityDate = DateUtils.ConvertToDate(data.MaturityDateAsString, "yyyy-MM-dd");

                                    command.Parameters[0].Value = data.FundTicker;
                                    command.Parameters[1].Value = data.PortDate;
                                    command.Parameters[2].Value = data.FundName;
                                    command.Parameters[3].Value = data.AssetType;
                                    command.Parameters[4].Value = data.SecuritySector;
                                    command.Parameters[5].Value = data.SecurityType;
                                    command.Parameters[6].Value = data.Ticker;
                                    command.Parameters[7].Value = data.Cusip;
                                    command.Parameters[8].Value = data.SecurityName;
                                    command.Parameters[9].Value = data.MaturityDate;
                                    command.Parameters[10].Value = data.Coupon;
                                    command.Parameters[11].Value = data.Position;
                                    command.Parameters[12].Value = data.Weight;
                                    command.Parameters[13].Value = data.MV;
                                    command.Parameters[14].Value = data.Price;
                                    command.Parameters[15].Value = data.Currency;
                                    command.Parameters[16].Value = data.FXRate;
                                    command.Parameters[17].Value = data.FXRateBase;
                                    command.Parameters[18].Value = data.Country;
                                    command.Parameters[19].Value = data.UserName;
                                    command.Parameters[20].Value = data.Beta;
                                    command.Parameters[21].Value = data.TotalRtn;

                                    command.ExecuteNonQuery();
                                }
                            }

                            _logger.LogInformation("Moving data to almitasc_ACTradingBBGData.usercef_port");
                            sql = "spPopulateUserFundHoldings";
                            using (MySqlCommand command = new MySqlCommand(sql, connection))
                            {
                                command.CommandType = System.Data.CommandType.StoredProcedure;
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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ticker"></param>
        /// <param name="startDate"></param>
        /// <param name="endDate"></param>
        /// <returns></returns>
        public IList<FundHistory> GetFundHistory(string ticker, string startDate, string endDate)
        {
            IList<FundHistory> list = new List<FundHistory>();

            try
            {
                string sql = GetFundHistoryQuery + " where 1=1";

                if (!string.IsNullOrEmpty(ticker))
                    sql += " and ticker = '" + ticker + "'";

                if (!string.IsNullOrEmpty(startDate))
                    sql += " and date >= '" + startDate + "'";

                if (!string.IsNullOrEmpty(endDate))
                    sql += " and date <= '" + endDate + "'";

                sql += " order by date desc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundHistory data = new FundHistory
                                {
                                    Ticker = reader["ticker"] as string,
                                    AlmitasId = reader["almitasid"] as string,
                                    EffectiveDate = reader.IsDBNull(reader.GetOrdinal("date")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("date")),
                                    ClosingPrice = (reader.IsDBNull(reader.GetOrdinal("priceclose"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("priceclose")),
                                    VWAPPrice = (reader.IsDBNull(reader.GetOrdinal("pricevwap"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("pricevwap")),
                                    Nav = (reader.IsDBNull(reader.GetOrdinal("nav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("nav")),
                                    PD = (reader.IsDBNull(reader.GetOrdinal("pd"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("pd")),
                                    CashDividend = (reader.IsDBNull(reader.GetOrdinal("cashdividend"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("cashdividend")),
                                    StockDividend = (reader.IsDBNull(reader.GetOrdinal("stockdividend"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("stockdividend")),
                                    StockSplit = (reader.IsDBNull(reader.GetOrdinal("stocksplit"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("stocksplit")),
                                    CorpAction = reader["corpaction"] as string,
                                    PriceReturn = (reader.IsDBNull(reader.GetOrdinal("trrpx"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("trrpx")),

                                    RightsDilutionFactor = (reader.IsDBNull(reader.GetOrdinal("rightsdilutionfactor"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("rightsdilutionfactor")),
                                    DividendIndicative = (reader.IsDBNull(reader.GetOrdinal("dividendindicative"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("dividendindicative")),
                                    DividendTrailing = (reader.IsDBNull(reader.GetOrdinal("dividendtrailing"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("dividendtrailing")),
                                    Volume = (reader.IsDBNull(reader.GetOrdinal("volume"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("volume")),
                                    PDInterpolated = (reader.IsDBNull(reader.GetOrdinal("pdinterpolated"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("pdinterpolated")),
                                    NavInterpolated = (reader.IsDBNull(reader.GetOrdinal("navinterpolated"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("navinterpolated")),
                                    PDWInterpolated = (reader.IsDBNull(reader.GetOrdinal("pdwinterpolated"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("pdwinterpolated")),

                                    PriceReturnCalculated = (reader.IsDBNull(reader.GetOrdinal("trrpxc"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("trrpxc")),
                                    VWAPPriceReturn = (reader.IsDBNull(reader.GetOrdinal("trrpxw"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("trrpxw")),
                                    NavReturn = (reader.IsDBNull(reader.GetOrdinal("trrnav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("trrnav")),
                                    DividendReturn = (reader.IsDBNull(reader.GetOrdinal("trrdvd"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("trrdvd")),
                                    DiscountReturn = (reader.IsDBNull(reader.GetOrdinal("trrpd"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("trrpd")),
                                    CrossReturn = (reader.IsDBNull(reader.GetOrdinal("trrcross"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("trrcross")),

                                    DividendChange = (reader.IsDBNull(reader.GetOrdinal("dvdchg"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("dvdchg")),
                                    DividendNavYieldChange = (reader.IsDBNull(reader.GetOrdinal("dvdnavyldchg"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("dvdnavyldchg")),
                                    DividendPriceYieldChange = (reader.IsDBNull(reader.GetOrdinal("dvdpxyldchg"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("dvdpxyldchg"))
                                };

                                data.EffectiveDateAsString = DateUtils.ConvertDate(data.EffectiveDate, "yyyy-MM-dd");

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
        /// <param name="figi"></param>
        /// <returns></returns>
        public IList<FundHistory> GetFundHistory(string figi)
        {
            IList<FundHistory> list = new List<FundHistory>();

            try
            {
                string sql = GetFundHistoryStatQuery;

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_FIGI", figi);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundHistory data = new FundHistory
                                {
                                    FIGI = reader["FIGI"] as string,
                                    Ticker = reader["Ticker"] as string,
                                    EffectiveDate = reader.IsDBNull(reader.GetOrdinal("EffDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffDate")),
                                    BaseMktCap = (reader.IsDBNull(reader.GetOrdinal("BaseMktCap"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BaseMktCap")),
                                    PDInterpolated = (reader.IsDBNull(reader.GetOrdinal("PDInterpolated"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PDInterpolated"))
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
        /// <param name="assetClass"></param>
        /// <param name="fundGroupStatsList"></param>
        public void SaveSectorDiscounts(string assetClass, IList<FundGroupStats> fundGroupStatsList)
        {
            string sql = "insert into almitasc_ACTradingBBGData.FundGroupStats (FundGroup, EffectiveDate, AvgDiscount, CapWtdDiscount) "
                + " values (@FundGroup, @EffectiveDate, @AvgDiscount, @CapWtdDiscount)";

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    if (fundGroupStatsList != null && fundGroupStatsList.Count > 0)
                    {
                        using (MySqlTransaction trans = connection.BeginTransaction())
                        {
                            _logger.LogInformation("Deleting data from almitasc_ACTradingBBGData.FundGroupStats for FundGroup = '" + assetClass + "'");
                            string sqlDelete = "delete from almitasc_ACTradingBBGData.FundGroupStats where FundGroup = '" + assetClass + "'";
                            using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                            {
                                command.ExecuteNonQuery();
                            }

                            _logger.LogInformation("Saving data to almitasc_ACTradingBBGData.FundGroupStats for UserName = '" + assetClass + "'");
                            using (MySqlCommand command = new MySqlCommand(sql, connection))
                            {
                                command.Parameters.Add(new MySqlParameter("@FundGroup", MySqlDbType.VarChar));
                                command.Parameters.Add(new MySqlParameter("@EffectiveDate", MySqlDbType.Date));
                                command.Parameters.Add(new MySqlParameter("@AvgDiscount", MySqlDbType.Decimal));
                                command.Parameters.Add(new MySqlParameter("@CapWtdDiscount", MySqlDbType.Decimal));

                                foreach (FundGroupStats data in fundGroupStatsList)
                                {
                                    command.Parameters[0].Value = data.FundGroup;
                                    command.Parameters[1].Value = data.EffectiveDate;
                                    command.Parameters[2].Value = data.AvgDiscount;
                                    command.Parameters[3].Value = data.CapWtdDiscount;

                                    command.ExecuteNonQuery();
                                }
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
        /// 
        /// </summary>
        /// <param name="assetClass"></param>
        /// <param name="fundList"></param>
        public void SaveSectorFundMap(string assetClass, HashSet<string> fundList)
        {
            string sql = "insert into almitasc_ACTradingBBGData.FundGroupList (FundGroup, FIGI) values (@FundGroup, @FIGI)";

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    if (fundList != null && fundList.Count > 0)
                    {
                        using (MySqlTransaction trans = connection.BeginTransaction())
                        {
                            _logger.LogInformation("Deleting data from almitasc_ACTradingBBGData.FundGroupList for FundGroup = '" + assetClass + "'");
                            string sqlDelete = "delete from almitasc_ACTradingBBGData.FundGroupList where FundGroup = '" + assetClass + "'";
                            using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                            {
                                command.ExecuteNonQuery();
                            }

                            _logger.LogInformation("Saving data to almitasc_ACTradingBBGData.FundGroupList for FundGroup = '" + assetClass + "'");
                            using (MySqlCommand command = new MySqlCommand(sql, connection))
                            {
                                command.Parameters.Add(new MySqlParameter("@FundGroup", MySqlDbType.VarChar));
                                command.Parameters.Add(new MySqlParameter("@FIGI", MySqlDbType.VarChar));

                                foreach (string figi in fundList)
                                {
                                    command.Parameters[0].Value = assetClass;
                                    command.Parameters[1].Value = figi;

                                    command.ExecuteNonQuery();
                                }
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
                        _logger.LogInformation("Truncating table.... " + tableName);
                        command.ExecuteNonQuery();
                        _logger.LogInformation("Truncated table.... " + tableName);
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
        /// <param name="filePath"></param>
        /// <param name="tableName"></param>
        public void Save(string filePath, string tableName)
        {
            try
            {
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
                        Columns = { "FundGroup", "EffectiveDate", "AvgDiscount", "CapWtdDiscount" }
                    };

                    int numberOfInsertedRows = bl.Load();
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving data into database");
                throw;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="country"></param>
        /// <param name="securityType"></param>
        /// <param name="cefInstrumentType"></param>
        /// <param name="sector"></param>
        /// <param name="fundCategory"></param>
        /// <param name="startDate"></param>
        /// <param name="endDate"></param>
        /// <returns></returns>
        public IList<FundNavHistory> GetSectorHistory(string country, string securityType, string cefInstrumentType, string sector, string fundCategory, DateTime startDate, DateTime endDate)
        {
            IList<FundNavHistory> list = new List<FundNavHistory>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetSectorHistoryQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Country", country);
                        command.Parameters.AddWithValue("p_SecurityType", securityType);
                        command.Parameters.AddWithValue("p_CEFInstrumentType", cefInstrumentType);
                        command.Parameters.AddWithValue("p_Sector", sector);
                        command.Parameters.AddWithValue("p_FundCategory", fundCategory);
                        command.Parameters.AddWithValue("p_StartDate", startDate);
                        command.Parameters.AddWithValue("p_EndDate", endDate);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundNavHistory data = new FundNavHistory
                                {
                                    Country = reader["Country"] as string,
                                    SecurityType = reader["SecurityType"] as string,
                                    CEFInstrumentType = reader["CEFInstrumentType"] as string,
                                    Sector = reader["FundGroup"] as string,
                                    AvgPD = (reader.IsDBNull(reader.GetOrdinal("AvgDiscount"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AvgDiscount")),
                                    CapWtdPD = (reader.IsDBNull(reader.GetOrdinal("CapWtdDiscount"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CapWtdDiscount")),
                                    EffectiveDate = reader.IsDBNull(reader.GetOrdinal("EffectiveDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate"))
                                };

                                data.EffectiveDateAsString = DateUtils.ConvertDate(data.EffectiveDate, "yyyy-MM-dd");

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
        public IDictionary<string, FundMaster> GetSecurityMaster()
        {
            IDictionary<string, FundMaster> securityMasterDict = new Dictionary<string, FundMaster>(StringComparer.CurrentCultureIgnoreCase);

            string ticker = string.Empty;

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetFullSecurityMasterQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                ticker = reader["FIGI"] as string;
                                FundMaster fundMaster = new FundMaster
                                {
                                    FIGI = reader["FIGI"] as string,
                                    Ticker = reader["Ticker"] as string,
                                    YKey = reader["YellowKey"] as string,
                                    Name = reader["SecurityDescription"] as string,
                                    Curr = reader["Currency"] as string,
                                    Cntry = reader["Country"] as string,
                                    GeoLvl1 = reader["GeoLevel1"] as string,
                                    GeoLvl2 = reader["GeoLevel2"] as string,
                                    GeoLvl3 = reader["GeoLevel3"] as string,
                                    AssetLvl1 = reader["AssetClassLevel1"] as string,
                                    AssetLvl2 = reader["AssetClassLevel2"] as string,
                                    AssetLvl3 = reader["AssetClassLevel3"] as string,
                                    Status = reader["MarketStatus"] as string,
                                    PayRank = reader["PaymentRank"] as string,
                                    AssetTyp = reader["AssetType"] as string,
                                    SecTyp = reader["SecurityType"] as string,
                                    CntryCd = reader["CountryCode"] as string,
                                    CEFTyp = reader["CefInstrumentType"] as string,
                                };

                                if (!securityMasterDict.ContainsKey(fundMaster.FIGI))
                                    securityMasterDict.Add(fundMaster.FIGI, fundMaster);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing query, ticker: " + ticker);
                throw;
            }

            return securityMasterDict;
        }

        /// <summary>
        /// Gets broker trading volume (1 week and 6 months)
        /// 
        /// </summary>
        /// <returns></returns>
        public IDictionary<string, BrokerTradingVolume> GetBrokerTradingVolume()
        {
            IDictionary<string, BrokerTradingVolume> dict = new Dictionary<string, BrokerTradingVolume>(StringComparer.CurrentCultureIgnoreCase);

            string ticker = string.Empty;

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetBrokerTradingVolumeQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                ticker = reader["Ticker"] as string;

                                BrokerTradingVolume data = new BrokerTradingVolume
                                {
                                    Ticker = reader["Ticker"] as string,
                                    Trailing1DayBroker1 = reader["T1DayBroker1"] as string,
                                    Trailing1DayBroker2 = reader["T1DayBroker2"] as string,
                                    Trailing1WeekBroker1 = reader["T1WeekBroker1"] as string,
                                    Trailing1WeekBroker2 = reader["T1WeekBroker2"] as string,
                                    Trailing6MonthsBroker1 = reader["T6MonthsBroker1"] as string,
                                    Trailing6MonthsBroker2 = reader["T6MonthsBroker2"] as string,
                                    Trailing1DayBroker1Vol = (reader.IsDBNull(reader.GetOrdinal("T1DayBroker1Vol"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("T1DayBroker1Vol")),
                                    Trailing1DayBroker2Vol = (reader.IsDBNull(reader.GetOrdinal("T1DayBroker2Vol"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("T1DayBroker2Vol")),
                                    Trailing1WeekBroker1Vol = (reader.IsDBNull(reader.GetOrdinal("T1WeekBroker1Vol"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("T1WeekBroker1Vol")),
                                    Trailing1WeekBroker2Vol = (reader.IsDBNull(reader.GetOrdinal("T1WeekBroker2Vol"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("T1WeekBroker2Vol")),
                                    Trailing6MonthsBroker1Vol = (reader.IsDBNull(reader.GetOrdinal("T6MonthsBroker1Vol"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("T6MonthsBroker1Vol")),
                                    Trailing6MonthsBroker2Vol = (reader.IsDBNull(reader.GetOrdinal("T6MonthsBroker2Vol"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("T6MonthsBroker2Vol")),
                                };

                                if (!dict.TryGetValue(ticker, out BrokerTradingVolume brokerTradingVolumeLookup))
                                    dict.Add(data.Ticker, data);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing query, ticker: " + ticker);
                throw;
            }

            return dict;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IDictionary<string, SecurityMaster> GetPositionSecurityDetails()
        {
            IDictionary<string, SecurityMaster> securityMasterDict = new Dictionary<string, SecurityMaster>(StringComparer.CurrentCultureIgnoreCase);

            string ticker = string.Empty;

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetPositionSecurityDetailsQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                SecurityMaster data = new SecurityMaster();
                                ticker = reader["Ticker"] as string;
                                data.Ticker = reader["Ticker"] as string;
                                data.YellowKey = reader["YellowKey"] as string;
                                data.Currency = reader["Currency"] as string;
                                data.SecurityDescription = reader["SecurityName"] as string;
                                data.Security13FFlag = reader["Security13FFlag"] as string;

                                if (!securityMasterDict.TryGetValue(ticker, out SecurityMaster securityMasterLookup))
                                    securityMasterDict.Add(ticker, data);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing query, ticker: " + ticker);
                throw;
            }

            return securityMasterDict;
        }

        /// <summary>
        /// Save Security Master Details
        /// </summary>
        /// <param name="securityMasterList"></param>
        public void SaveSecurityMasterDetails(IList<SecurityMaster> securityMasterList)
        {
            _logger.LogInformation("Saving Security Master Updates...");

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlTransaction trans = connection.BeginTransaction())
                    {
                        using (MySqlCommand command = new MySqlCommand(SaveSecurityMasterDetailsQuery, connection))
                        {
                            command.CommandType = System.Data.CommandType.StoredProcedure;
                            command.Parameters.Add(new MySqlParameter("p_AlmitasId", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_FIGI", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_SecId", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_ISIN", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_Ticker", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_YellowKey", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_SecurityDescription", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_Country", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_Currency", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_SecurityType", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_CefInstrumentType", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_PaymentRank", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_GeoLevel1", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_GeoLevel2", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_GeoLevel3", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_AssetClassLevel1", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_AssetClassLevel2", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_AssetClassLevel3", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_InceptionDate", MySqlDbType.Date));
                            command.Parameters.Add(new MySqlParameter("p_TerminationDate", MySqlDbType.Date));
                            command.Parameters.Add(new MySqlParameter("p_ExpenseRatio", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_Leverage", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_MarketStatus", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_TradingStatus", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_ParentCompany", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_Sec1940Act", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_FundCategory", MySqlDbType.VarChar));

                            foreach (SecurityMaster data in securityMasterList)
                            {
                                _logger.LogInformation("Saving FIGI: " + data.FIGI);

                                if (!string.IsNullOrEmpty(data.InceptionDateAsString))
                                    data.InceptionDate = DateUtils.ConvertToDate(data.InceptionDateAsString, "yyyy-MM-dd");

                                if (!string.IsNullOrEmpty(data.TerminationDateAsString))
                                    data.TerminationDate = DateUtils.ConvertToDate(data.TerminationDateAsString, "yyyy-MM-dd");

                                command.Parameters[0].Value = data.AlmitasId;
                                command.Parameters[1].Value = data.FIGI;
                                command.Parameters[2].Value = data.SecId;
                                command.Parameters[3].Value = data.ISIN;
                                command.Parameters[4].Value = data.Ticker;
                                command.Parameters[5].Value = data.YellowKey;
                                command.Parameters[6].Value = data.SecurityDescription;
                                command.Parameters[7].Value = data.Country;
                                command.Parameters[8].Value = data.Currency;
                                command.Parameters[9].Value = data.SecurityType;
                                command.Parameters[10].Value = data.CEFInstrumentType;
                                command.Parameters[11].Value = data.PaymentRank;
                                command.Parameters[12].Value = data.GeoLevel1;
                                command.Parameters[13].Value = data.GeoLevel2;
                                command.Parameters[14].Value = data.GeoLevel3;
                                command.Parameters[15].Value = data.AssetClassLevel1;
                                command.Parameters[16].Value = data.AssetClassLevel2;
                                command.Parameters[17].Value = data.AssetClassLevel3;
                                command.Parameters[18].Value = data.InceptionDate;
                                command.Parameters[19].Value = data.TerminationDate;
                                command.Parameters[20].Value = data.ExpenseRatio;
                                command.Parameters[21].Value = data.Leverage;
                                command.Parameters[22].Value = data.MarketStatus;
                                command.Parameters[23].Value = data.TradingStatus;
                                command.Parameters[24].Value = data.ParentCompany;
                                command.Parameters[25].Value = data.Security1940Act;
                                command.Parameters[26].Value = data.FundCategory;

                                command.ExecuteNonQuery();
                            }
                        }
                        trans.Commit();
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving data");
            }

            _logger.LogInformation("Saving Security Master Updates... - DONE");
        }

        /// <summary>
        /// Get Security Performance Details
        /// </summary>
        /// <returns></returns>
        public IDictionary<string, SecurityPerformance> GetSecurityPerformance()
        {
            IDictionary<string, SecurityPerformance> dict = new Dictionary<string, SecurityPerformance>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetSectorPerformanceQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                SecurityPerformance data = new SecurityPerformance
                                {
                                    FundType = reader["FundType"] as string,
                                    EffectiveDate = reader.IsDBNull(reader.GetOrdinal("EffectiveDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate")),

                                    Ticker = reader["Ticker"] as string,
                                    DatePeriod = reader["DatePeriod"] as string,
                                    Position = (reader.IsDBNull(reader.GetOrdinal("Position"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Position")),
                                    BegMV = (reader.IsDBNull(reader.GetOrdinal("BegMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BegMV")),
                                    EndMV = (reader.IsDBNull(reader.GetOrdinal("EndMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EndMV")),
                                    PnL = (reader.IsDBNull(reader.GetOrdinal("PnL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PnL")),
                                    TradingPnL = (reader.IsDBNull(reader.GetOrdinal("TradePnL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TradePnL")),
                                    DailyPnL = (reader.IsDBNull(reader.GetOrdinal("DailyPnL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DailyPnL"))
                                };

                                data.EffectiveDateAsString = DateUtils.ConvertDate(data.EffectiveDate, "yyyy-MM-dd");

                                string id = data.FundType + "|" + data.Ticker;
                                data.Id = id;
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

        /// <summary>
        /// Get Security Performance
        /// </summary>
        /// <param name="startDate"></param>
        /// <param name="endDate"></param>
        /// <returns></returns>
        public IDictionary<string, SecurityPerformance> GetSecurityPerformance(DateTime startDate, DateTime endDate)
        {
            IDictionary<string, SecurityPerformance> dict = new Dictionary<string, SecurityPerformance>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetSectorPerformanceByDateRangeQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_StartDate", startDate);
                        command.Parameters.AddWithValue("p_EndDate", endDate);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                SecurityPerformance data = new SecurityPerformance
                                {
                                    Ticker = reader["Ticker"] as string,
                                    YellowKey = reader["YellowKey"] as string,
                                    Currency = reader["Currency"] as string,
                                    Sector = reader["Sector"] as string,
                                    BroadSector = reader["BroadSector"] as string,
                                    SecurityType = reader["SecType"] as string,
                                    RiskFactor = reader["RiskFactor"] as string,

                                    MinPositionDateOpp = reader.IsDBNull(reader.GetOrdinal("MinPosDateOpp")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("MinPosDateOpp")),
                                    MaxPositionDateOpp = reader.IsDBNull(reader.GetOrdinal("MaxPosDateOpp")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("MaxPosDateOpp")),
                                    MinPositionDateTac = reader.IsDBNull(reader.GetOrdinal("MinPosDateTac")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("MinPosDateTac")),
                                    MaxPositionDateTac = reader.IsDBNull(reader.GetOrdinal("MaxPosDateTac")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("MaxPosDateTac")),

                                    PnLOpp = (reader.IsDBNull(reader.GetOrdinal("PnLOpp"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PnLOpp")),
                                    TradingPnLOpp = (reader.IsDBNull(reader.GetOrdinal("TradePnLOpp"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TradePnLOpp")),
                                    DailyPnLOpp = (reader.IsDBNull(reader.GetOrdinal("DailyPnLOpp"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DailyPnLOpp")),

                                    PnLTac = (reader.IsDBNull(reader.GetOrdinal("PnLTac"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PnLTac")),
                                    TradingPnLTac = (reader.IsDBNull(reader.GetOrdinal("TradePnLTac"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TradePnLTac")),
                                    DailyPnLTac = (reader.IsDBNull(reader.GetOrdinal("DailyPnLTac"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DailyPnLTac"))
                                };

                                data.EffectiveDateAsString = DateUtils.ConvertDate(data.EffectiveDate, "yyyy-MM-dd");
                                data.MinPositionDateOppAsString = DateUtils.ConvertDate(data.MinPositionDateOpp, "yyyy-MM-dd");
                                data.MaxPositionDateOppAsString = DateUtils.ConvertDate(data.MaxPositionDateOpp, "yyyy-MM-dd");
                                data.MinPositionDateTacAsString = DateUtils.ConvertDate(data.MinPositionDateTac, "yyyy-MM-dd");
                                data.MaxPositionDateTacAsString = DateUtils.ConvertDate(data.MaxPositionDateTac, "yyyy-MM-dd");

                                string id = data.FundType + "|" + data.Ticker;
                                data.Id = id;
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

        /// <summary>
        /// Gets Preferred Common Shares Map
        /// </summary>
        /// <returns></returns>
        public IDictionary<string, string> GetPfdCommonSharesMap()
        {
            IDictionary<string, string> dict = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetPfdCommonSharesMapQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                string pfdTicker = reader["PfdTicker"] as string;
                                string commonShareTicker = reader["CommonShareTicker"] as string;

                                if (!dict.ContainsKey(pfdTicker))
                                    dict.Add(pfdTicker, commonShareTicker);
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
        /// Gets historical BDC returns (yearly returns and multi period returns)
        /// </summary>
        /// <returns></returns>
        public IDictionary<string, FundReturn> GetBDCFundReturns()
        {
            IDictionary<string, FundReturn> dict = new Dictionary<string, FundReturn>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetBDCFundReturnsQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundReturn data = new FundReturn
                                {
                                    FIGI = reader["Figi"] as string,
                                    Ticker = reader["Ticker"] as string,
                                    ReturnType = reader["RtnType"] as string,
                                    ReturnPeriod = reader["RtnPeriod"] as string,
                                    Return = (reader.IsDBNull(reader.GetOrdinal("Rtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Rtn"))
                                };

                                data.Id = data.Ticker + "|" + data.ReturnType + "|" + data.ReturnPeriod;
                                if (!dict.ContainsKey(data.Id))
                                    dict.Add(data.Id, data);
                                else
                                    _logger.LogInformation("Key already exists: " + data.Id);
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
        /// Gets Watchlist Securities
        /// </summary>
        /// <returns></returns>
        public IDictionary<string, SecurityMaster> GetWatchlistSecurities()
        {
            IDictionary<string, SecurityMaster> securityMasterDict = new Dictionary<string, SecurityMaster>(StringComparer.CurrentCultureIgnoreCase);

            string ticker = string.Empty;

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetWatchlistSecuritiesQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                ticker = reader["Ticker"] as string;
                                SecurityMaster data = new SecurityMaster
                                {
                                    Ticker = reader["Ticker"] as string,
                                    FundCategory = reader["FundCategory"] as string,
                                };

                                if (!securityMasterDict.TryGetValue(ticker, out SecurityMaster securityMasterLookup))
                                    securityMasterDict.Add(ticker, data);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing query, ticker: " + ticker);
                throw;
            }

            return securityMasterDict;
        }

        /// <summary>
        /// Gets Trading Targets
        /// </summary>
        /// <returns></returns>
        public IDictionary<string, TradingTarget> GetTradingTargets()
        {
            IDictionary<string, TradingTarget> dict = new Dictionary<string, TradingTarget>(StringComparer.CurrentCultureIgnoreCase);

            string ticker = string.Empty;

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetTradingTargetQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                ticker = reader["Ticker"] as string;

                                TradingTarget data = new TradingTarget
                                {
                                    Ticker = reader["Ticker"] as string,
                                    FundCategory = reader["FundCategory"] as string,
                                    TransactionType = reader["TransactionType"] as string,
                                    JoinCondition = reader["JoinCondition"] as string,
                                    NavType = reader["NavType"] as string,
                                    PriceTarget = (reader.IsDBNull(reader.GetOrdinal("PriceTarget"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceTarget")),
                                    DiscountTarget = (reader.IsDBNull(reader.GetOrdinal("DiscountTarget"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DiscountTarget")),
                                    IRRTarget = (reader.IsDBNull(reader.GetOrdinal("IRRTarget"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IRRTarget")),
                                    NavOverride = (reader.IsDBNull(reader.GetOrdinal("NavOverride"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NavOverride")),
                                    RefIndex = reader["RefIndex"] as string,
                                    PriceBeta = (reader.IsDBNull(reader.GetOrdinal("PriceBeta"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceBeta")),
                                    PriceBetaShiftInd = (reader.IsDBNull(reader.GetOrdinal("PriceBetaShiftInd"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("PriceBetaShiftInd")),
                                    PriceCap = (reader.IsDBNull(reader.GetOrdinal("PriceCap"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceCap")),
                                    PriceCapShiftInd = (reader.IsDBNull(reader.GetOrdinal("PriceCapShiftInd"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("PriceCapShiftInd")),
                                };

                                if (!dict.TryGetValue(ticker, out TradingTarget tradingTargetTemp))
                                    dict.Add(ticker, data);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing query, ticker: " + ticker);
                throw;
            }

            return dict;
        }

        /// <summary>
        /// Save Trading Targets
        /// </summary>
        /// <param name="tradingTargetList"></param>
        public void SaveTradingTargets(IList<TradingTarget> tradingTargetList)
        {
            _logger.LogInformation("Saving Trading Target Updates...");

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlTransaction trans = connection.BeginTransaction())
                    {
                        using (MySqlCommand command = new MySqlCommand(SaveTradingTargetQuery, connection))
                        {
                            command.CommandType = System.Data.CommandType.StoredProcedure;
                            command.Parameters.Add(new MySqlParameter("p_Ticker", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_TransactionType", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_JoinCondition", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_PriceTarget", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_DiscountTarget", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_IRRTarget", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_NavType", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_NavOverride", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_RefIndex", MySqlDbType.VarChar));
                            command.Parameters.Add(new MySqlParameter("p_PriceBeta", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_PriceBetaShiftInd", MySqlDbType.Int32));
                            command.Parameters.Add(new MySqlParameter("p_PriceCap", MySqlDbType.Decimal));
                            command.Parameters.Add(new MySqlParameter("p_PriceCapShiftInd", MySqlDbType.Int32));

                            foreach (TradingTarget data in tradingTargetList)
                            {
                                _logger.LogInformation("Saving Ticker: " + data.Ticker);

                                command.Parameters[0].Value = data.Ticker;
                                command.Parameters[1].Value = data.TransactionType;
                                command.Parameters[2].Value = data.JoinCondition;
                                command.Parameters[3].Value = data.PriceTarget;
                                command.Parameters[4].Value = data.DiscountTarget;
                                command.Parameters[5].Value = data.IRRTarget;
                                command.Parameters[6].Value = data.NavType;
                                command.Parameters[7].Value = data.NavOverride;
                                command.Parameters[8].Value = data.RefIndex;
                                command.Parameters[9].Value = data.PriceBeta;
                                command.Parameters[10].Value = data.PriceBetaShiftInd;
                                command.Parameters[11].Value = data.PriceCap;
                                command.Parameters[12].Value = data.PriceCapShiftInd;

                                command.ExecuteNonQuery();
                            }
                        }
                        trans.Commit();
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving data");
            }

            _logger.LogInformation("Saving Trading Target Updates... - DONE");
        }

        /// <summary>
        /// Gets expected alpha model params (NEW model)
        /// </summary>
        /// <returns></returns>
        public IDictionary<string, ExpectedAlphaModelParams> GetExpectedAlphaModelParams()
        {
            IDictionary<string, ExpectedAlphaModelParams> dict = new Dictionary<string, ExpectedAlphaModelParams>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetExpectedAlphaModelParamsQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                ExpectedAlphaModelParams data = new ExpectedAlphaModelParams
                                {
                                    SecurityType = reader["SecurityType"] as string,
                                    Country = reader["Country"] as string,
                                    DiscountConvergenceMultiplier = (reader.IsDBNull(reader.GetOrdinal("DiscountConvergenceMultiplier"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DiscountConvergenceMultiplier")),
                                    SecurityDScoreMultiplier = (reader.IsDBNull(reader.GetOrdinal("SecurityDScoreMultiplier"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SecurityDScoreMultiplier")),
                                    FundCategoryDScoreMultiplier = (reader.IsDBNull(reader.GetOrdinal("FundCategoryDScoreMultiplier"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FundCategoryDScoreMultiplier")),
                                    IRRMultiplier = (reader.IsDBNull(reader.GetOrdinal("IRRMultiplier"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("IRRMultiplier")),
                                    ActivistScoreMultiplier = (reader.IsDBNull(reader.GetOrdinal("ActivistScoreMultiplier"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ActivistScoreMultiplier")),
                                    MajorityVotingHaircut = (reader.IsDBNull(reader.GetOrdinal("MajorityVotingHaircut"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MajorityVotingHaircut")),
                                    MajorityVotingHaircutMin = (reader.IsDBNull(reader.GetOrdinal("MajorityVotingHaircutMin"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MajorityVotingHaircutMin")),
                                    BoardTermAdjMultiplier = (reader.IsDBNull(reader.GetOrdinal("BoardTermAdjMultiplier"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BoardTermAdjMultiplier")),
                                    BoardTermAdjMin = (reader.IsDBNull(reader.GetOrdinal("BoardTermAdjMin"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BoardTermAdjMin")),
                                    ExpenseDragMultiplier = (reader.IsDBNull(reader.GetOrdinal("ExpenseDragMultiplier"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ExpenseDragMultiplier"))
                                };

                                string key = data.SecurityType + "|" + data.Country;

                                if (!dict.TryGetValue(key, out ExpectedAlphaModelParams expectedAlphaModelParamsLookup))
                                    dict.Add(key, data);
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
        /// Gets Fund Historical Data
        /// </summary>
        /// <param name="ticker"></param>
        /// <param name="startDate"></param>
        /// <param name="endDate"></param>
        /// <param name="hasNav"></param>
        /// <returns></returns>
        public IList<FundNavHistoryNew> GetFundHistoricalData(string ticker, DateTime startDate, DateTime endDate, string hasNav)
        {
            IList<FundNavHistoryNew> list = new List<FundNavHistoryNew>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetFundHistoricalDataQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Ticker", ticker);
                        command.Parameters.AddWithValue("p_MeasureType", null);
                        command.Parameters.AddWithValue("p_StartDate", startDate);
                        command.Parameters.AddWithValue("p_EndDate", endDate);
                        command.Parameters.AddWithValue("p_HasNav", hasNav);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundNavHistoryNew data = new FundNavHistoryNew
                                {
                                    Ticker = reader["Ticker"] as string,
                                    EffectiveDate = reader.IsDBNull(reader.GetOrdinal("EffectiveDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate")),
                                    ALMNav = (reader.IsDBNull(reader.GetOrdinal("ALMNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ALMNav")),
                                    ALMNavInterpolated = (reader.IsDBNull(reader.GetOrdinal("ALMNavInterpolated"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ALMNavInterpolated")),
                                    ALMPrice = (reader.IsDBNull(reader.GetOrdinal("ALMPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ALMPrice")),
                                    ALMPD = (reader.IsDBNull(reader.GetOrdinal("ALMPD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ALMPD")),
                                    ALMPDInterpolated = (reader.IsDBNull(reader.GetOrdinal("ALMPDInterpolated"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ALMPDInterpolated")),
                                    ALMDvdAmount = (reader.IsDBNull(reader.GetOrdinal("ALMDvdAmt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ALMDvdAmt")),
                                    EstNav = (reader.IsDBNull(reader.GetOrdinal("EstNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EstNav")),
                                    EstPD = (reader.IsDBNull(reader.GetOrdinal("EstPD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EstPD")),
                                    PriceRtn = (reader.IsDBNull(reader.GetOrdinal("PriceRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceRtn")),
                                    NavRtn = (reader.IsDBNull(reader.GetOrdinal("NavRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("NavRtn")),
                                    CorpAction = reader["CorpAction"] as string,
                                };

                                data.EffectiveDateAsString = DateUtils.ConvertDate(data.EffectiveDate, "yyyy-MM-dd");
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
        /// Gets Sector Historical Data
        /// </summary>
        /// <param name="country"></param>
        /// <param name="securityType"></param>
        /// <param name="sector"></param>
        /// <param name="fundCategory"></param>
        /// <param name="startDate"></param>
        /// <param name="endDate"></param>
        /// <returns></returns>
        public IList<FundNavHistoryNew> GetSectorHistoricalData(string country, string securityType, string sector, string fundCategory, DateTime startDate, DateTime endDate)
        {
            IList<FundNavHistoryNew> list = new List<FundNavHistoryNew>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetSectorHistoricalDataQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Country", country);
                        command.Parameters.AddWithValue("p_SecurityType", securityType);
                        command.Parameters.AddWithValue("p_Sector", sector);
                        command.Parameters.AddWithValue("p_FundCategory", fundCategory);
                        command.Parameters.AddWithValue("p_MeasureType", null);
                        command.Parameters.AddWithValue("p_StartDate", startDate);
                        command.Parameters.AddWithValue("p_EndDate", endDate);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundNavHistoryNew data = new FundNavHistoryNew
                                {
                                    Ticker = reader["FundGroup"] as string,
                                    ALMPD = (reader.IsDBNull(reader.GetOrdinal("MedianDiscount"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MedianDiscount")),
                                    EffectiveDate = reader.IsDBNull(reader.GetOrdinal("EffectiveDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EffectiveDate"))
                                };

                                data.EffectiveDateAsString = DateUtils.ConvertDate(data.EffectiveDate, "yyyy-MM-dd");
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
        /// Get Historical Stats
        /// </summary>
        /// <param name="ticker"></param>
        /// <param name="measureType"></param>
        /// <returns></returns>
        public FundGroupStatsSummary GetFundHistoricalStats(string ticker, string measureType)
        {
            FundGroupStatsSummary data = null;

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetFundHistoricalDataQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Ticker", ticker);
                        command.Parameters.AddWithValue("p_MeasureType", measureType);
                        command.Parameters.AddWithValue("p_StartDate", null);
                        command.Parameters.AddWithValue("p_EndDate", null);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                data = new FundGroupStatsSummary
                                {
                                    Ticker = reader["Ticker"] as string,
                                    Stat1W = (reader.IsDBNull(reader.GetOrdinal("Stat1W"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Stat1W")),
                                    Stat2W = (reader.IsDBNull(reader.GetOrdinal("Stat2W"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Stat2W")),
                                    Stat1M = (reader.IsDBNull(reader.GetOrdinal("Stat1M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Stat1M")),
                                    Stat3M = (reader.IsDBNull(reader.GetOrdinal("Stat3M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Stat3M")),
                                    Stat6M = (reader.IsDBNull(reader.GetOrdinal("Stat6M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Stat6M")),
                                    Stat12M = (reader.IsDBNull(reader.GetOrdinal("Stat12M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Stat12M")),
                                    Stat24M = (reader.IsDBNull(reader.GetOrdinal("Stat24M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Stat24M")),
                                    Stat36M = (reader.IsDBNull(reader.GetOrdinal("Stat36M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Stat36M")),
                                    Stat60M = (reader.IsDBNull(reader.GetOrdinal("Stat60M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Stat60M")),
                                    Stat120M = (reader.IsDBNull(reader.GetOrdinal("Stat120M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Stat120M")),
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
        /// Gets Historical Stats for a Sector
        /// </summary>
        /// <param name="country"></param>
        /// <param name="securityType"></param>
        /// <param name="sector"></param>
        /// <param name="fundCategory"></param>
        /// <param name="measureType"></param>
        /// <returns></returns>
        public FundGroupStatsSummary GetSectorHistoricalStats(string country, string securityType, string sector, string fundCategory, string measureType)
        {
            FundGroupStatsSummary data = null;

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetSectorHistoricalDataQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Country", country);
                        command.Parameters.AddWithValue("p_SecurityType", securityType);
                        command.Parameters.AddWithValue("p_Sector", sector);
                        command.Parameters.AddWithValue("p_FundCategory", fundCategory);
                        command.Parameters.AddWithValue("p_MeasureType", measureType);
                        command.Parameters.AddWithValue("p_StartDate", null);
                        command.Parameters.AddWithValue("p_EndDate", null);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                data = new FundGroupStatsSummary
                                {
                                    Ticker = reader["FundGroup"] as string,
                                    Stat1W = (reader.IsDBNull(reader.GetOrdinal("Stat1W"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Stat1W")),
                                    Stat2W = (reader.IsDBNull(reader.GetOrdinal("Stat2W"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Stat2W")),
                                    Stat1M = (reader.IsDBNull(reader.GetOrdinal("Stat1M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Stat1M")),
                                    Stat3M = (reader.IsDBNull(reader.GetOrdinal("Stat3M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Stat3M")),
                                    Stat6M = (reader.IsDBNull(reader.GetOrdinal("Stat6M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Stat6M")),
                                    Stat12M = (reader.IsDBNull(reader.GetOrdinal("Stat12M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Stat12M")),
                                    Stat24M = (reader.IsDBNull(reader.GetOrdinal("Stat24M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Stat24M")),
                                    Stat36M = (reader.IsDBNull(reader.GetOrdinal("Stat36M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Stat36M")),
                                    Stat60M = (reader.IsDBNull(reader.GetOrdinal("Stat60M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Stat60M")),
                                    Stat120M = (reader.IsDBNull(reader.GetOrdinal("Stat120M"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Stat120M")),
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

        public IList<JobDetail> GetJobDetails(string jobName)
        {
            IList<JobDetail> list = new List<JobDetail>();

            try
            {
                string sql = GetJobDetailsQuery + " where 1=1";
                sql += " and JobName = '" + jobName + "'";
                sql += " and RunDate = (select max(RunDate) from almitasc_ACTradingBBGData.JobDetail where JobName = '" + jobName + "')";
                sql += " order by JobEndTime desc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                JobDetail data = new JobDetail();
                                data.JobId = reader.IsDBNull(reader.GetOrdinal("JobId")) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("JobId"));
                                data.JobName = reader["JobName"] as string;
                                data.JobStatus = reader["JobStatus"] as string;

                                data.RunDate = reader.IsDBNull(reader.GetOrdinal("RunDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("RunDate"));
                                data.JobEndTime = reader.IsDBNull(reader.GetOrdinal("JobEndTime")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("JobEndTime"));
                                data.RunDateAsString = DateUtils.ConvertDate(data.RunDate, "yyyy-MM-dd");
                                data.JobEndTimeAsString = DateUtils.ConvertDate(data.JobEndTime, "yyyy-MM-dd hh:mm:ss tt");

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

        public IList<JobDetail> GetJobDetailsByDate(string EndTime)
        {
            IList<JobDetail> list = new List<JobDetail>();

            try
            {
                string sql = GetJobDetailsByDateQuery + " where 1=1";
                sql += " and EndTime >= '" + EndTime + "'";
                sql += " order by EndTime desc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                JobDetail data = new JobDetail();
                                data.JobId = reader.IsDBNull(reader.GetOrdinal("JobId")) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("JobId"));
                                data.JobName = reader["JobName"] as string;
                                data.JobStatus = reader["JobStatus"] as string;
                                data.RunDate = reader.IsDBNull(reader.GetOrdinal("RunDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("RunDate"));
                                data.EndTime = reader.IsDBNull(reader.GetOrdinal("EndTime")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EndTime"));
                                data.Notes = reader["Notes"] as string;
                                data.Task = reader["Task"] as string;
                                list.Add(data);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing Job Details query");
                throw;
            }

            return list;
        }

        public DateTime? GetPfdCurvesUpdateTime()
        {
            DateTime? data = null;

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetPfdCurvesUpdateTimeQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                data = reader.IsDBNull(reader.GetOrdinal("JobEndTime")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("JobEndTime"));
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

        public IList<FundHolding> GetFundHoldings(string country)
        {
            IList<FundHolding> list = new List<FundHolding>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetPortHoldingsQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Country", country);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundHolding fundHoldings = new FundHolding
                                {
                                    FundTicker = reader["FundTicker"] as string,
                                    FundName = reader["SecurityDescription"] as string,
                                    PortDate = reader.IsDBNull(reader.GetOrdinal("PortDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("PortDate")),
                                    AssetType = reader["AssetType"] as string,
                                    SecuritySector = reader["Sector"] as string,
                                    SecurityType = reader["SecurityType"] as string,
                                    Ticker = reader["Ticker"] as string,
                                    Cusip = reader["Cusip"] as string,
                                    SecurityName = reader["SecurityName"] as string,
                                    Currency = reader["SecurityCurrency"] as string,
                                    Position = (reader.IsDBNull(reader.GetOrdinal("ReportedPosition"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ReportedPosition")),
                                    Weight = (reader.IsDBNull(reader.GetOrdinal("ReportedWeight"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ReportedWeight")),
                                    MV = (reader.IsDBNull(reader.GetOrdinal("ReportedMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ReportedMV")),
                                };

                                fundHoldings.PortDateAsString = DateUtils.ConvertDate(fundHoldings.PortDate, "yyyy-MM-dd");

                                list.Add(fundHoldings);
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

        public IList<BDCData> GetBDCHistoricalData(string ticker, DateTime startDate, DateTime endDate, string hasNav)
        {
            IList<BDCData> list = new List<BDCData>();
            int i = 0;
            BDCData data;

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetFundHistoricalDataQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Ticker", ticker);
                        command.Parameters.AddWithValue("p_MeasureType", null);
                        command.Parameters.AddWithValue("p_StartDate", startDate);
                        command.Parameters.AddWithValue("p_EndDate", endDate);
                        command.Parameters.AddWithValue("p_HasNav", hasNav);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                i++;
                                data = new BDCData
                                {
                                    Ticker = reader["Ticker"] as string,
                                    DateAS = DateUtils.ConvertDate(reader.GetDateTime(reader.GetOrdinal("Date")), "yyyy-MM-dd"),
                                    Prc = (reader.IsDBNull(reader.GetOrdinal("Price"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price")),
                                    PubNav = (reader.IsDBNull(reader.GetOrdinal("PubNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PubNav")),
                                    PubPD = (reader.IsDBNull(reader.GetOrdinal("PubPD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PubPD")),

                                    //
                                    ENavDate = reader.IsDBNull(reader.GetOrdinal("NavDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("NavDate")),
                                    EPubNav = (reader.IsDBNull(reader.GetOrdinal("EPubNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EPubNav")),
                                    EPubAdjNav = (reader.IsDBNull(reader.GetOrdinal("EPubAdjNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EPubAdjNav")),
                                    ENav = (reader.IsDBNull(reader.GetOrdinal("ENav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ENav")),
                                    EPD = (reader.IsDBNull(reader.GetOrdinal("EPD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EPD")),

                                    //
                                    VPrc = (reader.IsDBNull(reader.GetOrdinal("VPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("VPrice")),
                                    Vol = (reader.IsDBNull(reader.GetOrdinal("Vol"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Vol")),
                                    Dvd = (reader.IsDBNull(reader.GetOrdinal("Dvd"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Dvd")),
                                    Split = (reader.IsDBNull(reader.GetOrdinal("Split"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Split")),
                                    AI = (reader.IsDBNull(reader.GetOrdinal("AI"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AI")),
                                    AccRate = (reader.IsDBNull(reader.GetOrdinal("AccRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AccRate")),
                                    Lev = (reader.IsDBNull(reader.GetOrdinal("Lev"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Lev")),
                                    Exp = (reader.IsDBNull(reader.GetOrdinal("Exp"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Exp")),
                                    PRtn = (reader.IsDBNull(reader.GetOrdinal("PriceRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriceRtn")),
                                    VPRtn = (reader.IsDBNull(reader.GetOrdinal("VPriceRtn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("VPriceRtn")),
                                };

                                if (data.ENavDate.HasValue)
                                    data.NavDateAS = DateUtils.ConvertDate(data.ENavDate, "yyyy-MM-dd");
                                list.Add(data);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing query: " + i);
                throw;
            }

            return list;
        }

        public IList<FundHolding> GetPortHoldingsForTicker(string holdingTicker)
        {
            IList<FundHolding> list = new List<FundHolding>();

            try
            {
                string sql = GetPortHoldingsQueryForHoldingsTicker + " where Ticker like '%" + holdingTicker + "%' order by MV desc";
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                FundHolding fundHoldings = new FundHolding
                                {
                                    FundTicker = reader["FundTicker"] as string,
                                    PortDate = reader.IsDBNull(reader.GetOrdinal("PortDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("PortDate")),
                                    Ticker = reader["Ticker"] as string,
                                    Position = (reader.IsDBNull(reader.GetOrdinal("Position"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Position")),
                                    Weight = (reader.IsDBNull(reader.GetOrdinal("Wt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Wt")),
                                    MV = (reader.IsDBNull(reader.GetOrdinal("MV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MV")),
                                    Price = (reader.IsDBNull(reader.GetOrdinal("Price"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price")),
                                    FundCurrency = reader["Curr"] as string,
                                    FXRate = (reader.IsDBNull(reader.GetOrdinal("FxRate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("FxRate")),
                                    Coupon = (reader.IsDBNull(reader.GetOrdinal("Cpn"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Cpn")),
                                    FundName = reader["FundName"] as string,
                                    AssetType = reader["AssetType"] as string,
                                    SecuritySector = reader["SecuritySector"] as string,
                                    SecurityType = reader["SecurityType"] as string,
                                    Country = reader["Country"] as string,
                                    BroadIndustry = reader["BroadIndustry"] as string,
                                    Industry = reader["Industry"] as string,
                                    YieldToWorst = (reader.IsDBNull(reader.GetOrdinal("YTW"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("YTW")),
                                    YieldToMaturity = (reader.IsDBNull(reader.GetOrdinal("YTM"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("YTM")),
                                };

                                list.Add(fundHoldings);
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

        public IList<PortHoldingsExposuresTO> GetPortHoldingsExposures(string Ticker)
        {
            IList<PortHoldingsExposuresTO> list = new List<PortHoldingsExposuresTO>();

            try
            {
                string sql = GetPortHoldingsExposuresQuery;
                if (!string.IsNullOrEmpty(Ticker))
                {
                    sql += " where Ticker like '%" + Ticker + "%'";
                }
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                PortHoldingsExposuresTO portholdings = new PortHoldingsExposuresTO
                                {
                                    Ticker = reader["ticker"] as string,
                                    FundName = reader["fundname"] as string,
                                    AssetClass = reader["fundassetclass"] as string,
                                    Geography = reader["fundgeography"] as string,
                                    FundType = reader["fundtype"] as string,
                                    Curr = reader["fundcurrency"] as string,
                                    MVLocal = (reader.IsDBNull(reader.GetOrdinal("marketcap_local"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("marketcap_local")),
                                    MVUSD = (reader.IsDBNull(reader.GetOrdinal("marketcap_usd"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("marketcap_usd")),
                                    PortDate = (reader.IsDBNull(reader.GetOrdinal("portdate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("portdate")),
                                    ExpDevelopedMkt = (reader.IsDBNull(reader.GetOrdinal("developedmarket"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("developedmarket")),
                                    ExpEmergingMkt = (reader.IsDBNull(reader.GetOrdinal("emergingmarket"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("emergingmarket")),
                                    ExpFrontierMkt = (reader.IsDBNull(reader.GetOrdinal("frontiermarket"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("frontiermarket")),
                                    SecCount = (reader.IsDBNull(reader.GetOrdinal("securitycount"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("securitycount")),
                                    SecPriced = (reader.IsDBNull(reader.GetOrdinal("securitypriced"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("securitypriced")),
                                    SecNotPriced = (reader.IsDBNull(reader.GetOrdinal("securitynotpriced"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("securitynotpriced")),
                                    ExpCash = (reader.IsDBNull(reader.GetOrdinal("aa_cash"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("aa_cash")),
                                    ExpEquity = (reader.IsDBNull(reader.GetOrdinal("aa_equity"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("aa_equity")),
                                    ExpFI = (reader.IsDBNull(reader.GetOrdinal("aa_fixedincome"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("aa_fixedincome")),
                                    ExpREIT = (reader.IsDBNull(reader.GetOrdinal("aa_reit"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("aa_reit")),
                                    ExpFunds = (reader.IsDBNull(reader.GetOrdinal("aa_fund"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("aa_fund")),
                                    ExpDerivatives = (reader.IsDBNull(reader.GetOrdinal("aa_derivatives"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("aa_derivatives")),
                                    ExpIndustryPhysical = (reader.IsDBNull(reader.GetOrdinal("industry_physical"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("industry_physical")),
                                    ExpIndustryFinancial = (reader.IsDBNull(reader.GetOrdinal("industry_financial"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("industry_financial")),
                                    ExpIndustryCommunications = (reader.IsDBNull(reader.GetOrdinal("industry_technologyandcommunication"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("industry_technologyandcommunication")),
                                    ExpIndustryEnergy = (reader.IsDBNull(reader.GetOrdinal("industry_energy"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("industry_energy")),
                                    ExpIndustryOthers = (reader.IsDBNull(reader.GetOrdinal("industry_others"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("industry_others")),
                                    AvgMV = (reader.IsDBNull(reader.GetOrdinal("average_marketcap"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("average_marketcap")),
                                    ExpLargeCap = (reader.IsDBNull(reader.GetOrdinal("largecap"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("largecap")),
                                    ExpMidCap = (reader.IsDBNull(reader.GetOrdinal("midcap"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("midcap")),
                                    ExpSmallCap = (reader.IsDBNull(reader.GetOrdinal("smallcap"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("smallcap")),
                                    Alpha = (reader.IsDBNull(reader.GetOrdinal("alpha"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("alpha")),
                                    Beta = (reader.IsDBNull(reader.GetOrdinal("beta"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("beta")),
                                    PERatio = (reader.IsDBNull(reader.GetOrdinal("pe_ration"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("pe_ration")),
                                    PBRatio = (reader.IsDBNull(reader.GetOrdinal("pb_ratio"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("pb_ratio")),
                                    DvdYield = (reader.IsDBNull(reader.GetOrdinal("dividendyield"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("dividendyield")),
                                    Country1 = reader["country1"] as string,
                                    Country2 = reader["country2"] as string,
                                    Country3 = reader["country3"] as string,
                                    Country4 = reader["country4"] as string,
                                    Country5 = reader["country5"] as string,
                                    ExpCountry1 = (reader.IsDBNull(reader.GetOrdinal("country1exposure"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("country1exposure")),
                                    ExpCountry2 = (reader.IsDBNull(reader.GetOrdinal("country2exposure"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("country2exposure")),
                                    ExpCountry3 = (reader.IsDBNull(reader.GetOrdinal("country3exposure"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("country3exposure")),
                                    ExpCountry4 = (reader.IsDBNull(reader.GetOrdinal("country4exposure"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("country4exposure")),
                                    ExpCountry5 = (reader.IsDBNull(reader.GetOrdinal("country5exposure"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("country5exposure")),
                                    AvgRating = reader["average_rating"] as string,
                                    YTW = (reader.IsDBNull(reader.GetOrdinal("yield_to_worst"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("yield_to_worst")),
                                    OAS = (reader.IsDBNull(reader.GetOrdinal("OAS"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OAS")),
                                    Duration = (reader.IsDBNull(reader.GetOrdinal("duration"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("duration")),
                                    Convexity = (reader.IsDBNull(reader.GetOrdinal("convexity"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("convexity")),
                                    YTM = (reader.IsDBNull(reader.GetOrdinal("yield_to_maturity"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("yield_to_maturity")),
                                    DurationWithLev = (reader.IsDBNull(reader.GetOrdinal("duration_withleverage"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("duration_withleverage")),
                                    ConvexitryWithLev = (reader.IsDBNull(reader.GetOrdinal("convexitry_withleverage"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("convexitry_withleverage")),
                                    PctRated = (reader.IsDBNull(reader.GetOrdinal("percent_rated"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("percent_rated")),
                                    PctIG = (reader.IsDBNull(reader.GetOrdinal("percent_ig"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("percent_ig")),
                                    PctNonIG = (reader.IsDBNull(reader.GetOrdinal("percent_nonig"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("percent_nonig")),
                                    PctUSTreasury = (reader.IsDBNull(reader.GetOrdinal("us_treasury"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("us_treasury")),
                                    PctOtherGovt = (reader.IsDBNull(reader.GetOrdinal("other_government"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("other_government")),
                                    PctCorporate = (reader.IsDBNull(reader.GetOrdinal("corporate"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("corporate")),
                                    PctMBS = (reader.IsDBNull(reader.GetOrdinal("MBS"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MBS")),
                                    PctAgencyCMO = (reader.IsDBNull(reader.GetOrdinal("agency_cmo"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("agency_cmo")),
                                    PctRMBS = (reader.IsDBNull(reader.GetOrdinal("RMBS"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("RMBS")),
                                    PctABS = (reader.IsDBNull(reader.GetOrdinal("ABS"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ABS")),
                                    PctCMBS = (reader.IsDBNull(reader.GetOrdinal("CMBS"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CMBS")),
                                    PctCDO = (reader.IsDBNull(reader.GetOrdinal("CDO"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CDO")),
                                    PctMuni = (reader.IsDBNull(reader.GetOrdinal("muni"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("muni")),
                                    PctLoan = (reader.IsDBNull(reader.GetOrdinal("loan"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("loan")),
                                    PctConvertible = (reader.IsDBNull(reader.GetOrdinal("convertible"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("convertible")),
                                    PctPfd = (reader.IsDBNull(reader.GetOrdinal("preferred"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("preferred")),
                                    PctTaxableMuni = (reader.IsDBNull(reader.GetOrdinal("taxable_muni"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("taxable_muni")),
                                    PctBankLoan1stLien = (reader.IsDBNull(reader.GetOrdinal("bankloan_1stlien"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("bankloan_1stlien")),
                                    PctBankLoan2ndLien = (reader.IsDBNull(reader.GetOrdinal("bankloan_2ndlien"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("bankloan_2ndlien")),
                                    PctBankLoan3rdLien = (reader.IsDBNull(reader.GetOrdinal("bankloan_3rdlien"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("bankloan_3rdlien")),
                                    State1 = reader["state1"] as string,
                                    State2 = reader["state2"] as string,
                                    State3 = reader["state3"] as string,
                                    State4 = reader["state4"] as string,
                                    State5 = reader["state5"] as string,
                                    ExpState1 = (reader.IsDBNull(reader.GetOrdinal("state1exposure"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("state1exposure")),
                                    ExpState2 = (reader.IsDBNull(reader.GetOrdinal("state2exposure"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("state2exposure")),
                                    ExpState3 = (reader.IsDBNull(reader.GetOrdinal("state3exposure"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("state3exposure")),
                                    ExpState4 = (reader.IsDBNull(reader.GetOrdinal("state4exposure"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("state4exposure")),
                                    ExpState5 = (reader.IsDBNull(reader.GetOrdinal("state5exposure"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("state5exposure")),
                                    Currency1 = reader["currency1"] as string,
                                    Currency2 = reader["currency2"] as string,
                                    Currency3 = reader["currency3"] as string,
                                    Currency4 = reader["currency4"] as string,
                                    Currency5 = reader["currency5"] as string,
                                    ExpCurrency1 = (reader.IsDBNull(reader.GetOrdinal("currency1exposure"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("currency1exposure")),
                                    ExpCurrency2 = (reader.IsDBNull(reader.GetOrdinal("currency2exposure"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("currency2exposure")),
                                    ExpCurrency3 = (reader.IsDBNull(reader.GetOrdinal("currency3exposure"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("currency3exposure")),
                                    ExpCurrency4 = (reader.IsDBNull(reader.GetOrdinal("currency4exposure"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("currency4exposure")),
                                    ExpCurrency5 = (reader.IsDBNull(reader.GetOrdinal("currency5exposure"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("currency5exposure")),
                                    Industry1 = reader["industry1"] as string,
                                    Industry2 = reader["industry2"] as string,
                                    Industry3 = reader["industry3"] as string,
                                    Industry4 = reader["industry4"] as string,
                                    Industry5 = reader["industry5"] as string,
                                    ExpIndustry1 = (reader.IsDBNull(reader.GetOrdinal("industry1exposure"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("industry1exposure")),
                                    ExpIndustry2 = (reader.IsDBNull(reader.GetOrdinal("industry2exposure"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("industry2exposure")),
                                    ExpIndustry3 = (reader.IsDBNull(reader.GetOrdinal("industry3exposure"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("industry3exposure")),
                                    ExpIndustry4 = (reader.IsDBNull(reader.GetOrdinal("industry4exposure"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("industry4exposure")),
                                    ExpIndustry5 = (reader.IsDBNull(reader.GetOrdinal("industry5exposure"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("industry5exposure")),

                                };

                                list.Add(portholdings);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing Port Holding Exposures query");
                throw;
            }
            return list;

        }
        public IList<TaxLotSummaryTO> GetTaxLotSummary()
        {
            IList<TaxLotSummaryTO> list = new List<TaxLotSummaryTO>();

            try
            {
                string sql = GetTaxLotSummaryDetailsQuery;
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                TaxLotSummaryTO data = new TaxLotSummaryTO
                                {
                                    Ticker = reader["ALMTicker"] as string,
                                    UnrealizedPL = (reader.IsDBNull(reader.GetOrdinal("UnrealizedPL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnrealizedPL")),
                                    UnrealizedPLLT = (reader.IsDBNull(reader.GetOrdinal("UnrealizedPLLT"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnrealizedPLLT")),
                                    UnrealizedPLST = (reader.IsDBNull(reader.GetOrdinal("UnrealizedPLST"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("UnrealizedPLST")),
                                    OppUnrealizedPL = (reader.IsDBNull(reader.GetOrdinal("OppUnrealizedPL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OppUnrealizedPL")),
                                    OppUnrealizedPLLT = (reader.IsDBNull(reader.GetOrdinal("OppUnrealizedPLLT"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OppUnrealizedPLLT")),
                                    OppUnrealizedPLST = (reader.IsDBNull(reader.GetOrdinal("OppUnrealizedPLST"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OppUnrealizedPLST")),
                                    TacUnrealizedPL = (reader.IsDBNull(reader.GetOrdinal("TacUnrealizedPL"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TacUnrealizedPL")),
                                    TacUnrealizedPLLT = (reader.IsDBNull(reader.GetOrdinal("TacUnrealizedPLLT"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TacUnrealizedPLLT")),
                                    TacUnrealizedPLST = (reader.IsDBNull(reader.GetOrdinal("TacUnrealizedPLST"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TacUnrealizedPLST")),
                                };
                                list.Add(data);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing Tax summary query");
                throw;
            }

            return list;
        }

        private const string GetFundHoldingsQuery = "spGetFundHoldings";
        private const string GetPortHoldingsQuery = "spGetPortHoldings";
        private const string GetSecurityMasterQuery = "select s.*, u.fundcategory"
                                + " from almitasc_ACTradingBBGLink.globaltrading_securitymaster s"
                                + " left join almitasc_ACTradingBBGData.UserOverrideNew u on (s.ticker = u.ticker)";
        private const string GetCustomViewQuery = "select * from almitasc_ACTradingBBGData.CustomViewMst";
        private const string GetCustomViewsQuery = "select distinct ViewName, UserName, date(CreateDate) as CreateDate from almitasc_ACTradingBBGData.CustomViewMst order by ViewName";
        private const string GetBDCNavHistoryQuery = "select s.ticker, b.figi, b.date as navdate, b.nav, b.username, b.createdate from almitasc_ACTradingBBGData.globalbdc_navhistory b join almitasc_ACTradingBBGLink.globaltrading_securitymaster s on (b.figi = s.figi)";
        private const string GetReitBookValueHistoryQuery = "select s.ticker, b.figi, b.date as bookvaluedate, b.bookvalue, b.username, b.createdate from almitasc_ACTradingBBGData.globalreit_bookvaluehistory b join almitasc_ACTradingBBGLink.globaltrading_securitymaster s on (b.figi = s.figi)";
        private const string GetFundCurrencyHedgeQuery = "select * from almitasc_ACTradingBBGData.globalcef_fundmaster_fxhedge";
        private const string GetFundAlertQuery = "select * from almitasc_ACTradingBBGData.FundAlert";
        private const string GetPopulateFundAlertQuery = "call almitasc_ACTradingBBGData.spPopulateFundAlerts";
        private const string GetFundAlertTargetsQuery = "select * from almitasc_ACTradingBBGData.FundAlertTarget";
        private const string GetTaxLotsQuery = "Primebrokerfiles.spGetTaxLotsNew";
        private const string GetTaxLotSummaryQuery = "spGetTaxLotSummaryNew";
        private const string GetFundNavHistoryQuery = "spGetFundNavHistory";
        private const string GetFundEarningsHistoryQuery = "spGetFundEarningsHistory";
        private const string GetFundHistoryQuery = "select s.ticker, f.* from almitasc_ACTradingBBGLink.globaltrading_securitymaster s join almitasc_ACTradingData.globalcef_fundhistoryfinal4study f on (s.almitasid = f.almitasid)";
        private const string GetFundHistoryStatQuery = "spGetFundHistory";
        private const string GetSectorHistoryQuery = "spGetSectorHistory";
        private const string GetFullSecurityMasterQuery = "call almitasc_ACTradingBBGData.spGetSecurityMasterDetails";
        private const string GetBrokerTradingVolumeQuery = "call almitasc_ACTradingBBGData.spGetBrokerTradingVolumes";
        private const string GetPositionSecurityDetailsQuery = "select"
                                    + " h.ticker as Ticker"
                                    + " ,h.YellowKey as YellowKey"
                                    + " ,h.Currency as Currency"
                                    + " ,p.SecurityName as SecurityName"
                                    + " ,p.Security13FFlag as Security13FFlag"
                                    + " from"
                                    + " (select distinct replace(ifnull(ticker, brokersystemid),'SwapUnderlying#','') as Ticker, ykey as YellowKey, fx as Currency from almitasc_ACTradingPM.portfoliomanager_holdings where ifnull(ticker, brokersystemid) is not null and position <> 0) h"
                                    + " left join almitasc_ACTradingBBGData.PfSecurityMst p on (p.ticker = h.ticker)";
        private const string SaveSecurityMasterDetailsQuery = "spSaveSecurityNew";
        private const string GetSectorPerformanceQuery = "call almitasc_ACTradingBBGData.spGetDailyPnL";
        private const string GetSectorPerformanceByDateRangeQuery = "spGetPnL";
        private const string GetPfdCommonSharesMapQuery = "select PfdTicker, CommonShareTicker from almitasc_ACTradingBBGData.SecPfdCommonShareMst";
        private const string GetBDCFundReturnsQuery = "call almitasc_ACTradingBBGData.spGetBDCReturns";
        private const string GetWatchlistSecuritiesQuery = "select f.Ticker, ifnull(u.FundCategory, 'Other') as FundCategory"
                                    + " from almitasc_ACTradingBBGData.TradingAlertTarget f"
                                    + " left join almitasc_ACTradingBBGData.UserOverrideNew u on (f.ticker = u.ticker)";
        private const string GetTradingTargetQuery = "select ifnull(u.FundCategory, 'Other') as FundCategory,"
                                    + " f.Ticker, f.TransactionType, f.PriceTarget, f.DiscountTarget, f.NavType, f.NavOverride,"
                                    + " f.IRRTarget, f.JoinCondition, f.RefIndex, f.PriceBeta, ifnull(f.PriceBetaShiftInd, 0) as PriceBetaShiftInd,"
                                    + " f.PriceCap, ifnull(f.PriceCapShiftInd, 0) as PriceCapShiftInd"
                                    + " from almitasc_ACTradingBBGData.TradingAlertTarget f"
                                    + " left join almitasc_ACTradingBBGData.UserOverrideNew u on(f.ticker = u.ticker)";
        private const string SaveTradingTargetQuery = "spSaveTradingAlertTargetNew";
        private const string GetExpectedAlphaModelParamsQuery = "select * from almitasc_ACTradingBBGData.ExpectedAlphaModelParams";
        private const string GetFundHistoricalDataQuery = "spGetFundHistoricalData";
        private const string GetSectorHistoricalDataQuery = "spGetSectorHistoryNew";
        private const string GetJobDetailsQuery = "select JobId, RunDate, JobName, JobStatus, EndTime, CONVERT_TZ(EndTime,'+00:00','-8:00') as JobEndTime"
                                    + " from almitasc_ACTradingBBGData.JobDetail";
        private const string GetJobDetailsByDateQuery = "select JobId, RunDate, JobName, JobStatus, EndTime, Task, Notes from almitasc_ACTradingBBGData.JobDetail";

        private const string GetPfdCurvesUpdateTimeQuery = "select CONVERT_TZ(EndTime,'+00:00','-8:00') as JobEndTime"
                                    + " from almitasc_ACTradingBBGData.JobDetail"
                                    + " where JobName = 'RateCurveInterpolationService'"
                                    + " and JobId = (select max(JobId) from almitasc_ACTradingBBGData.JobDetail where JobName = 'RateCurveInterpolationService')";

        private const string GetPortHoldingsQueryForHoldingsTicker = "select c.fundticker as FundTicker, m.portdate as PortDate"
            + ", c.ticker as Ticker, c.position as Position, c.marketvaluepct as Wt, c.marketvalue as MV, c.price as Price"
            + ", c.fx as Curr, c.fxrate as FxRate, c.coupon as Cpn"
            + ", c.fundname as FundName, c.assetclass as AssetType, c.securitytype as SecuritySector"
            + ", c.securitytype as SecurityType, c.country as Country"
            + ", c.broadindustry as BroadIndustry, c.industry as Industry"
            + ", c.yield2worst as  YTW, c.yield2maturity as YTM"
            + " from Model.globalcef_port c"
            + " left join Model.globalcef_portmaster m on(c.fundticker = m.fundticker)";
        private const string GetTaxLotSummaryDetailsQuery = "Primebrokerfiles.spGetTaxLotSummary";
        private const string GetPortHoldingsExposuresQuery = "select * from almitasc_ACGlobalSecurity.globaltrading_securitymastersupp_portsummary";
    }
}
