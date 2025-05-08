using aACTrader.DAO.Interface;
using aCommons.DTO;
using aCommons.Trading;
using aCommons.Utils;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace aACTrader.DAO.Repository
{
    public class TradingDao : ITradingDao
    {
        private readonly ILogger<TradingDao> _logger;
        private readonly IConfiguration _configuration;

        private const string DATEFORMAT = "yyyy-MM-dd";
        private const string DELIMITER = ",";

        public TradingDao(ILogger<TradingDao> logger, IConfiguration configuration)
        {
            _logger = logger;
            this._configuration = configuration;
            _logger.LogInformation("Initializing TradingDao...");
        }

        /// <summary>
        /// Gets Order Details
        /// </summary>
        /// <returns></returns>
        public IDictionary<string, OrderSummary> GetOrderSummary()
        {
            IDictionary<string, OrderSummary> orderSummaryDict = new Dictionary<string, OrderSummary>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                string sql = string.Empty;

                if (_configuration["ConnectionStrings:ENV"].Equals("PROD"))
                    sql = GetOrderSummaryProdQuery;
                else
                    sql = GetOrderSummaryDevQuery;

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                OrderSummary data = new OrderSummary
                                {
                                    OrdRank = reader.GetInt32(reader.GetOrdinal("OrderRank")),
                                    AccNum = (reader.IsDBNull(reader.GetOrdinal("AccountNumber"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("AccountNumber")),
                                    AccName = reader["AccountName"] as string,
                                    ParOrdId = reader["ParentOrderId"] as string,
                                    MainOrdId = reader["MainOrderId"] as string,
                                    OrdId = reader["OrderId"] as string,
                                    CancOrdId = reader["CancelOrderId"] as string,
                                    TrkId = reader["TrackingId"] as string,
                                    RefId = reader["ReferenceId"] as string,
                                    NVExecIdLng = (reader.IsDBNull(reader.GetOrdinal("NeovestExecId"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("NeovestExecId")),
                                    Sym = reader["Symbol"] as string,
                                    BBGSym = reader["BBGSymbol"] as string,
                                    ALMSym = reader["ALMSymbol"] as string,
                                    ISIN = reader["ISIN"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    Cusip = reader["Cusip"] as string,
                                    Curr = reader["Currency"] as string,
                                    OrdDt = reader.IsDBNull(reader.GetOrdinal("OrderDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("OrderDate")),
                                    OrdTm = reader["OrderTime"] as string,
                                    OrdTyp = reader["OrderType"] as string,
                                    OrdSide = reader["OrderSide"] as string,
                                    OrdQty = (reader.IsDBNull(reader.GetOrdinal("OrderQty"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("OrderQty")),
                                    OrdExch = reader["OrderExchange"] as string,
                                    OrdExchId = (reader.IsDBNull(reader.GetOrdinal("OrderExchangeId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("OrderExchangeId")),
                                    OrdPr = (reader.IsDBNull(reader.GetOrdinal("OrderPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OrderPrice")),
                                    OrdStopPr = (reader.IsDBNull(reader.GetOrdinal("OrderStopPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OrderStopPrice")),
                                    OrdOrigPr = (reader.IsDBNull(reader.GetOrdinal("OrderOrigPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OrderOrigPrice")),
                                    OrdSt = reader["OrderStatus"] as string,
                                    OrdMemo = reader["OrderMemo"] as string,
                                    OrdDest = reader["OrderDest"] as string,
                                    OrdBkrStrat = reader["OrderBkrStrategy"] as string,
                                    PairTrdSprd = (reader.IsDBNull(reader.GetOrdinal("PairTradeSpread"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PairTradeSpread")),
                                    Trader = reader["Trader"] as string,
                                    ALMTrader = reader["ALMTrader"] as string,

                                    //Status
                                    TrdQty = (reader.IsDBNull(reader.GetOrdinal("TradedQty"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("TradedQty")),
                                    TrdPr = (reader.IsDBNull(reader.GetOrdinal("TradedPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TradedPrice")),
                                    AvgTrdPr = (reader.IsDBNull(reader.GetOrdinal("AvgTradedPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AvgTradedPrice")),
                                    TrdCumQty = (reader.IsDBNull(reader.GetOrdinal("TradedCumulativeQty"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("TradedCumulativeQty")),
                                    CancQty = (reader.IsDBNull(reader.GetOrdinal("CanceledQty"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("CanceledQty")),
                                    LeavesQty = (reader.IsDBNull(reader.GetOrdinal("LeavesQty"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("LeavesQty")),
                                    ExecQty = (reader.IsDBNull(reader.GetOrdinal("ExecutedQty"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("ExecutedQty")),
                                    TrdMsg = reader["TradedMsg"] as string,
                                    OrdStatusUpdTm = reader["OrderStatusUpdateTime"] as string,
                                    AlgoParameters = reader["AlgoParameters"] as string,
                                    TrdSrc = reader["TradeSource"] as string,

                                    //Options
                                    OptPos = reader["OptionPosition"] as string,
                                    OptEqPos = reader["OptionEquityPosition"] as string,
                                    OptFill = reader["OptionFill"] as string,

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
                                    UpdateAlgoParams = reader["UpdateAlgoParams"] as string,

                                    LastPr = (reader.IsDBNull(reader.GetOrdinal("LastPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LastPrice")),
                                    BidPr = (reader.IsDBNull(reader.GetOrdinal("BidPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BidPrice")),
                                    AskPr = (reader.IsDBNull(reader.GetOrdinal("AskPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AskPrice")),
                                };

                                data.Key = data.Sym + "|" + data.OrdSide;
                                data.OrdDtAsString = DateUtils.ConvertDate(data.OrdDt, "yyyy-MM-dd");

                                //Check for Order Status
                                if (!string.IsNullOrEmpty(data.OrdSt))
                                {
                                    if (data.OrdSt.Equals("Canceled")
                                        || data.OrdSt.Equals("Canceled/ Partial")
                                        || data.OrdSt.Equals("Completed")
                                        || data.OrdSt.Equals("Executed")
                                        || data.OrdSt.Equals("Rejected")
                                        || data.OrdSt.Equals("Replace Rejected")
                                        )
                                    {
                                        data.OrdActFlag = "N";
                                    }
                                }

                                if (!orderSummaryDict.ContainsKey(data.OrdId))
                                    orderSummaryDict.Add(data.OrdId, data);
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

            return orderSummaryDict;
        }

        /// <summary>
        /// Saves Order Summary
        /// </summary>
        /// <param name="orderSummaryDict"></param>
        public void SaveOrderSummary(IDictionary<string, OrderSummary> orderSummaryDict)
        {
            try
            {
                TruncateTable(OrderSummaryStgTableName);
                SaveOrderSummaryStg(orderSummaryDict, OrderSummaryStgTableName);
                MoveDataToTargetTable(SaveOrderSummaryQuery);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving data");
            }
        }

        /// <summary>
        /// Saves Order Execution Details
        /// </summary>
        /// <param name="orderExecutionDetailsDict"></param>
        public void SaveOrderExecutionDetails(IDictionary<string, OrderSummary> orderExecutionDetailsDict)
        {
            try
            {
                TruncateTable(OrderExecutionsStgTableName);
                SaveOrderExecutionsStg(orderExecutionDetailsDict);
                MoveDataToTargetTable(SaveOrderExecutionsQuery);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving data");
            }
        }

        /// <summary>
        /// Saves Order Execution Details (Simulation)
        /// </summary>
        /// <param name="orderExecutionDetailsDict"></param>
        public void SaveSimOrderExecutionDetails(IDictionary<string, OrderSummary> orderExecutionDetailsDict)
        {
            try
            {
                TruncateTable(SimOrderExecutionsStgTableName);
                SaveSimOrderExecutionsStg(orderExecutionDetailsDict);
                MoveDataToTargetTable(SaveSimOrderExecutionsQuery);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving data");
            }
        }

        /// <summary>
        /// Saves Order Summary to Staging table
        /// </summary>
        /// <param name="orderSummaryDict"></param>
        public void SaveOrderSummaryStg(IDictionary<string, OrderSummary> orderSummaryDict, string tableName)
        {
            try
            {
                StringBuilder sCommand = new StringBuilder("insert into " + tableName
                    + " (AccountNumber, AccountName, ParentOrderId, MainOrderId, OrderId, TrackingId, ReferenceId, NeovestExecId,"
                    + " Symbol, BBGSymbol, ALMSymbol, OrderDate, OrderTime, OrderType, OrderSide, OrderQty, OrderExchange,"
                    + " OrderExchangeId, OrderPrice, OrderStopPrice, OrderStatus, OrderMemo, OrderDest, OrderBkrStrategy,"
                    + " PairTradeSpread, TradedQty, TradedPrice, AvgTradedPrice, TradedCumulativeQty, CanceledQty, LeavesQty, TradedMsg,"
                    + " OrderStatusUpdateTime, AlgoParameters, Trader, ALMTrader, ISIN, Sedol, Cusip, Currency, TradeSource,"
                    + " OptionPosition, OptionEquityPosition, OptionFill,"
                    + " RefIndex, RefIndexLastPrice, "
                    + " RefIndexBeta, RefIndexBetaInd, "
                    + " RefIndexPriceCap, RefIndexPriceInd, RefIndexPriceChng, TargetPrice,"
                    + " DiscountTarget, DiscountTargetLastNav,"
                    + " RefIndexPriceBetaAdj, RefIndexMaxPrice,"
                    + " EstimatedNav, EstimatedDiscount,"
                    + " RefIndexLivePrice, RefIndexPriceType, AutoUpdate, AutoUpdateThreshold,"
                    + " OrderOrigPrice, RefIndexPriceChngFinal, DiscountToLastPrice, DiscountToLivePrice,"
                    + " MarketPriceThreshold, MarketPriceSpread, CancelOrderId,"
                    + " LastPrice, BidPrice, AskPrice, OrderIdRank, MarketPriceField, EstNavType, UpdateAlgoParams"
                    + " ) values ");

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    IList<string> Rows = new List<string>();
                    StringBuilder sb = new StringBuilder();
                    foreach (OrderSummary data in orderSummaryDict.Values.ToList())
                    {
                        //AccountNumber
                        if (data.AccNum.HasValue)
                            sb.Append(data.AccNum).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //AccountName
                        if (!string.IsNullOrEmpty(data.AccName))
                            sb.Append(string.Concat("'", data.AccName, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //ParentOrderId
                        if (!string.IsNullOrEmpty(data.ParOrdId))
                            sb.Append(string.Concat("'", data.ParOrdId, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //MainOrderId
                        if (!string.IsNullOrEmpty(data.MainOrdId))
                            sb.Append(string.Concat("'", data.MainOrdId, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //OrderId
                        if (!string.IsNullOrEmpty(data.OrdId))
                            sb.Append(string.Concat("'", data.OrdId, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //TrackingId
                        if (!string.IsNullOrEmpty(data.TrkId))
                            sb.Append(string.Concat("'", data.TrkId, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //ReferenceId
                        if (!string.IsNullOrEmpty(data.RefId))
                            sb.Append(string.Concat("'", data.RefId, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //NeovestExecId
                        if (data.NVExecIdLng.HasValue)
                            sb.Append(data.NVExecIdLng).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //Symbol
                        if (!string.IsNullOrEmpty(data.Sym))
                            sb.Append(string.Concat("'", data.Sym, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //BBGSymbol
                        if (!string.IsNullOrEmpty(data.BBGSym))
                            sb.Append(string.Concat("'", data.BBGSym, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //ALMSymbol
                        if (!string.IsNullOrEmpty(data.ALMSym))
                            sb.Append(string.Concat("'", data.ALMSym, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //OrderDate
                        if (data.OrdDt.HasValue)
                            sb.Append(string.Concat("'", DateUtils.ConvertDate(data.OrdDt, "yyyy-MM-dd"), "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //OrderTime
                        if (!string.IsNullOrEmpty(data.OrdTm))
                            sb.Append(string.Concat("'", data.OrdTm, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //OrderType
                        if (!string.IsNullOrEmpty(data.OrdTyp))
                            sb.Append(string.Concat("'", data.OrdTyp, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //OrderSide
                        if (!string.IsNullOrEmpty(data.OrdSide))
                            sb.Append(string.Concat("'", data.OrdSide, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //OrderQty
                        if (data.OrdQty.HasValue)
                            sb.Append(data.OrdQty).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //OrderExchange
                        if (!string.IsNullOrEmpty(data.OrdExch))
                            sb.Append(string.Concat("'", data.OrdExch, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //OrderExchangeId
                        if (data.OrdExchId.HasValue)
                            sb.Append(data.OrdExchId).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //OrderPrice
                        if (data.OrdPr.HasValue)
                            sb.Append(data.OrdPr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //OrderStopPrice
                        if (data.OrdStopPr.HasValue)
                            sb.Append(data.OrdStopPr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //OrderStatus
                        if (!string.IsNullOrEmpty(data.OrdSt))
                            sb.Append(string.Concat("'", data.OrdSt, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //OrderMemo
                        if (!string.IsNullOrEmpty(data.OrdMemo))
                            sb.Append(string.Concat("'", data.OrdMemo, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //OrderDest
                        if (!string.IsNullOrEmpty(data.OrdDest))
                            sb.Append(string.Concat("'", data.OrdDest, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //OrderBkrStrategy
                        if (!string.IsNullOrEmpty(data.OrdBkrStrat))
                            sb.Append(string.Concat("'", data.OrdBkrStrat, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //PairTradeSpread
                        if (data.PairTrdSprd.HasValue)
                            sb.Append(data.PairTrdSprd).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //TradedQty
                        if (data.TrdQty.HasValue)
                            sb.Append(data.TrdQty).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //TradedPrice
                        if (data.TrdPr.HasValue)
                            sb.Append(data.TrdPr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //AvgTradedPrice
                        if (data.AvgTrdPr.HasValue)
                            sb.Append(data.AvgTrdPr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //TradedCumulativeQty
                        if (data.TrdCumQty.HasValue)
                            sb.Append(data.TrdCumQty).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //CanceledQty
                        if (data.CancQty.HasValue)
                            sb.Append(data.CancQty).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //LeavesQty
                        if (data.LeavesQty.HasValue)
                            sb.Append(data.LeavesQty).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //TradedMsg
                        if (!string.IsNullOrEmpty(data.TrdMsg))
                            sb.Append(string.Concat("'", MySqlHelper.EscapeString(data.TrdMsg), "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //OrderStatusUpdateTime
                        if (!string.IsNullOrEmpty(data.OrdStatusUpdTm))
                            sb.Append(string.Concat("'", data.OrdStatusUpdTm, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //AlgoParameters
                        if (!string.IsNullOrEmpty(data.AlgoParameters))
                            sb.Append(string.Concat("'", data.AlgoParameters, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //Trader
                        if (!string.IsNullOrEmpty(data.Trader))
                            sb.Append(string.Concat("'", data.Trader, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //ALMTrader
                        if (!string.IsNullOrEmpty(data.ALMTrader))
                            sb.Append(string.Concat("'", data.ALMTrader, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //ISIN
                        if (!string.IsNullOrEmpty(data.ISIN))
                            sb.Append(string.Concat("'", data.ISIN, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //Sedol
                        if (!string.IsNullOrEmpty(data.Sedol))
                            sb.Append(string.Concat("'", data.Sedol, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //Cusip
                        if (!string.IsNullOrEmpty(data.Cusip))
                            sb.Append(string.Concat("'", data.Cusip, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //Currency
                        if (!string.IsNullOrEmpty(data.Curr))
                            sb.Append(string.Concat("'", data.Curr, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //TradeSource
                        if (!string.IsNullOrEmpty(data.TrdSrc))
                            sb.Append(string.Concat("'", data.TrdSrc, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //OptionPosition
                        if (!string.IsNullOrEmpty(data.OptPos))
                            sb.Append(string.Concat("'", data.OptPos, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //OptionEquityPosition
                        if (!string.IsNullOrEmpty(data.OptEqPos))
                            sb.Append(string.Concat("'", data.OptEqPos, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //OptionFill
                        if (!string.IsNullOrEmpty(data.OptFill))
                            sb.Append(string.Concat("'", data.OptFill, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //RefIndex
                        if (!string.IsNullOrEmpty(data.RI))
                            sb.Append(string.Concat("'", data.RI, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //RefIndexLastPrice
                        if (data.RILastPr.HasValue)
                            sb.Append(data.RILastPr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //RefIndexBeta
                        if (data.RIBeta.HasValue)
                            sb.Append(data.RIBeta).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //RefIndexBetaInd
                        if (data.RIPrBetaShiftInd.HasValue)
                            sb.Append(data.RIPrBetaShiftInd).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //RefIndexPriceCap
                        if (data.RIPrCap.HasValue)
                            sb.Append(data.RIPrCap).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //RefIndexPriceInd
                        if (data.RIPrCapShiftInd.HasValue)
                            sb.Append(data.RIPrCapShiftInd).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //RefIndexPriceChng
                        if (data.RIPrChng.HasValue)
                            sb.Append(data.RIPrChng).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //TargetPrice
                        if (data.RITgtPr.HasValue)
                            sb.Append(data.RITgtPr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //DiscountTarget
                        if (data.DscntTgt.HasValue)
                            sb.Append(data.DscntTgt).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //DiscountTargetLastNav
                        if (data.DscntTgtLastNav.HasValue)
                            sb.Append(data.DscntTgtLastNav).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // RefIndexPriceBetaAdj
                        if (!string.IsNullOrEmpty(data.RIBetaAdjTyp))
                            sb.Append(string.Concat("'", data.RIBetaAdjTyp, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //RefIndexMaxPrice
                        if (data.RIMaxPr.HasValue)
                            sb.Append(data.RIMaxPr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //EstimatedNav
                        if (data.EstNav.HasValue)
                            sb.Append(data.EstNav).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //EstimatedDiscount
                        if (data.DscntToLastPr.HasValue)
                            sb.Append(data.DscntToLastPr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //RefIndexLivePrice
                        if (data.RILivePr.HasValue)
                            sb.Append(data.RILivePr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //RefIndexPriceType
                        if (!string.IsNullOrEmpty(data.RIPrTyp))
                            sb.Append(string.Concat("'", data.RIPrTyp, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //AutoUpdate
                        if (!string.IsNullOrEmpty(data.AutoUpdate))
                            sb.Append(string.Concat("'", data.AutoUpdate, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //AutoUpdateThreshold
                        if (data.AutoUpdateThld.HasValue)
                            sb.Append(data.AutoUpdateThld).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //OrderOrigPrice
                        if (data.OrdOrigPr.HasValue)
                            sb.Append(data.OrdOrigPr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //RefIndexPriceChngFinal
                        if (data.RIPrChngFinal.HasValue)
                            sb.Append(data.RIPrChngFinal).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //DiscountToLastPrice
                        if (data.DscntToLastPr.HasValue)
                            sb.Append(data.DscntToLastPr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //DiscountToLivePrice
                        if (data.DscntToLivePr.HasValue)
                            sb.Append(data.DscntToLivePr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //MarketPriceThreshold
                        if (data.MktPrThld.HasValue)
                            sb.Append(data.MktPrThld).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //MarketPriceSpread
                        if (data.MktPrSprd.HasValue)
                            sb.Append(data.MktPrSprd).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //CancelOrderId
                        if (!string.IsNullOrEmpty(data.CancOrdId))
                            sb.Append(string.Concat("'", data.CancOrdId, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //Last Price
                        if (data.LastPr.HasValue)
                            sb.Append(data.LastPr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //Bid Price
                        if (data.BidPr.HasValue)
                            sb.Append(data.BidPr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //Ask Price
                        if (data.AskPr.HasValue)
                            sb.Append(data.AskPr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //OrderIdRank
                        sb.Append(data.OrdIdRank).Append(DELIMITER);

                        //MarketPriceField
                        if (!string.IsNullOrEmpty(data.MktPrFld))
                            sb.Append(string.Concat("'", data.MktPrFld, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //EstNavType
                        if (!string.IsNullOrEmpty(data.EstNavTyp))
                            sb.Append(string.Concat("'", data.EstNavTyp, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //UpdateAlgoParams
                        if (!string.IsNullOrEmpty(data.UpdateAlgoParams))
                            sb.Append(string.Concat("'", data.UpdateAlgoParams, "'"));
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
                _logger.LogError(ex, "Error saving Trade Order details into database");
                throw;
            }
        }

        /// <summary>
        /// Saves Order Summary to Staging table (Simulation)
        /// </summary>
        /// <param name="orderSummaryDict"></param>
        public void SaveSimOrderSummaryStg(IDictionary<string, OrderSummary> orderSummaryDict)
        {
            try
            {
                StringBuilder sCommand = new StringBuilder("insert into " + SimOrderSummaryStgTableName
                    + " (AccountNumber, AccountName, ParentOrderId, MainOrderId, OrderId, TrackingId, ReferenceId, NeovestExecId,"
                    + " Symbol, BBGSymbol, ALMSymbol, OrderDate, OrderTime, OrderType, OrderSide, OrderQty, OrderExchange,"
                    + " OrderExchangeId, OrderPrice, OrderStopPrice, OrderStatus, OrderMemo, OrderDest, OrderBkrStrategy,"
                    + " PairTradeSpread, TradedQty, TradedPrice, AvgTradedPrice, TradedCumulativeQty, CanceledQty, LeavesQty, TradedMsg,"
                    + " OrderStatusUpdateTime, AlgoParameters, Trader, ALMTrader, ISIN, Sedol, Cusip, Currency, TradeSource,"
                    + " OptionPosition, OptionEquityPosition, OptionFill,"
                    + " RefIndex, RefIndexLastPrice, "
                    + " RefIndexBeta, RefIndexBetaInd, "
                    + " RefIndexPriceCap, RefIndexPriceInd, RefIndexPriceChng, TargetPrice,"
                    + " DiscountTarget, DiscountTargetLastNav,"
                    + " RefIndexPriceBetaAdj, RefIndexMaxPrice,"
                    + " EstimatedNav, EstimatedDiscount,"
                    + " RefIndexLivePrice, RefIndexPriceType, AutoUpdate, AutoUpdateThreshold,"
                    + " OrderOrigPrice, RefIndexPriceChngFinal, DiscountToLastPrice, DiscountToLivePrice,"
                    + " MarketPriceThreshold, MarketPriceSpread, CancelOrderId,"
                    + " LastPrice, BidPrice, AskPrice, OrderIdRank, MarketPriceField, EstNavType, UpdateAlgoParams"
                    + " ) values ");

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    IList<string> Rows = new List<string>();
                    StringBuilder sb = new StringBuilder();
                    //foreach (KeyValuePair<string, OrderSummary> kvp in orderSummaryDict)
                    foreach (OrderSummary data in orderSummaryDict.Values.ToList())
                    {
                        //OrderSummary data = kvp.Value;

                        //AccountNumber
                        if (data.AccNum.HasValue)
                            sb.Append(data.AccNum).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //AccountName
                        if (!string.IsNullOrEmpty(data.AccName))
                            sb.Append(string.Concat("'", data.AccName, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //ParentOrderId
                        if (!string.IsNullOrEmpty(data.ParOrdId))
                            sb.Append(string.Concat("'", data.ParOrdId, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //MainOrderId
                        if (!string.IsNullOrEmpty(data.MainOrdId))
                            sb.Append(string.Concat("'", data.MainOrdId, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //OrderId
                        if (!string.IsNullOrEmpty(data.OrdId))
                            sb.Append(string.Concat("'", data.OrdId, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //TrackingId
                        if (!string.IsNullOrEmpty(data.TrkId))
                            sb.Append(string.Concat("'", data.TrkId, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //ReferenceId
                        if (!string.IsNullOrEmpty(data.RefId))
                            sb.Append(string.Concat("'", data.RefId, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //NeovestExecId
                        if (data.NVExecIdLng.HasValue)
                            sb.Append(data.NVExecIdLng).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //Symbol
                        if (!string.IsNullOrEmpty(data.Sym))
                            sb.Append(string.Concat("'", data.Sym, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //BBGSymbol
                        if (!string.IsNullOrEmpty(data.BBGSym))
                            sb.Append(string.Concat("'", data.BBGSym, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //ALMSymbol
                        if (!string.IsNullOrEmpty(data.ALMSym))
                            sb.Append(string.Concat("'", data.ALMSym, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //OrderDate
                        if (data.OrdDt.HasValue)
                            sb.Append(string.Concat("'", DateUtils.ConvertDate(data.OrdDt, "yyyy-MM-dd"), "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //OrderTime
                        if (!string.IsNullOrEmpty(data.OrdTm))
                            sb.Append(string.Concat("'", data.OrdTm, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //OrderType
                        if (!string.IsNullOrEmpty(data.OrdTyp))
                            sb.Append(string.Concat("'", data.OrdTyp, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //OrderSide
                        if (!string.IsNullOrEmpty(data.OrdSide))
                            sb.Append(string.Concat("'", data.OrdSide, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //OrderQty
                        if (data.OrdQty.HasValue)
                            sb.Append(data.OrdQty).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //OrderExchange
                        if (!string.IsNullOrEmpty(data.OrdExch))
                            sb.Append(string.Concat("'", data.OrdExch, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //OrderExchangeId
                        if (data.OrdExchId.HasValue)
                            sb.Append(data.OrdExchId).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //OrderPrice
                        if (data.OrdPr.HasValue)
                            sb.Append(data.OrdPr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //OrderStopPrice
                        if (data.OrdStopPr.HasValue)
                            sb.Append(data.OrdStopPr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //OrderStatus
                        if (!string.IsNullOrEmpty(data.OrdSt))
                            sb.Append(string.Concat("'", data.OrdSt, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //OrderMemo
                        if (!string.IsNullOrEmpty(data.OrdMemo))
                            sb.Append(string.Concat("'", data.OrdMemo, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //OrderDest
                        if (!string.IsNullOrEmpty(data.OrdDest))
                            sb.Append(string.Concat("'", data.OrdDest, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //OrderBkrStrategy
                        if (!string.IsNullOrEmpty(data.OrdBkrStrat))
                            sb.Append(string.Concat("'", data.OrdBkrStrat, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //PairTradeSpread
                        if (data.PairTrdSprd.HasValue)
                            sb.Append(data.PairTrdSprd).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //TradedQty
                        if (data.TrdQty.HasValue)
                            sb.Append(data.TrdQty).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //TradedPrice
                        if (data.TrdPr.HasValue)
                            sb.Append(data.TrdPr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //AvgTradedPrice
                        if (data.AvgTrdPr.HasValue)
                            sb.Append(data.AvgTrdPr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //TradedCumulativeQty
                        if (data.TrdCumQty.HasValue)
                            sb.Append(data.TrdCumQty).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //CanceledQty
                        if (data.CancQty.HasValue)
                            sb.Append(data.CancQty).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //LeavesQty
                        if (data.LeavesQty.HasValue)
                            sb.Append(data.LeavesQty).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //TradedMsg
                        if (!string.IsNullOrEmpty(data.TrdMsg))
                            sb.Append(string.Concat("'", MySqlHelper.EscapeString(data.TrdMsg), "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //OrderStatusUpdateTime
                        if (!string.IsNullOrEmpty(data.OrdStatusUpdTm))
                            sb.Append(string.Concat("'", data.OrdStatusUpdTm, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //AlgoParameters
                        if (!string.IsNullOrEmpty(data.AlgoParameters))
                            sb.Append(string.Concat("'", data.AlgoParameters, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //Trader
                        if (!string.IsNullOrEmpty(data.Trader))
                            sb.Append(string.Concat("'", data.Trader, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //ALMTrader
                        if (!string.IsNullOrEmpty(data.ALMTrader))
                            sb.Append(string.Concat("'", data.ALMTrader, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //ISIN
                        if (!string.IsNullOrEmpty(data.ISIN))
                            sb.Append(string.Concat("'", data.ISIN, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //Sedol
                        if (!string.IsNullOrEmpty(data.Sedol))
                            sb.Append(string.Concat("'", data.Sedol, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //Cusip
                        if (!string.IsNullOrEmpty(data.Cusip))
                            sb.Append(string.Concat("'", data.Cusip, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //Currency
                        if (!string.IsNullOrEmpty(data.Curr))
                            sb.Append(string.Concat("'", data.Curr, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //TradeSource
                        if (!string.IsNullOrEmpty(data.TrdSrc))
                            sb.Append(string.Concat("'", data.TrdSrc, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //OptionPosition
                        if (!string.IsNullOrEmpty(data.OptPos))
                            sb.Append(string.Concat("'", data.OptPos, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //OptionEquityPosition
                        if (!string.IsNullOrEmpty(data.OptEqPos))
                            sb.Append(string.Concat("'", data.OptEqPos, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //OptionFill
                        if (!string.IsNullOrEmpty(data.OptFill))
                            sb.Append(string.Concat("'", data.OptFill, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //RefIndex
                        if (!string.IsNullOrEmpty(data.RI))
                            sb.Append(string.Concat("'", data.RI, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //RefIndexLastPrice
                        if (data.RILastPr.HasValue)
                            sb.Append(data.RILastPr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //RefIndexBeta
                        if (data.RIBeta.HasValue)
                            sb.Append(data.RIBeta).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //RefIndexBetaInd
                        if (data.RIPrBetaShiftInd.HasValue)
                            sb.Append(data.RIPrBetaShiftInd).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //RefIndexPriceCap
                        if (data.RIPrCap.HasValue)
                            sb.Append(data.RIPrCap).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //RefIndexPriceInd
                        if (data.RIPrCapShiftInd.HasValue)
                            sb.Append(data.RIPrCapShiftInd).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //RefIndexPriceChng
                        if (data.RIPrChng.HasValue)
                            sb.Append(data.RIPrChng).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //TargetPrice
                        if (data.RITgtPr.HasValue)
                            sb.Append(data.RITgtPr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //DiscountTarget
                        if (data.DscntTgt.HasValue)
                            sb.Append(data.DscntTgt).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //DiscountTargetLastNav
                        if (data.DscntTgtLastNav.HasValue)
                            sb.Append(data.DscntTgtLastNav).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // RefIndexPriceBetaAdj
                        if (!string.IsNullOrEmpty(data.RIBetaAdjTyp))
                            sb.Append(string.Concat("'", data.RIBetaAdjTyp, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //RefIndexMaxPrice
                        if (data.RIMaxPr.HasValue)
                            sb.Append(data.RIMaxPr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //EstimatedNav
                        if (data.EstNav.HasValue)
                            sb.Append(data.EstNav).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //EstimatedDiscount
                        if (data.DscntToLastPr.HasValue)
                            sb.Append(data.DscntToLastPr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //RefIndexLivePrice
                        if (data.RILivePr.HasValue)
                            sb.Append(data.RILivePr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //RefIndexPriceType
                        if (!string.IsNullOrEmpty(data.RIPrTyp))
                            sb.Append(string.Concat("'", data.RIPrTyp, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //AutoUpdate
                        if (!string.IsNullOrEmpty(data.AutoUpdate))
                            sb.Append(string.Concat("'", data.AutoUpdate, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //AutoUpdateThreshold
                        if (data.AutoUpdateThld.HasValue)
                            sb.Append(data.AutoUpdateThld).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //OrderOrigPrice
                        if (data.OrdOrigPr.HasValue)
                            sb.Append(data.OrdOrigPr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //RefIndexPriceChngFinal
                        if (data.RIPrChngFinal.HasValue)
                            sb.Append(data.RIPrChngFinal).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //DiscountToLastPrice
                        if (data.DscntToLastPr.HasValue)
                            sb.Append(data.DscntToLastPr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //DiscountToLivePrice
                        if (data.DscntToLivePr.HasValue)
                            sb.Append(data.DscntToLivePr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //MarketPriceThreshold
                        if (data.MktPrThld.HasValue)
                            sb.Append(data.MktPrThld).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //MarketPriceSpread
                        if (data.MktPrSprd.HasValue)
                            sb.Append(data.MktPrSprd).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //CancelOrderId
                        if (!string.IsNullOrEmpty(data.CancOrdId))
                            sb.Append(string.Concat("'", data.CancOrdId, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //Last Price
                        if (data.LastPr.HasValue)
                            sb.Append(data.LastPr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //Bid Price
                        if (data.BidPr.HasValue)
                            sb.Append(data.BidPr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //Ask Price
                        if (data.AskPr.HasValue)
                            sb.Append(data.AskPr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //OrderIdRank
                        sb.Append(data.OrdIdRank).Append(DELIMITER);

                        //MarketPricField
                        if (!string.IsNullOrEmpty(data.MktPrFld))
                            sb.Append(string.Concat("'", data.MktPrFld, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //EstNavType
                        if (!string.IsNullOrEmpty(data.EstNavTyp))
                            sb.Append(string.Concat("'", data.EstNavTyp, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //UpdateAlgoParams
                        if (!string.IsNullOrEmpty(data.UpdateAlgoParams))
                            sb.Append(string.Concat("'", data.UpdateAlgoParams, "'"));
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
                _logger.LogError(ex, "Error saving Stg Trade Order details into database");
                throw;
            }
        }

        /// <summary>
        /// Saves Order Queue to Staging table
        /// </summary>
        /// <param name="orderSummaryDict"></param>
        public void SaveOrderQueueStg(IDictionary<string, OrderSummary> orderSummaryDict, string tableName)
        {
            try
            {
                StringBuilder sCommand = new StringBuilder("insert into " + tableName
                    + " (RefId, Symbol, OrderDate, OrderTime, OrderType, OrderSide, OrderQty,"
                    + " OrderPrice, OrderStopPrice, OrderOrigPrice, OrderStatus, OrderDest, OrderBkrStrategy,"
                    + " AccountName, Locate, Trader, ALMTrader, AlgoParameters,"
                    + " RefIndex, RefIndexBeta, RefIndexBetaInd, RefIndexPriceBetaAdj, RefIndexLastPrice,"
                    + " RefIndexLivePrice, RefIndexPriceCap, RefIndexPriceInd, RefIndexPriceChng, RefIndexPriceChngFinal,"
                    + " RefIndexMaxPrice, RefIndexPriceType, TargetPrice, DiscountTarget, DiscountToLastPrice,"
                    + " DiscountToBidPrice, DiscountToAskPrice, EstNavType, EstNav, MktPriceThreshold,"
                    + " MktPriceSpread, MktPriceField, LastPrice, BidPrice, AskPrice, ActionType"
                    + " ) values ");

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    IList<string> Rows = new List<string>();
                    StringBuilder sb = new StringBuilder();
                    foreach (OrderSummary data in orderSummaryDict.Values.ToList())
                    {
                        //ReferenceId
                        if (!string.IsNullOrEmpty(data.RefId))
                            sb.Append(string.Concat("'", data.RefId, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //Symbol
                        if (!string.IsNullOrEmpty(data.Sym))
                            sb.Append(string.Concat("'", data.Sym, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //OrderDate
                        if (data.OrdDt.HasValue)
                            sb.Append(string.Concat("'", DateUtils.ConvertDate(data.OrdDt, "yyyy-MM-dd"), "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //OrderTime
                        if (!string.IsNullOrEmpty(data.OrdTm))
                            sb.Append(string.Concat("'", data.OrdTm, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //OrderType
                        if (!string.IsNullOrEmpty(data.OrdTyp))
                            sb.Append(string.Concat("'", data.OrdTyp, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //OrderSide
                        if (!string.IsNullOrEmpty(data.OrdSide))
                            sb.Append(string.Concat("'", data.OrdSide, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //OrderQty
                        if (data.OrdQty.HasValue)
                            sb.Append(data.OrdQty).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //OrderPrice
                        if (data.OrdPr.HasValue)
                            sb.Append(data.OrdPr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //OrderStopPrice
                        if (data.OrdStopPr.HasValue)
                            sb.Append(data.OrdStopPr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //OrderOrigPrice
                        if (data.OrdOrigPr.HasValue)
                            sb.Append(data.OrdOrigPr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //OrderStatus
                        if (!string.IsNullOrEmpty(data.OrdSt))
                            sb.Append(string.Concat("'", data.OrdSt, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //OrderDest
                        if (!string.IsNullOrEmpty(data.OrdDest))
                            sb.Append(string.Concat("'", data.OrdDest, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //OrderBkrStrategy
                        if (!string.IsNullOrEmpty(data.OrdBkrStrat))
                            sb.Append(string.Concat("'", data.OrdBkrStrat, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //AccountName
                        if (!string.IsNullOrEmpty(data.AccName))
                            sb.Append(string.Concat("'", data.AccName, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //Locate
                        if (!string.IsNullOrEmpty(data.Locate))
                            sb.Append(string.Concat("'", data.Locate, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //Trader
                        if (!string.IsNullOrEmpty(data.Trader))
                            sb.Append(string.Concat("'", data.Trader, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //ALMTrader
                        if (!string.IsNullOrEmpty(data.ALMTrader))
                            sb.Append(string.Concat("'", data.ALMTrader, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //AlgoParameters
                        if (!string.IsNullOrEmpty(data.AlgoParameters))
                            sb.Append(string.Concat("'", data.AlgoParameters, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //RefIndex
                        if (!string.IsNullOrEmpty(data.RI))
                            sb.Append(string.Concat("'", data.RI, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //RefIndexBeta
                        if (data.RIBeta.HasValue)
                            sb.Append(data.RIBeta).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //RefIndexBetaInd
                        if (data.RIPrBetaShiftInd.HasValue)
                            sb.Append(data.RIPrBetaShiftInd).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //RefIndexPriceBetaAdj
                        if (!string.IsNullOrEmpty(data.RIBetaAdjTyp))
                            sb.Append(string.Concat("'", data.RIBetaAdjTyp, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //RefIndexLastPrice
                        if (data.RILastPr.HasValue)
                            sb.Append(data.RILastPr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //RefIndexLivePrice
                        if (data.RILivePr.HasValue)
                            sb.Append(data.RILivePr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //RefIndexPriceCap
                        if (data.RIPrCap.HasValue)
                            sb.Append(data.RIPrCap).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //RefIndexPriceInd
                        if (data.RIPrCapShiftInd.HasValue)
                            sb.Append(data.RIPrCapShiftInd).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //RefIndexPriceChng
                        if (data.RIPrChng.HasValue)
                            sb.Append(data.RIPrChng).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //RefIndexPriceChngFinal
                        if (data.RIPrChngFinal.HasValue)
                            sb.Append(data.RIPrChngFinal).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // RefIndexMaxPrice
                        if (data.RIMaxPr.HasValue)
                            sb.Append(data.RIMaxPr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //RefIndexPriceType
                        if (!string.IsNullOrEmpty(data.RIPrTyp))
                            sb.Append(string.Concat("'", data.RIPrTyp, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //TargetPrice
                        if (data.RITgtPr.HasValue)
                            sb.Append(data.RITgtPr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //DiscountTarget
                        if (data.DscntTgt.HasValue)
                            sb.Append(data.DscntTgt).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //DiscountToLastPrice
                        if (data.DscntToLastPr.HasValue)
                            sb.Append(data.DscntToLastPr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //DiscountToBidPrice
                        if (data.DscntToBidPr.HasValue)
                            sb.Append(data.DscntToBidPr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //DiscountToAskPrice
                        if (data.DscntToAskPr.HasValue)
                            sb.Append(data.DscntToAskPr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //EstNavType
                        if (!string.IsNullOrEmpty(data.EstNavTyp))
                            sb.Append(string.Concat("'", data.EstNavTyp, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //EstNav
                        if (data.EstNav.HasValue)
                            sb.Append(data.EstNav).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //MktPriceThreshold
                        if (data.MktPrThld.HasValue)
                            sb.Append(data.MktPrThld).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //MktPriceSpread
                        if (data.MktPrSprd.HasValue)
                            sb.Append(data.MktPrSprd).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //MktPriceField
                        if (!string.IsNullOrEmpty(data.MktPrFld))
                            sb.Append(string.Concat("'", data.MktPrFld, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //LastPrice
                        if (data.LastPr.HasValue)
                            sb.Append(data.LastPr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //BidPrice
                        if (data.BidPr.HasValue)
                            sb.Append(data.BidPr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //AskPrice
                        if (data.AskPr.HasValue)
                            sb.Append(data.AskPr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        //ActionType
                        if (!string.IsNullOrEmpty(data.Action))
                            sb.Append(string.Concat("'", data.Action, "'"));
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
                _logger.LogError(ex, "Error saving Order Queue into database");
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
        /// Get Order Summary (simulated orders)
        /// </summary>
        /// <returns></returns>
        public IDictionary<string, OrderSummary> GetSimOrderSummary()
        {
            IDictionary<string, OrderSummary> orderSummaryDict = new Dictionary<string, OrderSummary>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetSimOrderSummaryQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                OrderSummary data = new OrderSummary
                                {
                                    AccNum = (reader.IsDBNull(reader.GetOrdinal("AccountNumber"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("AccountNumber")),
                                    AccName = reader["AccountName"] as string,
                                    ParOrdId = reader["ParentOrderId"] as string,
                                    MainOrdId = reader["MainOrderId"] as string,
                                    OrdId = reader["OrderId"] as string,
                                    CancOrdId = reader["CancelOrderId"] as string,
                                    TrkId = reader["TrackingId"] as string,
                                    RefId = reader["ReferenceId"] as string,
                                    NVExecIdLng = (reader.IsDBNull(reader.GetOrdinal("NeovestExecId"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("NeovestExecId")),
                                    Sym = reader["Symbol"] as string,
                                    BBGSym = reader["BBGSymbol"] as string,
                                    ALMSym = reader["ALMSymbol"] as string,
                                    ISIN = reader["ISIN"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    Cusip = reader["Cusip"] as string,
                                    Curr = reader["Currency"] as string,
                                    OrdDt = reader.IsDBNull(reader.GetOrdinal("OrderDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("OrderDate")),
                                    OrdTm = reader["OrderTime"] as string,
                                    OrdTyp = reader["OrderType"] as string,
                                    OrdSide = reader["OrderSide"] as string,
                                    OrdQty = (reader.IsDBNull(reader.GetOrdinal("OrderQty"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("OrderQty")),
                                    OrdExch = reader["OrderExchange"] as string,
                                    OrdExchId = (reader.IsDBNull(reader.GetOrdinal("OrderExchangeId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("OrderExchangeId")),
                                    OrdPr = (reader.IsDBNull(reader.GetOrdinal("OrderPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OrderPrice")),
                                    OrdStopPr = (reader.IsDBNull(reader.GetOrdinal("OrderStopPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OrderStopPrice")),
                                    OrdOrigPr = (reader.IsDBNull(reader.GetOrdinal("OrderOrigPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OrderOrigPrice")),
                                    OrdSt = reader["OrderStatus"] as string,
                                    OrdMemo = reader["OrderMemo"] as string,
                                    OrdDest = reader["OrderDest"] as string,
                                    OrdBkrStrat = reader["OrderBkrStrategy"] as string,
                                    PairTrdSprd = (reader.IsDBNull(reader.GetOrdinal("PairTradeSpread"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PairTradeSpread")),
                                    Trader = reader["Trader"] as string,
                                    ALMTrader = reader["ALMTrader"] as string,

                                    //Status
                                    TrdQty = (reader.IsDBNull(reader.GetOrdinal("TradedQty"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("TradedQty")),
                                    TrdPr = (reader.IsDBNull(reader.GetOrdinal("TradedPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TradedPrice")),
                                    AvgTrdPr = (reader.IsDBNull(reader.GetOrdinal("AvgTradedPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AvgTradedPrice")),
                                    TrdCumQty = (reader.IsDBNull(reader.GetOrdinal("TradedCumulativeQty"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("TradedCumulativeQty")),
                                    CancQty = (reader.IsDBNull(reader.GetOrdinal("CanceledQty"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("CanceledQty")),
                                    LeavesQty = (reader.IsDBNull(reader.GetOrdinal("LeavesQty"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("LeavesQty")),
                                    TrdMsg = reader["TradedMsg"] as string,
                                    OrdStatusUpdTm = reader["OrderStatusUpdateTime"] as string,
                                    AlgoParameters = reader["AlgoParameters"] as string,

                                    //Options
                                    OptPos = reader["OptionPosition"] as string,
                                    OptEqPos = reader["OptionEquityPosition"] as string,
                                    OptFill = reader["OptionFill"] as string,

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
                                };

                                data.Key = data.Sym + "|" + data.OrdSide;
                                data.OrdDtAsString = DateUtils.ConvertDate(data.OrdDt, "yyyy-MM-dd");

                                //Check for Order Status
                                if (!string.IsNullOrEmpty(data.OrdSt))
                                {
                                    if (data.OrdSt.Equals("Canceled")
                                        || data.OrdSt.Equals("Canceled/ Partial")
                                        || data.OrdSt.Equals("Completed")
                                        || data.OrdSt.Equals("Executed")
                                        || data.OrdSt.Equals("Rejected")
                                        || data.OrdSt.Equals("Replace Rejected")
                                        )
                                    {
                                        data.OrdActFlag = "N";
                                    }
                                }

                                if (!orderSummaryDict.ContainsKey(data.OrdId))
                                    orderSummaryDict.Add(data.OrdId, data);
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

            return orderSummaryDict;
        }

        /// <summary>
        /// Saves Order Summary (simulation orders)
        /// </summary>
        /// <param name="orderSummaryDict"></param>
        public void SaveSimOrderSummary(IDictionary<string, OrderSummary> orderSummaryDict)
        {
            try
            {
                TruncateTable(SimOrderSummaryStgTableName);
                SaveSimOrderSummaryStg(orderSummaryDict);
                MoveDataToTargetTable(SaveSimOrderSummaryQuery);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving data");
            }
        }

        /// <summary>
        /// Saves Batch Orders
        /// </summary>
        /// <param name="orderList"></param>
        public void SaveBatchOrders(IList<NewOrder> orderList)
        {
            try
            {
                TruncateTable(BatchOrdersStgTableName);
                SaveBatchOrdersStg(orderList, BatchOrdersStgTableName);
                MoveDataToTargetTable(SaveBatchOrdersQuery);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving data");
            }
        }

        /// <summary>
        /// Saves Batch Orders to staging table
        /// </summary>
        /// <param name="orderList"></param>
        public void SaveBatchOrdersStg(IList<NewOrder> orderList, string tableName)
        {
            try
            {
                StringBuilder sCommand = new StringBuilder("insert into " + tableName
                    + " (ActionType, Id, MainOrderId, OrderId,"
                    + " Ticker, Sedol, OrderSide, OrderType, OrderPrice, NewOrderPrice,"
                    + " OrderLimitPrice, OrderStopPrice, OrderQuantity, NewOrderQuantity, OrderExpiration,"
                    + " AccountId, AccountName, ExchangeId, Destination, BrokerStrategy, Trader, Environment,"
                    + " Comments, AlgoParameters, Locate, OptionPosition, OptionEquityPosition, OptionFill,"
                    + " RefIndex, RefIndexPrice, RefIndexBeta, RefIndexBetaInd, RefIndexPriceBetaAdj,"
                    + " RefIndexPriceCap, RefIndexPriceInd, RefIndexMaxPrice, DiscountTarget,"
                    + " AutoUpdate, AutoUpdateThreshold, RefIndexPriceType, OrderExpire, NeovestSymbol,"
                    + " ParentId, PairRatio, PairSpread, MarketPriceThreshold, OrigOrderPrice, MarketPriceField,"
                    + " EstNavType, UpdateAlgoParams)"
                    + " values ");

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    IList<string> Rows = new List<string>();
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
                            sb.Append(string.Concat("'", order.EstNavType, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // UpdateAlgoParams
                        if (!string.IsNullOrEmpty(order.UpdateAlgoParams))
                            sb.Append(string.Concat("'", order.UpdateAlgoParams, "'"));
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
                _logger.LogError(ex, "Error saving Batch Order details");
                throw;
            }
        }

        /// <summary>
        /// Gets Batch Orders
        /// </summary>
        /// <returns></returns>
        public IList<NewOrder> GetBatchOrders()
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

        /// <summary>
        /// Gets Batch Order Templates
        /// </summary>
        /// <returns></returns>
        public IList<string> GetBatchOrderTemplates()
        {
            IList<string> list = new List<string>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetBatchOrderTemplatesQuery, connection))
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
        /// Gets Sample Batch Order Templates
        /// </summary>
        /// <returns></returns>
        public IList<string> GetSampleBatchOrderTemplates()
        {
            IList<string> list = new List<string>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetSampleBatchOrderTemplatesQuery, connection))
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
        /// Gets Batch Order Template for selected template
        /// </summary>
        /// <param name="templateName"></param>
        /// <returns></returns>
        public IList<BatchOrderTemplate> GetBatchOrderTemplate(string templateName)
        {
            IList<BatchOrderTemplate> list = new List<BatchOrderTemplate>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    string sql = GetBatchOrderTemplateQuery + " where TemplateName = '" + templateName + "' order by RowId asc";

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
                                data.APAggression = reader["APAggression"] as string;
                                data.APAggressiveness = reader["APAggressiveness"] as string;
                                data.APMinQty = (reader.IsDBNull(reader.GetOrdinal("APMinQty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("APMinQty"));

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

        /// <summary>
        /// Saves Batch Order Templates
        /// </summary>
        /// <param name="list"></param>
        public void SaveBatchOrderTemplate(IList<BatchOrderTemplate> list)
        {
            StringBuilder sCommand = new StringBuilder("INSERT INTO almitasc_ACTradingBBGData.BatchOrderTemplate"
                + " (TemplateName, Symbol, OrderSide, OrderType, AccountNumber, Destination, BrokerStrategy,"
                + " Qty, TotalTradeValue, PriceTarget, DiscountTarget, ChooseNav, NavOvr,"
                + " APExpire, APStartDate, APEndDate, APUrgency, APMaxPctVol, APWouldPrice, APMinDarkFillSize,"
                + " APDisplayMode, APDisplaySize, APLiquiditySeek, APSeekPrice, APSeekMinQty, APPctOfVolume, Locate,"
                + " RefIndex, RefIndexPriceBeta, RefIndexPriceCap, RefIndexPriceBetaShiftInd, RefIndexPriceCapShiftInd,"
                + " RefIndexPriceBetaAdj, DiscountTargetAdj, RefIndexMaxPrice, OptionPosition, OptionEquityPosition, OptionFill,"
                + " RefIndexPriceType, OrderExpire, SampleTemplate,"
                + " APTradingStyle, APTradingSession, APSORPreference, APSORSessionPreference, RefIndexPrice, RowId,"
                + " IsPairTrade, PairRatio, PairSpread,"
                + " ActionFlag, AutoUpdateFlag, AutoUpdatePriceThreshold, MarketPriceThreshold, MarketPriceField, APStyle, EstNavType,"
                + " APAggression, APAggressiveness, APMinQty"
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
                            _logger.LogInformation("Deleting data from almitasc_ACTradingBBGData.BatchOrderTemplate where TemplateName = '" + templateName + "'");
                            string sqlDelete = "delete from almitasc_ACTradingBBGData.BatchOrderTemplate where TemplateName = '" + templateName + "'";
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
                                    sb.Append(string.Concat("'", data.EstNavType, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // APAggression
                                if (!string.IsNullOrEmpty(data.APAggression))
                                    sb.Append(string.Concat("'", data.APAggression, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // APAggressiveness
                                if (!string.IsNullOrEmpty(data.APAggressiveness))
                                    sb.Append(string.Concat("'", data.APAggressiveness, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // APMinQty
                                if (data.APMinQty.HasValue)
                                    sb.Append(data.APMinQty);
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
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving data");
            }
        }

        /// <summary>
        /// Gets Completed Orders
        /// </summary>
        /// <param name="date"></param>
        /// <returns></returns>
        public IList<OrderSummary> GetCompletedOrders(DateTime date)
        {
            IList<OrderSummary> list = new List<OrderSummary>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetCompletedOrdersQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Date", date);
                        command.Parameters.AddWithValue("p_Flag", "Allocation");

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                OrderSummary data = new OrderSummary
                                {
                                    OrdDt = reader.IsDBNull(reader.GetOrdinal("OrderDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("OrderDate")),
                                    Sym = reader["Symbol"] as string,
                                    BBGSym = reader["BBGSymbol"] as string,
                                    ALMSym = reader["ALMSymbol"] as string,
                                    ISIN = reader["ISIN"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    FIGI = reader["FIGI"] as string,
                                    Curr = reader["Currency"] as string,
                                    Trader = reader["Trader"] as string,
                                    TrdSrc = reader["TradeSource"] as string,
                                    OrdSide = reader["OrderSide"] as string,
                                    TrdCumQty = (reader.IsDBNull(reader.GetOrdinal("TradedCumulativeQty"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("TradedCumulativeQty")),
                                    OrdTm = reader["OrderTime"] as string,
                                    OrdStatusUpdTm = reader["OrderStatusUpdateTime"] as string,
                                    AvgTrdPr = (reader.IsDBNull(reader.GetOrdinal("AvgTradedPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AvgTradedPrice")),
                                    OrdDest = reader["OrderDest"] as string,
                                    IsSwap = reader["Swap"] as string,
                                };

                                data.OrdDtAsString = DateUtils.ConvertDate(data.OrdDt, "yyyy-MM-dd");
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
        /// Gets Completed Orders
        /// </summary>
        /// <param name="date"></param>
        /// <returns></returns>
        public IList<OrderSummary> GetAllCompletedOrders(DateTime date)
        {
            IList<OrderSummary> list = new List<OrderSummary>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetCompletedOrdersQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Date", date);
                        command.Parameters.AddWithValue("p_Flag", "All Lots");

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                OrderSummary data = new OrderSummary
                                {
                                    ParOrdId = reader["ParentOrderId"] as string,
                                    MainOrdId = reader["MainOrderId"] as string,
                                    OrdId = reader["OrderId"] as string,
                                    RefId = reader["ReferenceId"] as string,
                                    Sym = reader["Symbol"] as string,
                                    BBGSym = reader["BBGSymbol"] as string,
                                    ALMSym = reader["ALMSymbol"] as string,
                                    OrdDt = reader.IsDBNull(reader.GetOrdinal("OrderDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("OrderDate")),
                                    OrdTm = reader["OrderTime"] as string,
                                    OrdTyp = reader["OrderType"] as string,
                                    OrdSide = reader["OrderSide"] as string,
                                    OrdQty = (reader.IsDBNull(reader.GetOrdinal("OrderQty"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("OrderQty")),
                                    OrdExch = reader["OrderExchange"] as string,
                                    OrdExchId = (reader.IsDBNull(reader.GetOrdinal("OrderExchangeId"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("OrderExchangeId")),
                                    OrdPr = (reader.IsDBNull(reader.GetOrdinal("OrderPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OrderPrice")),
                                    OrdStopPr = (reader.IsDBNull(reader.GetOrdinal("OrderStopPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OrderStopPrice")),
                                    OrdSt = reader["OrderStatus"] as string,
                                    OrdMemo = reader["OrderMemo"] as string,
                                    OrdDest = reader["OrderDest"] as string,
                                    OrdBkrStrat = reader["OrderBkrStrategy"] as string,
                                    Trader = reader["Trader"] as string,
                                    AlgoParameters = reader["AlgoParameters"] as string,
                                    PairTrdSprd = (reader.IsDBNull(reader.GetOrdinal("PairTradeSpread"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PairTradeSpread")),
                                    ISIN = reader["ISIN"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    Curr = reader["Currency"] as string,

                                    //Status
                                    TrdQty = (reader.IsDBNull(reader.GetOrdinal("TradedQty"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("TradedQty")),
                                    TrdCumQty = (reader.IsDBNull(reader.GetOrdinal("TradedCumulativeQty"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("TradedCumulativeQty")),
                                    CancQty = (reader.IsDBNull(reader.GetOrdinal("CanceledQty"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("CanceledQty")),
                                    LeavesQty = (reader.IsDBNull(reader.GetOrdinal("LeavesQty"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("LeavesQty")),
                                    TrdPr = (reader.IsDBNull(reader.GetOrdinal("TradedPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TradedPrice")),
                                    AvgTrdPr = (reader.IsDBNull(reader.GetOrdinal("AvgTradedPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AvgTradedPrice")),
                                    TrdMsg = reader["TradedMsg"] as string,
                                    OrdStatusUpdTm = reader["OrderStatusUpdateTime"] as string
                                };

                                data.OrdDtAsString = DateUtils.ConvertDate(data.OrdDt, "yyyy-MM-dd");
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
        /// Saves Order Execution Details to staging table
        /// </summary>
        /// <param name="orderExecutionDetailsDict"></param>
        public void SaveOrderExecutionsStg(IDictionary<string, OrderSummary> orderExecutionDetailsDict)
        {
            try
            {
                StringBuilder sCommand = new StringBuilder("insert into " + OrderExecutionsStgTableName
                    + " (AccountNumber, AccountName, ParentOrderId, MainOrderId, OrderId, TrackingId, ReferenceId,NeovestExecId,"
                    + " Symbol, BBGSymbol, ALMSymbol, OrderDate, OrderTime, OrderType, OrderSide, OrderQty, OrderExchange,"
                    + " OrderExchangeId, OrderPrice, OrderStopPrice, OrderStatus, OrderMemo, OrderDest, OrderBkrStrategy,"
                    + " PairTradeSpread, TradedQty, TradedPrice, AvgTradedPrice, TradedCumulativeQty, CanceledQty, LeavesQty, TradedMsg,"
                    + " OrderStatusUpdateTime, AlgoParameters, Trader, ISIN, Sedol, Cusip, Currency, ExecutionId, NVExecutionId, "
                    + " DscntLive, DscntFillPrice, EstNav, LastPrice, BidPrice, AskPrice"
                    + " ) values ");

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    IList<string> Rows = new List<string>();
                    StringBuilder sb = new StringBuilder();
                    foreach (KeyValuePair<string, OrderSummary> kvp in orderExecutionDetailsDict)
                    {
                        OrderSummary data = kvp.Value;

                        // AccountNumber
                        if (data.AccNum.HasValue)
                            sb.Append(data.AccNum).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // AccountName
                        if (!string.IsNullOrEmpty(data.AccName))
                            sb.Append(string.Concat("'", data.AccName, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // ParentOrderId
                        if (!string.IsNullOrEmpty(data.ParOrdId))
                            sb.Append(string.Concat("'", data.ParOrdId, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // MainOrderId
                        if (!string.IsNullOrEmpty(data.MainOrdId))
                            sb.Append(string.Concat("'", data.MainOrdId, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // OrderId
                        if (!string.IsNullOrEmpty(data.OrdId))
                            sb.Append(string.Concat("'", data.OrdId, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // TrackingId
                        if (!string.IsNullOrEmpty(data.TrkId))
                            sb.Append(string.Concat("'", data.TrkId, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // ReferenceId
                        if (!string.IsNullOrEmpty(data.RefId))
                            sb.Append(string.Concat("'", data.RefId, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // NeovestExecId
                        if (data.NVExecIdLng.HasValue)
                            sb.Append(data.NVExecIdLng).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // Symbol
                        if (!string.IsNullOrEmpty(data.Sym))
                            sb.Append(string.Concat("'", data.Sym, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // BBGSymbol
                        if (!string.IsNullOrEmpty(data.BBGSym))
                            sb.Append(string.Concat("'", data.BBGSym, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // ALMSymbol
                        if (!string.IsNullOrEmpty(data.ALMSym))
                            sb.Append(string.Concat("'", data.ALMSym, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // OrderDate
                        if (data.OrdDt.HasValue)
                            sb.Append(string.Concat("'", DateUtils.ConvertDate(data.OrdDt, "yyyy-MM-dd"), "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // OrderTime
                        if (!string.IsNullOrEmpty(data.OrdTm))
                            sb.Append(string.Concat("'", data.OrdTm, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // OrderType
                        if (!string.IsNullOrEmpty(data.OrdTyp))
                            sb.Append(string.Concat("'", data.OrdTyp, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // OrderSide
                        if (!string.IsNullOrEmpty(data.OrdSide))
                            sb.Append(string.Concat("'", data.OrdSide, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // OrderQty
                        if (data.OrdQty.HasValue)
                            sb.Append(data.OrdQty).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // OrderExchange
                        if (!string.IsNullOrEmpty(data.OrdExch))
                            sb.Append(string.Concat("'", data.OrdExch, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // OrderExchangeId
                        if (data.OrdExchId.HasValue)
                            sb.Append(data.OrdExchId).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // OrderPrice
                        if (data.OrdPr.HasValue)
                            sb.Append(data.OrdPr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // OrderStopPrice
                        if (data.OrdStopPr.HasValue)
                            sb.Append(data.OrdStopPr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // OrderStatus
                        if (!string.IsNullOrEmpty(data.OrdSt))
                            sb.Append(string.Concat("'", data.OrdSt, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // OrderMemo
                        if (!string.IsNullOrEmpty(data.OrdMemo))
                            sb.Append(string.Concat("'", data.OrdMemo, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // OrderDest
                        if (!string.IsNullOrEmpty(data.OrdDest))
                            sb.Append(string.Concat("'", data.OrdDest, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // OrderBkrStrategy
                        if (!string.IsNullOrEmpty(data.OrdBkrStrat))
                            sb.Append(string.Concat("'", data.OrdBkrStrat, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // PairTradeSpread
                        if (data.PairTrdSprd.HasValue)
                            sb.Append(data.PairTrdSprd).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // TradedQty
                        if (data.TrdQty.HasValue)
                            sb.Append(data.TrdQty).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // TradedPrice
                        if (data.TrdPr.HasValue)
                            sb.Append(data.TrdPr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // AvgTradedPrice
                        if (data.AvgTrdPr.HasValue)
                            sb.Append(data.AvgTrdPr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // TradedCumulativeQty
                        if (data.TrdCumQty.HasValue)
                            sb.Append(data.TrdCumQty).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // CanceledQty
                        if (data.CancQty.HasValue)
                            sb.Append(data.CancQty).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // LeavesQty
                        if (data.LeavesQty.HasValue)
                            sb.Append(data.LeavesQty).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // TradedMsg
                        if (!string.IsNullOrEmpty(data.TrdMsg))
                            sb.Append(string.Concat("'", data.TrdMsg, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // OrderStatusUpdateTime
                        if (!string.IsNullOrEmpty(data.OrdStatusUpdTm))
                            sb.Append(string.Concat("'", data.OrdStatusUpdTm, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // AlgoParameters
                        if (!string.IsNullOrEmpty(data.AlgoParameters))
                            sb.Append(string.Concat("'", data.AlgoParameters, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // Trader
                        if (!string.IsNullOrEmpty(data.Trader))
                            sb.Append(string.Concat("'", data.Trader, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // ISIN
                        if (!string.IsNullOrEmpty(data.ISIN))
                            sb.Append(string.Concat("'", data.ISIN, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // Sedol
                        if (!string.IsNullOrEmpty(data.Sedol))
                            sb.Append(string.Concat("'", data.Sedol, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // Cusip
                        if (!string.IsNullOrEmpty(data.Cusip))
                            sb.Append(string.Concat("'", data.Cusip, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // Currency
                        if (!string.IsNullOrEmpty(data.Curr))
                            sb.Append(string.Concat("'", data.Curr, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // ExecutionId
                        if (!string.IsNullOrEmpty(data.ExecId))
                            sb.Append(string.Concat("'", data.ExecId, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // NVExecutionId
                        if (!string.IsNullOrEmpty(data.NVExecId))
                            sb.Append(string.Concat("'", data.NVExecId, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // DscntLive
                        if (data.DscntToLivePr.HasValue)
                            sb.Append(data.DscntToLivePr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // DscntFillPrice
                        if (data.DscntToLastPr.HasValue)
                            sb.Append(data.DscntToLastPr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // EstNav
                        if (data.EstNav.HasValue)
                            sb.Append(data.EstNav).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // LastPrice
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
                            sb.Append(data.AskPr);
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
                _logger.LogError(ex, "Error saving Trade Order Execution details into database");
                throw;
            }
        }

        /// <summary>
        /// Saves Order Execution Details to staging table
        /// </summary>
        /// <param name="orderExecutionDetailsDict"></param>
        public void SaveSimOrderExecutionsStg(IDictionary<string, OrderSummary> orderExecutionDetailsDict)
        {
            try
            {
                StringBuilder sCommand = new StringBuilder("insert into " + SimOrderExecutionsStgTableName
                    + " (AccountNumber, AccountName, ParentOrderId, MainOrderId, OrderId, TrackingId, ReferenceId,NeovestExecId,"
                    + " Symbol, BBGSymbol, ALMSymbol, OrderDate, OrderTime, OrderType, OrderSide, OrderQty, OrderExchange,"
                    + " OrderExchangeId, OrderPrice, OrderStopPrice, OrderStatus, OrderMemo, OrderDest, OrderBkrStrategy,"
                    + " PairTradeSpread, TradedQty, TradedPrice, AvgTradedPrice, TradedCumulativeQty, CanceledQty, LeavesQty, TradedMsg,"
                    + " OrderStatusUpdateTime, AlgoParameters, Trader, ISIN, Sedol, Cusip, Currency, ExecutionId, NVExecutionId,"
                    + " DscntLive, DscntFillPrice, EstNav"
                    + " ) values ");

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    IList<string> Rows = new List<string>();
                    StringBuilder sb = new StringBuilder();
                    foreach (KeyValuePair<string, OrderSummary> kvp in orderExecutionDetailsDict)
                    {
                        OrderSummary data = kvp.Value;

                        // AccountNumber
                        if (data.AccNum.HasValue)
                            sb.Append(data.AccNum).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // AccountName
                        if (!string.IsNullOrEmpty(data.AccName))
                            sb.Append(string.Concat("'", data.AccName, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // ParentOrderId
                        if (!string.IsNullOrEmpty(data.ParOrdId))
                            sb.Append(string.Concat("'", data.ParOrdId, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // MainOrderId
                        if (!string.IsNullOrEmpty(data.MainOrdId))
                            sb.Append(string.Concat("'", data.MainOrdId, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // OrderId
                        if (!string.IsNullOrEmpty(data.OrdId))
                            sb.Append(string.Concat("'", data.OrdId, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // TrackingId
                        if (!string.IsNullOrEmpty(data.TrkId))
                            sb.Append(string.Concat("'", data.TrkId, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // ReferenceId
                        if (!string.IsNullOrEmpty(data.RefId))
                            sb.Append(string.Concat("'", data.RefId, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // NeovestExecId
                        if (data.NVExecIdLng.HasValue)
                            sb.Append(data.NVExecIdLng).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // Symbol
                        if (!string.IsNullOrEmpty(data.Sym))
                            sb.Append(string.Concat("'", data.Sym, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // BBGSymbol
                        if (!string.IsNullOrEmpty(data.BBGSym))
                            sb.Append(string.Concat("'", data.BBGSym, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // ALMSymbol
                        if (!string.IsNullOrEmpty(data.ALMSym))
                            sb.Append(string.Concat("'", data.ALMSym, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // OrderDate
                        if (data.OrdDt.HasValue)
                            sb.Append(string.Concat("'", DateUtils.ConvertDate(data.OrdDt, "yyyy-MM-dd"), "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // OrderTime
                        if (!string.IsNullOrEmpty(data.OrdTm))
                            sb.Append(string.Concat("'", data.OrdTm, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // OrderType
                        if (!string.IsNullOrEmpty(data.OrdTyp))
                            sb.Append(string.Concat("'", data.OrdTyp, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // OrderSide
                        if (!string.IsNullOrEmpty(data.OrdSide))
                            sb.Append(string.Concat("'", data.OrdSide, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // OrderQty
                        if (data.OrdQty.HasValue)
                            sb.Append(data.OrdQty).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // OrderExchange
                        if (!string.IsNullOrEmpty(data.OrdExch))
                            sb.Append(string.Concat("'", data.OrdExch, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // OrderExchangeId
                        if (data.OrdExchId.HasValue)
                            sb.Append(data.OrdExchId).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // OrderPrice
                        if (data.OrdPr.HasValue)
                            sb.Append(data.OrdPr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // OrderStopPrice
                        if (data.OrdStopPr.HasValue)
                            sb.Append(data.OrdStopPr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // OrderStatus
                        if (!string.IsNullOrEmpty(data.OrdSt))
                            sb.Append(string.Concat("'", data.OrdSt, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // OrderMemo
                        if (!string.IsNullOrEmpty(data.OrdMemo))
                            sb.Append(string.Concat("'", data.OrdMemo, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // OrderDest
                        if (!string.IsNullOrEmpty(data.OrdDest))
                            sb.Append(string.Concat("'", data.OrdDest, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // OrderBkrStrategy
                        if (!string.IsNullOrEmpty(data.OrdBkrStrat))
                            sb.Append(string.Concat("'", data.OrdBkrStrat, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // PairTradeSpread
                        if (data.PairTrdSprd.HasValue)
                            sb.Append(data.PairTrdSprd).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // TradedQty
                        if (data.TrdQty.HasValue)
                            sb.Append(data.TrdQty).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // TradedPrice
                        if (data.TrdPr.HasValue)
                            sb.Append(data.TrdPr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // AvgTradedPrice
                        if (data.AvgTrdPr.HasValue)
                            sb.Append(data.AvgTrdPr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // TradedCumulativeQty
                        if (data.TrdCumQty.HasValue)
                            sb.Append(data.TrdCumQty).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // CanceledQty
                        if (data.CancQty.HasValue)
                            sb.Append(data.CancQty).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // LeavesQty
                        if (data.LeavesQty.HasValue)
                            sb.Append(data.LeavesQty).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // TradedMsg
                        if (!string.IsNullOrEmpty(data.TrdMsg))
                            sb.Append(string.Concat("'", data.TrdMsg, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // OrderStatusUpdateTime
                        if (!string.IsNullOrEmpty(data.OrdStatusUpdTm))
                            sb.Append(string.Concat("'", data.OrdStatusUpdTm, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // AlgoParameters
                        if (!string.IsNullOrEmpty(data.AlgoParameters))
                            sb.Append(string.Concat("'", data.AlgoParameters, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // Trader
                        if (!string.IsNullOrEmpty(data.Trader))
                            sb.Append(string.Concat("'", data.Trader, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // ISIN
                        if (!string.IsNullOrEmpty(data.ISIN))
                            sb.Append(string.Concat("'", data.ISIN, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // Sedol
                        if (!string.IsNullOrEmpty(data.Sedol))
                            sb.Append(string.Concat("'", data.Sedol, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // Cusip
                        if (!string.IsNullOrEmpty(data.Cusip))
                            sb.Append(string.Concat("'", data.Cusip, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // Currency
                        if (!string.IsNullOrEmpty(data.Curr))
                            sb.Append(string.Concat("'", data.Curr, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // ExecutionId
                        if (!string.IsNullOrEmpty(data.ExecId))
                            sb.Append(string.Concat("'", data.ExecId, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // NVExecutionId
                        if (!string.IsNullOrEmpty(data.NVExecId))
                            sb.Append(string.Concat("'", data.NVExecId, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // DscntLive
                        if (data.DscntToLivePr.HasValue)
                            sb.Append(data.DscntToLivePr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // DscntFillPrice
                        if (data.DscntToLastPr.HasValue)
                            sb.Append(data.DscntToLastPr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // EstNav
                        if (data.EstNav.HasValue)
                            sb.Append(data.EstNav);
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
                _logger.LogError(ex, "Error saving [Simulation] Trade Order Execution details into database");
                throw;
            }
        }

        /// <summary>
        /// Gets Trade Positions
        /// </summary>
        /// <returns></returns>
        public IDictionary<string, TradePosition> GetTradePositions()
        {
            IDictionary<string, TradePosition> dict = new Dictionary<string, TradePosition>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetCompletedOrdersQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Date", null);
                        command.Parameters.AddWithValue("p_Flag", "TradedPositionsNew");

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                string symbol = reader["Symbol"] as string;
                                string side = reader["OrderSide"] as string;

                                if (!dict.TryGetValue(symbol, out TradePosition tradePosition))
                                {
                                    tradePosition = new TradePosition();
                                    tradePosition.Sym = symbol;
                                    dict.Add(symbol, tradePosition);
                                }

                                if ("BUY".Equals(side, StringComparison.CurrentCultureIgnoreCase))
                                {
                                    tradePosition.BuyPos = (reader.IsDBNull(reader.GetOrdinal("TradeQty"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("TradeQty"));
                                    tradePosition.BuyAvgPrc = (reader.IsDBNull(reader.GetOrdinal("AvgPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AvgPrice"));
                                }
                                else if ("SELL".Equals(side, StringComparison.CurrentCultureIgnoreCase))
                                {
                                    tradePosition.SellPos = (reader.IsDBNull(reader.GetOrdinal("TradeQty"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("TradeQty"));
                                    tradePosition.SellAvgPrc = (reader.IsDBNull(reader.GetOrdinal("AvgPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AvgPrice"));
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

        /// <summary>
        /// Saves Trades
        /// </summary>
        /// <param name="orderSummaryList"></param>
        public void SaveTrades(IList<OrderSummary> orderSummaryList)
        {
            try
            {
                TruncateTable(TradeSummaryStgTableName);
                SaveTradeSummaryStg(orderSummaryList);
                MoveDataToTargetTable(SaveTradeSummaryQuery);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving data");
            }
        }

        /// <summary>
        /// Saves Trades to staging table
        /// </summary>
        /// <param name="orderSummaryList"></param>
        public void SaveTradeSummaryStg(IList<OrderSummary> orderSummaryList)
        {
            try
            {
                StringBuilder sCommand = new StringBuilder("insert into almitasc_ACTradingBBGData.StgTradeEntry"
                    + " (Action, AccountName, Symbol, OrderId, OrderDate, OrderTime,"
                    + " OrderSide, OrderType, OrderQty, OrderPrice, OrderDest, CashOrSwap, OrderExchange,"
                    + " Commission, CommissionType, PrimeBroker, ExecutingBroker, Trader, TradeSource"
                    + " ) values ");

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    IList<string> Rows = new List<string>();
                    StringBuilder sb = new StringBuilder();
                    foreach (OrderSummary data in orderSummaryList)
                    {
                        // Action
                        if (!string.IsNullOrEmpty(data.Action))
                            sb.Append(string.Concat("'", data.Action, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // AccountName
                        if (!string.IsNullOrEmpty(data.AccName))
                            sb.Append(string.Concat("'", data.AccName, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // Symbol
                        if (!string.IsNullOrEmpty(data.Sym))
                            sb.Append(string.Concat("'", data.Sym, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // OrderId
                        if (!string.IsNullOrEmpty(data.OrdId))
                            sb.Append(string.Concat("'", data.OrdId, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        if (!string.IsNullOrEmpty(data.OrdDtAsString))
                            sb.Append(string.Concat("'", data.OrdDtAsString, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // OrderTime
                        if (!string.IsNullOrEmpty(data.OrdTm))
                            sb.Append(string.Concat("'", data.OrdTm, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // OrderSide
                        if (!string.IsNullOrEmpty(data.OrdSide))
                            sb.Append(string.Concat("'", data.OrdSide, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // OrderType
                        if (!string.IsNullOrEmpty(data.OrdTyp))
                            sb.Append(string.Concat("'", data.OrdTyp, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // OrderQty
                        if (data.OrdQty.HasValue)
                            sb.Append(data.OrdQty).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // OrderPrice
                        if (data.OrdPr.HasValue)
                            sb.Append(data.OrdPr).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // OrderDest
                        if (!string.IsNullOrEmpty(data.OrdDest))
                            sb.Append(string.Concat("'", data.OrdDest, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // CashOrSwap
                        if (!string.IsNullOrEmpty(data.CashOrSwap))
                            sb.Append(string.Concat("'", data.CashOrSwap, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // OrderExchange
                        if (!string.IsNullOrEmpty(data.OrdExch))
                            sb.Append(string.Concat("'", data.OrdExch, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // Commission
                        if (data.Comm.HasValue)
                            sb.Append(data.Comm).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // CommissionType
                        if (!string.IsNullOrEmpty(data.CommTyp))
                            sb.Append(string.Concat("'", data.CommTyp, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // PrimeBroker
                        if (!string.IsNullOrEmpty(data.PrimeBkr))
                            sb.Append(string.Concat("'", data.PrimeBkr, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // ExecutingBroker
                        if (!string.IsNullOrEmpty(data.ExecBkr))
                            sb.Append(string.Concat("'", data.ExecBkr, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // Trader
                        if (!string.IsNullOrEmpty(data.Trader))
                            sb.Append(string.Concat("'", data.Trader, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // TradeSource
                        if (!string.IsNullOrEmpty(data.TrdSrc))
                            sb.Append(string.Concat("'", data.TrdSrc, "'"));
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
                _logger.LogError(ex, "Error saving Trade Order details into database");
                throw;
            }
        }

        /// <summary>
        /// Gets Manual Trades
        /// </summary>
        /// <param name="orderDate"></param>
        /// <returns></returns>
        public IList<OrderSummary> GetManualTrades(string orderDate)
        {
            IList<OrderSummary> tradeList = new List<OrderSummary>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    string sql = GetManualTradesQuery + " where TradeSource not in ('NV') and OrderDate = '" + orderDate + "'";

                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                OrderSummary data = new OrderSummary
                                {
                                    AccName = reader["AccountName"] as string,
                                    OrdId = reader["OrderId"] as string,
                                    Sym = reader["Symbol"] as string,
                                    OrdDt = reader.IsDBNull(reader.GetOrdinal("OrderDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("OrderDate")),
                                    OrdTyp = reader["OrderType"] as string,
                                    OrdSide = reader["OrderSide"] as string,
                                    OrdQty = (reader.IsDBNull(reader.GetOrdinal("OrderQty"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("OrderQty")),
                                    OrdPr = (reader.IsDBNull(reader.GetOrdinal("OrderPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OrderPrice")),
                                    Trader = reader["Trader"] as string,
                                    TrdSrc = reader["TradeSource"] as string,
                                    PrimeBkr = reader["PrimeBroker"] as string,
                                    ExecBkr = reader["ExecutingBroker"] as string,
                                    CashOrSwap = reader["CashOrSwap"] as string,
                                    Comm = (reader.IsDBNull(reader.GetOrdinal("Commission"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Commission")),
                                };

                                data.OrdDtAsString = DateUtils.ConvertDate(data.OrdDt, "yyyy-MM-dd");
                                tradeList.Add(data);
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

            return tradeList;
        }

        /// <summary>
        /// Gets Trade Execution Details
        /// </summary>
        /// <returns></returns>
        public IDictionary<string, OrderSummary> GetTradeExecutionDetails()
        {
            IDictionary<string, OrderSummary> dict = new Dictionary<string, OrderSummary>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    string sql = string.Empty;

                    if (_configuration["ConnectionStrings:ENV"].Equals("PROD"))
                        sql = GetTradeExecutionDetailsProdQuery;
                    else
                        sql = GetTradeExecutionDetailsDevQuery;

                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                string executionId = reader["ExecutionId"] as string;

                                if (!dict.TryGetValue(executionId, out OrderSummary orderSummary))
                                {
                                    orderSummary = new OrderSummary();
                                    orderSummary.ExecId = executionId;
                                    orderSummary.MainOrdId = reader["MainOrderId"] as string;
                                    orderSummary.OrdId = reader["OrderId"] as string;
                                    orderSummary.RefId = reader["ReferenceId"] as string;
                                    orderSummary.Sym = reader["Symbol"] as string;
                                    orderSummary.BBGSym = reader["BBGSymbol"] as string;
                                    orderSummary.ALMSym = reader["ALMSymbol"] as string;
                                    orderSummary.ISIN = reader["ISIN"] as string;
                                    orderSummary.Sedol = reader["Sedol"] as string;
                                    orderSummary.Cusip = reader["Cusip"] as string;
                                    orderSummary.Curr = reader["Currency"] as string;
                                    orderSummary.OrdSide = reader["OrderSide"] as string;
                                    orderSummary.OrdTyp = reader["OrderType"] as string;
                                    orderSummary.OrdTm = reader["OrderTime"] as string;
                                    orderSummary.OrdDest = reader["OrderDest"] as string;
                                    orderSummary.OrdBkrStrat = reader["OrderBkrStrategy"] as string;
                                    orderSummary.Trader = reader["Trader"] as string;
                                    orderSummary.ALMTrader = reader["ALMTrader"] as string;

                                    //Status
                                    orderSummary.OrdQty = (reader.IsDBNull(reader.GetOrdinal("OrderQty"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("OrderQty"));
                                    orderSummary.OrdPr = (reader.IsDBNull(reader.GetOrdinal("OrderPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OrderPrice"));

                                    orderSummary.TrdQty = (reader.IsDBNull(reader.GetOrdinal("TradedQty"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("TradedQty"));
                                    orderSummary.TrdPr = (reader.IsDBNull(reader.GetOrdinal("TradedPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TradedPrice"));
                                    orderSummary.AvgTrdPr = (reader.IsDBNull(reader.GetOrdinal("AvgTradedPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AvgTradedPrice"));
                                    orderSummary.TrdCumQty = (reader.IsDBNull(reader.GetOrdinal("TradedCumulativeQty"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("TradedCumulativeQty"));
                                    orderSummary.CancQty = (reader.IsDBNull(reader.GetOrdinal("CanceledQty"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("CanceledQty"));
                                    orderSummary.LeavesQty = (reader.IsDBNull(reader.GetOrdinal("LeavesQty"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("LeavesQty"));
                                    orderSummary.TrdMsg = reader["TradedMsg"] as string;
                                    orderSummary.OrdStatusUpdTm = reader["OrderStatusUpdateTime"] as string;

                                    dict.Add(executionId, orderSummary);
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

        /// <summary>
        /// Gets ISIN to Position Ticker Map
        /// </summary>
        /// <returns></returns>
        public IDictionary<string, string> GetISINPositionTickerMap()
        {
            IDictionary<string, string> dict = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetPositionTickerMapQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                string id = reader["Id"] as string;
                                string positionTicker = reader["Ticker"] as string;

                                if (!dict.ContainsKey(id))
                                    dict.Add(id, positionTicker);
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
        /// Gets Batch Order Templates for User
        /// </summary>
        /// <param name="userName"></param>
        /// <param name="includePairTradeTemplate"></param>
        /// <returns></returns>
        public IList<string> GetBatchOrderTemplatesForUser(string userName, string includePairTradeTemplate)
        {
            IList<string> list = new List<string>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    string sql = GetBatchOrderTemplatesForUserQuery;

                    if (string.IsNullOrEmpty(includePairTradeTemplate))
                        sql += " where IsPairTrade is null";
                    else
                        sql += " where IsPairTrade = 'Y'";

                    if (!string.IsNullOrEmpty(userName))
                        if (!userName.Equals("All", StringComparison.CurrentCultureIgnoreCase))
                            sql += " and (UserName is null or UserName in ('admin', '" + userName + "'))";

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

        /// <summary>
        /// Saves Trades from Allocation Sheet
        /// </summary>
        /// <param name="trades"></param>
        public void SaveAllTrades(IList<ASTrade> trades)
        {
            try
            {
                StringBuilder sCommand = new StringBuilder("insert into almitasc_ACTradingBBGData.Trade"
                    + " (TradeDate, TradeId, TradeSource, SourceSymbol, BBGSymbol, ALMSymbol, BBGCode,"
                    + " Currency, Side, Qty, Price, MV, Trader, IsSwap, ExecutingBroker,"
                    + " Sedol, Cusip, ISIN, SecurityType, SecurityType2, Country, ExchangeCode, QuotedCurrency, SharesOutstanding"
                    + " ) values ");


                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlTransaction trans = connection.BeginTransaction())
                    {
                        _logger.LogInformation("Deleting data from almitasc_ACTradingBBGData.Trade for TradeDate = '" + trades[0].TradeDateAsString + "' and TradeSource not in ('Manual')");
                        string sqlDelete = "delete from almitasc_ACTradingBBGData.Trade where TradeDate = '" + trades[0].TradeDateAsString + "' and TradeSource not in ('Manual')";
                        using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                        {
                            command.ExecuteNonQuery();
                        }

                        IList<string> Rows = new List<string>();
                        StringBuilder sb = new StringBuilder();
                        foreach (ASTrade data in trades)
                        {
                            // TradeDate
                            if (!string.IsNullOrEmpty(data.TradeDateAsString))
                                sb.Append(string.Concat("'", data.TradeDateAsString, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // TradeId
                            if (!string.IsNullOrEmpty(data.TradeId))
                                sb.Append(string.Concat("'", data.TradeId, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // TradeSource
                            if (!string.IsNullOrEmpty(data.TradeSource))
                                sb.Append(string.Concat("'", data.TradeSource, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // SourceSymbol
                            if (!string.IsNullOrEmpty(data.SourceSymbol))
                                sb.Append(string.Concat("'", data.SourceSymbol, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // BBGSymbol
                            if (!string.IsNullOrEmpty(data.BBGSymbol))
                                sb.Append(string.Concat("'", data.BBGSymbol, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // ALMSymbol
                            if (!string.IsNullOrEmpty(data.ALMSymbol))
                                sb.Append(string.Concat("'", data.ALMSymbol, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // BBGCode
                            if (!string.IsNullOrEmpty(data.BBGCode))
                                sb.Append(string.Concat("'", data.BBGCode, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // Currency
                            if (!string.IsNullOrEmpty(data.Currency))
                                sb.Append(string.Concat("'", data.Currency, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // Side
                            if (!string.IsNullOrEmpty(data.Side))
                                sb.Append(string.Concat("'", data.Side, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // Qty
                            if (data.Qty.HasValue)
                                sb.Append(data.Qty).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // Price
                            if (data.Price.HasValue)
                                sb.Append(data.Price).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // MV
                            if (data.MV.HasValue)
                                sb.Append(data.MV).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // Trader
                            if (!string.IsNullOrEmpty(data.Trader))
                                sb.Append(string.Concat("'", data.Trader, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // IsSwap
                            if (!string.IsNullOrEmpty(data.IsSwap))
                                sb.Append(string.Concat("'", data.IsSwap, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // ExecutingBroker
                            if (!string.IsNullOrEmpty(data.ExecutingBroker))
                                sb.Append(string.Concat("'", data.ExecutingBroker, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // Sedol
                            if (!string.IsNullOrEmpty(data.Sedol))
                                sb.Append(string.Concat("'", data.Sedol, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // Cusip
                            if (!string.IsNullOrEmpty(data.Cusip))
                                sb.Append(string.Concat("'", data.Cusip, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // ISIN
                            if (!string.IsNullOrEmpty(data.ISIN))
                                sb.Append(string.Concat("'", data.ISIN, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // SecurityType
                            if (!string.IsNullOrEmpty(data.SecurityType))
                                sb.Append(string.Concat("'", data.SecurityType, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // SecurityType2
                            if (!string.IsNullOrEmpty(data.SecurityType2))
                                sb.Append(string.Concat("'", data.SecurityType2, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // Country
                            if (!string.IsNullOrEmpty(data.Country))
                                sb.Append(string.Concat("'", data.Country, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // ExchangeCode
                            if (!string.IsNullOrEmpty(data.ExchangeCode))
                                sb.Append(string.Concat("'", data.ExchangeCode, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // QuotedCurrency
                            if (!string.IsNullOrEmpty(data.QuotedCurrency))
                                sb.Append(string.Concat("'", data.QuotedCurrency, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // SharesOutstanding
                            if (data.SharesOutstanding.HasValue)
                                sb.Append(data.SharesOutstanding);
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
                _logger.LogError(ex, "Error saving Trade details into database");
                throw;
            }
        }

        /// <summary>
        /// Saves Allocation Details (from Allocation Sheet)
        /// </summary>
        /// <param name="tradeAllocations"></param>
        public void SaveAllTradeAllocations(IList<ASTradeAllocation> tradeAllocations)
        {
            try
            {
                StringBuilder sCommand = new StringBuilder("insert into almitasc_ACTradingBBGData.TradeAllocation"
                    + " (TradeDate, TradeId, FundName, Symbol, Qty, Side, Price, PrimeBroker, ExecutingBroker,"
                    + " Currency, IsSwap, Commission, StampTax, PMTFee, Sedol, Cusip, ISIN,"
                    + " SecurityType, OptionTicker, OptionSide, ExchangeCode, TradeSource, Trader,"
                    + " Shares, DerivedShares, CashShares, SwapShares, FidoShares, IBShares,"
                    + " JPMCashShares, JPMSwapShares, TradedShares,"
                    + " PriorSecurityWt, SecurityWt, PriorSecurityOwnershipPct, SecurityOwnershipPct,"
                    + " PriorSectorWt, SectorWt, BalanceBy, Notes"
                    + " ) values ");

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlTransaction trans = connection.BeginTransaction())
                    {
                        _logger.LogInformation("Deleting data from almitasc_ACTradingBBGData.TradeAllocation for TradeDate = '" + tradeAllocations[0].TradeDateAsString + "'");
                        string sqlDelete = "delete from almitasc_ACTradingBBGData.TradeAllocation where TradeDate = '" + tradeAllocations[0].TradeDateAsString + "'";
                        using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                        {
                            command.ExecuteNonQuery();
                        }

                        IList<string> Rows = new List<string>();
                        StringBuilder sb = new StringBuilder();
                        foreach (ASTradeAllocation data in tradeAllocations)
                        {
                            // TradeDate
                            if (!string.IsNullOrEmpty(data.TradeDateAsString))
                                sb.Append(string.Concat("'", data.TradeDateAsString, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // TradeId
                            if (!string.IsNullOrEmpty(data.TradeId))
                                sb.Append(string.Concat("'", data.TradeId, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // FundName
                            if (!string.IsNullOrEmpty(data.FundName))
                                sb.Append(string.Concat("'", data.FundName, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // Symbol
                            if (!string.IsNullOrEmpty(data.Symbol))
                                sb.Append(string.Concat("'", data.Symbol, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // Qty
                            if (data.Qty.HasValue)
                                sb.Append(data.Qty).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // Side
                            if (!string.IsNullOrEmpty(data.Side))
                                sb.Append(string.Concat("'", data.Side, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // Price
                            if (data.Price.HasValue)
                                sb.Append(data.Price).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // PrimeBroker
                            if (!string.IsNullOrEmpty(data.PrimeBroker))
                                sb.Append(string.Concat("'", data.PrimeBroker, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // ExecutingBroker
                            if (!string.IsNullOrEmpty(data.ExecutingBroker))
                                sb.Append(string.Concat("'", data.ExecutingBroker, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // Currency
                            if (!string.IsNullOrEmpty(data.Currency))
                                sb.Append(string.Concat("'", data.Currency, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // IsSwap
                            if (!string.IsNullOrEmpty(data.IsSwap))
                                sb.Append(string.Concat("'", data.IsSwap, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // Commission
                            if (data.Commission.HasValue)
                                sb.Append(data.Commission).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // StampTax
                            if (data.StampTax.HasValue)
                                sb.Append(data.StampTax).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // PMTFee
                            if (data.PMTFee.HasValue)
                                sb.Append(data.PMTFee).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // Sedol
                            if (!string.IsNullOrEmpty(data.Sedol))
                                sb.Append(string.Concat("'", data.Sedol, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // Cusip
                            if (!string.IsNullOrEmpty(data.Cusip))
                                sb.Append(string.Concat("'", data.Cusip, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // ISIN
                            if (!string.IsNullOrEmpty(data.ISIN))
                                sb.Append(string.Concat("'", data.ISIN, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // SecurityType
                            if (!string.IsNullOrEmpty(data.SecurityType))
                                sb.Append(string.Concat("'", data.SecurityType, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // OptionTicker
                            if (!string.IsNullOrEmpty(data.OptionTicker))
                                sb.Append(string.Concat("'", data.OptionTicker, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // OptionSide
                            if (!string.IsNullOrEmpty(data.OptionSide))
                                sb.Append(string.Concat("'", data.OptionSide, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // ExchangeCode
                            if (!string.IsNullOrEmpty(data.ExchangeCode))
                                sb.Append(string.Concat("'", data.ExchangeCode, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // TradeSource
                            if (!string.IsNullOrEmpty(data.TradeSource))
                                sb.Append(string.Concat("'", data.TradeSource, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // Trader
                            if (!string.IsNullOrEmpty(data.Trader))
                                sb.Append(string.Concat("'", data.Trader, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // Shares
                            if (data.Shares.HasValue)
                                sb.Append(data.Shares).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // DerivedShares
                            if (data.DerivedShares.HasValue)
                                sb.Append(data.DerivedShares).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // CashShares
                            if (data.CashShares.HasValue)
                                sb.Append(data.CashShares).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // SwapShares
                            if (data.SwapShares.HasValue)
                                sb.Append(data.SwapShares).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // FidoShares
                            if (data.FidoShares.HasValue)
                                sb.Append(data.FidoShares).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // IBShares
                            if (data.IBShares.HasValue)
                                sb.Append(data.IBShares).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // JPMCashShares
                            if (data.JPMCashShares.HasValue)
                                sb.Append(data.JPMCashShares).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // JPMSwapShares
                            if (data.JPMSwapShares.HasValue)
                                sb.Append(data.JPMSwapShares).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // TradedShares
                            if (data.TradedShares.HasValue)
                                sb.Append(data.TradedShares).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // PriorSecurityWt
                            if (data.PriorSecurityWt.HasValue)
                                sb.Append(data.PriorSecurityWt).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // SecurityWt
                            if (data.SecurityWt.HasValue)
                                sb.Append(data.SecurityWt).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // PriorSecurityOwnershipPct
                            if (data.PriorSecurityOwnershipPct.HasValue)
                                sb.Append(data.PriorSecurityOwnershipPct).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // SecurityOwnershipPct
                            if (data.SecurityOwnershipPct.HasValue)
                                sb.Append(data.SecurityOwnershipPct).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // PriorSectorWt
                            if (data.PriorSectorWt.HasValue)
                                sb.Append(data.PriorSectorWt).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // SectorWt
                            if (data.SectorWt.HasValue)
                                sb.Append(data.SectorWt).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // BalanceBy
                            if (!string.IsNullOrEmpty(data.BalanceBy))
                                sb.Append(string.Concat("'", data.BalanceBy, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // Notes
                            if (!string.IsNullOrEmpty(data.Notes))
                                sb.Append(string.Concat("'", data.Notes, "'"));
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
                _logger.LogError(ex, "Error saving Allocation details into database");
                throw;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IList<ASTrade> GetASTrades(string ticker, DateTime startDate, DateTime endDate, string source)
        {
            IList<ASTrade> list = new List<ASTrade>();

            try
            {
                string startDateAsString = DateUtils.ConvertDate(startDate, "yyyy-MM-dd");
                string endDateAsString = DateUtils.ConvertDate(endDate, "yyyy-MM-dd");

                string sql = GetASTradesQuery + " where 1=1";

                if (!string.IsNullOrEmpty(source))
                    sql += " and TradeSource in (" + source + ")";

                if (!string.IsNullOrEmpty(ticker))
                    sql += " and SourceSymbol like '%" + ticker + "%'";

                if (!string.IsNullOrEmpty(startDateAsString))
                    sql += " and TradeDate >= '" + startDateAsString + "'";

                if (!string.IsNullOrEmpty(endDateAsString))
                    sql += " and TradeDate <= '" + endDateAsString + "'";

                sql += " Order by TradeDate desc, ALMSymbol";


                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                ASTrade data = new ASTrade();
                                data.TradeDate = reader.IsDBNull(reader.GetOrdinal("TradeDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("TradeDate"));
                                data.TradeId = reader["TradeId"] as string;
                                data.TradeSource = reader["TradeSource"] as string;
                                data.SourceSymbol = reader["SourceSymbol"] as string;
                                data.BBGSymbol = reader["BBGSymbol"] as string;
                                data.ALMSymbol = reader["ALMSymbol"] as string;
                                data.BBGCode = reader["BBGCode"] as string;
                                data.Currency = reader["QuotedCurrency"] as string;
                                data.Side = reader["Side"] as string;
                                data.Qty = (reader.IsDBNull(reader.GetOrdinal("Qty"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("Qty"));
                                data.Price = (reader.IsDBNull(reader.GetOrdinal("Price"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price"));
                                data.MV = (reader.IsDBNull(reader.GetOrdinal("MV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MV"));
                                data.Trader = reader["Trader"] as string;
                                data.IsSwap = reader["IsSwap"] as string;
                                data.ExecutingBroker = reader["ExecutingBroker"] as string;
                                data.Sedol = reader["Sedol"] as string;
                                data.Cusip = reader["Cusip"] as string;
                                data.ISIN = reader["ISIN"] as string;
                                data.SecurityType = reader["SecurityType"] as string;
                                data.SecurityType2 = reader["SecurityType2"] as string;
                                data.Country = reader["Country"] as string;
                                data.ExchangeCode = reader["ExchangeCode"] as string;
                                data.QuotedCurrency = reader["QuotedCurrency"] as string;
                                data.SharesOutstanding = (reader.IsDBNull(reader.GetOrdinal("SharesOutstanding"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SharesOutstanding"));
                                data.TradeDateAsString = DateUtils.ConvertDate(data.TradeDate, "yyyy-MM-dd");
                                data.CreateDate = reader.IsDBNull(reader.GetOrdinal("CreateDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("CreateDate"));

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

        public IList<ASTradeAllocation> GetASTradeAllocations(string ticker, DateTime startDate, DateTime endDate)
        {
            IList<ASTradeAllocation> list = new List<ASTradeAllocation>();

            try
            {
                string startDateAsString = DateUtils.ConvertDate(startDate, "yyyy-MM-dd");
                string endDateAsString = DateUtils.ConvertDate(endDate, "yyyy-MM-dd");

                string sql = GetASTradeAllocationsQuery + " where 1=1";

                if (!string.IsNullOrEmpty(ticker))
                    sql += " and Symbol like '%" + ticker + "%'";

                if (!string.IsNullOrEmpty(startDateAsString))
                    sql += " and TradeDate >= '" + startDateAsString + "'";

                if (!string.IsNullOrEmpty(endDateAsString))
                    sql += " and TradeDate <= '" + endDateAsString + "'";

                sql += " Order by TradeDate desc, Symbol, FundName";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                ASTradeAllocation data = new ASTradeAllocation();
                                data.TradeDate = reader.IsDBNull(reader.GetOrdinal("TradeDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("TradeDate"));
                                data.TradeId = reader["TradeId"] as string;
                                data.FundName = reader["FundName"] as string;
                                data.Symbol = reader["Symbol"] as string;
                                data.Qty = (reader.IsDBNull(reader.GetOrdinal("Qty"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("Qty"));
                                data.Side = reader["Side"] as string;
                                data.Price = (reader.IsDBNull(reader.GetOrdinal("Price"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price"));
                                data.PrimeBroker = reader["PrimeBroker"] as string;
                                data.ExecutingBroker = reader["ExecutingBroker"] as string;
                                data.Currency = reader["Currency"] as string;
                                data.IsSwap = reader["IsSwap"] as string;
                                data.Commission = (reader.IsDBNull(reader.GetOrdinal("Commission"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Commission"));
                                data.StampTax = (reader.IsDBNull(reader.GetOrdinal("StampTax"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("StampTax"));
                                data.PMTFee = (reader.IsDBNull(reader.GetOrdinal("PMTFee"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PMTFee"));
                                data.Sedol = reader["Sedol"] as string;
                                data.Cusip = reader["Cusip"] as string;
                                data.ISIN = reader["ISIN"] as string;
                                data.SecurityType = reader["SecurityType"] as string;
                                data.OptionTicker = reader["OptionTicker"] as string;
                                data.OptionSide = reader["OptionSide"] as string;
                                data.ExchangeCode = reader["ExchangeCode"] as string;
                                data.TradeSource = reader["TradeSource"] as string;
                                data.Trader = reader["Trader"] as string;

                                data.Shares = (reader.IsDBNull(reader.GetOrdinal("Shares"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("Shares"));
                                data.DerivedShares = (reader.IsDBNull(reader.GetOrdinal("DerivedShares"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("DerivedShares"));
                                data.CashShares = (reader.IsDBNull(reader.GetOrdinal("CashShares"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("CashShares"));
                                data.SwapShares = (reader.IsDBNull(reader.GetOrdinal("SwapShares"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("SwapShares"));
                                data.FidoShares = (reader.IsDBNull(reader.GetOrdinal("FidoShares"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("FidoShares"));
                                data.IBShares = (reader.IsDBNull(reader.GetOrdinal("IBShares"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("IBShares"));
                                data.JPMCashShares = (reader.IsDBNull(reader.GetOrdinal("JPMCashShares"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("JPMCashShares"));
                                data.JPMSwapShares = (reader.IsDBNull(reader.GetOrdinal("JPMSwapShares"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("JPMSwapShares"));
                                data.TDShares = (reader.IsDBNull(reader.GetOrdinal("TDShares"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("TDShares"));
                                data.EDFShares = (reader.IsDBNull(reader.GetOrdinal("EDFShares"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("EDFShares"));

                                data.PriorSecurityWt = (reader.IsDBNull(reader.GetOrdinal("PriorSecurityWt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriorSecurityWt"));
                                data.SecurityWt = (reader.IsDBNull(reader.GetOrdinal("SecurityWt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SecurityWt"));
                                data.PriorSecurityOwnershipPct = (reader.IsDBNull(reader.GetOrdinal("PriorSecurityOwnershipPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriorSecurityOwnershipPct"));
                                data.SecurityOwnershipPct = (reader.IsDBNull(reader.GetOrdinal("SecurityOwnershipPct"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SecurityOwnershipPct"));
                                data.PriorSectorWt = (reader.IsDBNull(reader.GetOrdinal("PriorSectorWt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("PriorSectorWt"));
                                data.SectorWt = (reader.IsDBNull(reader.GetOrdinal("SectorWt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("SectorWt"));
                                data.BalanceBy = reader["BalanceBy"] as string;
                                data.Notes = reader["Notes"] as string;

                                data.TradeDateAsString = DateUtils.ConvertDate(data.TradeDate, "yyyy-MM-dd");

                                list.Add(data);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("Error executing query", ex);
                throw;
            }

            return list;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="date"></param>
        /// <returns></returns>
        public IList<OrderSummary> GetExecutedOrders(DateTime date)
        {
            IList<OrderSummary> list = new List<OrderSummary>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetCompletedOrdersQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_Date", date);
                        command.Parameters.AddWithValue("p_Flag", "ExecutedOrders");

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                OrderSummary data = new OrderSummary
                                {
                                    MainOrdId = reader["MainOrderId"] as string,
                                    OrdId = reader["OrderId"] as string,
                                    OrdDt = reader.IsDBNull(reader.GetOrdinal("OrderDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("OrderDate")),
                                    Sym = reader["Symbol"] as string,
                                    BBGSym = reader["BBGSymbol"] as string,
                                    ALMSym = reader["ALMSymbol"] as string,
                                    ISIN = reader["ISIN"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    OrdSide = reader["OrderSide"] as string,
                                    OrdTyp = reader["OrderType"] as string,
                                    OrdDest = reader["OrderDest"] as string,
                                    OrdBkrStrat = reader["OrderBkrStrategy"] as string,
                                    Curr = reader["Currency"] as string,
                                    Trader = reader["Trader"] as string,
                                    ALMTrader = reader["ALMTrader"] as string,
                                    CashOrSwap = reader["Swap"] as string,
                                    TrdSrc = reader["TradeSource"] as string,
                                    OrdQty = (reader.IsDBNull(reader.GetOrdinal("OrderQty"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("OrderQty")),
                                    TrdCumQty = (reader.IsDBNull(reader.GetOrdinal("TradedQty"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("TradedQty")),
                                    OrdTm = reader["OrderTime"] as string,
                                    OrdStatusUpdTm = reader["OrderStatusUpdateTime"] as string,
                                    OrdPr = (reader.IsDBNull(reader.GetOrdinal("OrderPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OrderPrice")),
                                    AvgTrdPr = (reader.IsDBNull(reader.GetOrdinal("AvgTradedPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AvgTradedPrice")),
                                };

                                data.OrdDtAsString = DateUtils.ConvertDate(data.OrdDt, "yyyy-MM-dd");
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
        /// <param name="trades"></param>
        public void RemoveManualTrades(IList<ASTrade> trades)
        {
            try
            {
                int i = 0;
                string tradeIds = string.Empty;
                foreach (ASTrade trade in trades)
                {
                    if (!string.IsNullOrEmpty(trade.TradeId))
                    {
                        if (i == 0)
                            tradeIds = string.Join("'", trade.TradeId, "'");
                        else
                            tradeIds = string.Join(", '", trade.TradeId, "'");
                        i++;
                    }
                }

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlTransaction trans = connection.BeginTransaction())
                    {
                        _logger.LogInformation("Deleting data from almitasc_ACTradingBBGData.Trade for TradeId in (" + tradeIds + ")");
                        string sqlDelete = "delete from almitasc_ACTradingBBGData.Trade where TradeId in (" + tradeIds + ")";
                        using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                        {
                            command.ExecuteNonQuery();
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
        /// <param name="trades"></param>
        public void SaveManualTrades(IList<ASTrade> trades)
        {
            try
            {
                StringBuilder sCommand = new StringBuilder("insert into almitasc_ACTradingBBGData.Trade"
                    + " (TradeDate, TradeId, TradeSource, SourceSymbol, "
                    + " Side, Qty, Price, Trader, IsSwap, ExecutingBroker, QuotedCurrency, Currency"
                    + " ) values ");

                int i = 0;
                int rowsToInsert = 0;
                string tradeIds = string.Empty;
                foreach (ASTrade trade in trades)
                {
                    //if ("D".Equals(trade.Action, StringComparison.CurrentCultureIgnoreCase)
                    //    || "Y".Equals(trade.Action, StringComparison.CurrentCultureIgnoreCase))
                    {
                        if (string.IsNullOrEmpty(trade.TradeId))
                            trade.TradeId = string.Join("|", trade.TradeDateAsString, trade.SourceSymbol, trade.Side, trade.Qty, trade.Price, trade.ExecutingBroker);

                        i++;
                        if (i == 1)
                            tradeIds = "'" + trade.TradeId + "'";
                        else
                            tradeIds = tradeIds + ", '" + trade.TradeId + "'";
                    }
                }

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlTransaction trans = connection.BeginTransaction())
                    {
                        if (i > 0)
                        {
                            _logger.LogInformation("Deleting data from almitasc_ACTradingBBGData.Trade for TradeId in (" + tradeIds + ") and TradeSource = 'Manual'");
                            string sqlDelete = "delete from almitasc_ACTradingBBGData.Trade where TradeId in (" + tradeIds + ") and TradeSource = 'Manual'";
                            using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                            {
                                command.ExecuteNonQuery();
                            }
                        }

                        IList<string> Rows = new List<string>();
                        StringBuilder sb = new StringBuilder();
                        foreach (ASTrade data in trades)
                        {
                            if (!string.IsNullOrEmpty(data.SourceSymbol)
                                && !"D".Equals(data.Action, StringComparison.CurrentCultureIgnoreCase))
                            {
                                rowsToInsert++;

                                // TradeDate
                                if (!string.IsNullOrEmpty(data.TradeDateAsString))
                                    sb.Append(string.Concat("'", data.TradeDateAsString, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // TradeId
                                if (!string.IsNullOrEmpty(data.TradeId))
                                    sb.Append(string.Concat("'", data.TradeId, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // TradeSource
                                if (!string.IsNullOrEmpty(data.TradeSource))
                                    sb.Append(string.Concat("'", data.TradeSource, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // SourceSymbol
                                if (!string.IsNullOrEmpty(data.SourceSymbol))
                                    sb.Append(string.Concat("'", data.SourceSymbol, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Side
                                if (!string.IsNullOrEmpty(data.Side))
                                    sb.Append(string.Concat("'", data.Side, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Qty
                                if (data.Qty.HasValue)
                                    sb.Append(data.Qty).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Price
                                if (data.Price.HasValue)
                                    sb.Append(data.Price).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Trader
                                if (!string.IsNullOrEmpty(data.Trader))
                                    sb.Append(string.Concat("'", data.Trader, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // IsSwap
                                if (!string.IsNullOrEmpty(data.IsSwap))
                                    sb.Append(string.Concat("'", data.IsSwap, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // ExecutingBroker
                                if (!string.IsNullOrEmpty(data.ExecutingBroker))
                                    sb.Append(string.Concat("'", data.ExecutingBroker, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // QuotedCurrency
                                if (!string.IsNullOrEmpty(data.QuotedCurrency))
                                    sb.Append(string.Concat("'", data.QuotedCurrency, "'")).Append(DELIMITER);
                                else
                                    sb.Append("null").Append(DELIMITER);

                                // Currency
                                if (!string.IsNullOrEmpty(data.Currency))
                                    sb.Append(string.Concat("'", data.Currency, "'"));
                                else
                                    sb.Append("null");

                                string row = sb.ToString();
                                Rows.Add(string.Concat("(", row, ")"));
                                sb.Clear();
                            }
                        }

                        sCommand.Append(string.Join(",", Rows));
                        sCommand.Append(";");

                        if (rowsToInsert > 0)
                        {
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), connection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                        }

                        trans.Commit();
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving Trade details into database");
                throw;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IList<NewOrder> GetSimBatchOrders()
        {
            IList<NewOrder> list = new List<NewOrder>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetSimBatchOrdersQuery, connection))
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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="orderList"></param>
        public void SaveSimBatchOrders(IList<NewOrder> orderList)
        {
            try
            {
                TruncateTable(SimBatchOrdersStgTableName);
                SaveBatchOrdersStg(orderList, SimBatchOrdersStgTableName);
                MoveDataToTargetTable(SaveSimBatchOrdersQuery);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving data");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IDictionary<string, OrderSummary> GetSimTradeExecutionDetails()
        {
            IDictionary<string, OrderSummary> dict = new Dictionary<string, OrderSummary>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetSimTradeExecutionDetailsProdQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                string executionId = reader["ExecutionId"] as string;

                                if (!dict.TryGetValue(executionId, out OrderSummary orderSummary))
                                {
                                    orderSummary = new OrderSummary();
                                    orderSummary.ExecId = executionId;
                                    orderSummary.MainOrdId = reader["MainOrderId"] as string;
                                    orderSummary.OrdId = reader["OrderId"] as string;
                                    orderSummary.RefId = reader["ReferenceId"] as string;
                                    orderSummary.Sym = reader["Symbol"] as string;
                                    orderSummary.BBGSym = reader["BBGSymbol"] as string;
                                    orderSummary.ALMSym = reader["ALMSymbol"] as string;
                                    orderSummary.ISIN = reader["ISIN"] as string;
                                    orderSummary.Sedol = reader["Sedol"] as string;
                                    orderSummary.Cusip = reader["Cusip"] as string;
                                    orderSummary.Curr = reader["Currency"] as string;
                                    orderSummary.OrdSide = reader["OrderSide"] as string;
                                    orderSummary.TrdQty = (reader.IsDBNull(reader.GetOrdinal("TradedQty"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("TradedQty"));
                                    orderSummary.TrdPr = (reader.IsDBNull(reader.GetOrdinal("TradedPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TradedPrice"));

                                    orderSummary.OrdDest = reader["OrderDest"] as string;
                                    orderSummary.OrdBkrStrat = reader["OrderBkrStrategy"] as string;
                                    orderSummary.Trader = reader["Trader"] as string;
                                    orderSummary.OrdTm = reader["OrderTime"] as string;

                                    dict.Add(executionId, orderSummary);
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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="list"></param>
        public void SaveTradingTargets(IList<TradeTarget> list)
        {
            try
            {
                TruncateTable(TradingTargetStgTableName);
                SaveTradingTargetStg(list, TradingTargetStgTableName);
                MoveDataToTargetTable(SaveTradingTargetsQuery);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving data");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="list"></param>
        public void SavePairTradingTargets(IList<PairTradeTarget> list)
        {
            try
            {
                TruncateTable(PairTradingTargetStgTableName);
                SavePairTradingTargetStg(list, PairTradingTargetStgTableName);
                MoveDataToTargetTable(SavePairTradingTargetsQuery);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving data");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="orderList"></param>
        /// <param name="tableName"></param>
        public void SaveTradingTargetStg(IList<TradeTarget> orderList, string tableName)
        {
            try
            {
                StringBuilder sCommand = new StringBuilder("insert into " + tableName
                    + "(Ticker, Broker, BrokerStrategy, Side, Qty, TargetDiscount, Increment,"
                    + " MaxPrice, Notes, Trader)"
                    + " values ");

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    IList<string> Rows = new List<string>();
                    StringBuilder sb = new StringBuilder();
                    foreach (TradeTarget order in orderList)
                    {
                        // Ticker
                        if (!string.IsNullOrEmpty(order.Ticker))
                            sb.Append(string.Concat("'", order.Ticker, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // Broker
                        if (!string.IsNullOrEmpty(order.Broker))
                            sb.Append(string.Concat("'", order.Broker, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // BrokerStrategy
                        if (!string.IsNullOrEmpty(order.BrokerStrategy))
                            sb.Append(string.Concat("'", order.BrokerStrategy, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // Side
                        if (!string.IsNullOrEmpty(order.Side))
                            sb.Append(string.Concat("'", order.Side, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // Qty
                        if (order.Qty.HasValue)
                            sb.Append(order.Qty).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // TargetDiscount
                        if (order.TgtDscnt.HasValue)
                            sb.Append(order.TgtDscnt).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // Increment
                        if (order.Increment.HasValue)
                            sb.Append(order.Increment).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // MaxPrice
                        if (order.MaxPrice.HasValue)
                            sb.Append(order.MaxPrice).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // Notes
                        if (!string.IsNullOrEmpty(order.Notes))
                            sb.Append(string.Concat("'", order.Notes, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // Trader
                        if (!string.IsNullOrEmpty(order.Trader))
                            sb.Append(string.Concat("'", order.Trader, "'"));
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
                _logger.LogError(ex, "Error saving Trading Targets");
                throw;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="orderList"></param>
        /// <param name="tableName"></param>
        public void SavePairTradingTargetStg(IList<PairTradeTarget> orderList, string tableName)
        {
            try
            {
                StringBuilder sCommand = new StringBuilder("insert into " + tableName
                    + "(SellTicker, BuyTicker, BrokerStrategy, Locate, TargetDiscount, OrderSize)"
                    + " values ");

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    IList<string> Rows = new List<string>();
                    StringBuilder sb = new StringBuilder();
                    foreach (PairTradeTarget order in orderList)
                    {
                        // SellTicker
                        if (!string.IsNullOrEmpty(order.SellTicker))
                            sb.Append(string.Concat("'", order.SellTicker, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // BuyTicker
                        if (!string.IsNullOrEmpty(order.BuyTicker))
                            sb.Append(string.Concat("'", order.BuyTicker, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // BrokerStrategy
                        if (!string.IsNullOrEmpty(order.BrokerStrategy))
                            sb.Append(string.Concat("'", order.BrokerStrategy, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // Locate
                        if (!string.IsNullOrEmpty(order.Locate))
                            sb.Append(string.Concat("'", order.Locate, "'")).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // TargetDiscount
                        if (order.TgtDscnt.HasValue)
                            sb.Append(order.TgtDscnt).Append(DELIMITER);
                        else
                            sb.Append("null").Append(DELIMITER);

                        // OrderSize
                        if (order.Size.HasValue)
                            sb.Append(order.Size);
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
                _logger.LogError(ex, "Error saving Pair Trading Targets");
                throw;
            }
        }

        public IList<TradeTarget> GetTradeTargets()
        {
            IList<TradeTarget> list = new List<TradeTarget>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetLatestTradeTargetsQuery, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                TradeTarget data = new TradeTarget();
                                data.Ticker = reader["Ticker"] as string;
                                data.Broker = reader["Broker"] as string;
                                data.BrokerStrategy = reader["BrokerStrategy"] as string;
                                data.Side = reader["Side"] as string;
                                data.Notes = reader["Notes"] as string;
                                data.Trader = reader["Trader"] as string;
                                data.Qty = (reader.IsDBNull(reader.GetOrdinal("Qty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Qty"));
                                data.TgtDscnt = (reader.IsDBNull(reader.GetOrdinal("TargetDiscount"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TargetDiscount"));
                                data.Increment = (reader.IsDBNull(reader.GetOrdinal("Increment"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Increment"));
                                data.MaxPrice = (reader.IsDBNull(reader.GetOrdinal("MaxPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MaxPrice"));
                                data.LastUpdated = (reader.IsDBNull(reader.GetOrdinal("LastModifyDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("LastModifyDate"));

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
        /// <param name="orderSummaryDict"></param>
        public void SaveOrderQueue(IDictionary<string, OrderSummary> orderSummaryDict)
        {
            try
            {
                TruncateTable(OrderQueueStgTableName);
                SaveOrderQueueStg(orderSummaryDict, OrderQueueStgTableName);
                MoveDataToTargetTable(SaveOrderQueueQuery);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving data");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="orderSummaryDict"></param>
        public void SaveSimOrderQueue(IDictionary<string, OrderSummary> orderSummaryDict)
        {
            try
            {
                TruncateTable(SimOrderQueueStgTableName);
                SaveOrderQueueStg(orderSummaryDict, SimOrderQueueStgTableName);
                MoveDataToTargetTable(SaveSimOrderQueueQuery);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving data");
            }
        }

        /// <summary>
        /// Gets Order Details
        /// </summary>
        /// <returns></returns>
        public IDictionary<string, OrderSummary> GetOrderQueue()
        {
            IDictionary<string, OrderSummary> orderSummaryDict = new Dictionary<string, OrderSummary>(StringComparer.CurrentCultureIgnoreCase);

            try
            {
                string sql = string.Empty;

                if (_configuration["ConnectionStrings:ENV"].Equals("PROD"))
                    sql = GetOrderQueueProdQuery;
                else
                    sql = GetOrderQueueDevQuery;

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                OrderSummary data = new OrderSummary
                                {
                                    RefId = reader["RefId"] as string,
                                    Sym = reader["Symbol"] as string,
                                    Action = reader["ActionType"] as string,
                                    OrdDt = reader.IsDBNull(reader.GetOrdinal("OrderDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("OrderDate")),
                                    OrdTm = reader["OrderTime"] as string,
                                    OrdTyp = reader["OrderType"] as string,
                                    OrdSide = reader["OrderSide"] as string,
                                    OrdQty = (reader.IsDBNull(reader.GetOrdinal("OrderQty"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("OrderQty")),
                                    OrdPr = (reader.IsDBNull(reader.GetOrdinal("OrderPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OrderPrice")),
                                    OrdStopPr = (reader.IsDBNull(reader.GetOrdinal("OrderStopPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OrderStopPrice")),
                                    OrdOrigPr = (reader.IsDBNull(reader.GetOrdinal("OrderOrigPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OrderOrigPrice")),
                                    OrdSt = reader["OrderStatus"] as string,
                                    OrdDest = reader["OrderDest"] as string,
                                    OrdBkrStrat = reader["OrderBkrStrategy"] as string,
                                    AccName = reader["AccountName"] as string,
                                    Locate = reader["Locate"] as string,
                                    Trader = reader["Trader"] as string,
                                    ALMTrader = reader["ALMTrader"] as string,
                                    AlgoParameters = reader["AlgoParameters"] as string,

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
                                    DscntToLastPr = (reader.IsDBNull(reader.GetOrdinal("DiscountToLastPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DiscountToLastPrice")),
                                    DscntToBidPr = (reader.IsDBNull(reader.GetOrdinal("DiscountToBidPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DiscountToBidPrice")),
                                    DscntToAskPr = (reader.IsDBNull(reader.GetOrdinal("DiscountToAskPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DiscountToAskPrice")),
                                    EstNav = (reader.IsDBNull(reader.GetOrdinal("EstNav"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("EstNav")),
                                    EstNavTyp = reader["EstNavType"] as string,

                                    //Auto Update & Market Price Threshold Settings
                                    MktPrThld = (reader.IsDBNull(reader.GetOrdinal("MktPriceThreshold"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MktPriceThreshold")),
                                    MktPrSprd = (reader.IsDBNull(reader.GetOrdinal("MktPriceSpread"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MktPriceSpread")),
                                    MktPrFld = reader["MktPriceField"] as string,

                                    LastPr = (reader.IsDBNull(reader.GetOrdinal("LastPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LastPrice")),
                                    BidPr = (reader.IsDBNull(reader.GetOrdinal("BidPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BidPrice")),
                                    AskPr = (reader.IsDBNull(reader.GetOrdinal("AskPrice"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("AskPrice")),
                                };

                                data.Key = data.RefId;
                                data.OrdDtAsString = DateUtils.ConvertDate(data.OrdDt, "yyyy-MM-dd");

                                if (!orderSummaryDict.ContainsKey(data.RefId))
                                    orderSummaryDict.Add(data.RefId, data);
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

            return orderSummaryDict;
        }

        public IList<TradeExecutionSummaryTO> GetTradeExecutionSummary(DateTime startDate, DateTime endDate, string broker)
        {
            IList<TradeExecutionSummaryTO> list = new List<TradeExecutionSummaryTO>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(GetTradeExecutionSummaryQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_StartDate", startDate);
                        command.Parameters.AddWithValue("p_EndDate", endDate);
                        command.Parameters.AddWithValue("p_Broker", broker);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                TradeExecutionSummaryTO data = new TradeExecutionSummaryTO
                                {
                                    Curr = reader["Currency"] as string,
                                    BkrName = reader["BrokerName"] as string,
                                    Bkr = reader["OrderDest"] as string,
                                    Strategy = reader["OrderBkrStrategy"] as string,
                                    StartDt = reader.IsDBNull(reader.GetOrdinal("StartOrdDt")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("StartOrdDt")),
                                    EndDt = reader.IsDBNull(reader.GetOrdinal("EndOrdDt")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EndOrdDt")),
                                    TotTrdQty = (reader.IsDBNull(reader.GetOrdinal("TotTrdQty"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("TotTrdQty")),
                                    LongTrdQty = (reader.IsDBNull(reader.GetOrdinal("LongQty"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("LongQty")),
                                    ShortTrdQty = (reader.IsDBNull(reader.GetOrdinal("ShortQty"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("ShortQty")),
                                    TotTrades = (reader.IsDBNull(reader.GetOrdinal("TotOrd"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("TotOrd")),
                                    TotTrdMV = (reader.IsDBNull(reader.GetOrdinal("TotTrdMV"))) ? (double?)null : reader.GetDouble(reader.GetOrdinal("TotTrdMV")),
                                    LongTrdMV = (reader.IsDBNull(reader.GetOrdinal("LongMV"))) ? (double?)null : reader.GetDouble(reader.GetOrdinal("LongMV")),
                                    ShortTrdMV = (reader.IsDBNull(reader.GetOrdinal("ShortMV"))) ? (double?)null : reader.GetDouble(reader.GetOrdinal("ShortMV")),
                                    ScoPB1MV = (reader.IsDBNull(reader.GetOrdinal("ScoPB1MV"))) ? (double?)null : reader.GetDouble(reader.GetOrdinal("ScoPB1MV")),
                                    ScoPB5Qty = (reader.IsDBNull(reader.GetOrdinal("ScoPB5Qty"))) ? (double?)null : reader.GetDouble(reader.GetOrdinal("ScoPB5Qty")),
                                    ScoPG5Qty = (reader.IsDBNull(reader.GetOrdinal("ScoPG5Qty"))) ? (double?)null : reader.GetDouble(reader.GetOrdinal("ScoPG5Qty")),
                                    PB35MV = (reader.IsDBNull(reader.GetOrdinal("PB35MV"))) ? (double?)null : reader.GetDouble(reader.GetOrdinal("PB35MV")),
                                    PG35Qty = (reader.IsDBNull(reader.GetOrdinal("PG35Qty"))) ? (double?)null : reader.GetDouble(reader.GetOrdinal("PG35Qty")),
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

        public void SaveTradeSummary(IList<TradeSummaryTO> list)
        {
            try
            {
                int i = 0;
                int rowsToInsert = 0;

                StringBuilder sCommand = new StringBuilder("INSERT INTO almitasc_ACTradingBBGData.TradeSummary"
                    + " (TradeDate, Src, GrpName, Symbol, ALMSymbol, Side, Qty, Price, MV, BetaContr, DurContr,"
                    + " OppClsPos, OppAllocPos, OppAllocMV, OppAllocBeta, OppAllocDur, OppClsSecWt, OppLiveSecWt,"
                    + " TacClsPos, TacAllocPos, TacAllocMV, TacAllocBeta, TacAllocDur, TacClsSecWt, TacLiveSecWt,"
                    + " Trader, OrderDest, OrderStrategy, Sedol, ISIN, Curr, IsSwap,"
                    + " Beta, Dur, Fx, Multiplier, ShOut, DurSrc"
                    + " ) values ");

                //DateTime tradeDate = list[0].OrdDate.GetValueOrDefault();
                DateTime tradeDate = DateTime.Today;
                string tradeDateAsString = DateUtils.ConvertDate(tradeDate, DATEFORMAT, "0000-00-00");

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlTransaction trans = connection.BeginTransaction())
                    {
                        _logger.LogInformation("delete from almitasc_ACTradingBBGData.TradeSummary where TradeDate = '" + tradeDateAsString + "'");
                        string sqlDelete = "delete from almitasc_ACTradingBBGData.TradeSummary where TradeDate = '" + tradeDateAsString + "'";
                        using (MySqlCommand command = new MySqlCommand(sqlDelete, connection, trans))
                        {
                            command.ExecuteNonQuery();
                        }

                        IList<string> Rows = new List<string>();
                        StringBuilder sb = new StringBuilder();
                        foreach (TradeSummaryTO data in list)
                        {
                            // TradeDate
                            if (!string.IsNullOrEmpty(tradeDateAsString))
                                sb.Append(string.Concat("'", tradeDateAsString, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // Src
                            if (!string.IsNullOrEmpty(data.Src))
                                sb.Append(string.Concat("'", data.Src, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // GrpName
                            if (!string.IsNullOrEmpty(data.Sym))
                                sb.Append(string.Concat("'", data.Sym, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // Symbol
                            if (!string.IsNullOrEmpty(data.Sym))
                                sb.Append(string.Concat("'", data.Sym, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // ALMSymbol
                            if (!string.IsNullOrEmpty(data.ALMSym))
                                sb.Append(string.Concat("'", data.ALMSym, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // Side
                            if (!string.IsNullOrEmpty(data.Side))
                                sb.Append(string.Concat("'", data.Side, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // Qty
                            sb.Append(data.Qty).Append(DELIMITER);

                            // Price
                            if (data.Prc.HasValue)
                                sb.Append(data.Prc).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // MV
                            if (data.MV.HasValue)
                                sb.Append(data.MV).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // Beta
                            if (data.BetaContr.HasValue)
                                sb.Append(data.BetaContr).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // Duration
                            if (data.DurContr.HasValue)
                                sb.Append(data.DurContr).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            /////////////////////////////////////////////////////////////////////

                            // OppClsPos
                            if (data.FundOpp.All.TotalPos.HasValue)
                                sb.Append(data.FundOpp.All.TotalPos).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // OppAllocPos
                            if (data.FundOpp.AllocPos.HasValue)
                                sb.Append(data.FundOpp.AllocPos).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // OppAllocMV
                            if (data.FundOpp.AllocMV.HasValue)
                                sb.Append(data.FundOpp.AllocMV).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // OppAllocBeta
                            if (data.FundOpp.AllocBetaContr.HasValue)
                                sb.Append(data.FundOpp.AllocBetaContr).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // OppAllocDur
                            if (data.FundOpp.AllocDurContr.HasValue)
                                sb.Append(data.FundOpp.AllocDurContr).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // OppClsSecWt
                            if (data.FundOpp.SecWt.HasValue)
                                sb.Append(data.FundOpp.SecWt).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // OppLiveSecWt
                            if (data.FundOpp.LiveSecWt.HasValue)
                                sb.Append(data.FundOpp.LiveSecWt).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            /////////////////////////////////////////////////////////////////////

                            // TacClsPos
                            if (data.FundTac.All.TotalPos.HasValue)
                                sb.Append(data.FundTac.All.TotalPos).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // TacAllocPos
                            if (data.FundTac.AllocPos.HasValue)
                                sb.Append(data.FundTac.AllocPos).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // TacAllocMV
                            if (data.FundTac.AllocMV.HasValue)
                                sb.Append(data.FundTac.AllocMV).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // TacAllocBeta
                            if (data.FundTac.AllocBetaContr.HasValue)
                                sb.Append(data.FundTac.AllocBetaContr).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // TacAllocDur
                            if (data.FundTac.AllocDurContr.HasValue)
                                sb.Append(data.FundTac.AllocDurContr).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // TacClsSecWt
                            if (data.FundTac.SecWt.HasValue)
                                sb.Append(data.FundTac.SecWt).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // TacLiveSecWt
                            if (data.FundTac.LiveSecWt.HasValue)
                                sb.Append(data.FundTac.LiveSecWt).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // Trader
                            if (!string.IsNullOrEmpty(data.Trader))
                                sb.Append(string.Concat("'", data.Trader, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // OrderDest
                            if (!string.IsNullOrEmpty(data.Dest))
                                sb.Append(string.Concat("'", data.Dest, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // OrderStrategy
                            if (!string.IsNullOrEmpty(data.Strategy))
                                sb.Append(string.Concat("'", data.Strategy, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // Sedol
                            if (!string.IsNullOrEmpty(data.Sedol))
                                sb.Append(string.Concat("'", data.Sedol, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // ISIN
                            if (!string.IsNullOrEmpty(data.ISIN))
                                sb.Append(string.Concat("'", data.ISIN, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // Curr
                            if (!string.IsNullOrEmpty(data.Curr))
                                sb.Append(string.Concat("'", data.Curr, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // IsSwap
                            if (!string.IsNullOrEmpty(data.IsSwap))
                                sb.Append(string.Concat("'", data.IsSwap, "'")).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // Beta
                            if (data.Beta.HasValue)
                                sb.Append(data.Beta).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // Dur
                            if (data.Dur.HasValue)
                                sb.Append(data.Dur).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // Fx
                            if (data.Fx.HasValue)
                                sb.Append(data.Fx).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // Multiplier
                            if (data.Mult.HasValue)
                                sb.Append(data.Mult).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // ShOut
                            if (data.ShOut.HasValue)
                                sb.Append(data.ShOut).Append(DELIMITER);
                            else
                                sb.Append("null").Append(DELIMITER);

                            // DurSrc
                            if (!string.IsNullOrEmpty(data.DurSrc))
                                sb.Append(string.Concat("'", data.DurSrc, "'"));
                            else
                                sb.Append("null");

                            string row = sb.ToString();
                            Rows.Add(string.Concat("(", row, ")"));
                            sb.Clear();

                            rowsToInsert++;
                        }

                        sCommand.Append(string.Join(",", Rows));
                        sCommand.Append(";");

                        if (rowsToInsert > 0)
                        {
                            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), connection))
                            {
                                myCmd.CommandType = CommandType.Text;
                                myCmd.ExecuteNonQuery();
                            }
                        }

                        trans.Commit();
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving Trade Summary into database");
                throw;
            }
        }

        public IList<TradeSummaryTO> GetTradeSummaryHistory(string startDate, string endDate, string ticker)
        {
            IList<TradeSummaryTO> list = new List<TradeSummaryTO>();

            try
            {
                string sql = GetTradeSummaryQuery;
                sql += " where TradeDate between '" + startDate + "' and '" + endDate + "'";
                if (!string.IsNullOrEmpty(ticker))
                    sql += " and ALMSymbol = '" + ticker + "'";
                sql += " order by TradeDate desc";

                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(sql, connection))
                    {
                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                TradeSummaryTO data = new TradeSummaryTO
                                {
                                    TradeDate = (reader.IsDBNull(reader.GetOrdinal("TradeDate"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("TradeDate")),
                                    Src = reader["Src"] as string,
                                    Sym = reader["GrpName"] as string,
                                    ALMSym = reader["ALMSymbol"] as string,
                                    Side = reader["Side"] as string,
                                    Qty = Convert.ToInt32((reader.IsDBNull(reader.GetOrdinal("Qty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Qty"))),
                                    Prc = (reader.IsDBNull(reader.GetOrdinal("Price"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Price")),
                                    MV = (reader.IsDBNull(reader.GetOrdinal("MV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MV")),
                                    BetaContr = (reader.IsDBNull(reader.GetOrdinal("BetaContr"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("BetaContr")),
                                    DurContr = (reader.IsDBNull(reader.GetOrdinal("DurContr"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("DurContr")),
                                    OppClsPos = (reader.IsDBNull(reader.GetOrdinal("OppClsPos"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OppClsPos")),
                                    OppAllocPos = (reader.IsDBNull(reader.GetOrdinal("OppAllocPos"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OppAllocPos")),
                                    OppAllocMV = (reader.IsDBNull(reader.GetOrdinal("OppAllocMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OppAllocMV")),
                                    OppAllocBeta = (reader.IsDBNull(reader.GetOrdinal("OppAllocBeta"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OppAllocBeta")),
                                    OppAllocDur = (reader.IsDBNull(reader.GetOrdinal("OppAllocDur"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OppAllocDur")),
                                    OppClsSecWt = (reader.IsDBNull(reader.GetOrdinal("OppClsSecWt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OppClsSecWt")),
                                    OppLiveSecWt = (reader.IsDBNull(reader.GetOrdinal("OppLiveSecWt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("OppLiveSecWt")),
                                    TacClsPos = (reader.IsDBNull(reader.GetOrdinal("TacClsPos"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TacClsPos")),
                                    TacAllocPos = (reader.IsDBNull(reader.GetOrdinal("TacAllocPos"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TacAllocPos")),
                                    TacAllocMV = (reader.IsDBNull(reader.GetOrdinal("TacAllocMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TacAllocMV")),
                                    TacAllocBeta = (reader.IsDBNull(reader.GetOrdinal("TacAllocBeta"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TacAllocBeta")),
                                    TacAllocDur = (reader.IsDBNull(reader.GetOrdinal("TacAllocDur"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TacAllocDur")),
                                    TacClsSecWt = (reader.IsDBNull(reader.GetOrdinal("TacClsSecWt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TacClsSecWt")),
                                    TacLiveSecWt = (reader.IsDBNull(reader.GetOrdinal("TacLiveSecWt"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TacLiveSecWt")),
                                    Trader = reader["Trader"] as string,
                                    Dest = reader["OrderDest"] as string,
                                    Strategy = reader["OrderStrategy"] as string,
                                    Sedol = reader["Sedol"] as string,
                                    ISIN = reader["ISIN"] as string,
                                    Curr = reader["Curr"] as string,
                                    IsSwap = reader["IsSwap"] as string,
                                    Beta = (reader.IsDBNull(reader.GetOrdinal("Beta"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Beta")),
                                    Dur = (reader.IsDBNull(reader.GetOrdinal("Dur"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Dur")),
                                    DurSrc = reader["DurSrc"] as string,
                                    Fx = (reader.IsDBNull(reader.GetOrdinal("Fx"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Fx")),
                                    Mult = (reader.IsDBNull(reader.GetOrdinal("Multiplier"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("Multiplier")),
                                    ShOut = (reader.IsDBNull(reader.GetOrdinal("ShOut"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ShOut")),
                                };
                                list.Add(data);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing TradeSummaryHistory query");
                throw;
            }

            return list;
        }

        public IList<TradeExecutionSummaryTO> GetTradeCommissions(DateTime startDate, DateTime endDate, string broker)
        {
            IList<TradeExecutionSummaryTO> list = new List<TradeExecutionSummaryTO>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetTradeCommissionsQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_StartDate", startDate);
                        command.Parameters.AddWithValue("p_EndDate", endDate);
                        command.Parameters.AddWithValue("p_Broker", broker);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                TradeExecutionSummaryTO data = new TradeExecutionSummaryTO
                                {
                                    BkrName = reader["Broker"] as string,
                                    StartDt = reader.IsDBNull(reader.GetOrdinal("MinTradeDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("MinTradeDate")),
                                    EndDt = reader.IsDBNull(reader.GetOrdinal("MaxTradeDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("MaxTradeDate")),
                                    TotTradedQty = (reader.IsDBNull(reader.GetOrdinal("Qty"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("Qty")),
                                    LongTradedQty = (reader.IsDBNull(reader.GetOrdinal("LongQty"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("LongQty")),
                                    ShortTradedQty = (reader.IsDBNull(reader.GetOrdinal("ShortQty"))) ? (Int64?)null : reader.GetInt64(reader.GetOrdinal("ShortQty")),
                                    TotTrdMV = (reader.IsDBNull(reader.GetOrdinal("MVLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MVLocal")),
                                    LongTrdMV = (reader.IsDBNull(reader.GetOrdinal("LongMVLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LongMVLocal")),
                                    ShortTrdMV = (reader.IsDBNull(reader.GetOrdinal("ShortMVLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ShortMVLocal")),
                                    TotTrdMVUSD = (reader.IsDBNull(reader.GetOrdinal("MVUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("MVUSD")),
                                    LongTrdMVUSD = (reader.IsDBNull(reader.GetOrdinal("LongMVUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LongMVUSD")),
                                    ShortTrdMVUSD = (reader.IsDBNull(reader.GetOrdinal("ShortMVUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ShortMVUSD")),
                                    Comm = (reader.IsDBNull(reader.GetOrdinal("CommLocal"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CommLocal")),
                                    CommUSD = (reader.IsDBNull(reader.GetOrdinal("CommUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("CommUSD")),
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

        public IList<TradeTrackerTO> GetTradeTrackerDetails(DateTime startDate, DateTime endDate, string broker)
        {
            IList<TradeTrackerTO> list = new List<TradeTrackerTO>();

            try
            {
                using (MySqlConnection connection = new MySqlConnection(ConnectionFactory.CONNECTION_STRING))
                {
                    connection.Open();
                    using (MySqlCommand command = new MySqlCommand(GetTradeTrackerQuery, connection))
                    {
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.Parameters.AddWithValue("p_StartDate", startDate);
                        command.Parameters.AddWithValue("p_EndDate", endDate);
                        command.Parameters.AddWithValue("p_Broker", broker);

                        using (MySqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                TradeTrackerTO data = new TradeTrackerTO
                                {
                                    ExecutingBroker = reader["ExecutingBroker"] as string,
                                    NumTrades = (reader.IsDBNull(reader.GetOrdinal("NumTrades"))) ? (Int32?)null : reader.GetInt32(reader.GetOrdinal("NumTrades")),
                                    TotTrdQty = (reader.IsDBNull(reader.GetOrdinal("TotTrdQty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotTrdQty")),
                                    LongTrdQty = (reader.IsDBNull(reader.GetOrdinal("LongTrdQty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LongTrdQty")),
                                    ShortTrdQty = (reader.IsDBNull(reader.GetOrdinal("ShortTrdQty"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ShortTrdQty")),
                                    TotTrdMV = (reader.IsDBNull(reader.GetOrdinal("TotTrdMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotTrdMV")),
                                    TotTrdMVUSD = (reader.IsDBNull(reader.GetOrdinal("TotTrdMVUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("TotTrdMVUSD")),
                                    LongMV = (reader.IsDBNull(reader.GetOrdinal("LongMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LongMV")),
                                    LongMVUSD = (reader.IsDBNull(reader.GetOrdinal("LongMVUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("LongMVUSD")),
                                    ShortMV = (reader.IsDBNull(reader.GetOrdinal("ShortMV"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ShortMV")),
                                    ShortMVUSD = (reader.IsDBNull(reader.GetOrdinal("ShortMVUSD"))) ? (Double?)null : reader.GetDouble(reader.GetOrdinal("ShortMVUSD")),
                                    StartOrdDt = (reader.IsDBNull(reader.GetOrdinal("StartOrdDt"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("StartOrdDt")),
                                    EndOrdDt = (reader.IsDBNull(reader.GetOrdinal("EndOrdDt"))) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("EndOrdDt")),

                                };
                                list.Add(data);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing Trade Tracker query");
                throw;
            }
            return list;
        }

        public IList<ASTrade> GetTradeSummaryHistory(DateTime startDate, DateTime endDate)
        {
            throw new NotImplementedException();
        }

        //private const string GetOrderSummaryProdQuery = "select Rank() over (partition by MainOrderId order by OrderTime desc) as OrderRank, t.* from almitasc_ACTradingBBGData.TradeOrderDetail t where TradeSource = 'NV' and date_format(OrderDate, '%Y-%m-%d') = date_format(CONVERT_TZ(current_timestamp(),'+00:00','-8:00'), '%Y-%m-%d')";
        private const string GetOrderSummaryProdQuery = "select Rank() over (partition by MainOrderId order by OrderTime desc) as OrderRank, t.* from almitasc_ACTradingBBGData.TradeOrderDetail t where TradeSource = 'NV' and OrderDate = date_format(CONVERT_TZ(current_timestamp(),'+00:00','-8:00'), '%Y-%m-%d')";
        private const string GetOrderSummaryDevQuery = "select Rank() over (partition by MainOrderId order by OrderTime desc) as OrderRank, t.* from almitasc_ACTradingBBGData.TradeOrderDetail t where TradeSource = 'NV' and OrderDate = '2025-02-04'";

        //private const string GetOrderQueueProdQuery = "select * from almitasc_ACTradingBBGData.TradeOrderQueue t where date_format(OrderDate, '%Y-%m-%d') = date_format(CONVERT_TZ(current_timestamp(),'+00:00','-8:00'), '%Y-%m-%d')";
        //private const string GetOrderQueueDevQuery = "select * from almitasc_ACTradingBBGData.TradeOrderQueueTest t where date_format(OrderDate, '%Y-%m-%d') = date_format(CONVERT_TZ(current_timestamp(),'+00:00','-8:00'), '%Y-%m-%d')";

        private const string GetOrderQueueProdQuery = "select * from almitasc_ACTradingBBGData.TradeOrderQueue t where OrderDate = date_format(CONVERT_TZ(current_timestamp(),'+00:00','-8:00'), '%Y-%m-%d')";
        private const string GetOrderQueueDevQuery = "select * from almitasc_ACTradingBBGData.TradeOrderQueueTest t where OrderDate = '2025-02-04'";

        //private const string GetSimOrderSummaryQuery = "select * from almitasc_ACTradingBBGData.TradeOrderDetailTest where date_format(OrderDate, '%Y-%m-%d') = date_format(CONVERT_TZ(current_timestamp(),'+00:00','-8:00'), '%Y-%m-%d')";
        //private const string GetBatchOrdersQuery = "select * from almitasc_ACTradingBBGData.BatchOrderDetail where date_format(OrderDate, '%Y-%m-%d') = date_format(CONVERT_TZ(current_timestamp(),'+00:00','-8:00'), '%Y-%m-%d')";
        //private const string GetSimBatchOrdersQuery = "select * from almitasc_ACTradingBBGData.BatchOrderDetailTest where date_format(OrderDate, '%Y-%m-%d') = date_format(CONVERT_TZ(current_timestamp(),'+00:00','-8:00'), '%Y-%m-%d')";

        private const string GetSimOrderSummaryQuery = "select * from almitasc_ACTradingBBGData.TradeOrderDetailTest where OrderDate = date_format(CONVERT_TZ(current_timestamp(),'+00:00','-8:00'), '%Y-%m-%d')";
        private const string GetBatchOrdersQuery = "select * from almitasc_ACTradingBBGData.BatchOrderDetail where OrderDate = date_format(CONVERT_TZ(current_timestamp(),'+00:00','-8:00'), '%Y-%m-%d')";
        private const string GetSimBatchOrdersQuery = "select * from almitasc_ACTradingBBGData.BatchOrderDetailTest where OrderDate = date_format(CONVERT_TZ(current_timestamp(),'+00:00','-8:00'), '%Y-%m-%d')";

        private const string GetBatchOrderTemplatesQuery = "select distinct TemplateName from almitasc_ACTradingBBGData.BatchOrderTemplate where SampleTemplate is null order by TemplateName";
        private const string GetSampleBatchOrderTemplatesQuery = "select distinct TemplateName from almitasc_ACTradingBBGData.BatchOrderTemplate where SampleTemplate = 'Y' order by TemplateName";
        private const string GetBatchOrderTemplateQuery = "select * from almitasc_ACTradingBBGData.BatchOrderTemplate";
        private const string GetBatchOrderTemplatesForUserQuery = "select distinct TemplateName from almitasc_ACTradingBBGData.BatchOrderTemplate";
        private const string GetCompletedOrdersQuery = "spGetCompletedOrders";
        private const string GetTradeExecutionSummaryQuery = "spGetExecutionSummary";
        private const string GetTradeCommissionsQuery = "spGetTradeCommissions";
        private const string GetTradeTrackerQuery = "Reporting.spGetTradeSummary";
        private const string GetManualTradesQuery = "select * from almitasc_ACTradingBBGData.TradeOrderDetail";

        private const string SaveOrderSummaryQuery = "spPopulateTradeOrderDetails";
        private const string OrderSummaryStgTableName = "almitasc_ACTradingBBGData.StgTradeOrderDetail";
        private const string SaveSimOrderSummaryQuery = "spPopulateTradeOrderDetailsTest";
        private const string SimOrderSummaryStgTableName = "almitasc_ACTradingBBGData.StgTradeOrderDetailTest";
        private const string SaveBatchOrdersQuery = "spPopulateBatchOrders";
        private const string SaveSimBatchOrdersQuery = "spPopulateBatchOrdersTest";
        private const string BatchOrdersStgTableName = "almitasc_ACTradingBBGData.StgBatchOrderDetail";
        //private const string BatchQueueOrdersStgTableName = "almitasc_ACTradingBBGData.StgBatchOrderQueue";
        private const string SimBatchOrdersStgTableName = "almitasc_ACTradingBBGData.StgBatchOrderDetailTest";
        private const string OrderExecutionsStgTableName = "almitasc_ACTradingBBGData.StgTradeOrderExecution";
        private const string SimOrderExecutionsStgTableName = "almitasc_ACTradingBBGData.StgTradeOrderExecutionTest";
        private const string SaveOrderExecutionsQuery = "spPopulateTradeExecutionDetails";
        private const string SaveSimOrderExecutionsQuery = "spPopulateTradeExecutionDetailsTest";

        //private const string GetTradeExecutionDetailsProdQuery = "select * from almitasc_ACTradingBBGData.TradeOrderExecution where date_format(OrderDate, '%Y-%m-%d') = date_format(CONVERT_TZ(current_timestamp(),'+00:00','-8:00'), '%Y-%m-%d')";
        private const string GetTradeExecutionDetailsProdQuery = "select * from almitasc_ACTradingBBGData.TradeOrderExecution where OrderDate = date_format(CONVERT_TZ(current_timestamp(),'+00:00','-8:00'), '%Y-%m-%d')";
        private const string GetTradeExecutionDetailsDevQuery = "select * from almitasc_ACTradingBBGData.TradeOrderExecution where OrderDate = '2025-02-04'";
        //private const string GetSimTradeExecutionDetailsProdQuery = "select * from almitasc_ACTradingBBGData.TradeOrderExecutionTest where date_format(OrderDate, '%Y-%m-%d') = date_format(CONVERT_TZ(current_timestamp(),'+00:00','-8:00'), '%Y-%m-%d')";
        private const string GetSimTradeExecutionDetailsProdQuery = "select * from almitasc_ACTradingBBGData.TradeOrderExecutionTest where OrderDate = date_format(CONVERT_TZ(current_timestamp(),'+00:00','-8:00'), '%Y-%m-%d')";

        private const string GetPositionTickerMapQuery = "select concat('ISIN','|',ISIN) as Id, Ticker, ISIN from (select Ticker, ISIN, rank() over (partition by ISIN order by CreateDate desc) Rnk from almitasc_ACTradingBBGData.PfSecurityMst where ISIN is not null) s where s.Rnk = 1"
                                        + " union all"
                                        + " select concat('Sedol','|', Sedol) as Id, Ticker, Sedol from(select Ticker, Sedol, rank() over (partition by Sedol order by CreateDate desc) Rnk from almitasc_ACTradingBBGData.PfSecurityMst where Sedol is not null) s where s.Rnk = 1"
                                        + " union all"
                                        + " select concat('Cusip','|', Cusip) as Id, Ticker, Cusip from(select Ticker, Cusip, rank() over (partition by Cusip order by CreateDate desc) Rnk from almitasc_ACTradingBBGData.PfSecurityMst where Cusip is not null) s where s.Rnk = 1";

        //Trades and Allocations (Allocation Sheet)
        private const string TradeSummaryStgTableName = "almitasc_ACTradingBBGData.StgTradeEntry";
        private const string SaveTradeSummaryQuery = "spPopulateTradeSummary";
        private const string GetASTradesQuery = "select * from almitasc_ACTradingBBGData.Trade";
        private const string GetASTradeAllocationsQuery = "select * from almitasc_ACTradingBBGData.TradeAllocation";

        //Trade Targets
        private const string GetLatestTradeTargetsQuery = "select * from Trading.TradeTarget";
        private const string SaveTradingTargetsQuery = "Trading.spPopulateTradeTargets";
        private const string TradingTargetStgTableName = "Trading.StgTradeTarget";

        //Pair Trade Targets
        private const string GetLatestPairTradeTargetsQuery = "select * from Trading.PairTradeTarget";
        private const string SavePairTradingTargetsQuery = "Trading.spPopulatePairTradeTargets";
        private const string PairTradingTargetStgTableName = "Trading.StgPairTradeTarget";

        //Trade Queue
        private const string SaveOrderQueueQuery = "spPopulateTradeOrderQueue";
        private const string OrderQueueStgTableName = "almitasc_ACTradingBBGData.StgTradeOrderQueue";

        //Trade Queue (Simulation)
        private const string SaveSimOrderQueueQuery = "spPopulateTradeOrderQueueTest";
        private const string SimOrderQueueStgTableName = "almitasc_ACTradingBBGData.StgTradeOrderQueueTest";

        private const string GetTradeSummaryQuery = "select * from almitasc_ACTradingBBGData.TradeSummary";
    }
}