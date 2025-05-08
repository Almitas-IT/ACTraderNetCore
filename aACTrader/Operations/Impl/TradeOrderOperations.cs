using aACTrader.DAO.Repository;
using aCommons;
using aCommons.Cef;
using aCommons.DTO;
using aCommons.Trading;
using aCommons.Utils;
using LazyCache;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using PositionDetailTO = aCommons.DTO.PositionDetailTO;

namespace aACTrader.Operations.Impl
{
    public class TradeOrderOperations
    {
        private readonly ILogger<TradeOrderOperations> _logger;
        private readonly CachingService _cache;
        private readonly TradingDao _tradingDao;
        private readonly BaseDao _baseDao;
        private readonly EmailOperations _emailOperations;
        private readonly AllocationOperations _allocationOperations;
        private readonly BrokerCommissionOperations _brokerCommissionOperations;

        private HashSet<string> _tradeSet;
        private int _tradeStatsCountrySortId = 240;
        private int _tradeStatsSecurityTypeSortId = 450;

        private Dictionary<string, int> tradeExecutionGroups = new Dictionary<string, int>(StringComparer.CurrentCultureIgnoreCase)
        {
            {"Total", 100},
            {"AUD", 20},
            {"CAD", 300},
            {"CHF", 400},
            {"EUR", 500},
            {"GBP", 600},
            {"GBX", 700},
            {"USD", 800},
            {"Other",9000},
        };

        public TradeOrderOperations(ILogger<TradeOrderOperations> logger
            , CachingService cache
            , TradingDao tradingDao
            , BaseDao baseDao
            , EmailOperations emailOperations
            , AllocationOperations allocationOperations
            , BrokerCommissionOperations brokerCommissionOperations)
        {
            _logger = logger;
            _cache = cache;
            _tradingDao = tradingDao;
            _baseDao = baseDao;
            _emailOperations = emailOperations;
            _allocationOperations = allocationOperations;
            _brokerCommissionOperations = brokerCommissionOperations;
            _tradeSet = new HashSet<string>();
            _logger.LogInformation("Initializing TradeOrderOperations...");
        }

        /// <summary>
        /// 
        /// </summary>
        public void ProcessOrders()
        {
            _logger.LogInformation("Processing Orders - STARTED");
            GetExecutedOrders();
            //GetActiveOrders();
            _logger.LogInformation("Processing Orders - DONE");
        }

        /// <summary>
        /// Gets active orders
        /// </summary>
        private void GetActiveOrders()
        {
            IDictionary<string, IDictionary<string, OrderSummary>> orderSummaryBySymbolDict = _cache.Get<IDictionary<string, IDictionary<string, OrderSummary>>>(CacheKeys.ORDER_SUMMARY_BY_SYMBOL);

            //Mark Orders with Highest Bid and Lowest Offer
            if (orderSummaryBySymbolDict != null && orderSummaryBySymbolDict.Count > 0)
            {
                foreach (KeyValuePair<string, IDictionary<string, OrderSummary>> kvp in orderSummaryBySymbolDict)
                {
                    string key = kvp.Key;
                    IDictionary<string, OrderSummary> dict = kvp.Value;

                    string[] keyValues = key.Split('|');
                    string orderType = keyValues[1];

                    if ("BUY".Equals(orderType, StringComparison.CurrentCultureIgnoreCase))
                        GetHighestBid(dict);
                    else if ("SELL".Equals(orderType, StringComparison.CurrentCultureIgnoreCase) ||
                        "SELL SHORT".Equals(orderType, StringComparison.CurrentCultureIgnoreCase) ||
                        "SSx".Equals(orderType, StringComparison.CurrentCultureIgnoreCase))
                        GetLowestOffer(dict);
                    else if ("PAIR_TRADE".Equals(orderType, StringComparison.CurrentCultureIgnoreCase))
                        ProcessPairTrade(dict);
                }
            }
        }

        private void ProcessPairTrade(IDictionary<string, OrderSummary> orderSummaryDict)
        {
            foreach (KeyValuePair<string, OrderSummary> kvp in orderSummaryDict)
            {
                OrderSummary order = kvp.Value;
                order.BestBidOfferFlag = "Y";
            }
        }

        /// <summary>
        /// Identifies and flags Orders with Highest Bid
        /// </summary>
        /// <param name="orderSummaryDict"></param>
        private void GetHighestBid(IDictionary<string, OrderSummary> orderSummaryDict)
        {
            string orderId = string.Empty;
            double maxPrice = 0;
            foreach (KeyValuePair<string, OrderSummary> kvp in orderSummaryDict)
            {
                OrderSummary order = kvp.Value;
                order.BestBidOfferFlag = "N";

                if (string.IsNullOrEmpty(order.OrdActFlag))
                {
                    if (order.OrdPr > maxPrice)
                    {
                        maxPrice = order.OrdPr.GetValueOrDefault();
                        orderId = order.OrdId;
                    }
                }
            }

            OrderSummary maxOrder;
            if (orderSummaryDict.TryGetValue(orderId, out maxOrder))
                maxOrder.BestBidOfferFlag = "Y";
        }

        /// <summary>
        /// Identifies and flags Orders with Lowest Offer
        /// </summary>
        /// <param name="orderSummaryDict"></param>
        private void GetLowestOffer(IDictionary<string, OrderSummary> orderSummaryDict)
        {
            string orderId = string.Empty;
            double minPrice = Double.MaxValue;
            foreach (KeyValuePair<string, OrderSummary> kvp in orderSummaryDict)
            {
                OrderSummary order = kvp.Value;
                order.BestBidOfferFlag = "N";

                if (string.IsNullOrEmpty(order.OrdActFlag))
                {
                    if (order.OrdPr < minPrice)
                    {
                        minPrice = order.OrdPr.GetValueOrDefault();
                        orderId = order.OrdId;
                    }
                }
            }

            if (orderSummaryDict.TryGetValue(orderId, out OrderSummary maxOrder))
                maxOrder.BestBidOfferFlag = "Y";
        }

        /// <summary>
        /// Gets executed & open orders (Buy and Sell positions and prices)
        /// </summary>
        private void GetExecutedOrders()
        {
            IDictionary<string, TradePosition> executedTradeSummaryDict = new Dictionary<string, TradePosition>(StringComparer.CurrentCultureIgnoreCase);
            IDictionary<string, IDictionary<string, PositionDetailTO>> executedTradeDetailsDict = new Dictionary<string, IDictionary<string, PositionDetailTO>>(StringComparer.CurrentCultureIgnoreCase);

            //NEOVEST Trades
            //populate (summary) of all executed trades by symbol
            IDictionary<string, string> positionTickerMap = _cache.Get<IDictionary<string, string>>(CacheKeys.POSITION_IDENTIFIER_MAP);
            IDictionary<string, OrderSummary> orderSummaryDict = _cache.Get<IDictionary<string, OrderSummary>>(CacheKeys.ORDER_SUMMARY);
            IDictionary<string, OrderSummary> orderExecutionDetails = _cache.Get<IDictionary<string, OrderSummary>>(CacheKeys.ORDER_EXECUTION_DETAILS);
            IDictionary<string, MainOrderSummary> mainOrderSummaryDict = _cache.Get<IDictionary<string, MainOrderSummary>>(CacheKeys.MAIN_ORDER_SUMMARY);

            //NEOVEST Trades
            AddNeovestTrades(orderExecutionDetails, positionTickerMap, orderSummaryDict, executedTradeSummaryDict, executedTradeDetailsDict);

            //NON-NEOVEST Trades
            AddManualTrades(orderSummaryDict, executedTradeSummaryDict, executedTradeDetailsDict);

            //EMSX Trades
            AddEMSXTrades(executedTradeSummaryDict, executedTradeDetailsDict);

            //OPEN Orders
            foreach (KeyValuePair<string, MainOrderSummary> kvp in mainOrderSummaryDict)
            {
                MainOrderSummary mainOrderSummary = kvp.Value;
                if (orderSummaryDict.TryGetValue(mainOrderSummary.OrderId, out OrderSummary orderSummary))
                {
                    if (string.IsNullOrEmpty(orderSummary.OrdActFlag))
                    {
                        string symbol = orderSummary.ALMSym;
                        if (string.IsNullOrEmpty(symbol))
                        {
                            symbol = orderSummary.BBGSym;
                            if (string.IsNullOrEmpty(symbol))
                                symbol = orderSummary.Sym;
                        }

                        string positionSymbol = GetTradeSymbol(orderSummary, positionTickerMap);
                        AddOpenTrade(executedTradeSummaryDict, orderSummary, symbol, positionSymbol);
                    }
                }
            }

            //
            PopulateTradeSummaryDetails(executedTradeSummaryDict);
            PopulateTradeDetails(executedTradeDetailsDict);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="orderExecutionDetails"></param>
        /// <param name="positionTickerMap"></param>
        /// <param name="orderSummaryDict"></param>
        /// <param name="executedTradeSummaryDict"></param>
        /// <param name="executedTradeDetailsDict"></param>
        private void AddNeovestTrades(IDictionary<string, OrderSummary> orderExecutionDetails
            , IDictionary<string, string> positionTickerMap
            , IDictionary<string, OrderSummary> orderSummaryDict
            , IDictionary<string, TradePosition> executedTradeSummaryDict
            , IDictionary<string, IDictionary<string, PositionDetailTO>> executedTradeDetailsDict)
        {
            foreach (KeyValuePair<string, OrderSummary> kvp in orderExecutionDetails)
            {
                OrderSummary orderSummary = kvp.Value;
                string symbol = orderSummary.ALMSym;
                if (string.IsNullOrEmpty(symbol))
                    symbol = (!string.IsNullOrEmpty(orderSummary.BBGSym)) ? orderSummary.BBGSym : orderSummary.Sym;

                string positionSymbol = GetTradeSymbol(orderSummary, positionTickerMap);
                if (string.IsNullOrEmpty(positionSymbol))
                    positionSymbol = symbol;

                try
                {
                    AddTrade(orderSummaryDict, executedTradeSummaryDict, executedTradeDetailsDict, orderSummary, symbol, positionSymbol, "NV");
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error processing NEOVEST Execution Order: " + symbol + "/" + orderSummary.OrdId);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="orderSummaryDict"></param>
        /// <param name="executedTradeSummaryDict"></param>
        /// <param name="executedTradeDetailsDict"></param>
        private void AddManualTrades(IDictionary<string, OrderSummary> orderSummaryDict
            , IDictionary<string, TradePosition> executedTradeSummaryDict
            , IDictionary<string, IDictionary<string, PositionDetailTO>> executedTradeDetailsDict)
        {
            try
            {
                IList<ASTrade> tradeList = _cache.Get<IList<ASTrade>>(CacheKeys.MANUAL_TRADES);
                foreach (ASTrade trade in tradeList)
                {
                    OrderSummary mTrade = new OrderSummary();
                    mTrade.MainOrdId = trade.SourceSymbol;
                    mTrade.OrdSide = trade.Side;
                    mTrade.TrdQty = trade.Qty;
                    mTrade.TrdPr = trade.Price;
                    mTrade.Curr = trade.Currency;
                    mTrade.Trader = trade.Trader;
                    mTrade.OrdDest = trade.ExecutingBroker;

                    try
                    {
                        AddTrade(orderSummaryDict, executedTradeSummaryDict, executedTradeDetailsDict, mTrade, trade.SourceSymbol, trade.SourceSymbol, "Non-NV");
                    }
                    catch (Exception ex)
                    {

                        _logger.LogError(ex, "Error processing MANUAL Execution Order: " + trade.SourceSymbol);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing Manual/Non-Neovest Trades");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="executedTradeSummaryDict"></param>
        /// <param name="executedTradeDetailsDict"></param>
        private void AddEMSXTrades(IDictionary<string, TradePosition> executedTradeSummaryDict
            , IDictionary<string, IDictionary<string, PositionDetailTO>> executedTradeDetailsDict)
        {
            IDictionary<Int32, EMSXRouteStatus> emsxDict = _cache.Get<IDictionary<Int32, EMSXRouteStatus>>(CacheKeys.EMSX_ROUTE_STATUS);
            foreach (EMSXRouteStatus data in emsxDict.Values)
            {
                string symbol = data.Symbol;
                try
                {
                    if (string.IsNullOrEmpty(symbol))
                        symbol = (!string.IsNullOrEmpty(data.Ticker)) ? data.Ticker : data.Symbol;
                    AddEMSXTrade(emsxDict, executedTradeSummaryDict, executedTradeDetailsDict, data, symbol, "EMSX");
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error processing NEOVEST Execution Order: " + symbol);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tradeExecutionDict"></param>
        private void PopulateTradeSummaryDetails(IDictionary<string, TradePosition> tradeExecutionDict)
        {
            IDictionary<string, TradePosition> tradePositionDict = new Dictionary<string, TradePosition>();
            foreach (KeyValuePair<string, TradePosition> kvp in tradeExecutionDict)
            {
                TradePosition tradePosition = kvp.Value;
                string symbol = kvp.Key;

                if (tradePosition.IsExecuted.Equals("Y"))
                {
                    try
                    {
                        if (tradePosition.TotPos.GetValueOrDefault() != 0)
                            tradePosition.AvgPrc = tradePosition.AvgPrc / tradePosition.TotPos;
                        if (tradePosition.BuyPos.GetValueOrDefault() > 0)
                            tradePosition.BuyAvgPrc = tradePosition.BuyAvgPrc / tradePosition.BuyPos;
                        if (tradePosition.SellPos.GetValueOrDefault() > 0)
                            tradePosition.SellAvgPrc = tradePosition.SellAvgPrc / tradePosition.SellPos;

                        tradePositionDict.Add(symbol, tradePosition);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error calculating Avg Fill Price: " + symbol);
                    }
                }
                else
                {
                    tradePositionDict.Add(symbol, tradePosition);
                }
            }

            //_cache.Remove(CacheKeys.TRADE_EXECUTIONS);
            _cache.Add(CacheKeys.TRADE_EXECUTIONS, tradePositionDict, DateTimeOffset.MaxValue);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="positionReportDict"></param>
        private void PopulateTradeDetails(IDictionary<string, IDictionary<string, PositionDetailTO>> positionReportDict)
        {
            IDictionary<string, IDictionary<string, PositionDetailTO>> cachedTradeDetailsDict = _cache.Get<IDictionary<string, IDictionary<string, PositionDetailTO>>>(CacheKeys.LIVE_TRADE_DETAILS);
            cachedTradeDetailsDict.Clear();

            foreach (KeyValuePair<string, IDictionary<string, PositionDetailTO>> kvp in positionReportDict)
            {
                string symbol = kvp.Key;
                IDictionary<string, PositionDetailTO> dict = kvp.Value;

                foreach (KeyValuePair<string, PositionDetailTO> kvp1 in dict)
                {
                    try
                    {
                        PositionDetailTO tradePosition = kvp1.Value;
                        if (tradePosition.TotPos > 0)
                            tradePosition.AvgTrdPrc = tradePosition.TotTrdMV / tradePosition.TotPos;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error calculating Avg Fill Price: " + symbol);
                    }
                }

                cachedTradeDetailsDict.Add(symbol, dict);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="orderSummary"></param>
        /// <param name="positionTickerMap"></param>
        /// <returns></returns>
        private string GetTradeSymbol(OrderSummary orderSummary, IDictionary<string, string> positionTickerMap)
        {
            string positionSymbol = null;

            //link trade to existing position by SEDOL
            if (!string.IsNullOrEmpty(orderSummary.Sedol))
                positionTickerMap.TryGetValue("Sedol|" + orderSummary.Sedol, out positionSymbol);

            //link trade to existing position by CUSIP
            if (string.IsNullOrEmpty(positionSymbol) && !string.IsNullOrEmpty(orderSummary.Cusip))
                positionTickerMap.TryGetValue("Cusip|" + orderSummary.Cusip, out positionSymbol);
            //link trade to existing position by ISIN
            else if (string.IsNullOrEmpty(positionSymbol) && !string.IsNullOrEmpty(orderSummary.ISIN))
                positionTickerMap.TryGetValue("ISIN|" + orderSummary.ISIN, out positionSymbol);

            return positionSymbol;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="orderDict"></param>
        /// <param name="tradeDict"></param>
        /// <param name="positionDict"></param>
        /// <param name="order"></param>
        /// <param name="orderSymbol"></param>
        /// <param name="positionSymbol"></param>
        /// <param name="source"></param>
        private void AddTrade(IDictionary<string, OrderSummary> orderDict
            , IDictionary<string, TradePosition> tradeDict
            , IDictionary<string, IDictionary<string, PositionDetailTO>> positionDict
            , OrderSummary order
            , string orderSymbol
            , string positionSymbol
            , string source)
        {
            int orderSideId = 0;
            string orderSide = order.OrdSide;
            if (orderSide.Equals("B") || orderSide.Equals("BC") || orderSide.Equals("Buy") || orderSide.Equals("Buy Cover"))
                orderSideId = 1;
            else if (orderSide.Equals("S") || orderSide.Equals("SS") || orderSide.Equals("Sell") || orderSide.Equals("Sell Short") || orderSide.Equals("SSx"))
                orderSideId = 2;

            try
            {
                if (!tradeDict.TryGetValue(orderSymbol, out TradePosition trade))
                {
                    trade = new TradePosition();
                    trade.Sym = orderSymbol;
                    trade.PosSym = positionSymbol;
                    trade.Curr = order.Curr;
                    trade.IsExecuted = "Y";
                    tradeDict.Add(orderSymbol, trade);
                }

                if (trade != null)
                {
                    int? trdQty = order.TrdQty;
                    double trdMV = trdQty.GetValueOrDefault() * order.TrdPr.GetValueOrDefault();

                    trade.TotPos = CommonUtils.AddNullableInts(trade.TotPos, trdQty);
                    if (orderSideId == 1)
                    {
                        trade.Pos = CommonUtils.AddNullableInts(trade.Pos, trdQty);
                        trade.BuyPos = CommonUtils.AddNullableInts(trade.BuyPos, trdQty);
                        trade.AvgPrc = CommonUtils.AddNullableDoubles(trade.AvgPrc, trdMV);
                        trade.BuyAvgPrc = CommonUtils.AddNullableDoubles(trade.BuyAvgPrc, trdMV);
                    }
                    else if (orderSideId == 2)
                    {
                        trade.Pos = CommonUtils.AddNullableInts(trade.Pos, -1 * trdQty);
                        trade.SellPos = CommonUtils.AddNullableInts(trade.SellPos, trdQty);
                        trade.AvgPrc = CommonUtils.AddNullableDoubles(trade.AvgPrc, trdMV);
                        trade.SellAvgPrc = CommonUtils.AddNullableDoubles(trade.SellAvgPrc, trdMV);
                    }

                    ////TODO: 1/26/2023 - separate out SELLs from SHORT SELLs
                    ////This is a temporary fix to adjust unexecuted positions
                    //if (orderSide.Equals("SS") || orderSide.Equals("Sell Short") || orderSide.Equals("SSx"))
                    //    trade.SSellPos = CommonUtils.AddNullableInts(trade.SSellPos, trdQty);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error adding Trade details for Symbol: " + orderSymbol);
            }

            try
            {
                //capture trade details in Live Position report
                PositionDetailTO position;
                if (!positionDict.TryGetValue(orderSymbol, out IDictionary<string, PositionDetailTO> positionDetailDict))
                {
                    positionDetailDict = new Dictionary<string, PositionDetailTO>();
                    positionDict.Add(orderSymbol, positionDetailDict);
                }

                if (!positionDetailDict.TryGetValue(order.MainOrdId, out position))
                {
                    position = new PositionDetailTO();
                    position.Sym = orderSymbol;
                    position.PosSym = positionSymbol;
                    position.MainOrdId = order.MainOrdId;
                    position.Side = order.OrdSide;
                    position.Dest = order.OrdDest;
                    position.Strategy = order.OrdBkrStrat;
                    position.Trader = order.Trader;
                    position.Src = source;
                    position.Curr = order.Curr;

                    if (orderDict.TryGetValue(order.MainOrdId, out OrderSummary initialOrder))
                    {
                        position.OrdQty = initialOrder.OrdQty.GetValueOrDefault();
                        position.OrdPrc = initialOrder.OrdPr.GetValueOrDefault();
                        position.OrdTime = initialOrder.OrdTm;
                        position.RefIndex = initialOrder.RI;
                        position.DscntTgt = initialOrder.DscntTgt;
                        position.TrdId = initialOrder.NVExecIdLng.GetValueOrDefault();
                    }

                    positionDetailDict.Add(order.MainOrdId, position);
                }

                if (position != null)
                {
                    int trdQty = order.TrdQty.GetValueOrDefault();
                    double trdMV = trdQty * order.TrdPr.GetValueOrDefault();

                    position.TotPos += trdQty;
                    if (orderSideId == 1)
                    {
                        position.TotTrdQty += trdQty;
                        position.TotTrdMV += trdMV;
                    }
                    else if (orderSideId == 2)
                    {
                        position.TotTrdQty += -1 * trdQty;
                        position.TotTrdMV += trdMV;
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error adding Trade details for Symbol: " + orderSymbol);
                throw;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="emsxDict"></param>
        /// <param name="tradeDict"></param>
        /// <param name="positionDict"></param>
        /// <param name="order"></param>
        /// <param name="orderSymbol"></param>
        /// <param name="source"></param>
        private void AddEMSXTrade(IDictionary<Int32, EMSXRouteStatus> emsxDict
            , IDictionary<string, TradePosition> tradeDict
            , IDictionary<string, IDictionary<string, PositionDetailTO>> positionDict
            , EMSXRouteStatus order
            , string orderSymbol
            , string source)
        {
            int orderSideId = 0;
            string ordSeq = order.OrdSeq.GetValueOrDefault().ToString();

            try
            {
                string orderSide = order.OrdSide;
                if (!string.IsNullOrEmpty(orderSide))
                {
                    if (orderSide.Equals("BUY", StringComparison.CurrentCultureIgnoreCase) || orderSide.Equals("COVR", StringComparison.CurrentCultureIgnoreCase))
                        orderSideId = 1;
                    else if (orderSide.Equals("SELL", StringComparison.CurrentCultureIgnoreCase) || orderSide.Equals("SHRT", StringComparison.CurrentCultureIgnoreCase))
                        orderSideId = 2;
                }

                if (!tradeDict.TryGetValue(orderSymbol, out TradePosition trade))
                {
                    trade = new TradePosition();
                    trade.Sym = orderSymbol;
                    trade.PosSym = orderSymbol;
                    //trade.Curr = order.Curr;
                    trade.IsExecuted = "Y";
                    tradeDict.Add(orderSymbol, trade);
                }

                if (trade != null)
                {
                    int? trdQty = order.DayFill;
                    double trdMV = trdQty.GetValueOrDefault() * order.DayAvgPrc.GetValueOrDefault();
                    trade.TotPos = CommonUtils.AddNullableInts(trade.TotPos, trdQty);
                    if (orderSideId == 1)
                    {
                        trade.Pos = CommonUtils.AddNullableInts(trade.Pos, trdQty);
                        trade.BuyPos = CommonUtils.AddNullableInts(trade.BuyPos, trdQty);
                        trade.AvgPrc = CommonUtils.AddNullableDoubles(trade.AvgPrc, trdMV);
                        trade.BuyAvgPrc = CommonUtils.AddNullableDoubles(trade.BuyAvgPrc, trdMV);
                    }
                    else if (orderSideId == 2)
                    {
                        trade.Pos = CommonUtils.AddNullableInts(trade.Pos, -1 * trdQty);
                        trade.SellPos = CommonUtils.AddNullableInts(trade.SellPos, trdQty);
                        trade.AvgPrc = CommonUtils.AddNullableDoubles(trade.AvgPrc, trdMV);
                        trade.SellAvgPrc = CommonUtils.AddNullableDoubles(trade.SellAvgPrc, trdMV);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error adding EMSX Trade details for Symbol: " + orderSymbol + "/" + ordSeq);
            }

            try
            {
                //capture trade details in Live Position report
                PositionDetailTO position;
                if (!positionDict.TryGetValue(orderSymbol, out IDictionary<string, PositionDetailTO> positionDetailDict))
                {
                    positionDetailDict = new Dictionary<string, PositionDetailTO>();
                    positionDict.Add(orderSymbol, positionDetailDict);
                }


                if (!positionDetailDict.TryGetValue(ordSeq, out position))
                {
                    position = new PositionDetailTO();
                    position.Sym = orderSymbol;
                    position.PosSym = orderSymbol;
                    position.MainOrdId = ordSeq;
                    position.Side = order.OrdSide;
                    position.Dest = order.Bkr;
                    position.Strategy = order.Strategy;
                    position.Trader = order.Trader;
                    position.Src = source;
                    //position.Curr = order.Curr;

                    //if (emsxDict.TryGetValue(order.OrdSeq.GetValueOrDefault(), out EMSXRouteStatus initialOrder))
                    //{
                    //    position.OrdQty = initialOrder.OrdQty.GetValueOrDefault();
                    //    position.OrdPrc = initialOrder.OrdPr.GetValueOrDefault();
                    //    position.OrdTime = initialOrder.OrdTm;
                    //    position.RefIndex = initialOrder.RI;
                    //    position.DscntTgt = initialOrder.DscntTgt;
                    //    position.TrdId = initialOrder.NVExecIdLng.GetValueOrDefault();
                    //}

                    positionDetailDict.Add(ordSeq, position);
                }

                if (position != null)
                {
                    int trdQty = order.DayFill.GetValueOrDefault();
                    double trdMV = trdQty * order.DayAvgPrc.GetValueOrDefault();
                    position.TotPos += trdQty;
                    if (orderSideId == 1)
                    {
                        position.TotTrdQty += trdQty;
                        position.TotTrdMV += trdMV;
                    }
                    else if (orderSideId == 2)
                    {
                        position.TotTrdQty += -1 * trdQty;
                        position.TotTrdMV += trdMV;
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error adding EMSX Trade details for Symbol: " + orderSymbol + "/" + ordSeq);
                throw;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tradeDict"></param>
        /// <param name="orderSummary"></param>
        /// <param name="orderSymbol"></param>
        /// <param name="positionSymbol"></param>
        private void AddOpenTrade(IDictionary<string, TradePosition> tradeDict
            , OrderSummary orderSummary
            , string orderSymbol
            , string positionSymbol)
        {
            int orderSideId = 0;
            string orderSide = orderSummary.OrdSide;
            if (orderSide.Equals("B") || orderSide.Equals("BC") || orderSide.Equals("Buy") || orderSide.Equals("Buy Cover"))
                orderSideId = 1;
            else if (orderSide.Equals("S") || orderSide.Equals("Sell"))
                orderSideId = 2;
            else if (orderSide.Equals("SS") || orderSide.Equals("Sell Short") || orderSide.Equals("SSx"))
                orderSideId = 3;

            try
            {
                if (!tradeDict.TryGetValue(orderSymbol, out TradePosition trade))
                {
                    trade = new TradePosition();
                    trade.Sym = orderSymbol;
                    trade.PosSym = positionSymbol;
                    trade.Curr = orderSummary.Curr;
                    trade.IsExecuted = "N";
                    tradeDict.Add(orderSymbol, trade);
                }

                if (trade != null)
                {
                    if (orderSideId == 1)
                        trade.OpenBuyPos = CommonUtils.AddNullableInts(trade.OpenBuyPos, orderSummary.LeavesQty);
                    else if (orderSideId == 2)
                        trade.OpenSellPos = CommonUtils.AddNullableInts(trade.OpenSellPos, orderSummary.LeavesQty);
                    else if (orderSideId == 3)
                        trade.OpenSSPos = CommonUtils.AddNullableInts(trade.OpenSSPos, orderSummary.LeavesQty);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error adding Open Trade details for Symbol: " + orderSymbol);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void ProcessTradeList()
        {
            IList<OrderSummary> newTrades = new List<OrderSummary>();
            IList<OrderSummary> existingTrades = new List<OrderSummary>();
            IList<OrderSummary> tradeList = _tradingDao.GetExecutedOrders(DateTime.Today);

            foreach (OrderSummary order in tradeList)
            {
                if (_tradeSet.Contains(order.MainOrdId))
                {
                    existingTrades.Add(order);
                }
                else
                {
                    newTrades.Add(order);
                    existingTrades.Add(order);
                    _tradeSet.Add(order.MainOrdId);
                }
            }

            if (newTrades.Count > 0)
            {
                string message = _emailOperations.GenerateTradeExecutionEmail(existingTrades, newTrades);
                if (!string.IsNullOrEmpty(message) && message.Length > 0)
                    _emailOperations.SendEmail(message, "AC Trader - Trade Execution Alert");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IList<TradeTarget> GetTradeTargets()
        {
            IList<TradeTarget> tradeTargets = _tradingDao.GetTradeTargets();
            IDictionary<string, PositionMaster> positionDict = _cache.Get<IDictionary<string, PositionMaster>>(CacheKeys.POSITION_MASTER);
            IDictionary<string, string> securityDict = _cache.Get<IDictionary<string, string>>(CacheKeys.POSITION_IDENTIFIER_MAP);
            IDictionary<string, SecurityPrice> securityPriceDict = _cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);
            IDictionary<string, string> priceTickerMap = _cache.Get<IDictionary<string, string>>(CacheKeys.PRICE_TICKER_MAP);
            IDictionary<string, FundForecast> fundForecastDict = _cache.Get<IDictionary<string, FundForecast>>(CacheKeys.FUND_FORECASTS);

            foreach (TradeTarget tradeTarget in tradeTargets)
            {
                //Position
                PositionMaster positionMaster = _allocationOperations.GetPositionDetails(tradeTarget.Ticker, tradeTarget.Ticker, tradeTarget.Ticker, positionDict, securityDict);
                if (positionMaster != null)
                {
                    tradeTarget.ShOut = positionMaster.ShOut;
                    tradeTarget.Avg20DayVol = positionMaster.Avg20DayVol;
                    tradeTarget.Pos = positionMaster.FundAll.PosHeld;
                    tradeTarget.PosWt = positionMaster.FundAll.ClsMVPct;
                    tradeTarget.ShOwnWt = positionMaster.FundAll.PosOwnPct;
                }

                SecurityPrice securityPrice = SecurityPriceLookupOperations.GetSecurityPrice(tradeTarget.Ticker, priceTickerMap, securityPriceDict);
                if (securityPrice == null)
                {
                    string newTicker = tradeTarget.Ticker.Replace(" CN", " CT");
                    securityPrice = SecurityPriceLookupOperations.GetSecurityPrice(newTicker, priceTickerMap, securityPriceDict);
                }
                if (securityPrice != null)
                {
                    tradeTarget.Price = securityPrice.LastPrc;
                    tradeTarget.MV = tradeTarget.Qty * tradeTarget.Price;
                }

                FundForecast fundForecast;
                fundForecastDict.TryGetValue(tradeTarget.Ticker, out fundForecast);
                if (fundForecast == null)
                {
                    //search by CT ticker if composite ticker CN is provided
                    if (tradeTarget.Ticker.EndsWith(" CN", StringComparison.CurrentCultureIgnoreCase))
                    {
                        string newTicker = tradeTarget.Ticker.Replace(" CN", " CT");
                        fundForecastDict.TryGetValue(newTicker, out fundForecast);
                    }
                }
                if (fundForecast != null)
                {
                    tradeTarget.Nav = fundForecast.LastDvdAdjNav;
                    tradeTarget.Dscnt = fundForecast.LastPD;
                    tradeTarget.SecurityType = fundForecast.SecType;
                    tradeTarget.PDLastPrc = fundForecast.PDLastPrc;
                    tradeTarget.PDBidPrc = fundForecast.PDBidPrc;
                    tradeTarget.PDAskPrc = fundForecast.PDAskPrc;
                    if (tradeTarget.PDLastPrc.HasValue)
                        tradeTarget.TgtToPDLastPrc = Math.Abs(tradeTarget.PDLastPrc.GetValueOrDefault() - tradeTarget.TgtDscnt.GetValueOrDefault());
                    if ("B".Equals(tradeTarget.Side) || "BC".Equals(tradeTarget.Side))
                    {
                        if ((tradeTarget.PDLastPrc.HasValue) && (tradeTarget.PDLastPrc.GetValueOrDefault() < tradeTarget.TgtDscnt.GetValueOrDefault()))
                            tradeTarget.PDRangeFlag = "Y";

                        if ((tradeTarget.PDAskPrc.HasValue) && (tradeTarget.PDAskPrc.GetValueOrDefault() < tradeTarget.TgtDscnt.GetValueOrDefault()))
                            tradeTarget.PDRangeFlag = "Y";
                    }
                    else if ("S".Equals(tradeTarget.Side) || "SS".Equals(tradeTarget.Side))
                    {
                        if ((tradeTarget.PDLastPrc.HasValue) && (tradeTarget.PDLastPrc.GetValueOrDefault() > tradeTarget.TgtDscnt.GetValueOrDefault()))
                            tradeTarget.PDRangeFlag = "Y";

                        if ((tradeTarget.PDBidPrc.HasValue) && (tradeTarget.PDBidPrc.GetValueOrDefault() > tradeTarget.TgtDscnt.GetValueOrDefault()))
                            tradeTarget.PDRangeFlag = "Y";
                    }

                }
            }
            return tradeTargets;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IList<TradeTargetTO> GetTradeTargetDiscounts()
        {
            IList<TradeTargetTO> list = new List<TradeTargetTO>();
            IList<TradeTarget> tradeTargets = _tradingDao.GetTradeTargets();
            foreach (TradeTarget tradeTarget in tradeTargets)
            {
                TradeTargetTO data = new TradeTargetTO
                {
                    Ticker = tradeTarget.Ticker,
                    Side = tradeTarget.Side,
                    TgtDscnt = tradeTarget.TgtDscnt,
                };
            }
            return list;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IList<TradeGroupStats> CalculateTradeStats()
        {
            IDictionary<string, TradeGroupStats> tradeStatsDict = new Dictionary<string, TradeGroupStats>();
            IDictionary<string, OrderSummary> orderSummaryDict = _cache.Get<IDictionary<string, OrderSummary>>(CacheKeys.ORDER_SUMMARY);
            IDictionary<string, OrderSummary> orderExecutionDetails = _cache.Get<IDictionary<string, OrderSummary>>(CacheKeys.ORDER_EXECUTION_DETAILS);
            IDictionary<string, MainOrderSummary> mainOrderSummaryDict = _cache.Get<IDictionary<string, MainOrderSummary>>(CacheKeys.MAIN_ORDER_SUMMARY);
            IDictionary<string, FXRate> fxRateDict = _cache.Get<IDictionary<string, FXRate>>(CacheKeys.FX_RATES);
            IDictionary<string, SecurityMasterExt> securityMasterExtDict = _cache.Get<IDictionary<string, SecurityMasterExt>>(CacheKeys.SECURITY_EXT_DETAILS);
            IDictionary<string, string> securityDict = _cache.Get<IDictionary<string, string>>(CacheKeys.POSITION_IDENTIFIER_MAP);
            IDictionary<string, FundMaster> fundMasterDict = _cache.Get<IDictionary<string, FundMaster>>(CacheKeys.FUND_MASTER);

            //Create Trade Groups
            CreateTradeStatsGroups(tradeStatsDict);

            IList<OrderSummary> orderExecutionList = new List<OrderSummary>();

            foreach (KeyValuePair<string, MainOrderSummary> kvp in mainOrderSummaryDict)
            {
                MainOrderSummary mainOrderSummary = kvp.Value;

                //List of Fills
                orderExecutionList.Clear();
                HashSet<string> executionIdList = mainOrderSummary.ExecutionIdList;
                foreach (string executionId in executionIdList)
                {
                    if (orderExecutionDetails.TryGetValue(executionId, out OrderSummary orderExecution))
                        orderExecutionList.Add(orderExecution);
                }

                //
                if (orderSummaryDict.TryGetValue(mainOrderSummary.OrderId, out OrderSummary orderSummary))
                {
                    string symbol = !string.IsNullOrEmpty(orderSummary.ALMSym) ? orderSummary.ALMSym : orderSummary.Sym;
                    string tradingSymbol = orderSummary.Sym;
                    SecurityMasterExt securityMasterExt = CommonOperationsUtil.GetSecurityMasterExt(symbol, tradingSymbol, orderSummary.Sedol, orderSummary.ISIN, securityMasterExtDict, securityDict);

                    //Total
                    AddToTradeStatsGroup(tradeStatsDict, "Total", mainOrderSummary, orderSummary, orderExecutionList, fxRateDict, securityMasterExt);

                    //By Order Side
                    if (orderSummary.OrdSide.Equals("Sell"))
                        AddToTradeStatsGroup(tradeStatsDict, "Order Side/Sell", mainOrderSummary, orderSummary, orderExecutionList, fxRateDict, securityMasterExt);
                    else if (orderSummary.OrdSide.Equals("Buy"))
                        AddToTradeStatsGroup(tradeStatsDict, "Order Side/Buy", mainOrderSummary, orderSummary, orderExecutionList, fxRateDict, securityMasterExt);
                    else if (orderSummary.OrdSide.Equals("Sell Short"))
                        AddToTradeStatsGroup(tradeStatsDict, "Order Side/Sell Short", mainOrderSummary, orderSummary, orderExecutionList, fxRateDict, securityMasterExt);

                    //By Trader
                    AddToTradeStatsGroup(tradeStatsDict, "Trader/" + orderSummary.Trader, mainOrderSummary, orderSummary, orderExecutionList, fxRateDict, securityMasterExt);

                    //By Country
                    fundMasterDict.TryGetValue(symbol, out FundMaster fundMaster);
                    string country = "Other";
                    if (fundMaster != null && !string.IsNullOrEmpty(fundMaster.Cntry))
                    {
                        country = fundMaster.Cntry;
                        if (country.Equals("Unknown"))
                            country = "Other";
                        if (!tradeStatsDict.TryGetValue("Country/" + country, out TradeGroupStats tradeGroupStats))
                            AddTradeStatsCountry(tradeStatsDict, country);
                    }
                    AddToTradeStatsGroup(tradeStatsDict, "Country/" + country, mainOrderSummary, orderSummary, orderExecutionList, fxRateDict, securityMasterExt);

                    //By Security Type
                    string securityType = "Other";
                    if (fundMaster != null && !string.IsNullOrEmpty(fundMaster.SecTyp))
                    {
                        if (fundMaster.SecTyp.StartsWith("Reit", StringComparison.CurrentCultureIgnoreCase))
                            securityType = "Reit";
                        else if (fundMaster.SecTyp.StartsWith("BDC", StringComparison.CurrentCultureIgnoreCase))
                            securityType = "BDC";
                        else
                            securityType = fundMaster.SecTyp;

                        if (securityType.Equals("Unknown"))
                            securityType = "Other";
                        if (!tradeStatsDict.TryGetValue("Security Type/" + securityType, out TradeGroupStats tradeGroupStats))
                            AddTradeStatsSecurityType(tradeStatsDict, securityType);
                    }
                    AddToTradeStatsGroup(tradeStatsDict, "Security Type/" + securityType, mainOrderSummary, orderSummary, orderExecutionList, fxRateDict, securityMasterExt);
                }
            }

            IList<TradeGroupStats> list = tradeStatsDict.Values.ToList<TradeGroupStats>();
            return list.OrderBy(x => x.SortId).ToList<TradeGroupStats>();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tradeStatsDict"></param>
        /// <param name="groupName"></param>
        /// <param name="mainOrderSummary"></param>
        /// <param name="orderSummary"></param>
        /// <param name="orderExecutionList"></param>
        /// <param name="fxRateDict"></param>
        /// <param name="securityMasterExt"></param>
        public void AddToTradeStatsGroup(IDictionary<string, TradeGroupStats> tradeStatsDict
            , string groupName
            , MainOrderSummary mainOrderSummary
            , OrderSummary orderSummary
            , IList<OrderSummary> orderExecutionList
            , IDictionary<string, FXRate> fxRateDict
            , SecurityMasterExt securityMasterExt)
        {
            if (tradeStatsDict.TryGetValue(groupName, out TradeGroupStats tradeGroupStats))
            {
                tradeGroupStats.Count++;

                if ("N".Equals(orderSummary.OrdActFlag))
                    tradeGroupStats.InActv++;
                else
                    tradeGroupStats.Actv++;

                /////////////////////////////////////
                /// MV
                /////////////////////////////////////
                double longShortInd = 1;
                int qty = orderSummary.OrdQty.GetValueOrDefault();
                double orderPrice = orderSummary.OrdPr.GetValueOrDefault();
                string currency = orderSummary.Curr;
                double mv = qty * orderPrice;
                double factor = 1.0;
                int tradedQty = 0;
                double beta = 1.0;
                int cancelled = 0;

                if (securityMasterExt != null)
                    beta = securityMasterExt.RiskBeta.GetValueOrDefault();

                if (orderSummary.OrdSide.Equals("Sell") || orderSummary.OrdSide.Equals("Sell Short"))
                    longShortInd = -1;

                if (!(currency.Equals("USD", StringComparison.CurrentCultureIgnoreCase)))
                {
                    if ("GBp".Equals(currency) || "GBX".Equals(currency))
                    {
                        factor /= 100.0;
                        currency = "GBP";
                    }

                    if (fxRateDict.TryGetValue(currency, out FXRate fxRateFund))
                        factor *= fxRateFund.FXRateLatest.GetValueOrDefault();
                }

                tradeGroupStats.MVUSD += (mv * factor * longShortInd);
                if (longShortInd == 1)
                    tradeGroupStats.MVLongUSD += (mv * factor);
                else
                    tradeGroupStats.MVShortUSD += (mv * factor * longShortInd);

                if (orderExecutionList.Count > 0)
                {
                    foreach (OrderSummary order in orderExecutionList)
                    {
                        tradedQty += order.TrdQty.GetValueOrDefault();
                        double filledMV = (order.TrdPr.GetValueOrDefault() * order.TrdQty.GetValueOrDefault() * factor * longShortInd);
                        tradeGroupStats.NumFills++;
                        tradeGroupStats.MVFilledUSD += filledMV;
                        tradeGroupStats.BetaUSD += (filledMV * beta);

                        if (longShortInd == 1)
                            tradeGroupStats.MVLongFilledUSD += filledMV;
                        else
                            tradeGroupStats.MVShortFilledUSD += filledMV;
                    }
                }

                if (orderSummary.OrdSt.Equals("Canceled") || orderSummary.OrdSt.Equals("Rejected"))
                    cancelled = 1;

                if (tradedQty == qty)
                    tradeGroupStats.Comp++;
                else if (tradedQty == 0 && cancelled == 1)
                    tradeGroupStats.Canc++;
                else if (tradedQty < qty)
                    tradeGroupStats.Partial++;

                OrderStatsTO data = new OrderStatsTO
                {
                    MainOrderId = orderSummary.MainOrdId,
                    OrderId = orderSummary.OrdId,
                    BBGSymbol = orderSummary.BBGSym,
                    ALMSymbol = orderSummary.ALMSym,
                    OrderTime = orderSummary.OrdTm,
                    OrderType = orderSummary.OrdTyp,
                    OrderSide = orderSummary.OrdSide,
                    Curr = orderSummary.Curr,
                    OrderQty = orderSummary.OrdQty,
                    OrderOrigPrice = orderSummary.OrdOrigPr,
                    OrderPrice = orderSummary.OrdPr,
                    OrderStatus = orderSummary.OrdSt,
                    OrderMemo = orderSummary.OrdMemo,
                    OrderDest = orderSummary.OrdDest,
                    OrderBkrStrategy = orderSummary.OrdBkrStrat,
                    Trader = orderSummary.Trader,
                    ALMTrader = orderSummary.ALMTrader,
                    TradedQty = orderSummary.TrdCumQty,
                    CanceledQty = orderSummary.CancQty,
                    LeavesQty = orderSummary.LeavesQty,
                    TradedPrice = orderSummary.AvgTrdPr,
                    OrderStatusUpdateTime = orderSummary.OrdStatusUpdTm,
                    NumFills = orderExecutionList.Count,
                };

                if (tradedQty > 0 && data.OrderQty.GetValueOrDefault() > 0)
                    data.FillPct = (double)tradedQty / data.OrderQty;

                tradeGroupStats.OrderList.Add(data);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tradeStatsDict"></param>
        private void CreateTradeStatsGroups(IDictionary<string, TradeGroupStats> tradeStatsDict)
        {
            //Total
            TradeGroupStats tradeGroupStats = new TradeGroupStats();
            tradeGroupStats.GrpName = "Total";
            tradeGroupStats.DispName = "Total";
            tradeGroupStats.SortId = 1;
            tradeStatsDict.Add(tradeGroupStats.GrpName, tradeGroupStats);

            /////////////////////////////////////////////////////////
            //By Order Side

            //OrderSide
            tradeGroupStats = new TradeGroupStats();
            tradeGroupStats.GrpName = "Order Side";
            tradeGroupStats.DispName = "Order Side";
            tradeGroupStats.Header = "Y";
            tradeGroupStats.SortId = 100;
            tradeStatsDict.Add(tradeGroupStats.GrpName, tradeGroupStats);

            //Buy
            tradeGroupStats = new TradeGroupStats();
            tradeGroupStats.GrpName = "Order Side/Buy";
            tradeGroupStats.DispName = "--Buy";
            tradeGroupStats.SortId = 110;
            tradeStatsDict.Add(tradeGroupStats.GrpName, tradeGroupStats);

            //Sell
            tradeGroupStats = new TradeGroupStats();
            tradeGroupStats.GrpName = "Order Side/Sell";
            tradeGroupStats.DispName = "--Sell";
            tradeGroupStats.SortId = 120;
            tradeStatsDict.Add(tradeGroupStats.GrpName, tradeGroupStats);

            //Sell Short
            tradeGroupStats = new TradeGroupStats();
            tradeGroupStats.GrpName = "Order Side/Sell Short";
            tradeGroupStats.DispName = "--Sell Short";
            tradeGroupStats.SortId = 130;
            tradeStatsDict.Add(tradeGroupStats.GrpName, tradeGroupStats);

            /////////////////////////////////////////////////////////
            //By Country

            //Country
            tradeGroupStats = new TradeGroupStats();
            tradeGroupStats.GrpName = "Country";
            tradeGroupStats.DispName = "Country";
            tradeGroupStats.Header = "Y";
            tradeGroupStats.SortId = 200;
            tradeStatsDict.Add(tradeGroupStats.GrpName, tradeGroupStats);

            //US
            tradeGroupStats = new TradeGroupStats();
            tradeGroupStats.GrpName = "Country/United States";
            tradeGroupStats.DispName = "--United States";
            tradeGroupStats.SortId = 210;
            tradeStatsDict.Add(tradeGroupStats.GrpName, tradeGroupStats);

            //UK
            tradeGroupStats = new TradeGroupStats();
            tradeGroupStats.GrpName = "Country/United Kingdom";
            tradeGroupStats.DispName = "--United Kingdom";
            tradeGroupStats.SortId = 220;
            tradeStatsDict.Add(tradeGroupStats.GrpName, tradeGroupStats);

            //Canada
            tradeGroupStats = new TradeGroupStats();
            tradeGroupStats.GrpName = "Country/Canada";
            tradeGroupStats.DispName = "--Canada";
            tradeGroupStats.SortId = 230;
            tradeStatsDict.Add(tradeGroupStats.GrpName, tradeGroupStats);

            //Australia
            tradeGroupStats = new TradeGroupStats();
            tradeGroupStats.GrpName = "Country/Australia";
            tradeGroupStats.DispName = "--Australia";
            tradeGroupStats.SortId = 240;
            tradeStatsDict.Add(tradeGroupStats.GrpName, tradeGroupStats);

            //Other
            tradeGroupStats = new TradeGroupStats();
            tradeGroupStats.GrpName = "Country/Other";
            tradeGroupStats.DispName = "--Other";
            tradeGroupStats.SortId = 290;
            tradeStatsDict.Add(tradeGroupStats.GrpName, tradeGroupStats);

            /////////////////////////////////////////////////////////
            //By Trader
            tradeGroupStats = new TradeGroupStats();
            tradeGroupStats.GrpName = "Trader";
            tradeGroupStats.DispName = "Trader";
            tradeGroupStats.Header = "Y";
            tradeGroupStats.SortId = 300;
            tradeStatsDict.Add(tradeGroupStats.GrpName, tradeGroupStats);

            //ac-rmass
            tradeGroupStats = new TradeGroupStats();
            tradeGroupStats.GrpName = "Trader/ac-rmass";
            tradeGroupStats.DispName = "--ac-rmass";
            tradeGroupStats.SortId = 310;
            tradeStatsDict.Add(tradeGroupStats.GrpName, tradeGroupStats);

            //ac-rmass-mb1
            tradeGroupStats = new TradeGroupStats();
            tradeGroupStats.GrpName = "Trader/ac-rmass-mb1";
            tradeGroupStats.DispName = "--ac-rmass-mb1";
            tradeGroupStats.SortId = 311;
            tradeStatsDict.Add(tradeGroupStats.GrpName, tradeGroupStats);

            //ac-wchia
            tradeGroupStats = new TradeGroupStats();
            tradeGroupStats.GrpName = "Trader/ac-wchia";
            tradeGroupStats.DispName = "--ac-wchia";
            tradeGroupStats.SortId = 320;
            tradeStatsDict.Add(tradeGroupStats.GrpName, tradeGroupStats);

            //ac-tnan
            tradeGroupStats = new TradeGroupStats();
            tradeGroupStats.GrpName = "Trader/ac-tnan";
            tradeGroupStats.DispName = "--ac-tnan";
            tradeGroupStats.SortId = 330;
            tradeStatsDict.Add(tradeGroupStats.GrpName, tradeGroupStats);

            //ac-azouhar
            tradeGroupStats = new TradeGroupStats();
            tradeGroupStats.GrpName = "Trader/ac-azouhar";
            tradeGroupStats.DispName = "--ac-azouhar";
            tradeGroupStats.SortId = 340;
            tradeStatsDict.Add(tradeGroupStats.GrpName, tradeGroupStats);

            //ac-kleuteneker
            tradeGroupStats = new TradeGroupStats();
            tradeGroupStats.GrpName = "Trader/ac-kleuteneker";
            tradeGroupStats.DispName = "--ac-kleuteneker";
            tradeGroupStats.SortId = 350;
            tradeStatsDict.Add(tradeGroupStats.GrpName, tradeGroupStats);

            //almitas-api-prod
            tradeGroupStats = new TradeGroupStats();
            tradeGroupStats.GrpName = "Trader/almitas-api-prod";
            tradeGroupStats.DispName = "--almitas-api-prod";
            tradeGroupStats.SortId = 360;
            tradeStatsDict.Add(tradeGroupStats.GrpName, tradeGroupStats);

            /////////////////////////////////////////////////////////
            //By Security Type

            //Security Type
            tradeGroupStats = new TradeGroupStats();
            tradeGroupStats.GrpName = "Security Type";
            tradeGroupStats.DispName = "Security Type";
            tradeGroupStats.Header = "Y";
            tradeGroupStats.SortId = 400;
            tradeStatsDict.Add(tradeGroupStats.GrpName, tradeGroupStats);

            //Closed End Fund
            tradeGroupStats = new TradeGroupStats();
            tradeGroupStats.GrpName = "Security Type/Closed End Fund";
            tradeGroupStats.DispName = "--Closed End Fund";
            tradeGroupStats.SortId = 410;
            tradeStatsDict.Add(tradeGroupStats.GrpName, tradeGroupStats);

            //BDC
            tradeGroupStats = new TradeGroupStats();
            tradeGroupStats.GrpName = "Security Type/BDC";
            tradeGroupStats.DispName = "--BDC";
            tradeGroupStats.SortId = 420;
            tradeStatsDict.Add(tradeGroupStats.GrpName, tradeGroupStats);

            //Reit
            tradeGroupStats = new TradeGroupStats();
            tradeGroupStats.GrpName = "Security Type/Reit";
            tradeGroupStats.DispName = "--Reit";
            tradeGroupStats.SortId = 430;
            tradeStatsDict.Add(tradeGroupStats.GrpName, tradeGroupStats);

            //Corporation
            tradeGroupStats = new TradeGroupStats();
            tradeGroupStats.GrpName = "Security Type/Corporation";
            tradeGroupStats.DispName = "--Corporation";
            tradeGroupStats.SortId = 440;
            tradeStatsDict.Add(tradeGroupStats.GrpName, tradeGroupStats);

            //Holding Companies
            tradeGroupStats = new TradeGroupStats();
            tradeGroupStats.GrpName = "Security Type/Holding Companies";
            tradeGroupStats.DispName = "--Holding Companies";
            tradeGroupStats.SortId = 450;
            tradeStatsDict.Add(tradeGroupStats.GrpName, tradeGroupStats);

            //Other
            tradeGroupStats = new TradeGroupStats();
            tradeGroupStats.GrpName = "Security Type/Other";
            tradeGroupStats.DispName = "--Other";
            tradeGroupStats.SortId = 500;
            tradeStatsDict.Add(tradeGroupStats.GrpName, tradeGroupStats);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tradeStatsDict"></param>
        /// <param name="country"></param>
        private void AddTradeStatsCountry(IDictionary<string, TradeGroupStats> tradeStatsDict, string country)
        {
            TradeGroupStats tradeGroupStats = new TradeGroupStats();
            tradeGroupStats.GrpName = "Country/" + country;
            tradeGroupStats.DispName = "--" + country;
            _tradeStatsCountrySortId += 5;
            tradeGroupStats.SortId = _tradeStatsCountrySortId;
            tradeStatsDict.Add(tradeGroupStats.GrpName, tradeGroupStats);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tradeStatsDict"></param>
        /// <param name="securityType"></param>
        private void AddTradeStatsSecurityType(IDictionary<string, TradeGroupStats> tradeStatsDict, string securityType)
        {
            TradeGroupStats tradeGroupStats = new TradeGroupStats();
            tradeGroupStats.GrpName = "Security Type/" + securityType;
            tradeGroupStats.DispName = "--" + securityType;
            _tradeStatsSecurityTypeSortId += 5;
            tradeGroupStats.SortId = _tradeStatsSecurityTypeSortId;
            tradeStatsDict.Add(tradeGroupStats.GrpName, tradeGroupStats);
        }

        public IList<TradeExecutionSummaryTO> GetTradeExecutionSummary(DateTime startDate, DateTime endDate, string broker)
        {
            IDictionary<string, TradeExecutionSummaryTO> dict = new Dictionary<string, TradeExecutionSummaryTO>();
            IList<TradeExecutionSummaryTO> list = _tradingDao.GetTradeExecutionSummary(startDate, endDate, broker);

            int sortId;
            foreach (TradeExecutionSummaryTO tradeExecutionSummary in list)
            {
                sortId = 9000; //Other Currency group
                CalculateCommissions(tradeExecutionSummary);

                //Add to Currency group
                string grpName = tradeExecutionSummary.Curr;
                if (tradeExecutionGroups.ContainsKey(grpName))
                    sortId = tradeExecutionGroups[grpName];
                if (!dict.TryGetValue(grpName, out TradeExecutionSummaryTO currencyGroup))
                {
                    currencyGroup = new TradeExecutionSummaryTO(grpName, grpName, sortId);
                    currencyGroup.IsGrpRow = 2;
                    currencyGroup.Curr = grpName;
                    dict.Add(grpName, currencyGroup);
                }

                //Add to Broker group
                grpName = tradeExecutionSummary.Curr + "/" + tradeExecutionSummary.BkrName;
                if (!dict.TryGetValue(grpName, out TradeExecutionSummaryTO bkrGroup))
                {
                    currencyGroup.ChildSortId++;
                    bkrGroup = new TradeExecutionSummaryTO(grpName, tradeExecutionSummary.BkrName, currencyGroup.ChildSortId);
                    bkrGroup.IsGrpRow = 1;
                    bkrGroup.BkrName = tradeExecutionSummary.BkrName;
                    dict.Add(grpName, bkrGroup);
                }

                //Add to Broker Strategy group
                grpName = tradeExecutionSummary.Curr + "/" + tradeExecutionSummary.Bkr + "/" + tradeExecutionSummary.Strategy;
                if (!dict.TryGetValue(grpName, out TradeExecutionSummaryTO bkrDestStrategyGroup))
                {
                    bkrGroup.ChildSortId++;
                    bkrDestStrategyGroup = new TradeExecutionSummaryTO(grpName, tradeExecutionSummary.Bkr, bkrGroup.ChildSortId);
                    bkrDestStrategyGroup.IsGrpRow = 1;
                    bkrDestStrategyGroup.BkrStrategy = tradeExecutionSummary.Bkr + "|" + tradeExecutionSummary.Strategy;
                    bkrDestStrategyGroup.StartDt = tradeExecutionSummary.StartDt;
                    bkrDestStrategyGroup.EndDt = tradeExecutionSummary.EndDt;
                    dict.Add(grpName, bkrDestStrategyGroup);
                }

                AddExecutionSummary(currencyGroup, tradeExecutionSummary);
                AddExecutionSummary(bkrGroup, tradeExecutionSummary);
                AddExecutionSummary(bkrDestStrategyGroup, tradeExecutionSummary);
            }

            return dict.Values.ToList<TradeExecutionSummaryTO>();
        }

        private TradeExecutionSummaryTO GetGroup(IDictionary<string, TradeExecutionSummaryTO> dict, string grpName, int sortId, string displayName)
        {
            if (!dict.TryGetValue(grpName, out TradeExecutionSummaryTO summary))
            {
                summary = new TradeExecutionSummaryTO(grpName, displayName, sortId++);
                dict.Add(grpName, summary);
            }
            return summary;
        }

        private void AddExecutionSummary(TradeExecutionSummaryTO group, TradeExecutionSummaryTO data)
        {
            group.TotTrades = CommonUtils.AddNullableLongs(group.TotTrades, data.TotTrades);
            group.TotTrdQty = CommonUtils.AddNullableLongs(group.TotTrdQty, data.TotTrdQty);
            group.LongTrdQty = CommonUtils.AddNullableLongs(group.LongTrdQty, data.LongTrdQty);
            group.ShortTrdQty = CommonUtils.AddNullableLongs(group.ShortTrdQty, data.ShortTrdQty);
            group.TotTrdMV = CommonUtils.AddNullableDoubles(group.TotTrdMV, data.TotTrdMV);
            group.LongTrdMV = CommonUtils.AddNullableDoubles(group.LongTrdMV, data.LongTrdMV);
            group.ShortTrdMV = CommonUtils.AddNullableDoubles(group.ShortTrdMV, data.ShortTrdMV);
            group.ScoPB1MV = CommonUtils.AddNullableDoubles(group.ScoPB1MV, data.ScoPB1MV);
            group.ScoPB5Qty = CommonUtils.AddNullableDoubles(group.ScoPB5Qty, data.ScoPB5Qty);
            group.PB35MV = CommonUtils.AddNullableDoubles(group.PB35MV, data.PB35MV);
            group.PG35Qty = CommonUtils.AddNullableDoubles(group.PG35Qty, data.PG35Qty);
            group.Comm = CommonUtils.AddNullableDoubles(group.Comm, data.Comm);
        }

        private void CalculateCommissions(TradeExecutionSummaryTO data)
        {
            data.Comm = _brokerCommissionOperations.GetCommission(data);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ticker"></param>
        /// <param name="account"></param>
        /// <param name="broker"></param>
        /// <param name="multiBrokerFlag"></param>
        /// <param name="AgeRangeFrom"></param>
        /// <param name="AgeRangeTo"></param>
        /// <returns></returns>
        public IList<TradeTaxLot> GetTradeTaxLots(string ticker, string account, string broker, string multiBrokerFlag, string AgeRangeFrom, string AgeRangeTo)
        {
            IList<TradeTaxLot> list = _baseDao.GetTaxLots(ticker, account, broker, AgeRangeFrom, AgeRangeTo);
            IList<TradeTaxLot> resultList = new List<TradeTaxLot>();
            IDictionary<string, TradeTaxLot> dict = new Dictionary<string, TradeTaxLot>();

            bool includeTaxLot;
            int multiBrokerFlagInd = 0;
            if (!string.IsNullOrEmpty(multiBrokerFlag) && "Y".Equals(multiBrokerFlag))
                multiBrokerFlagInd = 1;

            if (list != null && list.Count > 0)
            {
                AddTaxLotGroup(dict, "Total", 1, 0);
                AddTaxLotGroup(dict, "Long Term", 2, 0);
                AddTaxLotGroup(dict, "Short Term", 3, 0);

                foreach (TradeTaxLot data in list)
                {
                    includeTaxLot = true;
                    if (multiBrokerFlagInd == 1)
                    {
                        if (data.MultBkrFlag == 1)
                            includeTaxLot = true;
                        else
                            includeTaxLot = false;
                    }

                    if (includeTaxLot)
                    {
                        //Currency Conversion
                        if ("GBP".Equals(data.TaxLotCurr) && "GBp".Equals(data.SecCurr))
                        {
                            if (data.UnitCostLcl.HasValue)
                                data.UnitCostLcl *= 100;
                            if (data.UnitCostUSD.HasValue)
                                data.UnitCostUSD *= 100;
                            if (data.PrcLcl.HasValue)
                                data.PrcLcl *= 100;
                            if (data.PrcUSD.HasValue)
                                data.PrcUSD *= 100;
                        }

                        //Total
                        AddTaxLotDataToGroup(dict, "Total", data, 1, 0);
                        //Long/Short Term
                        if (data.Term.Equals("Long Term"))
                            AddTaxLotDataToGroup(dict, "Long Term", data, 2, 0);
                        else
                            AddTaxLotDataToGroup(dict, "Short Term", data, 3, 0);
                        //Fund
                        AddTaxLotDataToGroup(dict, data.Acct, data, 4, 0);
                        //Broker
                        AddTaxLotDataToGroup(dict, data.Bkr, data, 5, 0);
                        //Ticker
                        if (!string.IsNullOrEmpty(ticker))
                            AddTaxLotDataToGroup(dict, ticker, data, 6, 1);

                        resultList.Add(data);
                    }
                }

                //Add group records
                foreach (TradeTaxLot data in dict.Values)
                {
                    try
                    {
                        if (data.IsTickerGroup == 1)
                        {
                            if (data.UnitCostLcl.HasValue && data.Qty.GetValueOrDefault() != 0)
                                data.UnitCostLcl /= data.Qty.GetValueOrDefault();
                            if (data.UnitCostUSD.HasValue && data.Qty.GetValueOrDefault() != 0)
                                data.UnitCostUSD /= data.Qty.GetValueOrDefault();
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError("Error calculating Weighted Averages for group: " + data.Ticker, ex);
                    }

                    resultList.Add(data);
                }
            }

            //return resultList.OrderBy(t => t.SortId).OrderBy(t => t.Ticker).OrderByDescending(t => t.OpenDate).ToList<TradeTaxLot>();
            return resultList.OrderBy(t => t.SortId).ToList<TradeTaxLot>();
        }

        private void AddTaxLotDataToGroup(IDictionary<string, TradeTaxLot> dict, string groupName, TradeTaxLot data, int sortId, int isTickerGrp)
        {
            if (!dict.TryGetValue(groupName, out TradeTaxLot group))
                group = AddTaxLotGroup(dict, groupName, sortId, isTickerGrp);

            if (group != null)
            {
                group.MVUSD = CommonUtils.AddNullableDoubles(group.MVUSD, data.MVUSD);
                group.PnLUSD = CommonUtils.AddNullableDoubles(group.PnLUSD, data.PnLUSD);
                group.TotalCostUSD = CommonUtils.AddNullableDoubles(group.TotalCostUSD, data.TotalCostUSD);
                if (group.IsTickerGroup == 1)
                {
                    group.Qty = CommonUtils.AddNullableDoubles(group.Qty, data.Qty);
                    group.UnitCostLcl = CommonUtils.AddNullableDoubles(group.UnitCostLcl, data.UnitCostLcl * data.Qty);
                    group.UnitCostUSD = CommonUtils.AddNullableDoubles(group.UnitCostUSD, data.UnitCostUSD * data.Qty);
                    group.PrcUSD = data.PrcUSD;
                }
            }
        }

        private TradeTaxLot AddTaxLotGroup(IDictionary<string, TradeTaxLot> dict, string groupName, int sortId, int isTickerGrp)
        {
            TradeTaxLot group = new TradeTaxLot { Ticker = groupName, IsGrpRow = 1, SortId = sortId, IsTickerGroup = isTickerGrp };
            dict.Add(groupName, group);
            return group;
        }

        public IList<TradeExecutionSummaryTO> GetTradeCommissions(DateTime startDate, DateTime endDate, string broker)
        {
            IDictionary<string, TradeExecutionSummaryTO> dict = new Dictionary<string, TradeExecutionSummaryTO>();
            IList<TradeExecutionSummaryTO> list = _tradingDao.GetTradeCommissions(startDate, endDate, broker);
            return list;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IDictionary<string, TradeActivity> GetTradeActivity()
        {
            IDictionary<string, TradeActivity> tradeActivityDict = new Dictionary<string, TradeActivity>(StringComparer.CurrentCultureIgnoreCase);

            IDictionary<string, PositionMaster> positionDict = _cache.Get<IDictionary<string, PositionMaster>>(CacheKeys.POSITION_MASTER);
            IDictionary<string, string> securityDict = _cache.Get<IDictionary<string, string>>(CacheKeys.POSITION_IDENTIFIER_MAP);
            IDictionary<string, string> positionTickerMap = _cache.Get<IDictionary<string, string>>(CacheKeys.POSITION_IDENTIFIER_MAP);
            IDictionary<string, OrderSummary> orderExecutionDetails = _cache.Get<IDictionary<string, OrderSummary>>(CacheKeys.ORDER_EXECUTION_DETAILS);
            IList<ASTrade> tradeList = _cache.Get<IList<ASTrade>>(CacheKeys.MANUAL_TRADES);
            IDictionary<Int32, EMSXRouteStatus> emsxDict = _cache.Get<IDictionary<Int32, EMSXRouteStatus>>(CacheKeys.EMSX_ROUTE_STATUS);
            int orderSideId = 0;

            //NEOVEST Trades
            try
            {
                foreach (OrderSummary data in orderExecutionDetails.Values)
                {
                    string symbol = data.ALMSym;
                    if (string.IsNullOrEmpty(symbol))
                        symbol = (!string.IsNullOrEmpty(data.BBGSym)) ? data.BBGSym : data.Sym;

                    string positionSymbol = GetTradeSymbol(data, positionTickerMap);
                    if (string.IsNullOrEmpty(positionSymbol))
                        positionSymbol = symbol;

                    try
                    {
                        string orderSide = data.OrdSide;
                        if (orderSide.Equals("Buy", StringComparison.CurrentCultureIgnoreCase) || orderSide.Equals("Buy Cover", StringComparison.CurrentCultureIgnoreCase))
                            orderSideId = 1;
                        else if (orderSide.Equals("Sell", StringComparison.CurrentCultureIgnoreCase) || orderSide.Equals("Sell Short", StringComparison.CurrentCultureIgnoreCase) || orderSide.Equals("SSx", StringComparison.CurrentCultureIgnoreCase))
                            orderSideId = 2;

                        string lookupSymbol = symbol + "|" + orderSideId;
                        if (!tradeActivityDict.TryGetValue(lookupSymbol, out TradeActivity tradeActivity))
                        {
                            tradeActivity = new TradeActivity();
                            tradeActivity.Sym = symbol;
                            tradeActivity.PosSym = positionSymbol;
                            tradeActivity.SrcSym = data.Sym;
                            PositionMaster positionMaster = CommonOperationsUtil.GetPositionDetails(symbol, data.Sedol, data.ISIN, positionDict, securityDict);
                            if (positionMaster != null)
                                tradeActivity.BODPos = positionMaster.FundAll.PosHeld;

                            tradeActivityDict.Add(lookupSymbol, tradeActivity);
                        }

                        if (tradeActivity != null)
                        {
                            tradeActivity.NVCurr = data.Curr;
                            tradeActivity.NVOrdSide = data.OrdSide;

                            int? trdQty = data.TrdQty;
                            double trdMV = trdQty.GetValueOrDefault() * data.TrdPr.GetValueOrDefault();
                            if (orderSideId == 1)
                            {
                                tradeActivity.NVOrdQty = CommonUtils.AddNullableInts(tradeActivity.NVOrdQty, trdQty);
                                tradeActivity.NVOrdPrc = CommonUtils.AddNullableDoubles(tradeActivity.NVOrdPrc, trdMV);
                            }
                            else if (orderSideId == 2)
                            {
                                tradeActivity.NVOrdQty = CommonUtils.AddNullableInts(tradeActivity.NVOrdQty, -1 * trdQty);
                                tradeActivity.NVOrdPrc = CommonUtils.AddNullableDoubles(tradeActivity.NVOrdPrc, trdMV);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error processing NEOVEST Order: " + symbol + "/" + data.OrdId);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error populating NEOVEST Trades");
            }

            //MANUAL Trades
            try
            {
                foreach (ASTrade trade in tradeList)
                {
                    try
                    {
                        string orderSide = trade.Side;
                        if (orderSide.Equals("Buy", StringComparison.CurrentCultureIgnoreCase) || orderSide.Equals("BuyToCover", StringComparison.CurrentCultureIgnoreCase))
                            orderSideId = 1;
                        else if (orderSide.Equals("Sell", StringComparison.CurrentCultureIgnoreCase) || orderSide.Equals("SellShort", StringComparison.CurrentCultureIgnoreCase))
                            orderSideId = 2;

                        string lookupSymbol = trade.SourceSymbol + "|" + orderSideId;
                        if (!tradeActivityDict.TryGetValue(lookupSymbol, out TradeActivity tradeActivity))
                        {
                            tradeActivity = new TradeActivity();
                            tradeActivity.Sym = trade.SourceSymbol;
                            tradeActivity.PosSym = trade.SourceSymbol;
                            tradeActivity.SrcSym = trade.SourceSymbol;
                            PositionMaster positionMaster = CommonOperationsUtil.GetPositionDetails(trade.SourceSymbol, trade.Sedol, trade.ISIN, positionDict, securityDict);
                            if (positionMaster != null)
                                tradeActivity.BODPos = positionMaster.FundAll.PosHeld;

                            tradeActivityDict.Add(lookupSymbol, tradeActivity);
                        }

                        if (tradeActivity != null)
                        {
                            tradeActivity.OTHCurr = trade.Currency;
                            tradeActivity.OTHOrdSide = trade.Side;

                            int? trdQty = trade.Qty;
                            double trdMV = trdQty.GetValueOrDefault() * trade.Price.GetValueOrDefault();
                            if (orderSideId == 1)
                            {
                                tradeActivity.OTHOrdQty = CommonUtils.AddNullableInts(tradeActivity.OTHOrdQty, trdQty);
                                tradeActivity.OTHOrdPrc = CommonUtils.AddNullableDoubles(tradeActivity.OTHOrdPrc, trdMV);
                            }
                            else if (orderSideId == 2)
                            {
                                tradeActivity.OTHOrdQty = CommonUtils.AddNullableInts(tradeActivity.OTHOrdQty, -1 * trdQty);
                                tradeActivity.OTHOrdPrc = CommonUtils.AddNullableDoubles(tradeActivity.OTHOrdPrc, trdMV);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error processing MANUAL Order: " + trade.SourceSymbol);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error populating Manual/Non-Neovest Trades");
            }

            //EMSX Trades
            try
            {
                foreach (EMSXRouteStatus data in emsxDict.Values)
                {
                    if (data.DayFill.HasValue && data.DayFill.GetValueOrDefault() > 0)
                    {
                        string symbol = data.Symbol;
                        try
                        {
                            if (string.IsNullOrEmpty(symbol))
                                symbol = (!string.IsNullOrEmpty(data.Ticker)) ? data.Ticker : data.Symbol;

                            string orderSide = data.OrdSide;
                            if (orderSide.Equals("BUY", StringComparison.CurrentCultureIgnoreCase) || orderSide.Equals("COVR", StringComparison.CurrentCultureIgnoreCase))
                                orderSideId = 1;
                            else if (orderSide.Equals("SELL", StringComparison.CurrentCultureIgnoreCase) || orderSide.Equals("SHRT", StringComparison.CurrentCultureIgnoreCase))
                                orderSideId = 2;

                            string lookupSymbol = symbol + "|" + orderSideId;
                            if (!tradeActivityDict.TryGetValue(lookupSymbol, out TradeActivity tradeActivity))
                            {
                                tradeActivity = new TradeActivity();
                                tradeActivity.Sym = symbol;
                                tradeActivity.PosSym = symbol;
                                tradeActivity.SrcSym = symbol;
                                PositionMaster positionMaster = CommonOperationsUtil.GetPositionDetails(symbol, data.Sedol, data.Isin, positionDict, securityDict);
                                if (positionMaster != null)
                                    tradeActivity.BODPos = positionMaster.FundAll.PosHeld;

                                tradeActivityDict.Add(lookupSymbol, tradeActivity);
                            }

                            if (tradeActivity != null)
                            {
                                tradeActivity.EMSXOrdSide = data.OrdSide;

                                int? trdQty = data.DayFill;
                                double trdMV = trdQty.GetValueOrDefault() * data.DayAvgPrc.GetValueOrDefault();
                                if (orderSideId == 1)
                                {
                                    tradeActivity.EMSXOrdQty = CommonUtils.AddNullableInts(tradeActivity.EMSXOrdQty, trdQty);
                                    tradeActivity.EMSXOrdPrc = CommonUtils.AddNullableDoubles(tradeActivity.EMSXOrdPrc, trdMV);
                                }
                                else if (orderSideId == 2)
                                {
                                    tradeActivity.EMSXOrdQty = CommonUtils.AddNullableInts(tradeActivity.EMSXOrdQty, -1 * trdQty);
                                    tradeActivity.EMSXOrdPrc = CommonUtils.AddNullableDoubles(tradeActivity.EMSXOrdPrc, trdMV);
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "Error processing EMSX Execution Order: " + symbol + "/" + data.OrdSeq);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error populating EMSX Trades");
            }

            foreach (TradeActivity data in tradeActivityDict.Values)
            {
                try
                {
                    if (data.NVOrdQty.GetValueOrDefault() != 0)
                        data.NVOrdPrc = data.NVOrdPrc / data.NVOrdQty;
                    if (data.OTHOrdQty.GetValueOrDefault() != 0)
                        data.OTHOrdPrc = data.OTHOrdPrc / data.OTHOrdQty;
                    if (data.EMSXOrdQty.GetValueOrDefault() != 0)
                        data.EMSXOrdPrc = data.EMSXOrdPrc / data.EMSXOrdQty;

                    double?[] dPos = new double?[] { data.BODPos, data.NVOrdQty, data.OTHOrdQty, data.EMSXOrdQty };
                    data.LivePos = CommonUtils.AddNullableDoubles(dPos);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error calculating Avg Fill Price: " + data.Sym);
                }
            }
            return tradeActivityDict;
        }
    }
}