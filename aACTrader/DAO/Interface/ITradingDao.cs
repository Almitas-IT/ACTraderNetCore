using aCommons.DTO;
using aCommons.Trading;
using System;
using System.Collections.Generic;

namespace aACTrader.DAO.Interface
{
    public interface ITradingDao
    {
        public IDictionary<string, OrderSummary> GetOrderSummary();
        public IDictionary<string, OrderSummary> GetSimOrderSummary();
        public IList<OrderSummary> GetCompletedOrders(DateTime date);
        public IList<OrderSummary> GetAllCompletedOrders(DateTime date);
        public IList<NewOrder> GetBatchOrders();
        public IList<NewOrder> GetSimBatchOrders();
        public IList<string> GetBatchOrderTemplates();
        public IList<string> GetBatchOrderTemplatesForUser(string userName, string includePairTradeTemplate);
        public IList<string> GetSampleBatchOrderTemplates();
        public IList<BatchOrderTemplate> GetBatchOrderTemplate(string templateName);
        public IDictionary<string, TradePosition> GetTradePositions();
        public IList<OrderSummary> GetManualTrades(string orderDate);
        public IDictionary<string, OrderSummary> GetTradeExecutionDetails();
        public IDictionary<string, OrderSummary> GetSimTradeExecutionDetails();
        public IDictionary<string, string> GetISINPositionTickerMap();
        public IList<ASTrade> GetASTrades(string ticker, DateTime startDate, DateTime endDate, string source);
        public IList<ASTradeAllocation> GetASTradeAllocations(string ticker, DateTime startDate, DateTime endDate);
        public IList<OrderSummary> GetExecutedOrders(DateTime date);
        public IList<TradeTarget> GetTradeTargets();
        public IList<TradeExecutionSummaryTO> GetTradeExecutionSummary(DateTime startDate, DateTime endDate, string broker);
        public IList<TradeExecutionSummaryTO> GetTradeCommissions(DateTime startDate, DateTime endDate, string broker);

        public void SaveTrades(IList<OrderSummary> orderSummaryList);
        public void SaveOrderSummary(IDictionary<string, OrderSummary> orderSummaryDict);
        public void SaveOrderExecutionDetails(IDictionary<string, OrderSummary> orderExecutionDetailsDict);
        public void SaveSimOrderExecutionDetails(IDictionary<string, OrderSummary> orderExecutionDetailsDict);
        public void SaveSimOrderSummary(IDictionary<string, OrderSummary> orderSummaryDict);
        public void SaveBatchOrders(IList<NewOrder> orderList);
        public void SaveSimBatchOrders(IList<NewOrder> orderList);
        public void SaveBatchOrderTemplate(IList<BatchOrderTemplate> list);

        public void SaveTradingTargets(IList<TradeTarget> list);
        public void SavePairTradingTargets(IList<PairTradeTarget> list);

        public void SaveAllTrades(IList<ASTrade> trades);
        public void SaveManualTrades(IList<ASTrade> trades);
        public void RemoveManualTrades(IList<ASTrade> trades);
        public void SaveAllTradeAllocations(IList<ASTradeAllocation> tradeAllocations);

        //Trade Order Queue
        public IDictionary<string, OrderSummary> GetOrderQueue();
        public void SaveOrderQueue(IDictionary<string, OrderSummary> orderSummaryDict);
        public void SaveSimOrderQueue(IDictionary<string, OrderSummary> orderSummaryDict);

        public void SaveTradeSummary(IList<TradeSummaryTO> list);
    }
}
