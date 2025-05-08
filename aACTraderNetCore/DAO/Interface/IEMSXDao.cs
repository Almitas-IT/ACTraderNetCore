using aCommons.Trading;
using System;
using System.Collections.Generic;

namespace aACTrader.DAO.Interface
{
    public interface IEMSXDao
    {
        public IList<string> GetBatchTemplates();
        public IList<BatchOrderTemplate> GetBatchTemplate(string templateName);
        public void SaveBatchTemplate(IList<BatchOrderTemplate> list);
        public IList<string> GetBatchTemplatesForUser(string userName, string includePairTradeTemplate);

        public IDictionary<Int32, EMSXOrderStatus> GetOrderStatus();
        public IDictionary<Int32, EMSXRouteStatus> GetRouteStatus();
        public IDictionary<string, EMSXRouteStatus> GetRouteStatusHist();
        public IDictionary<Int32, IDictionary<Int32, EMSXOrderFill>> GetOrderFills();
        public IList<EMSXOrderError> GetOrderErrors();
        public IList<EMSXOrderStatus> GetEMSXOrderStatus(string Env, string Trader);
        public IList<EMSXRouteStatus> GetEMSXRouteStatus(string Env, string Trader);
        public IList<EMSXOrderFill> GetEMSXOrderFills(string Env);
        public IList<EMSXRouteStatus> GetEMSXTradeHistory(string StartDate, string EndDate);

        public IList<NewOrder> GetBatchOrders();
        public IList<NewOrder> GetSimBatchOrders();
        public void SaveBatchOrders(IList<NewOrder> orderList);

        public void SaveRouteUpdates(IDictionary<Int32, EMSXRouteStatus> dict);
        public void SaveRouteHistUpdates(IDictionary<string, EMSXRouteStatus> dict);
        public void SaveOrderUpdates(IDictionary<Int32, EMSXOrderStatus> dict);
        public void SaveOrderFills(IDictionary<Int32, IDictionary<Int32, EMSXOrderFill>> dict);
        public void SaveOrderErrors(IList<EMSXOrderError> list);
    }
}
