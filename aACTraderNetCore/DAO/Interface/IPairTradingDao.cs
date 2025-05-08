using aCommons.Trading;
using System.Collections.Generic;

namespace aACTrader.DAO.Interface
{
    public interface IPairTradingDao
    {
        public IList<string> GetTemplates();
        public IList<PairOrderTemplate> GetTemplate(string templateName);
        public IList<string> GetTemplatesForUser(string userName);

        public IDictionary<string, PairOrder> GetPairOrders();
        public void SavePairOrders(IDictionary<string, PairOrder> orderDict);
        public void SavePairOrderDetails(IDictionary<string, PairOrder> orderDict);

        public void SaveTemplate(IList<PairOrderDetail> list);
    }
}
