using aCommons.Trading;
using System.Collections.Generic;

namespace aACTrader.DAO.Interface
{
    public interface IPairTradingNVDao
    {
        public IList<string> GetTemplates();
        public IList<PairOrderTemplateNV> GetTemplate(string templateName);
        public IList<string> GetTemplatesForUser(string userName);
        public void SaveTemplate(IList<PairOrderTemplateNV> list);

        public IDictionary<string, NewPairOrder> GetPairOrders();
        public void SavePairOrders(IDictionary<string, NewPairOrder> orderDict);
    }
}
