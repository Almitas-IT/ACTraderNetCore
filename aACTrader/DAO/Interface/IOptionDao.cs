using aCommons.Derivatives;
using System.Collections.Generic;

namespace aACTrader.DAO.Interface
{
    public interface IOptionDao
    {
        public IList<OptionChain> GetOptionChain(string ticker, string fundCategory);
        public IDictionary<string, OptionDetail> GetOptionDetails();
    }
}
