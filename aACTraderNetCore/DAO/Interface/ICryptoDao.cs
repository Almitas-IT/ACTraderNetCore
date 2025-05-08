using aCommons.Crypto;
using System.Collections.Generic;

namespace aACTrader.DAO.Interface
{
    public interface ICryptoDao
    {
        public IDictionary<string, CryptoSecMst> GetCryptoSecurityDetails();
        public IList<CryptoSecMst> GetCryptoSecurityList();
        public void SaveCryptoSecurityDetails(IList<CryptoSecMst> list);
    }
}
