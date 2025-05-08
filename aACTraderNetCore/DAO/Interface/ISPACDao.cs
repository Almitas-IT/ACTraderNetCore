using aCommons.SPACs;
using System.Collections.Generic;

namespace aACTrader.DAO.Interface
{
    public interface ISPACDao
    {
        public IList<SPACPosition> GetSPACPositions();
        public IList<SPACSecurity> GetSPACSecurities();
    }
}
