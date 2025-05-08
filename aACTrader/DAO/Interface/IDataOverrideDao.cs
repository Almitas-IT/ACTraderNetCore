using aCommons;
using System.Collections.Generic;

namespace aACTrader.DAO.Interface
{
    public interface IDataOverrideDao
    {
        public IDictionary<string, GlobalDataOverride> GetGlobalDataOverrides();
        public void SaveGlobalOverrides(IList<GlobalDataOverride> overrides);
    }
}
