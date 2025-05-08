using aCommons;
using aCommons.Admin;
using System.Collections.Generic;

namespace aACTrader.DAO.Interface
{
    public interface IAdminDao
    {
        public ApplicationData GetApplicationDataUpdateFlag();

        public IList<MorningMailData> GetMorningMailData(string asOfDate, string category);
        public void SaveApplicationDataUpdateFlag(ApplicationData data);
        public void SaveLogData(IList<LogData> logDataList);
        public void ExecuteStoredProcedure(string procedureName);

        //public void PopulateDailyDataChecks();
        //public void PopulateDataOverrideChecks();
        //public void PopulateSwapMarginDetails();
        //public void PopulateCryptoMarginDetails();
    }
}
