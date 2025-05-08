using aCommons;
using aCommons.Admin;
using aCommons.Cef;
using aCommons.DTO;
using aCommons.Web;
using System.Collections.Generic;

namespace aACTrader.DAO.Interface
{
    public interface IWebDao
    {
        public IList<DividendScheduleTO> GetDividendSchedule();
        public IList<CorporateAction> GetCorporateActions();
        public IList<SecurityDataErrorTO> GetSecurityDataCheckReport(string country, string ticker);
        public IList<ALMFunction> GetALMFunctions(string funcType, string funcCategory, string funcName, string dataSrc);
        public IList<string> GetALMFunctionCategories(string funcType);

        public IList<FundHoldingReturn> GetPortHoldingDataChecks();
        public IList<ManualDataOverrideTO> GetManualDataOverrides();
        public IList<SwapMarginDetTO> GetSwapMarginDetails(string broker, string ticker);
        public IList<SwapMarginDetTO> GetCryptoMarginDetails(string broker, string ticker);
        public IList<DailyDataLoadSummaryTO> GetDailyDataLoadSummary(string startDate, string endDate);
        public IList<NavOverridesTO> GetNavDataOverrides(string startDate, string endDate, string Ticker);
    }
}