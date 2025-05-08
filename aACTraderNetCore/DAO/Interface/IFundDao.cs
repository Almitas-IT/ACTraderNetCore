using aCommons;
using aCommons.Cef;
using aCommons.DTO.Fidelity;
using aCommons.DTO.JPM;
using aCommons.Pfd;
using aCommons.Web;
using System;
using System.Collections.Generic;

namespace aACTrader.DAO.Interface
{
    public interface IFundDao
    {
        public IDictionary<string, FundNotes> GetFundNotes();
        public void SaveFundNotes(IList<FundNotes> list);

        public IDictionary<string, FundPerformanceReturn> GetFundPerformanceReturns();
        public IDictionary<string, FundPerformanceRisk> GetFundPerformanceRiskStats();
        public IDictionary<string, IDictionary<string, FundPerformanceRank>> GetFundPerformanceRanks();

        public IList<SecurityPerformance> GetDailyPnLSummary(DateTime asofDate);
        public IList<SecurityPerformance> GetDailyPnLDetails(string asofDate);
        public IList<SecurityPerformance> GetDailyPnLDetailsWithClassifications(string asofDate);
        public IList<SecurityPerformance> GetDailyPnLDetailsExt(string startDate, string endDate, string fundName, string ticker);
        public IList<JPMSecurityPerfTO> GetJPMPnL(DateTime startDate, DateTime endDate, string fundName, string ticker);
        public IList<PositionTypeDetailsTO> GetFidelityPnL(DateTime startDate, DateTime endDate, string fundName, string ticker);

        public void SaveREITsDailyDiscounts(IList<BDCNavHistory> list);
        public void SaveREITsDailyBVs(IList<BDCNavHistory> list);
        public void SaveREITPfdAnalytics(IList<REITPfdAnalytics> list);

        public void SavePortCurrencyExposureOverrides(IList<FundCurrExpTO> list);
        public IDictionary<string, FundCurrExpTO> GetPortCurrencyExposureOverrides();
    }
}
