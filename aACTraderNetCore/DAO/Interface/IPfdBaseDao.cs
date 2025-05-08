using aCommons.Pfd;
using System.Collections.Generic;

namespace aACTrader.DAO.Interface
{
    public interface IPfdBaseDao
    {
        public IDictionary<string, PfdSecurity> GetPfdSecurityMaster();
        public IDictionary<string, IRCurve> GetInterestRateCurves();
        public IDictionary<string, IRCurve> GetInterestRateCurvesNew();
        public IDictionary<string, RateIndexProxy> GetIndexProxies();
        public IDictionary<string, PfdSecurityExt> GetPfdSecurityExtData();
        public IDictionary<string, IDictionary<int, SpotRate>> GetSpotRates();
        public IDictionary<string, IDictionary<int, SpotRate>> GetSpotRatesNew();
        public IList<PfdSecurityAnalytic> GetPfdSecurityAnalytics(string ticker, string startDate, string endDate);
        public IDictionary<string, PfdSecurityMap> GetPfdTickerBloombergCodeMap();

        public void SavePfdSecurityMasterOverrides(IList<PfdSecurity> overrides);
        public void SaveIRCurveRates(IList<IRCurveMember> curveMembers);
        public void SavePfdSecurityAnalytics(IList<PfdSecurityAnalytic> analytics);
        public void SavePfdSecurityAnalyticsNew(IList<PfdSecurityAnalytic> analytics);
    }
}
