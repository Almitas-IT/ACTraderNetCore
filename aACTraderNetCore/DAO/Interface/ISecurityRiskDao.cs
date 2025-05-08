using aCommons;
using aCommons.Cef;
using System;
using System.Collections.Generic;

namespace aACTrader.DAO.Interface
{
    public interface ISecurityRiskDao
    {
        public IDictionary<string, SecurityRiskFactor> GetSecurityRiskFactors();
        public IDictionary<string, SecurityRiskFactor> GetSecurityRiskFactorsWithDates();
        public IList<SecurityReturn> GetSecurityReturns(string fundTicker, string ticker);
        public IDictionary<string, IList<HistSecurityReturn>> GetHistSecurityReturns();
        public IDictionary<string, IDictionary<DateTime, HistFXRate>> GetHistFXRates();

        public IDictionary<string, SecurityMasterExt> GetSecurityMasterExtDetails();

        public IList<FundSecurityTotalReturn> GetSecurityTotalReturns();

        public void SaveSecurityRiskFactors(IList<SecurityRiskFactor> list);
        public void SaveSecurityMasterExtDetails(IList<SecurityMasterExt> list);
        public void SaveSecurityRiskFactorsWithDates(IList<SecurityRiskFactor> list);
    }
}
