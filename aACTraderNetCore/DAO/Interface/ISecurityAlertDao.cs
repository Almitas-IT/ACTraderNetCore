using aCommons.Alerts;
using aCommons.Compliance;
using System.Collections.Generic;

namespace aACTrader.DAO.Interface
{
    public interface ISecurityAlertDao
    {
        public IList<SecurityAlert> GetSecurityAlerts();
        public IList<SecurityAlert> GetSecurityAlerts(string userName);
        public IList<SecOwnership> GetSecOwnershipDetails(string ticker, string startDate, string endDate);
        public IList<SecFilingDetail> GetSecFilingDetails(string ticker, string startDate, string endDate);

        public void SaveSecurityAlerts(IList<SecurityAlert> list);
        public void SaveSecurityOwnershipLimits(IList<SecOwnership> list);
        public void SaveSecurityFilingDetails(IList<SecFilingDetail> list);
    }
}
