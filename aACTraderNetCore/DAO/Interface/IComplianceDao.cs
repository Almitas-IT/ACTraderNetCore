using aCommons.Compliance;
using aCommons.Web;
using System;
using System.Collections.Generic;

namespace aACTrader.DAO.Interface
{
    public interface IComplianceDao
    {
        public IList<RuleMst> GetRules();
        public IList<RuleCategory> GetRuleCategories();
        public IList<RuleRunMst> GetRuleRunList(DateTime startDate, DateTime endDate);
        public IList<RuleRunSummary> GetRuleRunSummary(int ruleRunMstId);
        public IList<RuleRunDetail> GetRuleRunDetails(int ruleRunMstId);
        public IList<RuleRunDetail> GetRuleRunDetails(int ruleRunMstId, string ticker);

        public IDictionary<string, DividendScheduleTO> GetDvdExDates();
        public IDictionary<string, string> Get1940ActFunds();
        public IDictionary<string, RestrictedSecurity> GetRestrictedSecurities();

        public void SaveRuleRunDetails(RuleRunMst data);
    }
}
