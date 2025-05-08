using aACTrader.DAO.Repository;
using aCommons;
using aCommons.Cef;
using aCommons.Compliance;
using aCommons.Utils;
using aCommons.Web;
using LazyCache;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;

namespace aACTrader.Compliance
{
    public class RulesManager
    {
        private readonly ILogger<RulesManager> _logger;
        private readonly CachingService _cache;
        private readonly ComplianceDao _complianceDao;
        private readonly RulesEngine _rulesEngine;

        public RulesManager(ILogger<RulesManager> logger
            , CachingService cache
            , ComplianceDao complianceDao
            , RulesEngine rulesEngine)
        {
            _logger = logger;
            _cache = cache;
            _complianceDao = complianceDao;
            _rulesEngine = rulesEngine;
            _logger.LogInformation("Initializing RulesManager...");
        }

        public void Initialize()
        {
            _logger.LogInformation("Populating Compliance Rules - STARTED");
            IList<RuleMst> rulesList = _complianceDao.GetRules();
            _cache.Remove(CacheKeys.RULES_MST);
            _cache.Add(CacheKeys.RULES_MST, rulesList, DateTimeOffset.MaxValue);

            _logger.LogInformation("Populating Ex Dvd Dates - STARTED");
            IDictionary<string, DividendScheduleTO> dvdExDatedict = _complianceDao.GetDvdExDates();
            _cache.Remove(CacheKeys.FUNDS_EX_DVD_DATE);
            _cache.Add(CacheKeys.FUNDS_EX_DVD_DATE, dvdExDatedict, DateTimeOffset.MaxValue);

            _logger.LogInformation("Populating 1940 Act Funds - STARTED");
            IDictionary<string, string> sec1940Dict = _complianceDao.Get1940ActFunds();
            _cache.Remove(CacheKeys.FUNDS_1940_ACT);
            _cache.Add(CacheKeys.FUNDS_1940_ACT, sec1940Dict, DateTimeOffset.MaxValue);

            _logger.LogInformation("Populating Restricted Security List - STARTED");
            IDictionary<string, RestrictedSecurity> restrictedSecurityList = _complianceDao.GetRestrictedSecurities();
            _cache.Remove(CacheKeys.RESTRICTED_SECURITY_LIST);
            _cache.Add(CacheKeys.RESTRICTED_SECURITY_LIST, restrictedSecurityList, DateTimeOffset.MaxValue);
        }

        public void RunDefaultRules()
        {
            RunRulesOnFund("OPP");
            RunRulesOnFund("TAC");
        }

        public void RunRulesOnFund(string fundName)
        {
            try
            {
                int i = 0; //index to assign rule count on RuleRunMst
                IList<RuleMst> rulesList = _cache.Get<IList<RuleMst>>(CacheKeys.RULES_MST);

                string date = DateUtils.ConvertDate(DateTime.Now, "yyyy-MM-ddTHH:mm:ss");
                RuleRunMst ruleRunMst = CreateRuleRunMasterRecord(fundName + "-" + date, "Fund: " + fundName);

                FundSummaryDetail fundSummaryDetail = null;
                IDictionary<string, PositionMaster> dict = _cache.Get<IDictionary<string, PositionMaster>>(CacheKeys.POSITION_MASTER);
                foreach (PositionMaster data in dict.Values)
                {
                    if ("OPP".Equals(fundName, StringComparison.CurrentCultureIgnoreCase))
                        fundSummaryDetail = data.FundOpp;
                    else if ("TAC".Equals(fundName, StringComparison.CurrentCultureIgnoreCase))
                        fundSummaryDetail = data.FundTac;

                    if (fundSummaryDetail != null)
                    {
                        //summary information for each security
                        string ticker = !string.IsNullOrEmpty(data.SecTicker) ? data.SecTicker : data.Ticker;
                        RuleRunSummary ruleRunSummary = AddSecurity(ruleRunMst, ticker, data.SecTicker, fundName, fundSummaryDetail);

                        //run active rules against each security
                        foreach (RuleMst rule in rulesList)
                        {
                            switch (rule.RuleMstId)
                            {
                                case 1:
                                    _rulesEngine.ExDividendDateCheck(ticker, data.SecTicker, fundName, ruleRunMst, ruleRunSummary, rule);
                                    break;
                                case 2:
                                    _rulesEngine.RestrictedSecurityCheck(ticker, data.SecTicker, fundName, ruleRunMst, ruleRunSummary, rule);
                                    break;
                                case 3:
                                    _rulesEngine.Rule1940Check(ticker, data.SecTicker, fundName, fundSummaryDetail, data, ruleRunMst, ruleRunSummary, rule);
                                    break;
                                case 6:
                                    _rulesEngine.PositionBreakCheck(ticker, data.SecTicker, fundName, fundSummaryDetail, ruleRunMst, ruleRunSummary, rule);
                                    break;
                            }
                        }

                        //summary run information
                        if (i == 0)
                        {
                            ruleRunMst.RuleCount = rulesList.Count;
                            i++;
                        }
                    }
                }

                _complianceDao.SaveRuleRunDetails(ruleRunMst);
            }
            catch (Exception ex)
            {
                _logger.LogError("Error running default Compliance rules on Fund: " + fundName, ex);
            }
        }

        private RuleRunMst CreateRuleRunMasterRecord(string batchName, string desc)
        {
            RuleRunMst ruleRunMst = new RuleRunMst();
            ruleRunMst.RunDate = DateTime.Today;
            ruleRunMst.Name = batchName;
            ruleRunMst.Desc = desc;
            ruleRunMst.UserName = "system";
            ruleRunMst.SecCount = 0;
            ruleRunMst.WarningSecCount = 0;
            ruleRunMst.RestrictionSecCount = 0;
            return ruleRunMst;
        }

        private RuleRunSummary AddSecurity(RuleRunMst ruleRunMst, string ticker, string almTicker, string fundName, FundSummaryDetail fundSummaryDetail)
        {
            try
            {
                RuleRunSummary data = new RuleRunSummary();
                data.Ticker = ticker;
                data.ALMTicker = almTicker;
                data.Fund = fundName;
                data.Position = (int?)fundSummaryDetail.PosHeld;
                data.RuleCount = ruleRunMst.RuleCount;
                ruleRunMst.AddDetail(data);
                ruleRunMst.SecCount++;
                return data;
            }
            catch (Exception ex)
            {
                _logger.LogError("Error adding Security for Ticker: " + ticker, ex);
                return null;
            }
        }
    }
}
