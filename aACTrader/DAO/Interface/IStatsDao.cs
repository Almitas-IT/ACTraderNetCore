using aCommons;
using aCommons.MarketMonitor;
using aCommons.Web;
using System;
using System.Collections.Generic;

namespace aACTrader.DAO.Interface
{
    public interface IStatsDao
    {
        public IList<FundGroupDiscountStats> GetFundDiscountStats(string securityType, string country);
        public FundGroupDiscountStats GetFundDiscountStats(string securityType, string country, string fundGroup, DateTime asofDate);
        public IDictionary<string, FundGroupDiscountStatsTO> GetFundDiscountStatsSummary(string securityType, string country);
        public IList<FundGroupDiscountStatsHist> GetFundDiscountStatsHistory(string groupName, string securityType, string country);
        public IList<FundGroupDiscountStatsHist> GetFundDiscountStatsDetails(string groupName, string securityType, string country, string startDate, string endDate);
        public IList<FundGroupDiscountDetails> GetFundDiscountDetails(string securityType, string country, string startDate, string endDate, IList<string> groupNames);
        public IList<GlobalMarketSummary> GetGlobalMarketStats();
        public IDictionary<string, IDictionary<DateTime, Nullable<double>>> GetGlobalMarketHistory();
        public IList<GlobalMarketHistory> GetGlobalMarketHistory(string ticker, DateTime startDate, DateTime endDate);
        public IList<SecurityMaster> GetFundDiscountStatsSecurityList(string securityType, string country, string groupName, string period);
        public IDictionary<string, GlobalMarketIndicator> GetGlobalMarketIndicators();
    }
}
