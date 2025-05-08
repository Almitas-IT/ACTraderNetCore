using aACTrader.DAO.Repository;
using aCommons;
using aCommons.Utils;
using LazyCache;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;

namespace aACTrader.Operations.Reports
{
    public class SecurityPerformanceOperations
    {
        private readonly ILogger<SecurityPerformanceOperations> _logger;
        private readonly CachingService _cache;
        private readonly FundDao _fundDao;
        private readonly IConfiguration _configuration;

        public SecurityPerformanceOperations(ILogger<SecurityPerformanceOperations> logger
            , CachingService cache
            , FundDao fundDao
            , IConfiguration configuration)
        {
            _logger = logger;
            _cache = cache;
            _fundDao = fundDao;
            _configuration = configuration;
        }

        public IList<SecurityPerformance> GetDailyPnLDetails(string startDate, string endDate, string fundName, string ticker)
        {
            IDictionary<string, SecurityPerformance> dict = new Dictionary<string, SecurityPerformance>(StringComparer.CurrentCultureIgnoreCase);
            IList<SecurityPerformance> list = _fundDao.GetDailyPnLDetailsExt(startDate, endDate, fundName, ticker);

            int rowId = 1;
            foreach (SecurityPerformance data in list)
            {
                dict.Add(data.RowId.ToString(), data);

                if (startDate.Equals(endDate))
                {
                    //Add Security to Fund
                    AddSecurityToFundGroup(dict, data);
                }
            }

            return dict.Values.ToList<SecurityPerformance>().OrderBy(s => s.RowId).ToList<SecurityPerformance>();
        }

        private SecurityPerformance GetSecurityGroup(IDictionary<string, SecurityPerformance> dict, SecurityPerformance data, string groupName, int rowId)
        {
            if (!dict.TryGetValue(groupName, out SecurityPerformance securityGroup))
            {
                securityGroup = new SecurityPerformance();
                securityGroup.EffectiveDate = data.EffectiveDate;
                securityGroup.Ticker = groupName;
                securityGroup.RowId = rowId;
                securityGroup.IsGrpRow = 1;
                dict.Add(groupName, securityGroup);
            }
            return securityGroup;
        }

        private void AddSecurityToFundGroup(IDictionary<string, SecurityPerformance> dict, SecurityPerformance data)
        {
            SecurityPerformance securityGroup = GetSecurityGroup(dict, data, data.FundType, 1);
            securityGroup.BegMV = CommonUtils.AddNullableDoubles(securityGroup.BegMV, data.BegMV);
            securityGroup.EndMV = CommonUtils.AddNullableDoubles(securityGroup.EndMV, data.EndMV);
            securityGroup.PnLUSD = CommonUtils.AddNullableDoubles(securityGroup.PnLUSD, data.PnLUSD);
            securityGroup.TradingPnL = CommonUtils.AddNullableDoubles(securityGroup.TradingPnL, data.TradingPnL);
        }
    }
}
