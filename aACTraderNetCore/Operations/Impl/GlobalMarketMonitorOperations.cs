using aCommons;
using aCommons.MarketMonitor;
using aCommons.Utils;
using LazyCache;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;

namespace aACTrader.Operations.Impl
{
    public class GlobalMarketMonitorOperations
    {
        private readonly ILogger<GlobalMarketMonitorOperations> _logger;
        private readonly CachingService _cache;

        public GlobalMarketMonitorOperations(ILogger<GlobalMarketMonitorOperations> logger
            , CachingService cache)
        {
            _logger = logger;
            _cache = cache;
            _logger.LogInformation("Initializing GlobalMarketMonitorOperations...");
        }

        public IList<GlobalMarketHistory> GetMonthEndLevels(string marketIndicator)
        {
            IDictionary<string, IDictionary<DateTime, Nullable<double>>> globalMarketMonthEndHist = _cache.Get<IDictionary<string, IDictionary<DateTime, Nullable<double>>>>(CacheKeys.GMM_MONTH_END_HIST);
            IDictionary<string, GlobalMarketIndicator> globalMarketIndicatorDict = _cache.Get<IDictionary<string, GlobalMarketIndicator>>(CacheKeys.GMM_INDICATORS);

            GlobalMarketIndicator gmmData;
            globalMarketIndicatorDict.TryGetValue(marketIndicator, out gmmData);

            if (globalMarketMonthEndHist != null
                && globalMarketMonthEndHist.TryGetValue(marketIndicator, out IDictionary<DateTime, Nullable<double>> dict))
            {
                IList<GlobalMarketHistory> list = new List<GlobalMarketHistory>();
                foreach (KeyValuePair<DateTime, Nullable<double>> kvp in dict)
                {
                    GlobalMarketHistory data = new GlobalMarketHistory();
                    data.MarketIndicator = marketIndicator;
                    data.EffectiveDate = kvp.Key;
                    data.MarketLevel = kvp.Value;
                    data.EffectiveDateAsString = DateUtils.ConvertDate(data.EffectiveDate, "yyyy-MM-dd");

                    if (gmmData != null)
                    {
                        data.DataType = gmmData.DataType;
                        data.DataSource = gmmData.DataSource;
                    }
                    list.Add(data);
                }
                return list.OrderByDescending(x => x.EffectiveDate).ToList<GlobalMarketHistory>();
            }
            return null;
        }
    }
}
