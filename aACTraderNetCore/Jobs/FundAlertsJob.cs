using aACTrader.Operations.Impl;
using aCommons;
using LazyCache;
using Microsoft.Extensions.Logging;
using Quartz;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace aACTrader.Jobs
{
    [DisallowConcurrentExecution]
    public class FundAlertsJob : IJob
    {
        private readonly ILogger<FundAlertsJob> _logger;
        private readonly CachingService _cache;
        private readonly FundAlertManager _fundAlertManager;

        public FundAlertsJob(ILogger<FundAlertsJob> logger, CachingService cache, FundAlertManager fundAlertManager)
        {
            _logger = logger;
            _cache = cache;
            _fundAlertManager = fundAlertManager;
        }

        public Task Execute(IJobExecutionContext context)
        {
            try
            {
                //_logger.LogInformation("FundAlertsJob - STARTED");
                List<FundAlert> fundAlertList = new List<FundAlert>();
                IList<FundAlert> ownershipList = _fundAlertManager.GetOwnershipAlerts();
                fundAlertList.AddRange(ownershipList);
                //_cache.Remove(CacheKeys.FUND_ALERTS);
                _cache.Add(CacheKeys.FUND_ALERTS, fundAlertList, DateTimeOffset.MaxValue);
                //_logger.LogInformation("FundAlertsJob - DONE");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error running FundAlertsJob");
            }
            return Task.CompletedTask;
        }
    }
}