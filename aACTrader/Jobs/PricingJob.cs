using aACTrader.Operations.Impl;
using aCommons;
using aCommons.Security;
using LazyCache;
using Microsoft.Extensions.Logging;
using Quartz;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace aACTrader.Jobs
{
    [DisallowConcurrentExecution]
    public class PricingJob : IJob
    {
        private readonly ILogger<PricingJob> _logger;
        private readonly CachingService _cache;
        private readonly SecurityPriceOperation _securityPriceOperation;

        public PricingJob(ILogger<PricingJob> logger, CachingService cache, SecurityPriceOperation securityPriceOperation)
        {
            _logger = logger;
            _cache = cache;
            _securityPriceOperation = securityPriceOperation;
        }

        public Task Execute(IJobExecutionContext context)
        {
            try
            {
                _logger.LogInformation("PricingJob - STARTED");
                _securityPriceOperation.UpdateFlagForClosedMarkets();
                _logger.LogInformation("PricingJob - DONE");
                _logger.LogInformation("Saving Security Imbalance Data - STARTED");
                IDictionary<string, SharesImbalance> securityImbalanceDict = _cache.Get<IDictionary<string, SharesImbalance>>(CacheKeys.SHARES_IMBALANCE);
                _securityPriceOperation.SaveSharesImbalanceList(securityImbalanceDict);
                _logger.LogInformation("Saved Security Imbalance Data - DONE");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error running PricingJob");
            }
            return Task.CompletedTask;
        }
    }
}