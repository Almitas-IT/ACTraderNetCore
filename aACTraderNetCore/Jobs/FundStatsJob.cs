using aACTrader.Operations.Impl;
using Microsoft.Extensions.Logging;
using Quartz;
using System;
using System.Threading.Tasks;

namespace aACTrader.Jobs
{
    [DisallowConcurrentExecution]
    public class FundStatsJob : IJob
    {
        private readonly ILogger<FundStatsJob> _logger;
        private readonly FundStatsCalculator _fundStatsCalculator;

        public FundStatsJob(ILogger<FundStatsJob> logger, FundStatsCalculator fundStatsCalculator)
        {
            _logger = logger;
            _fundStatsCalculator = fundStatsCalculator;
        }

        public Task Execute(IJobExecutionContext context)
        {
            try
            {
                _logger.LogInformation("FundStatsJob - STARTED");
                _fundStatsCalculator.CalculateLiveDiscountStats();
                _logger.LogInformation("FundStatsJob - DONE");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error running FundStatsJob");
            }
            return Task.CompletedTask;
        }
    }
}
