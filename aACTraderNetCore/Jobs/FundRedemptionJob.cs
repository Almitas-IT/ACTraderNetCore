using aACTrader.Operations.Impl;
using Microsoft.Extensions.Logging;
using Quartz;
using System;
using System.Threading.Tasks;

namespace aACTrader.Jobs
{
    [DisallowConcurrentExecution]
    public class FundRedemptionJob : IJob
    {
        private readonly ILogger<FundRedemptionJob> _logger;
        private readonly FundRedemptionTriggerOperations _operations;

        public FundRedemptionJob(ILogger<FundRedemptionJob> logger, FundRedemptionTriggerOperations operations)
        {
            _logger = logger;
            _operations = operations;
        }

        public Task Execute(IJobExecutionContext context)
        {
            try
            {
                _logger.LogInformation("FundRedemptionJob - STARTED");
                _operations.ProcessFundRedemptionTriggers();
                _logger.LogInformation("FundRedemptionJob - DONE");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error running FundRedemptionJob");
            }
            return Task.CompletedTask;
        }
    }
}