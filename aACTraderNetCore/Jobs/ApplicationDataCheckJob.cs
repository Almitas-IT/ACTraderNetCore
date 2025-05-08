using aACTrader.Operations.Impl;
using Microsoft.Extensions.Logging;
using Quartz;
using System;
using System.Threading.Tasks;

namespace aACTrader.Jobs
{
    [DisallowConcurrentExecution]
    public class ApplicationDataCheckJob : IJob
    {
        private readonly ILogger<ApplicationDataCheckJob> _logger;
        private readonly DataValidationChecks _dataValidationChecks;

        public ApplicationDataCheckJob(ILogger<ApplicationDataCheckJob> logger, DataValidationChecks dataValidationChecks)
        {
            _logger = logger;
            _dataValidationChecks = dataValidationChecks;
        }

        public Task Execute(IJobExecutionContext context)
        {
            try
            {
                //_logger.LogInformation("ApplicationDataCheckJob - STARTED");
                _dataValidationChecks.CheckLivePricing();
                _dataValidationChecks.CheckLiveDelayedPricing();
                //_logger.LogInformation("ApplicationDataCheckJob - DONE");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error running ApplicationDataCheckJob");
            }
            return Task.CompletedTask;
        }
    }
}