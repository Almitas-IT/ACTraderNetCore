using aACTrader.Operations.Impl;
using Microsoft.Extensions.Logging;
using Quartz;
using System;
using System.Threading.Tasks;

namespace aACTrader.Jobs
{
    [DisallowConcurrentExecution]
    public class SavePfdForecastJob : IJob
    {
        private readonly ILogger<SavePfdForecastJob> _logger;
        private readonly PfdCommonOperations _operations;

        public SavePfdForecastJob(ILogger<SavePfdForecastJob> logger, PfdCommonOperations operations)
        {
            _logger = logger;
            _operations = operations;
        }

        public Task Execute(IJobExecutionContext context)
        {
            try
            {
                _logger.LogInformation("SavePfdForecastJob - STARTED");
                _operations.SaveSecurityAnalytics();
                _logger.LogInformation("SavePfdForecastJob - DONE");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error running SavePfdForecastJob");
            }
            return Task.CompletedTask;
        }
    }
}