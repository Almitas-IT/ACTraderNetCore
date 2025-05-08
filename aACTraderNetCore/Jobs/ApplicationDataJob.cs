using aACTrader.Operations.Impl;
using Microsoft.Extensions.Logging;
using Quartz;
using System;
using System.Threading.Tasks;

namespace aACTrader.Jobs
{
    [DisallowConcurrentExecution]
    public class ApplicationDataJob : IJob
    {
        private readonly ILogger<ApplicationDataJob> _logger;
        private readonly CommonOperations _commonOperations;

        public ApplicationDataJob(ILogger<ApplicationDataJob> logger, CommonOperations commonOperations)
        {
            _logger = logger;
            _commonOperations = commonOperations;
        }

        public Task Execute(IJobExecutionContext context)
        {
            try
            {
                //_logger.LogInformation("ApplicationDataJob - STARTED");
                _commonOperations.UpdateApplicationDataUpdateFlag("N");
                //_logger.LogInformation("ApplicationDataJob - DONE");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error running ApplicationDataJob");
            }
            return Task.CompletedTask;
        }
    }
}