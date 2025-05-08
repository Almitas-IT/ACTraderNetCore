using aACTrader.Operations.Impl;
using Microsoft.Extensions.Logging;
using Quartz;
using System;
using System.Threading.Tasks;

namespace aACTrader.Jobs
{
    [DisallowConcurrentExecution]
    public class GlobalMarketMonitorJob : IJob
    {
        private readonly ILogger<ApplicationDataCheckJob> _logger;
        private readonly CommonOperations _commonOperations;

        public GlobalMarketMonitorJob(ILogger<ApplicationDataCheckJob> logger, CommonOperations commonOperations)
        {
            _logger = logger;
            _commonOperations = commonOperations;
        }

        public Task Execute(IJobExecutionContext context)
        {
            try
            {
                _logger.LogInformation("GlobalMarketMonitorJob - STARTED");
                _commonOperations.PopulateGlobalMarketMonthEndLevels();
                _commonOperations.PopulateFullFundHistory();
                _commonOperations.PopulateCEFFullFundHistory();
                _logger.LogInformation("GlobalMarketMonitorJob - DONE");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error running ApplicationDataCheckJob");
            }
            return Task.CompletedTask;
        }
    }
}