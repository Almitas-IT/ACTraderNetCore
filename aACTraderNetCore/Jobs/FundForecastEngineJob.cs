using aACTrader.Operations.Impl;
using Microsoft.Extensions.Logging;
using Quartz;
using System;
using System.Threading.Tasks;

namespace aACTrader.Jobs
{
    [DisallowConcurrentExecution]
    public class FundForecastEngineJob : IJob
    {
        private readonly ILogger<FundForecastEngineJob> _logger;
        private readonly FundForecastEngine _fundForecastEngine;

        public FundForecastEngineJob(ILogger<FundForecastEngineJob> logger, FundForecastEngine fundForecastEngine)
        {
            _logger = logger;
            _fundForecastEngine = fundForecastEngine;
        }

        public Task Execute(IJobExecutionContext context)
        {
            try
            {
                _logger.LogInformation("Fund Forecast Engine Job - STARTED");
                _fundForecastEngine.Calculate();
                _logger.LogInformation("Fund Forecast Engine Job - DONE");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error running Fund Forecast Engine Job");
            }
            return Task.CompletedTask;
        }
    }
}