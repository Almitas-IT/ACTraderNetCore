using aACTrader.Operations.Impl;
using Microsoft.Extensions.Logging;
using Quartz;
using System;
using System.Threading.Tasks;

namespace aACTrader.Jobs
{
    [DisallowConcurrentExecution]
    public class CryptoForecastJob : IJob
    {
        private readonly ILogger<CryptoForecastJob> _logger;
        private readonly FundForecastEngine _fundForecastEngine;

        public CryptoForecastJob(ILogger<CryptoForecastJob> logger, FundForecastEngine fundForecastEngine)
        {
            _logger = logger;
            _fundForecastEngine = fundForecastEngine;
        }

        public Task Execute(IJobExecutionContext context)
        {
            try
            {
                //_logger.LogInformation("CryptoForecastJob - STARTED");
                _fundForecastEngine.CalculateCryptoFundNavs();
                //_logger.LogInformation("CryptoForecastJob - DONE");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error running CryptoForecastJob");
            }
            return Task.CompletedTask;
        }
    }
}