using aACTrader.Operations.Impl;
using Microsoft.Extensions.Logging;
using Quartz;
using System;
using System.Threading.Tasks;

namespace aACTrader.Jobs
{
    [DisallowConcurrentExecution]
    public class SaveSecurityPriceJob : IJob
    {
        private readonly ILogger<SaveSecurityPriceJob> _logger;
        private readonly SecurityPriceOperation _securityPriceOperation;

        public SaveSecurityPriceJob(ILogger<SaveSecurityPriceJob> logger, SecurityPriceOperation securityPriceOperation)
        {
            _logger = logger;
            _securityPriceOperation = securityPriceOperation;
        }

        public Task Execute(IJobExecutionContext context)
        {
            try
            {
                _logger.LogInformation("SaveSecurityPriceJob - STARTED");
                _securityPriceOperation.SavePrices();
                _logger.LogInformation("SaveSecurityPriceJob - DONE");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error running SaveSecurityPriceJob");
            }
            return Task.CompletedTask;
        }
    }
}