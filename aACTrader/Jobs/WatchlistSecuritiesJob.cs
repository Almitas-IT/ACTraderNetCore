using aACTrader.Operations.Impl;
using Microsoft.Extensions.Logging;
using Quartz;
using System.Threading.Tasks;

namespace aACTrader.Jobs
{
    [DisallowConcurrentExecution]
    public class WatchlistSecuritiesJob : IJob
    {
        private readonly ILogger<WatchlistSecuritiesJob> _logger;
        private readonly FundAlertManagerNew _fundAlertManagerNew;

        public WatchlistSecuritiesJob(ILogger<WatchlistSecuritiesJob> logger, FundAlertManagerNew fundAlertManagerNew)
        {
            _logger = logger;
            _fundAlertManagerNew = fundAlertManagerNew;
        }

        public Task Execute(IJobExecutionContext context)
        {
            //_logger.LogInformation("WatchlistSecuritiesJob - STARTED");
            _fundAlertManagerNew.ProcessWatchlistSecurityAlerts();
            //_logger.LogInformation("WatchlistSecuritiesJob - DONE");
            return Task.CompletedTask;
        }
    }
}