using aACTrader.Operations.Reports;
using Microsoft.Extensions.Logging;
using Quartz;
using System;
using System.Threading.Tasks;

namespace aACTrader.Jobs
{
    [DisallowConcurrentExecution]
    public class PnLReportingJob : IJob
    {
        private readonly ILogger<PnLReportingJob> _logger;
        private readonly DailyPnLReport _dailyPnLReport;

        public PnLReportingJob(ILogger<PnLReportingJob> logger, DailyPnLReport dailyPnLReport)
        {
            _logger = logger;
            _dailyPnLReport = dailyPnLReport;
        }

        public Task Execute(IJobExecutionContext context)
        {
            try
            {
                //_logger.LogInformation("PnLReportingJob - STARTED");
                _dailyPnLReport.CalculateDailyPnL();
                //_logger.LogInformation("PnLReportingJob - DONE");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error running PnLReportingJob");
            }
            return Task.CompletedTask;
        }
    }
}
