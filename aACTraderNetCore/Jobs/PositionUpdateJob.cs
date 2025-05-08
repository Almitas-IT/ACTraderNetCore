using aACTrader.Operations.Impl;
using Microsoft.Extensions.Logging;
using Quartz;
using System;
using System.Threading.Tasks;

namespace aACTrader.Jobs
{
    [DisallowConcurrentExecution]
    public class PositionUpdateJob : IJob
    {
        private readonly ILogger<PositionUpdateJob> _logger;
        private readonly CommonOperations _commonOperations;


        public PositionUpdateJob(ILogger<PositionUpdateJob> logger, CommonOperations commonOperations)
        {
            _logger = logger;
            _commonOperations = commonOperations;
        }

        public Task Execute(IJobExecutionContext context)
        {
            try
            {
                _logger.LogInformation("Updating ALM Holdings - STARTED");
                _commonOperations.UpdateALMHoldings();
                _logger.LogInformation("Updating ALM Holdings - DONE");

                _logger.LogInformation("Updating Fund Cash, Margin and Security Borrow Rates (Broker Reports) Details - STARTED");
                _commonOperations.PopulateBrokerReportDetails();
                _logger.LogInformation("Updating Fund Cash, Margin and Security Borrow Rates (Broker Reports) Details - DONE");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error running PositionUpdateJob");
            }
            return Task.CompletedTask;
        }
    }
}