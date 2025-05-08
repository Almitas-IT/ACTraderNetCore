using aACTrader.Operations.Impl.BatchOrders;
using Microsoft.Extensions.Logging;
using Quartz;
using System;
using System.Threading.Tasks;

namespace aACTrader.Jobs
{
    [DisallowConcurrentExecution]
    public class AutoRefIndexOrdersJob : IJob
    {
        private readonly ILogger<AutoRefIndexOrdersJob> _logger;
        private readonly BatchOrderOperations _batchOrderOperations;

        public AutoRefIndexOrdersJob(ILogger<AutoRefIndexOrdersJob> logger, BatchOrderOperations batchOrderOperations)
        {
            _logger = logger;
            _batchOrderOperations = batchOrderOperations;
        }

        public Task Execute(IJobExecutionContext context)
        {
            try
            {
                //_logger.LogInformation("AutoRefIndexOrdersJob - STARTED");
                _batchOrderOperations.ProcessRefIndexOrders();
                //_logger.LogInformation("AutoRefIndexOrdersJob - DONE");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error running AutoRefIndexOrdersJob");
            }
            return Task.CompletedTask;
        }
    }
}
