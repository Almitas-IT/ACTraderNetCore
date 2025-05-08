using aACTrader.Operations.Impl.BatchOrders;
using Microsoft.Extensions.Logging;
using Quartz;
using System;
using System.Threading.Tasks;

namespace aACTrader.Jobs
{
    [DisallowConcurrentExecution]
    public class AutoDiscountOrdersJob : IJob
    {
        private readonly ILogger<AutoDiscountOrdersJob> _logger;
        private readonly BatchOrderOperations _batchOrderOperations;

        public AutoDiscountOrdersJob(ILogger<AutoDiscountOrdersJob> logger, BatchOrderOperations batchOrderOperations)
        {
            _logger = logger;
            _batchOrderOperations = batchOrderOperations;
        }

        public Task Execute(IJobExecutionContext context)
        {
            try
            {
                //_logger.LogInformation("AutoDiscountOrdersJob - STARTED");
                _batchOrderOperations.ProcessDiscountOrders();
                //_logger.LogInformation("AutoDiscountOrdersJob - DONE");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error running AutoDiscountOrdersJob");
            }
            return Task.CompletedTask;
        }
    }
}
