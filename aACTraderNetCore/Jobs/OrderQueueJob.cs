using aACTrader.Operations.Impl.BatchOrders;
using Microsoft.Extensions.Logging;
using Quartz;
using System;
using System.Threading.Tasks;

namespace aACTrader.Jobs
{
    [DisallowConcurrentExecution]
    public class OrderQueueJob : IJob
    {
        private readonly ILogger<OrderQueueJob> _logger;
        private readonly BatchOrderQueueOperations _batchOrderQueueOperations;

        public OrderQueueJob(ILogger<OrderQueueJob> logger, BatchOrderQueueOperations batchOrderQueueOperations)
        {
            _logger = logger;
            _batchOrderQueueOperations = batchOrderQueueOperations;
        }

        public Task Execute(IJobExecutionContext context)
        {
            try
            {
                //_logger.LogInformation("OrderQueueJob - STARTED");
                _batchOrderQueueOperations.ProcessRefIndexOrders();
                _batchOrderQueueOperations.ProcessDiscountOrders();
                //_logger.LogInformation("OrderQueueJob - DONE");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error running OrderQueueJob");
            }
            return Task.CompletedTask;
        }
    }
}
