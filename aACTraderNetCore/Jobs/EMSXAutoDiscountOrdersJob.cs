using aACTrader.Operations.Impl.BatchOrders;
using Microsoft.Extensions.Logging;
using Quartz;
using System;
using System.Threading.Tasks;

namespace aACTrader.Jobs
{
    [DisallowConcurrentExecution]
    public class EMSXAutoDiscountOrdersJob : IJob
    {
        private readonly ILogger<EMSXAutoDiscountOrdersJob> _logger;
        private readonly EMSXBatchOrderOperations _emsxBatchOrderOperations;

        public EMSXAutoDiscountOrdersJob(ILogger<EMSXAutoDiscountOrdersJob> logger, EMSXBatchOrderOperations emsxBatchOrderOperations)
        {
            _logger = logger;
            _emsxBatchOrderOperations = emsxBatchOrderOperations;
        }

        public Task Execute(IJobExecutionContext context)
        {
            try
            {
                //_logger.LogInformation("EMSXAutoDiscountOrdersJob - STARTED");
                _emsxBatchOrderOperations.ProcessDiscountOrders();
                //_logger.LogInformation("EMSXAutoDiscountOrdersJob - DONE");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error running EMSXAutoDiscountOrdersJob");
            }
            return Task.CompletedTask;
        }
    }
}
