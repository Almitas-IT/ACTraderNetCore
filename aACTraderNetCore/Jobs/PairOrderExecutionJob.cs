using aACTrader.Operations.Impl.BatchOrders;
using Microsoft.Extensions.Logging;
using Quartz;
using System;
using System.Threading.Tasks;

namespace aACTrader.Jobs
{
    [DisallowConcurrentExecution]
    public class PairOrderExecutionJob : IJob
    {
        private readonly ILogger<PairOrderExecutionJob> _logger;
        private readonly PairOrderOperations _pairOrderOperations;

        public PairOrderExecutionJob(ILogger<PairOrderExecutionJob> logger
            , PairOrderOperations pairOrderOperations)
        {
            _logger = logger;
            _pairOrderOperations = pairOrderOperations;
        }

        public Task Execute(IJobExecutionContext context)
        {
            try
            {
                //_logger.LogInformation("PairOrderExecutionJob - STARTED");
                _pairOrderOperations.ProcessOrders();
                //_logger.LogInformation("PairOrderExecutionJob - DONE");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error running PairOrderExecutionJob");
            }
            return Task.CompletedTask;
        }
    }
}