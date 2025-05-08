using aACTrader.Operations.Impl;
using Microsoft.Extensions.Logging;
using Quartz;
using System;
using System.Threading.Tasks;

namespace aACTrader.Jobs
{
    [DisallowConcurrentExecution]
    public class OrderExecutionJob : IJob
    {
        private readonly ILogger<OrderExecutionJob> _logger;
        private readonly TradeOrderOperations _tradeOrderOperations;
        //private readonly AllocationOperations _allocationOperations;

        public OrderExecutionJob(ILogger<OrderExecutionJob> logger
            , TradeOrderOperations tradeOrderOperations)
        //, AllocationOperations allocationOperations)
        {
            _logger = logger;
            _tradeOrderOperations = tradeOrderOperations;
            //_allocationOperations = allocationOperations;
        }

        public Task Execute(IJobExecutionContext context)
        {
            try
            {
                _logger.LogInformation("OrderExecutionJob - STARTED");
                _tradeOrderOperations.ProcessOrders();
                //_allocationOperations.PopulateLivePositions();
                _logger.LogInformation("OrderExecutionJob - DONE");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error running OrderExecutionJob");
            }
            return Task.CompletedTask;
        }
    }
}