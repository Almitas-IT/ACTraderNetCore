using aACTrader.DAO.Repository;
using aCommons;
using aCommons.Trading;
using LazyCache;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Quartz;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace aACTrader.Jobs
{
    [DisallowConcurrentExecution]
    public class TradeFillsDataJob : IJob
    {
        private readonly ILogger<TradeFillsDataJob> _logger;
        private readonly CachingService _cache;
        private readonly TradingDao _tradingDao;
        private readonly IConfiguration _configuration;

        public TradeFillsDataJob(ILogger<TradeFillsDataJob> logger
            , CachingService cache
            , TradingDao tradingDao
            , IConfiguration configuration)
        {
            _logger = logger;
            _cache = cache;
            _tradingDao = tradingDao;
            _configuration = configuration;
        }

        public Task Execute(IJobExecutionContext context)
        {
            try
            {
                _logger.LogInformation("Saving Trade Fills - STARTED");
                if (_configuration["ConnectionStrings:ENV"].Equals("PROD"))
                {
                    _logger.LogInformation("Saving [Production] Order Execution Details - STARTED");
                    IDictionary<string, OrderSummary> orderExecutionDetails = _cache.Get<Dictionary<string, OrderSummary>>(CacheKeys.ORDER_EXECUTION_DETAILS);
                    if (orderExecutionDetails != null && orderExecutionDetails.Count > 0)
                        _tradingDao.SaveOrderExecutionDetails(orderExecutionDetails);
                    _logger.LogInformation("Saving [Production] Order Execution Details - DONE");
                }
                else if (_configuration["ConnectionStrings:ENV"].Equals("DEV"))
                {
                    //_logger.LogInformation("Saving [Simulated] Order Execution Details - STARTED");
                    //IDictionary<string, OrderSummary> simOrderExecutionDetails = _cache.Get<Dictionary<string, OrderSummary>>(CacheKeys.SIM_ORDER_EXECUTION_DETAILS);
                    //if (simOrderExecutionDetails != null && simOrderExecutionDetails.Count > 0)
                    //    _tradingDao.SaveSimOrderExecutionDetails(simOrderExecutionDetails);
                    //_logger.LogInformation("Saving [Simulated] Order Execution Details - DONE");
                }
                _logger.LogInformation("Saving Trade Fills - DONE");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving Trade Fills", ex);
            }
            return Task.CompletedTask;
        }
    }
}