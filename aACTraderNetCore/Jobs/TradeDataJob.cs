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
    public class TradeDataJob : IJob
    {
        private readonly ILogger<TradeDataJob> _logger;
        private readonly CachingService _cache;
        private readonly TradingDao _tradingDao;
        private readonly PairTradingDao _pairTradingDao;
        private readonly PairTradingNVDao _pairTradingNVDao;
        private readonly IConfiguration _configuration;

        public TradeDataJob(ILogger<TradeDataJob> logger
            , CachingService cache
            , TradingDao tradingDao
            , PairTradingDao pairTradingDao
            , PairTradingNVDao pairTradingNVDao
            , IConfiguration configuration)
        {
            _logger = logger;
            _cache = cache;
            _tradingDao = tradingDao;
            _pairTradingDao = pairTradingDao;
            _pairTradingNVDao = pairTradingNVDao;
            _configuration = configuration;
        }

        public Task Execute(IJobExecutionContext context)
        {
            try
            {
                _logger.LogInformation("Saving Neovest Trades - STARTED");

                if (_configuration["ConnectionStrings:ENV"].Equals("PROD"))
                {
                    //_logger.LogInformation("Saving Neovest [Production] Trades - STARTED");
                    IDictionary<string, OrderSummary> orderSummaryDict = _cache.Get<IDictionary<string, OrderSummary>>(CacheKeys.ORDER_SUMMARY);
                    if (orderSummaryDict != null && orderSummaryDict.Count > 0)
                        _tradingDao.SaveOrderSummary(orderSummaryDict);
                    _logger.LogInformation("Saving Neovest [Production] Trades - DONE");

                    //_logger.LogInformation("Saving [Production] Order Execution Details - STARTED");
                    //IDictionary<string, OrderSummary> orderExecutionDetails = _cache.Get<Dictionary<string, OrderSummary>>(CacheKeys.ORDER_EXECUTION_DETAILS);
                    //if (orderExecutionDetails != null && orderExecutionDetails.Count > 0)
                    //    _tradingDao.SaveOrderExecutionDetails(orderExecutionDetails);
                    //_logger.LogInformation("Saving [Production] Order Execution Details - DONE");

                    //_logger.LogInformation("Saving [Simulated] Order Execution Details - STARTED");
                    //IDictionary<string, OrderSummary> simOrderExecutionDetails = _cache.Get<Dictionary<string, OrderSummary>>(CacheKeys.SIM_ORDER_EXECUTION_DETAILS);
                    //if (simOrderExecutionDetails != null && simOrderExecutionDetails.Count > 0)
                    //    _tradingDao.SaveSimOrderExecutionDetails(simOrderExecutionDetails);
                    //_logger.LogInformation("Saving [Simulated] Order Execution Details - DONE");

                    //_logger.LogInformation("Saving [Simulated] Orders - STARTED");
                    //IDictionary<string, OrderSummary> simOrderSummaryDict = _cache.Get<IDictionary<string, OrderSummary>>(CacheKeys.SIM_ORDER_SUMMARY);
                    //if (simOrderSummaryDict != null && simOrderSummaryDict.Count > 0)
                    //    _tradingDao.SaveSimOrderSummary(simOrderSummaryDict);
                    //_logger.LogInformation("Saved [Simulated] Orders  DONE");

                    //_logger.LogInformation("Saving Batch Orders (NEW, CANCEL/CORRECT) - STARTED");
                    IList<NewOrder> batchOrdersList = _cache.Get<List<NewOrder>>(CacheKeys.BATCH_ORDERS);
                    if (batchOrdersList != null && batchOrdersList.Count > 0)
                        _tradingDao.SaveBatchOrders(batchOrdersList);
                    _logger.LogInformation("Saving Batch Orders (NEW, CANCEL/CORRECT) - DONE");

                    IDictionary<string, PairOrder> pairOrdersDict = _cache.Get<IDictionary<string, PairOrder>>(CacheKeys.PAIR_TRADE_BATCH_ORDERS);
                    if (pairOrdersDict != null && pairOrdersDict.Count > 0)
                    {
                        //_logger.LogInformation("Saving Pair Trades Batch Orders (NEW, CANCEL/CORRECT) - STARTED");
                        _pairTradingDao.SavePairOrders(pairOrdersDict);
                        _logger.LogInformation("Saving Pair Trades Batch Orders (NEW, CANCEL/CORRECT) - DONE");

                        //_logger.LogInformation("Saving Pair Trade Details - STARTED");
                        _pairTradingDao.SavePairOrderDetails(pairOrdersDict);
                        _logger.LogInformation("Saving Pair Trade Details - DONE");
                    }

                    IDictionary<string, NewPairOrder> nvPairOrdersDict = _cache.Get<IDictionary<string, NewPairOrder>>(CacheKeys.NV_PAIR_TRADE_BATCH_ORDERS);
                    if (nvPairOrdersDict != null && nvPairOrdersDict.Count > 0)
                    {
                        //_logger.LogInformation("Saving NV Pair Trades Batch Orders (NEW, CANCEL/CORRECT) - STARTED");
                        _pairTradingNVDao.SavePairOrders(nvPairOrdersDict);
                        _logger.LogInformation("Saving NV Pair Trades Batch Orders (NEW, CANCEL/CORRECT) - DONE");
                    }

                    //_logger.LogInformation("Saving Queued Orders - STARTED");
                    IDictionary<string, OrderSummary> orderQueueDict = _cache.Get<IDictionary<string, OrderSummary>>(CacheKeys.ORDER_QUEUE_SUMMARY);
                    if (orderQueueDict != null && orderQueueDict.Count > 0)
                        _tradingDao.SaveOrderQueue(orderQueueDict);
                    _logger.LogInformation("Saved Queued Orders - DONE");

                }
                else if (_configuration["ConnectionStrings:ENV"].Equals("DEV"))
                {
                    _logger.LogInformation("Saving [Simulated] Order Execution Details - STARTED");
                    IDictionary<string, OrderSummary> simOrderExecutionDetails = _cache.Get<Dictionary<string, OrderSummary>>(CacheKeys.SIM_ORDER_EXECUTION_DETAILS);
                    if (simOrderExecutionDetails != null && simOrderExecutionDetails.Count > 0)
                        _tradingDao.SaveSimOrderExecutionDetails(simOrderExecutionDetails);
                    _logger.LogInformation("Saving [Simulated] Order Execution Details - DONE");

                    _logger.LogInformation("Saving [Simulated] Orders - STARTED");
                    IDictionary<string, OrderSummary> simOrderSummaryDict = _cache.Get<IDictionary<string, OrderSummary>>(CacheKeys.SIM_ORDER_SUMMARY);
                    if (simOrderSummaryDict != null && simOrderSummaryDict.Count > 0)
                        _tradingDao.SaveSimOrderSummary(simOrderSummaryDict);
                    _logger.LogInformation("Saved [Simulated] Orders  DONE");

                    _logger.LogInformation("Saving [Simulated] Batch Orders (NEW, CANCEL/CORRECT) - STARTED");
                    IList<NewOrder> batchOrdersList = _cache.Get<List<NewOrder>>(CacheKeys.SIM_BATCH_ORDERS);
                    if (batchOrdersList != null && batchOrdersList.Count > 0)
                        _tradingDao.SaveSimBatchOrders(batchOrdersList);
                    _logger.LogInformation("Saving [Simulated] Batch Orders (NEW, CANCEL/CORRECT) - DONE");

                    //IDictionary<string, PairOrder> pairOrdersDict = _cache.Get<IDictionary<string, PairOrder>>(CacheKeys.PAIR_TRADE_BATCH_ORDERS);
                    //if (pairOrdersDict != null && pairOrdersDict.Count > 0)
                    //{
                    //    _logger.LogInformation("Saving Pair Trades Batch Orders (NEW, CANCEL/CORRECT) - STARTED");
                    //    _pairTradingDao.SavePairOrders(pairOrdersDict);
                    //    _logger.LogInformation("Saving Pair Trades Batch Orders (NEW, CANCEL/CORRECT) - DONE");

                    //    _logger.LogInformation("Saving Pair Trade Details - STARTED");
                    //    _pairTradingDao.SavePairOrderDetails(pairOrdersDict);
                    //    _logger.LogInformation("Saving Pair Trade Details - DONE");
                    //}

                    //IDictionary<string, NewPairOrder> newPairOrdersDict = _cache.Get<IDictionary<string, NewPairOrder>>(CacheKeys.NV_PAIR_TRADE_BATCH_ORDERS);
                    //if (newPairOrdersDict != null && newPairOrdersDict.Count > 0)
                    //{
                    //    _logger.LogInformation("Saving NV Pair Trades Batch Orders (NEW, CANCEL/CORRECT) - STARTED");
                    //    _pairTradingNVDao.SavePairOrders(newPairOrdersDict);
                    //    _logger.LogInformation("Saving NV Pair Trades Batch Orders (NEW, CANCEL/CORRECT) - DONE");
                    //}

                    //_logger.LogInformation("Saving [Simulated] Queued Orders - STARTED");
                    //IDictionary<string, OrderSummary> simOrderQueueDict = _cache.Get<IDictionary<string, OrderSummary>>(CacheKeys.SIM_ORDER_QUEUE_SUMMARY);
                    //if (simOrderQueueDict != null && simOrderQueueDict.Count > 0)
                    //    _tradingDao.SaveSimOrderQueue(simOrderQueueDict);
                    //_logger.LogInformation("Saved [Simulated] Queued Orders  DONE");
                }
                _logger.LogInformation("Saving Neovest Trades - DONE");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving Neovest Trades", ex);
            }
            return Task.CompletedTask;
        }
    }
}