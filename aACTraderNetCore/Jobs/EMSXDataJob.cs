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
    public class EMSXDataJob : IJob
    {
        private readonly ILogger<EMSXDataJob> _logger;
        private readonly CachingService _cache;
        private readonly IConfiguration _configuration;
        private readonly EMSXDao _emsxDao;

        public EMSXDataJob(ILogger<EMSXDataJob> logger
            , CachingService cache
            , IConfiguration configuration
            , EMSXDao emsxDao
            )
        {
            _logger = logger;
            _cache = cache;
            _configuration = configuration;
            _emsxDao = emsxDao;
        }

        public Task Execute(IJobExecutionContext context)
        {
            try
            {
                _logger.LogInformation("Saving EMSX Orders - STARTED");

                if (_configuration["ConnectionStrings:ENV"].Equals("PROD"))
                {
                    //_logger.LogInformation("Saving Order Details - STARTED");
                    IList<NewOrder> orderList = _cache.Get<IList<NewOrder>>(CacheKeys.EMSX_BATCH_ORDERS);
                    if (orderList != null && orderList.Count > 0)
                        _emsxDao.SaveBatchOrders(orderList);
                    _logger.LogInformation("Saved EMSX Order Details - DONE");

                    //_logger.LogInformation("Saving Order Status - STARTED");
                    IDictionary<Int32, EMSXOrderStatus> orderStatusDict = _cache.Get<IDictionary<Int32, EMSXOrderStatus>>(CacheKeys.EMSX_ORDER_STATUS);
                    if (orderStatusDict != null && orderStatusDict.Count > 0)
                        _emsxDao.SaveOrderUpdates(orderStatusDict);
                    _logger.LogInformation("Saved EMSX Order Status - DONE");

                    //_logger.LogInformation("Saving Route Status - STARTED");
                    IDictionary<Int32, EMSXRouteStatus> routeStatusDict = _cache.Get<IDictionary<Int32, EMSXRouteStatus>>(CacheKeys.EMSX_ROUTE_STATUS);
                    if (routeStatusDict != null && routeStatusDict.Count > 0)
                        _emsxDao.SaveRouteUpdates(routeStatusDict);
                    _logger.LogInformation("Saved EMSX Route Status - DONE");

                    //_logger.LogInformation("Saving Route Status Hist - STARTED");
                    IDictionary<string, EMSXRouteStatus> routeStatusHistDict = _cache.Get<IDictionary<string, EMSXRouteStatus>>(CacheKeys.EMSX_ROUTE_STATUS_HIST);
                    if (routeStatusHistDict != null && routeStatusHistDict.Count > 0)
                        _emsxDao.SaveRouteHistUpdates(routeStatusHistDict);
                    _logger.LogInformation("Saved EMSX Route Status Hist - DONE");

                    //_logger.LogInformation("Saving Order Fills - STARTED");
                    IDictionary<Int32, IDictionary<Int32, EMSXOrderFill>> orderFillDict = _cache.Get<IDictionary<Int32, IDictionary<Int32, EMSXOrderFill>>>(CacheKeys.EMSX_ORDER_FILLS);
                    if (orderFillDict != null && orderFillDict.Count > 0)
                        _emsxDao.SaveOrderFills(orderFillDict);
                    _logger.LogInformation("Saved EMSX Order Fills - DONE");

                    IList<EMSXOrderError> orderErrorList = _cache.Get<IList<EMSXOrderError>>(CacheKeys.EMSX_ORDER_ERRORS);
                    if (orderErrorList != null && orderErrorList.Count > 0)
                        _emsxDao.SaveOrderErrors(orderErrorList);
                    _logger.LogInformation("Saved EMSX Order Errors - DONE");
                }
                else if (_configuration["ConnectionStrings:ENV"].Equals("DEV") && _configuration["ConnectionStrings:EMSX_UAT_ON"].Equals("Y"))
                {
                    //_logger.LogInformation("Saving Order Details - STARTED");
                    IList<NewOrder> orderList = _cache.Get<IList<NewOrder>>(CacheKeys.EMSX_BATCH_ORDERS);
                    if (orderList != null && orderList.Count > 0)
                        _emsxDao.SaveBatchOrders(orderList);
                    _logger.LogInformation("Saved EMSX Order Details DONE");

                    //_logger.LogInformation("Saving Order Status - STARTED");
                    IDictionary<Int32, EMSXOrderStatus> orderStatusDict = _cache.Get<IDictionary<Int32, EMSXOrderStatus>>(CacheKeys.EMSX_ORDER_STATUS);
                    if (orderStatusDict != null && orderStatusDict.Count > 0)
                        _emsxDao.SaveOrderUpdates(orderStatusDict);
                    _logger.LogInformation("Saved EMSX Order Status - DONE");

                    //_logger.LogInformation("Saving Route Status - STARTED");
                    IDictionary<Int32, EMSXRouteStatus> routeStatusDict = _cache.Get<IDictionary<Int32, EMSXRouteStatus>>(CacheKeys.EMSX_ROUTE_STATUS);
                    if (routeStatusDict != null && routeStatusDict.Count > 0)
                        _emsxDao.SaveRouteUpdates(routeStatusDict);
                    _logger.LogInformation("Saved EMSX Route Status - DONE");

                    //_logger.LogInformation("Saving Route Status Hist - STARTED");
                    IDictionary<string, EMSXRouteStatus> routeStatusHistDict = _cache.Get<IDictionary<string, EMSXRouteStatus>>(CacheKeys.EMSX_ROUTE_STATUS_HIST);
                    if (routeStatusHistDict != null && routeStatusHistDict.Count > 0)
                        _emsxDao.SaveRouteHistUpdates(routeStatusHistDict);
                    _logger.LogInformation("Saved EMSX Route Status Hist - DONE");

                    //_logger.LogInformation("Saving Order Fills - STARTED");
                    IDictionary<Int32, IDictionary<Int32, EMSXOrderFill>> orderFillDict = _cache.Get<IDictionary<Int32, IDictionary<Int32, EMSXOrderFill>>>(CacheKeys.EMSX_ORDER_FILLS);
                    if (orderFillDict != null && orderFillDict.Count > 0)
                        _emsxDao.SaveOrderFills(orderFillDict);
                    _logger.LogInformation("Saved EMSX Order Fills - DONE");

                    IList<EMSXOrderError> orderErrorList = _cache.Get<IList<EMSXOrderError>>(CacheKeys.EMSX_ORDER_ERRORS);
                    if (orderErrorList != null && orderErrorList.Count > 0)
                        _emsxDao.SaveOrderErrors(orderErrorList);
                    _logger.LogInformation("Saved EMSX Order Errors - DONE");
                }
                _logger.LogInformation("Saving EMSX Orders - DONE");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving EMSX Orders", ex);
            }
            return Task.CompletedTask;
        }
    }
}
