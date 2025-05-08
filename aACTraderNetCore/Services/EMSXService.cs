using aACTrader.DAO.Repository;
using aACTrader.Services.EMSX;
using aCommons;
using aCommons.Trading;
using LazyCache;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;

namespace aACTrader.Services
{
    public class EMSXService
    {
        private readonly ILogger<EMSXService> _logger;
        private readonly IConfiguration _configuration;
        private readonly CachingService _cache;
        private readonly EMSXDao _emsxDao;
        private readonly EMSXOrderStatusService _emsxOrderStatusService;
        private readonly EMSXRouteStatusService _emsxRouteStatusService;
        private readonly EMSXOrderErrorService _emsxOrderErrorService;

        public EMSXService(ILogger<EMSXService> logger
            , IConfiguration configuration
            , CachingService cache
            , EMSXDao emsxDao
            , EMSXOrderStatusService emsxOrderStatusService
            , EMSXRouteStatusService emsxRouteStatusService
            , EMSXOrderErrorService emsxOrderErrorService)
        {
            this._logger = logger;
            this._configuration = configuration;
            this._cache = cache;
            this._emsxDao = emsxDao;
            this._emsxOrderStatusService = emsxOrderStatusService;
            this._emsxRouteStatusService = emsxRouteStatusService;
            this._emsxOrderErrorService = emsxOrderErrorService;
        }

        public void Start()
        {
            _logger.LogInformation("Starting EMSX Service(s)...");

            ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            /// EMSX
            ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            _logger.LogInformation("Populated EMSX Batch Order Details...");
            PopulateEMSXBatchOrders();

            IDictionary<Int32, EMSXOrderStatus> orderStatusDict = _emsxDao.GetOrderStatus();
            _cache.Add(CacheKeys.EMSX_ORDER_STATUS, orderStatusDict, DateTimeOffset.MaxValue);

            IDictionary<Int32, EMSXRouteStatus> routeStatusDict = _emsxDao.GetRouteStatus();
            _cache.Add(CacheKeys.EMSX_ROUTE_STATUS, routeStatusDict, DateTimeOffset.MaxValue);

            IDictionary<string, EMSXRouteStatus> routeStatusHistDict = _emsxDao.GetRouteStatusHist();
            _cache.Add(CacheKeys.EMSX_ROUTE_STATUS_HIST, routeStatusHistDict, DateTimeOffset.MaxValue);

            IDictionary<Int32, IDictionary<Int32, EMSXOrderFill>> orderFillDict = _emsxDao.GetOrderFills();
            _cache.Add(CacheKeys.EMSX_ORDER_FILLS, orderFillDict, DateTimeOffset.MaxValue);

            IList<EMSXOrderError> orderErrorsList = _emsxDao.GetOrderErrors();
            _cache.Add(CacheKeys.EMSX_ORDER_ERRORS, orderErrorsList, DateTimeOffset.MaxValue);

            PopulateEMSXOrders(orderStatusDict, routeStatusDict);

            ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            // EMSX SIMULATION/TEST
            ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            _logger.LogInformation("Populated EMSX Batch Order Details [SIMULATED]...");
            PopulateEMSXSimBatchOrders();

            if (_configuration["ConnectionStrings:ENV"].Equals("PROD"))
            {
                _logger.LogInformation("Starting EMSXOrderStatusService...");
                _emsxOrderStatusService.Start();

                _logger.LogInformation("Starting EMSXRouteStatusService...");
                _emsxRouteStatusService.Start();

                _logger.LogInformation("Starting EMSXOrderErrorService...");
                _emsxOrderErrorService.Start();
            }
            else if (_configuration["ConnectionStrings:ENV"].Equals("DEV") && _configuration["ConnectionStrings:EMSX_UAT_ON"].Equals("Y"))
            {
                _logger.LogInformation("Starting EMSXOrderStatusService...");
                _emsxOrderStatusService.Start();

                _logger.LogInformation("Starting EMSXRouteStatusService...");
                _emsxRouteStatusService.Start();

                _logger.LogInformation("Starting EMSXOrderErrorService...");
                _emsxOrderErrorService.Start();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        private void PopulateEMSXBatchOrders()
        {
            IList<NewOrder> batchOrders = _emsxDao.GetBatchOrders();
            _cache.Add(CacheKeys.EMSX_BATCH_ORDERS, batchOrders, DateTimeOffset.MaxValue);

            IDictionary<string, NewOrder> batchOrdersByRefIdDict = new Dictionary<string, NewOrder>(StringComparer.CurrentCultureIgnoreCase);
            foreach (NewOrder nOrder in batchOrders)
            {
                if (!string.IsNullOrEmpty(nOrder.Id) &&
                    !batchOrdersByRefIdDict.ContainsKey(nOrder.Id))
                    batchOrdersByRefIdDict.Add(nOrder.Id, nOrder);
            }

            _cache.Remove(CacheKeys.EMSX_BATCH_ORDERS_REFID_MAP);
            _cache.Add(CacheKeys.EMSX_BATCH_ORDERS_REFID_MAP, batchOrdersByRefIdDict, DateTimeOffset.MaxValue);
        }

        /// <summary>
        /// 
        /// </summary>
        private void PopulateEMSXSimBatchOrders()
        {
            IList<NewOrder> batchOrders = _emsxDao.GetSimBatchOrders();
            _cache.Add(CacheKeys.EMSX_SIM_BATCH_ORDERS, batchOrders, DateTimeOffset.MaxValue);

            IDictionary<string, NewOrder> batchOrdersByRefIdDict = _cache.Get<IDictionary<string, NewOrder>>(CacheKeys.BATCH_ORDERS_REFID_MAP);
            foreach (NewOrder nOrder in batchOrders)
            {
                if (!string.IsNullOrEmpty(nOrder.Id) &&
                    !batchOrdersByRefIdDict.ContainsKey(nOrder.Id))
                    batchOrdersByRefIdDict.Add(nOrder.Id, nOrder);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="orderStatusDict"></param>
        /// <param name="routeStatusDict"></param>
        private void PopulateEMSXOrders(IDictionary<Int32, EMSXOrderStatus> orderStatusDict, IDictionary<Int32, EMSXRouteStatus> routeStatusDict)
        {
            IDictionary<Int32, EMSXOrder> orderDict = new Dictionary<Int32, EMSXOrder>();
            foreach (EMSXOrderStatus orderStatus in orderStatusDict.Values)
            {
                if (!orderDict.TryGetValue(orderStatus.OrdSeq.GetValueOrDefault(), out EMSXOrder order))
                {
                    order = new EMSXOrder();
                    order.OrdSeq = orderStatus.OrdSeq;
                    order.OrderStatus = orderStatus;
                    orderDict.Add(order.OrdSeq.GetValueOrDefault(), order);
                }
            }
            foreach (EMSXRouteStatus routeStatus in routeStatusDict.Values)
            {
                if (!orderDict.TryGetValue(routeStatus.OrdSeq.GetValueOrDefault(), out EMSXOrder order))
                {
                    order = new EMSXOrder();
                    order.OrdSeq = routeStatus.OrdSeq;
                    order.RouteStatus = routeStatus;
                    orderDict.Add(order.OrdSeq.GetValueOrDefault(), order);
                }
                else
                {
                    order.RouteStatus = routeStatus;
                }
            }
            _cache.Add(CacheKeys.EMSX_ORDERS, orderDict, DateTimeOffset.MaxValue);
        }
    }
}
