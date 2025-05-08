using aACTrader.DAO.Repository;
using aACTrader.Model;
using aACTrader.Operations.Impl.BatchOrders;
using aCommons;
using aCommons.Trading;
using LazyCache;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;

namespace aACTrader.Controllers
{
    [ApiController]
    public class EMSXServiceController : Controller
    {
        private readonly ILogger<EMSXServiceController> _logger;
        private readonly IConfiguration _configuration;
        private readonly CachingService _cache;
        private readonly EMSXDao _emsxDao;
        private readonly EMSXBatchOrderOperations _emsxBatchOrderOperations;

        public EMSXServiceController(ILogger<EMSXServiceController> logger
            , IConfiguration configuration
            , CachingService cachingService
            , EMSXDao emsxDao
            , EMSXBatchOrderOperations emsxBatchOrderOperations)
        {
            _logger = logger;
            _configuration = configuration;
            _cache = cachingService;
            _emsxDao = emsxDao;
            _emsxBatchOrderOperations = emsxBatchOrderOperations;
        }

        [Route("/EMSXService/SubmitEMSXOrders")]
        [HttpPost]
        public void SubmitEMSXOrders(IList<NewOrder> orders)
        {
            _emsxBatchOrderOperations.ProcessNewOrders(orders);
        }

        //[Route("/EMSXService/SubmitEMSXOrdersSim")]
        //[HttpPost]
        //public void SubmitEMSXOrdersSim(IList<NewOrder> orders)
        //{
        //    _emsxBatchOrderOperations.ProcessNewOrdersSim(orders);
        //}

        [Route("/EMSXService/GetBatchTemplates")]
        [HttpGet]
        public IList<string> GetBatchTemplates()
        {
            return _emsxDao.GetBatchTemplates();
        }

        [Route("/EMSXService/SaveBatchTemplate")]
        [HttpPost]
        public void SaveBatchTemplate(IList<BatchOrderTemplate> list)
        {
            _emsxDao.SaveBatchTemplate(list);
        }

        [Route("/EMSXService/GetBatchTemplateDetails")]
        [HttpPost]
        public IList<BatchOrderTemplate> GetBatchTemplateDetails(OrderParameters parameters)
        {
            return _emsxDao.GetBatchTemplate(parameters.TemplateName);
        }

        [Route("/EMSXService/GetBatchTemplatesForUser")]
        [HttpPost]
        public IList<string> GetBatchTemplatesForUser(OrderParameters parameters)
        {
            return _emsxDao.GetBatchTemplatesForUser(parameters.Trader, null);
        }

        [Route("/EMSXService/GetOrders")]
        [HttpPost]
        public IList<EMSXOrderStatus> GetOrders(OrderParameters parameters)
        {
            IDictionary<Int32, EMSXOrderStatus> dict = _cache.Get<IDictionary<Int32, EMSXOrderStatus>>(CacheKeys.EMSX_ORDER_STATUS);
            IDictionary<Int32, EMSXRouteStatus> routeDict = _cache.Get<IDictionary<Int32, EMSXRouteStatus>>(CacheKeys.EMSX_ROUTE_STATUS);

            bool includeOrder = true;
            IList<EMSXOrderStatus> list = new List<EMSXOrderStatus>();
            foreach (KeyValuePair<Int32, EMSXOrderStatus> kvp in dict)
            {
                int ordSeq = kvp.Key;
                EMSXOrderStatus data = kvp.Value;
                includeOrder = true;

                if (routeDict.TryGetValue(ordSeq, out EMSXRouteStatus route))
                {
                    data.StratType = route.StratType;
                    data.RouteId = route.RouteId;
                }

                if (!parameters.Trader.Equals("All", StringComparison.CurrentCultureIgnoreCase))
                    if (!parameters.Trader.Equals(data.Trader, StringComparison.CurrentCultureIgnoreCase))
                        includeOrder = false;

                if (includeOrder)
                    list.Add(data);
            }
            return list.OrderByDescending(o => o.OrdSeq).ToList<EMSXOrderStatus>();
        }

        [Route("/EMSXService/GetRoutes")]
        [HttpPost]
        public IList<EMSXRouteStatus> GetRoutes(OrderParameters parameters)
        {
            IDictionary<Int32, EMSXRouteStatus> dict = _cache.Get<IDictionary<Int32, EMSXRouteStatus>>(CacheKeys.EMSX_ROUTE_STATUS);
            IDictionary<Int32, EMSXOrderStatus> orderDict = _cache.Get<IDictionary<Int32, EMSXOrderStatus>>(CacheKeys.EMSX_ORDER_STATUS);

            bool includeOrder = true;
            IList<EMSXRouteStatus> list = new List<EMSXRouteStatus>();
            foreach (EMSXRouteStatus data in dict.Values)
            {
                includeOrder = true;
                if (orderDict.TryGetValue(data.OrdSeq.GetValueOrDefault(), out EMSXOrderStatus order))
                {
                    data.Ticker = order.Ticker;
                    data.OrdSide = order.OrdSide;
                    data.Trader = order.Trader;
                }

                if (!parameters.Trader.Equals("All", StringComparison.CurrentCultureIgnoreCase))
                    if (!parameters.Trader.Equals(data.Trader, StringComparison.CurrentCultureIgnoreCase))
                        includeOrder = false;

                if (includeOrder)
                    list.Add(data);
            }
            return list.OrderByDescending(o => o.OrdSeq).ToList<EMSXRouteStatus>();
        }

        [Route("/EMSXService/GetOrderFills")]
        [HttpPost]
        public IList<EMSXOrderFill> GetOrderFills(OrderParameters parameters)
        {
            IDictionary<Int32, IDictionary<Int32, EMSXOrderFill>> dict = _cache.Get<IDictionary<Int32, IDictionary<Int32, EMSXOrderFill>>>(CacheKeys.EMSX_ORDER_FILLS);
            IDictionary<Int32, EMSXOrderStatus> orderDict = _cache.Get<IDictionary<Int32, EMSXOrderStatus>>(CacheKeys.EMSX_ORDER_STATUS);

            bool includeOrder = true;
            IList<EMSXOrderFill> list = new List<EMSXOrderFill>();
            foreach (KeyValuePair<Int32, IDictionary<Int32, EMSXOrderFill>> kvp in dict)
            {
                int ordSeq = kvp.Key;
                IDictionary<Int32, EMSXOrderFill> data = kvp.Value;
                includeOrder = true;

                if (includeOrder)
                {
                    foreach (KeyValuePair<Int32, EMSXOrderFill> kvp1 in data)
                    {
                        EMSXOrderFill orderFill = kvp1.Value;
                        if (orderDict.TryGetValue(ordSeq, out EMSXOrderStatus order))
                        {
                            orderFill.Ticker = order.Ticker;
                            orderFill.Bkr = order.Bkr;
                            orderFill.OrdSide = order.OrdSide;
                            orderFill.OrdType = order.OrdType;
                            orderFill.StratStyle = order.StratStyle;
                            orderFill.Trader = order.Trader;
                        }
                        list.Add(orderFill);
                    }
                }
            }

            return list.OrderByDescending(o => o.OrdSeq).ThenByDescending(o => o.FillId).ToList<EMSXOrderFill>();
        }

        [Route("/EMSXService/GetEMSXOrderStatus")]
        [HttpPost]
        public IList<EMSXOrderStatus> GetEMSXOrderStatus(OrderParameters parameters)
        {
            IList<EMSXOrderStatus> list = _emsxDao.GetEMSXOrderStatus(parameters.Environment, parameters.Trader);
            return list;
        }

        [Route("/EMSXService/GetEMSXRouteStatus")]
        [HttpPost]
        public IList<EMSXRouteStatus> GetEMSXRouteStatus(OrderParameters parameters)
        {
            IList<EMSXRouteStatus> list = _emsxDao.GetEMSXRouteStatus(parameters.Environment, parameters.Trader);
            return list;
        }

        [Route("/EMSXService/GetEMSXOrderFills")]
        [HttpPost]
        public IList<EMSXOrderFill> GetEMSXOrderFills(OrderParameters parameters)
        {
            IList<EMSXOrderFill> list = _emsxDao.GetEMSXOrderFills(parameters.Environment);
            return list;
        }

        [Route("/EMSXService/GetEMSXTradeHistory")]
        [HttpPost]
        public IList<EMSXRouteStatus> GetEMSXTradeHistory(OrderParameters parameters)
        {
            IList<EMSXRouteStatus> list = _emsxDao.GetEMSXTradeHistory(parameters.StartDate, parameters.EndDate);
            return list;
        }

        [Route("/EMSXService/GetOrderErrors")]
        [HttpGet]
        public IList<EMSXOrderError> GetOrderErrors()
        {
            IList<EMSXOrderError> list = _cache.Get<IList<EMSXOrderError>>(CacheKeys.EMSX_ORDER_ERRORS);
            return list;
        }
    }
}