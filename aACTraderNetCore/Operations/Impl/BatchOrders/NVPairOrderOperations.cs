using aACTrader.Services.Neovest;
using aCommons;
using aCommons.Trading;
using LazyCache;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using ConfigurationManager = System.Configuration.ConfigurationManager;

namespace aACTrader.Operations.Impl.BatchOrders
{
    public class NVPairOrderOperations
    {
        private readonly ILogger<NVPairOrderOperations> _logger;
        private readonly CachingService _cache;
        private readonly IConfiguration _configuration;
        private readonly SimNeovestPairOrderPublisher _simNeovestPairOrderPublisher;
        private readonly NeovestPairOrderPublisher _neovestPairOrderPublisher;

        protected static readonly string ENV = ConfigurationManager.AppSettings["ENV"];

        public NVPairOrderOperations(ILogger<NVPairOrderOperations> logger
            , CachingService cache
            , IConfiguration configuration
            , SimNeovestPairOrderPublisher simNeovestPairOrderPublisher
            , NeovestPairOrderPublisher neovestPairOrderPublisher)
        {
            this._logger = logger;
            this._cache = cache;
            this._configuration = configuration;
            this._simNeovestPairOrderPublisher = simNeovestPairOrderPublisher;
            this._neovestPairOrderPublisher = neovestPairOrderPublisher;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="orders"></param>
        public void ProcessNewOrders(IList<NewOrder> orders)
        {
            try
            {
                IList<NewPairOrder> newOrders = new List<NewPairOrder>();
                if (orders != null && orders.Count > 0)
                {
                    IDictionary<string, NewPairOrder> batchOrders = _cache.Get<IDictionary<string, NewPairOrder>>(CacheKeys.NV_PAIR_TRADE_BATCH_ORDERS);
                    foreach (NewOrder order in orders)
                    {
                        if (!batchOrders.TryGetValue(order.ParentId, out NewPairOrder pairOrder))
                        {
                            pairOrder = new NewPairOrder();
                            pairOrder.ParentId = order.ParentId;
                            pairOrder.ActionType = order.ActionType;
                            batchOrders.Add(pairOrder.ParentId, pairOrder);
                            newOrders.Add(pairOrder);
                        }

                        if ("B".Equals(order.OrderSide, StringComparison.CurrentCultureIgnoreCase)
                         || "BUY".Equals(order.OrderSide, StringComparison.CurrentCultureIgnoreCase))
                            pairOrder.BuyOrder = order;
                        else if ("S".Equals(order.OrderSide, StringComparison.CurrentCultureIgnoreCase)
                            || "SELL".Equals(order.OrderSide, StringComparison.CurrentCultureIgnoreCase)
                            || "SS".Equals(order.OrderSide, StringComparison.CurrentCultureIgnoreCase)
                            || "SELL SHORT".Equals(order.OrderSide, StringComparison.CurrentCultureIgnoreCase)
                            )
                            pairOrder.SellOrder = order;
                    }
                }

                if (newOrders.Count > 0)
                    SubmitNewOrders(newOrders);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing NEW orders");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="newOrders"></param>
        private void SubmitNewOrders(IList<NewPairOrder> newOrders)
        {
            foreach (NewPairOrder pairOrder in newOrders)
            {
                if (pairOrder.BuyOrder != null && pairOrder.SellOrder != null)
                {
                    if ("New".Equals(pairOrder.ActionType, StringComparison.CurrentCultureIgnoreCase))
                    {
                        _logger.LogInformation("Processing NEW Pair Order: "
                                  + pairOrder.ParentId + "/"
                                  + pairOrder.BuyOrder.Symbol + "/"
                                  + pairOrder.BuyOrder.OrderSide + "/"
                                  + pairOrder.SellOrder.Symbol + "/"
                                  + pairOrder.SellOrder.OrderSide + "/"
                                  );

                        try
                        {
                            ValidateOrder(pairOrder);

                            if (_configuration["ConnectionStrings:ENV"].Equals("PROD"))
                                SubmitOrder(pairOrder);
                            else
                                SubmitOrderSim(pairOrder);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "Error submitting NEW PAIR order");
                        }
                    }
                }
            }
        }

        private void ValidateOrder(NewPairOrder order)
        {
            NewOrder buyOrder = order.BuyOrder;
            NewOrder sellOrder = order.SellOrder;

            //Round Order Qty
            buyOrder.OrderQty = Math.Round(buyOrder.OrderQty.GetValueOrDefault());
            sellOrder.OrderQty = Math.Round(sellOrder.OrderQty.GetValueOrDefault());
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="order"></param>
        private void SubmitOrder(NewPairOrder order)
        {
            try
            {
                _neovestPairOrderPublisher.PublishMessage(order);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error submitting New/Update PAIR orders [Production Environment]");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="order"></param>
        private void SubmitOrderSim(NewPairOrder order)
        {
            try
            {
                _simNeovestPairOrderPublisher.PublishMessage(order);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error submitting New/Update PAIR orders [Simulation Environment]");
            }
        }
    }
}
