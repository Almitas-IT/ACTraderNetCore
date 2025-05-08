using aCommons;
using aCommons.Trading;
using aCommons.Utils;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;

namespace aACTrader.Operations.Impl.BatchOrders
{
    public class OrderValidator
    {
        private readonly ILogger<OrderValidator> _logger;
        private readonly IConfiguration _configuration;

        protected static readonly double DEFAULT_MARKET_PRICE_THRESHOLD = 0.005;
        protected static readonly double DISCOUNT_DEFAULT_MARKET_PRICE_THRESHOLD = 0.010;

        public OrderValidator(ILogger<OrderValidator> logger
            , IConfiguration configuration)
        {
            this._logger = logger;
            this._configuration = configuration;
            _logger.LogInformation("Initializing OrderValidator...");
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="order"></param>
        public void PopulateDefaults(NewOrder order)
        {
            //Order Side
            if (!string.IsNullOrEmpty(order.OrderSide))
            {
                if (order.OrderSide.Equals("B", StringComparison.CurrentCultureIgnoreCase)
                    || order.OrderSide.Equals("BC", StringComparison.CurrentCultureIgnoreCase))
                    order.OrderSideInd = 1;
                else if (order.OrderSide.Equals("S", StringComparison.CurrentCultureIgnoreCase)
                    || order.OrderSide.Equals("SS", StringComparison.CurrentCultureIgnoreCase))
                    order.OrderSideInd = 2;
            }

            //Reference Id Beta Shift Indicator
            if (!string.IsNullOrEmpty(order.RefIndexPriceBetaShiftIndAsString))
            {
                if (order.RefIndexPriceBetaShiftIndAsString.Equals("Both", StringComparison.CurrentCultureIgnoreCase))
                    order.RefIndexPriceBetaShiftInd = 0;
                else if (order.RefIndexPriceBetaShiftIndAsString.Equals("Up", StringComparison.CurrentCultureIgnoreCase))
                    order.RefIndexPriceBetaShiftInd = 1;
                else if (order.RefIndexPriceBetaShiftIndAsString.Equals("Down", StringComparison.CurrentCultureIgnoreCase))
                    order.RefIndexPriceBetaShiftInd = -1;
            }

            //Reference Id/Discount Target Price Cap Shift Indicator
            if (!string.IsNullOrEmpty(order.RefIndexPriceCapShiftIndAsString))
            {
                if (order.RefIndexPriceCapShiftIndAsString.Equals("Both", StringComparison.CurrentCultureIgnoreCase))
                    order.RefIndexPriceCapShiftInd = 0;
                else if (order.RefIndexPriceCapShiftIndAsString.Equals("Up", StringComparison.CurrentCultureIgnoreCase))
                    order.RefIndexPriceCapShiftInd = 1;
                else if (order.RefIndexPriceCapShiftIndAsString.Equals("Down", StringComparison.CurrentCultureIgnoreCase))
                    order.RefIndexPriceCapShiftInd = -1;
            }

            //Default Market Price Field (if missing)
            if (string.IsNullOrEmpty(order.MarketPriceField))
            {
                order.MarketPriceField = "Bid"; //default

                if (order.OrderSideInd.GetValueOrDefault() == 1)
                    order.MarketPriceField = "Bid";
                else if (order.OrderSideInd.GetValueOrDefault() == 2)
                    order.MarketPriceField = "Ask";
            }

            //Default Market Price Threshold (if missing)
            if (!order.MarketPriceThreshold.HasValue)
            {
                if (order.DiscountTarget.HasValue)
                    order.MarketPriceThreshold = DISCOUNT_DEFAULT_MARKET_PRICE_THRESHOLD;
                else
                    order.MarketPriceThreshold = DEFAULT_MARKET_PRICE_THRESHOLD;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="orderSummary"></param>
        /// <param name="securityPrice"></param>
        /// <returns></returns>
        public void CalculateFinalTargetPrice(OrderSummary orderSummary, SecurityPrice securityPrice)
        {
            double? targetOrderPrice = orderSummary.RITgtPr;
            double newOrderPrice = targetOrderPrice.GetValueOrDefault();
            double currentOrderPrice = orderSummary.OrdPr.GetValueOrDefault();
            string orderSide = orderSummary.OrdSide;
            int orderSideInt = 0;

            //order side
            if (orderSide.Equals("Buy", StringComparison.CurrentCultureIgnoreCase) || orderSide.Equals("Buy Cover", StringComparison.CurrentCultureIgnoreCase))
                orderSideInt = 1;
            else if (orderSide.Equals("Sell", StringComparison.CurrentCultureIgnoreCase) || orderSide.Equals("Sell Short", StringComparison.CurrentCultureIgnoreCase) || orderSide.Equals("SSx", StringComparison.CurrentCultureIgnoreCase))
                orderSideInt = 2;

            /////////////////////////////////////////////////////////////////////////////////////////
            /// Max/Min Price check
            /////////////////////////////////////////////////////////////////////////////////////////
            //apply max/min order price check
            if (orderSummary.RIMaxPr.HasValue)
            {
                if (orderSideInt == 1)
                {
                    //max price to buy
                    if (targetOrderPrice.GetValueOrDefault() > orderSummary.RIMaxPr.GetValueOrDefault())
                    {
                        string errorFlag = "Target Price > Max Price: " +
                           orderSummary.Sym + "/" +
                           orderSummary.OrdId + "/" +
                           orderSummary.RIMaxPr + "/" +
                           targetOrderPrice;

                        orderSummary.ErrorFlag = errorFlag;
                        targetOrderPrice = orderSummary.RIMaxPr;
                    }
                }
                else if (orderSideInt == 2)
                {
                    //min price to sell
                    if (targetOrderPrice.GetValueOrDefault() < orderSummary.RIMaxPr.GetValueOrDefault())
                    {
                        string errorFlag = "Target Price < Min Price: " +
                          orderSummary.Sym + "/" +
                          orderSummary.OrdId + "/" +
                          orderSummary.RIMaxPr + "/" +
                          targetOrderPrice;

                        orderSummary.ErrorFlag = errorFlag;
                        targetOrderPrice = orderSummary.RIMaxPr;
                    }
                }
            }

            /////////////////////////////////////////////////////////////////////////////////////////
            /// Bid/Offer Price check
            /////////////////////////////////////////////////////////////////////////////////////////
            //check target price against live security price
            if (securityPrice != null)
            {
                orderSummary.LastPr = securityPrice.LastPrc;
                orderSummary.BidPr = securityPrice.BidPrc;
                orderSummary.AskPr = securityPrice.AskPrc;

                if (orderSideInt == 1)
                {
                    if (targetOrderPrice.GetValueOrDefault() > securityPrice.AskPrc.GetValueOrDefault())
                    {
                        string errorFlag = "Target Price > Ask Price: " +
                            orderSummary.Sym + "/" +
                            orderSummary.OrdId + "/" +
                            securityPrice.AskPrc + "/" +
                            targetOrderPrice;

                        _logger.LogInformation(errorFlag);
                        orderSummary.ErrorFlag = errorFlag;
                        targetOrderPrice = securityPrice.AskPrc;
                    }
                }
                else if (orderSideInt == 2)
                {
                    if (targetOrderPrice.GetValueOrDefault() < securityPrice.BidPrc.GetValueOrDefault())
                    {
                        string errorFlag = "Target Price < Bid Price: " +
                            orderSummary.Sym + "/" +
                            orderSummary.OrdId + "/" +
                            securityPrice.BidPrc + "/" +
                            targetOrderPrice;

                        _logger.LogInformation(errorFlag);
                        orderSummary.ErrorFlag = errorFlag;
                        targetOrderPrice = securityPrice.BidPrc;
                    }
                }
            }

            /////////////////////////////////////////////////////////////////////////////////////////
            /// Round Order price
            /////////////////////////////////////////////////////////////////////////////////////////
            try
            {
                if (orderSideInt == 1)
                    newOrderPrice = Math.Floor(targetOrderPrice.GetValueOrDefault() * 100.0) / 100.0;
                else if (orderSideInt == 2)
                    newOrderPrice = Math.Ceiling(targetOrderPrice.GetValueOrDefault() * 100.0) / 100.0;
                else
                    newOrderPrice = Math.Round(targetOrderPrice.GetValueOrDefault(), 2);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error rounding Order Price for Order Id: " +
                    orderSummary.Sym + "/" +
                    orderSummary.OrdId + "/" +
                    targetOrderPrice);
            }

            orderSummary.RITgtPr = targetOrderPrice;
            orderSummary.NewOrdPr = newOrderPrice;

            //calculate market price spread
            CalculateMarketPriceSpread(orderSummary, securityPrice, currentOrderPrice);
        }

        private void CalculateMarketPriceSpread(OrderSummary orderSummary, SecurityPrice securityPrice, double currentOrderPrice)
        {
            double spread = 0;
            try
            {
                double marketPrice = securityPrice.BidPrc.GetValueOrDefault(); // default
                if (orderSummary.MktPrFld.Equals("Bid", StringComparison.CurrentCultureIgnoreCase))
                    marketPrice = securityPrice.BidPrc.GetValueOrDefault();
                else if (orderSummary.MktPrFld.Equals("Ask", StringComparison.CurrentCultureIgnoreCase))
                    marketPrice = securityPrice.AskPrc.GetValueOrDefault();

                //TODO: Added for testing (as Bid and Ask prices are not generally available during off market hours)
                if (_configuration["ConnectionStrings:ENV"].Equals("DEV"))
                    marketPrice = securityPrice.LastPrc.GetValueOrDefault();

                if (marketPrice > 0 && currentOrderPrice > 0)
                    spread = (marketPrice / currentOrderPrice) - 1.0;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error calculating Market Price Spread for Order: " +
                    orderSummary.Sym + "/" +
                    orderSummary.OrdId + "/" +
                    orderSummary.RITgtPr + "/" +
                    orderSummary.NewOrdPr);
            }

            orderSummary.MktPrSprd = spread;

            if (orderSummary.EstNav.HasValue)
                orderSummary.DscntToLivePr = DataConversionUtils.CalculateReturn(orderSummary.NewOrdPr, orderSummary.EstNav);

            if (orderSummary.IsQueueOrd == 1)
            {
                double lastPrice = securityPrice.LastPrc.GetValueOrDefault();
                if (lastPrice > 0 && currentOrderPrice > 0)
                    orderSummary.LastPrSprd = (lastPrice / currentOrderPrice) - 1.0;

                double bidPrice = securityPrice.BidPrc.GetValueOrDefault();
                if (bidPrice > 0 && currentOrderPrice > 0)
                    orderSummary.BidPrSprd = (bidPrice / currentOrderPrice) - 1.0;

                double askPrice = securityPrice.AskPrc.GetValueOrDefault();
                if (askPrice > 0 && currentOrderPrice > 0)
                    orderSummary.AskPrSprd = (askPrice / currentOrderPrice) - 1.0;
            }
        }
    }
}