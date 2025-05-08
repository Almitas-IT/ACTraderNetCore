using aCommons;
using aCommons.Trading;
using aCommons.Utils;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;

namespace aACTrader.Operations.Impl.BatchOrders
{
    public class EMSXOrderValidator
    {
        private readonly ILogger<EMSXOrderValidator> _logger;
        private readonly IConfiguration _configuration;

        protected static readonly double DEFAULT_MARKET_PRICE_THRESHOLD = 0.005;
        protected static readonly double DISCOUNT_DEFAULT_MARKET_PRICE_THRESHOLD = 0.010;

        public EMSXOrderValidator(ILogger<EMSXOrderValidator> logger, IConfiguration configuration)
        {
            this._logger = logger;
            this._configuration = configuration;
            _logger.LogInformation("Initializing EMSXOrderValidator...");
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="routeStatus"></param>
        /// <param name="securityPrice"></param>
        /// <returns></returns>
        public void CalculateFinalTargetPrice(EMSXRouteStatus routeStatus, SecurityPrice securityPrice)
        {
            double? targetOrderPrice = routeStatus.RITgtPr;
            double newOrderPrice = targetOrderPrice.GetValueOrDefault();
            double currentOrderPrice = routeStatus.RoutePrc.GetValueOrDefault();
            string orderSide = routeStatus.OrdSide;
            int orderSideInt = 0;

            //order side
            if (orderSide.Equals("BUY", StringComparison.CurrentCultureIgnoreCase)
                || orderSide.Equals("Buy Cover", StringComparison.CurrentCultureIgnoreCase))
                orderSideInt = 1;
            else if (orderSide.Equals("SELL", StringComparison.CurrentCultureIgnoreCase)
                || orderSide.Equals("SHRT", StringComparison.CurrentCultureIgnoreCase))
                orderSideInt = 2;

            /////////////////////////////////////////////////////////////////////////////////////////
            /// Max/Min Price check
            /////////////////////////////////////////////////////////////////////////////////////////
            //apply max/min order price check
            if (routeStatus.RIMaxPr.HasValue)
            {
                if (orderSideInt == 1)
                {
                    //max price to buy
                    if (targetOrderPrice.GetValueOrDefault() > routeStatus.RIMaxPr.GetValueOrDefault())
                        targetOrderPrice = routeStatus.RIMaxPr;
                }
                else if (orderSideInt == 2)
                {
                    //min price to sell
                    if (targetOrderPrice.GetValueOrDefault() < routeStatus.RIMaxPr.GetValueOrDefault())
                        targetOrderPrice = routeStatus.RIMaxPr;
                }
            }

            /////////////////////////////////////////////////////////////////////////////////////////
            /// Bid/Offer Price check
            /////////////////////////////////////////////////////////////////////////////////////////
            //check target price against live security price
            if (securityPrice != null)
            {
                routeStatus.LastPr = securityPrice.LastPrc;
                routeStatus.BidPr = securityPrice.BidPrc;
                routeStatus.AskPr = securityPrice.AskPrc;

                if (orderSideInt == 1)
                {
                    double askPrice = securityPrice.AskPrc.GetValueOrDefault();
                    if (_configuration["ConnectionStrings:ENV"].Equals("DEV") && _configuration["ConnectionStrings:EMSX_UAT_ON"].Equals("Y"))
                        askPrice = securityPrice.LastPrc.GetValueOrDefault();

                    if (targetOrderPrice.GetValueOrDefault() > askPrice)
                    {
                        _logger.LogInformation("Target Price > Ask Price: " +
                            routeStatus.Ticker + "/" +
                            routeStatus.OrdSeq + "/" +
                            routeStatus.RouteId + "/" +
                            routeStatus.OrdRefId + "/" +
                            askPrice + "/" +
                            targetOrderPrice);

                        targetOrderPrice = askPrice;
                    }
                }
                else if (orderSideInt == 2)
                {
                    double bidPrice = securityPrice.BidPrc.GetValueOrDefault();
                    if (_configuration["ConnectionStrings:ENV"].Equals("DEV") && _configuration["ConnectionStrings:EMSX_UAT_ON"].Equals("Y"))
                        bidPrice = securityPrice.LastPrc.GetValueOrDefault();

                    if (targetOrderPrice.GetValueOrDefault() < bidPrice)
                    {
                        _logger.LogInformation("Target Price < Bid Price: " +
                            routeStatus.Ticker + "/" +
                            routeStatus.OrdSeq + "/" +
                            routeStatus.RouteId + "/" +
                            routeStatus.OrdRefId + "/" +
                            bidPrice + "/" +
                            targetOrderPrice);

                        targetOrderPrice = bidPrice;
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
                    routeStatus.Ticker + "/" +
                    routeStatus.OrdSeq + "/" +
                    routeStatus.RouteId + "/" +
                    routeStatus.OrdRefId + "/" +
                    targetOrderPrice);
            }

            routeStatus.RITgtPr = targetOrderPrice;
            routeStatus.NewOrdPr = newOrderPrice;

            //calculate market price spread
            CalculateMarketPriceSpread(routeStatus, securityPrice, currentOrderPrice);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="routeStatus"></param>
        /// <param name="securityPrice"></param>
        /// <param name="currentOrderPrice"></param>
        private void CalculateMarketPriceSpread(EMSXRouteStatus routeStatus, SecurityPrice securityPrice, double currentOrderPrice)
        {
            double spread = 0;
            try
            {
                double marketPrice = securityPrice.BidPrc.GetValueOrDefault(); // default
                if (routeStatus.MktPrFld.Equals("Bid", StringComparison.CurrentCultureIgnoreCase))
                    marketPrice = securityPrice.BidPrc.GetValueOrDefault();
                else if (routeStatus.MktPrFld.Equals("Ask", StringComparison.CurrentCultureIgnoreCase))
                    marketPrice = securityPrice.AskPrc.GetValueOrDefault();

                //TODO: Added for testing (as Bid and Ask prices are not generally available during off market hours)
                if (_configuration["ConnectionStrings:ENV"].Equals("DEV") && _configuration["ConnectionStrings:EMSX_UAT_ON"].Equals("Y"))
                    marketPrice = securityPrice.LastPrc.GetValueOrDefault();

                if (marketPrice > 0 && currentOrderPrice > 0)
                    spread = (marketPrice / currentOrderPrice) - 1.0;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error calculating Market Price Spread for Order: " +
                    routeStatus.Ticker + "/" +
                    routeStatus.OrdSeq + "/" +
                    routeStatus.RouteId + "/" +
                    routeStatus.OrdRefId + "/" +
                    routeStatus.RITgtPr + "/" +
                    routeStatus.NewOrdPr);
            }

            routeStatus.MktPrSprd = spread;
            if (routeStatus.EstNav.HasValue)
                routeStatus.DscntToLivePr = DataConversionUtils.CalculateReturn(routeStatus.NewOrdPr, routeStatus.EstNav);
        }
    }
}