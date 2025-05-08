using aCommons;
using aCommons.Cef;
using aCommons.Trading;
using aCommons.Utils;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;

namespace aACTrader.Operations.Impl.BatchOrders
{
    public class DiscountTargetOrder
    {
        private readonly ILogger<DiscountTargetOrder> _logger;
        private readonly IConfiguration _configuration;
        private readonly OrderValidator _orderValidator;
        private readonly Random RandomNumberGenerator = new Random();

        protected static readonly double DEFAULT_PRICE_CAP = 0.03;

        public DiscountTargetOrder(ILogger<DiscountTargetOrder> logger
            , IConfiguration configuration
            , OrderValidator orderValidator)
        {
            this._logger = logger;
            this._configuration = configuration;
            this._orderValidator = orderValidator;
            _logger.LogInformation("Initializing DiscountTargetOrder...");
        }

        /// <summary>
        /// Calculates target order price based of discount target and estimated nav
        /// </summary>
        /// <param name="orderSummary"></param>
        public void ProcessDiscountOrder(OrderSummary orderSummary,
            IDictionary<string, FundForecast> fundForecastDict, double? previousTargetPrice, SecurityPrice securityPrice)
        {
            try
            {
                string symbol = !string.IsNullOrEmpty(orderSummary.ALMSym) ? orderSummary.ALMSym : orderSummary.BBGSym;
                FundForecast fundForecast = FundForecastOperations.GetFundForecast(symbol, fundForecastDict);
                if (fundForecast != null)
                {
                    // double? nav = fundForecast.EstNav;
                    double? nav = GetEstimatedNav(orderSummary.EstNavTyp, fundForecast);
                    if (nav.HasValue)
                    {
                        ///////////////////////TODO: Added for TESTING only
                        //if (_configuration["ConnectionStrings:ENV"].Equals("DEV"))
                        //{
                        //    nav *= (1.0 + RandomNumberGenerator.NextDouble() / 10.0);
                        //}

                        //Price Cap and Price Shift Indicator
                        int priceCapShiftInd = orderSummary.RIPrCapShiftInd.GetValueOrDefault();
                        double priceCap = (orderSummary.RIPrCap.GetValueOrDefault() == 0) ? DEFAULT_PRICE_CAP : orderSummary.RIPrCap.GetValueOrDefault();

                        //New Price (based on Estimated Nav and Discount Target)
                        double discountTarget = orderSummary.DscntTgt.GetValueOrDefault() + orderSummary.DscntTgtAdj.GetValueOrDefault();
                        double newPrice = (1.0 + discountTarget) * nav.GetValueOrDefault();
                        double priceChange = (newPrice / previousTargetPrice.GetValueOrDefault()) - 1.0;

                        //1 = Price Move Up, -1 = Price Move Down
                        int priceMoveType = (priceChange > 0) ? 1 : -1;

                        //Apply Price Cap
                        //If Price Cap has to be applied to both Up and Down Price move of the Ref Entity
                        //Up Move
                        double finalPriceAdj = priceChange;
                        if (priceCapShiftInd >= 0 && priceMoveType > 0)
                            finalPriceAdj = Math.Min(priceCap, priceChange);
                        //Down Move
                        else if (priceCapShiftInd <= 0 && priceMoveType < 0)
                            finalPriceAdj = Math.Max(-1.0 * priceCap, priceChange);

                        orderSummary.EstNav = nav;
                        orderSummary.RIPrChng = priceChange;
                        orderSummary.RIPrChngFinal = finalPriceAdj;
                        orderSummary.RITgtPr = previousTargetPrice.GetValueOrDefault() * (1.0 + finalPriceAdj);

                        //
                        if (orderSummary.IsQueueOrd == 1)
                        {
                            orderSummary.PubNav = fundForecast.LastDvdAdjNav;
                            orderSummary.DscntPub = fundForecast.LastPD;
                            orderSummary.DscntToLastPr = fundForecast.PDLastPrc;
                            orderSummary.DscntToBidPr = fundForecast.PDBidPrc;
                            orderSummary.DscntToAskPr = fundForecast.PDAskPrc;
                        }

                        //Adjust Final Order Price
                        _orderValidator.CalculateFinalTargetPrice(orderSummary, securityPrice);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error calculating Discount Change Order: "
                    + orderSummary.Sym + "/"
                    + orderSummary.MainOrdId + "/"
                    + orderSummary.OrdId);
            }
        }

        public double? GetEstimatedNav(string estNavType, FundForecast fundForecast)
        {
            double? estimatedNav = null;
            try
            {
                estimatedNav = fundForecast.EstNav;
                if (!string.IsNullOrEmpty(estNavType))
                {
                    switch (estNavType)
                    {
                        case string s when s.Equals("Holdings", StringComparison.InvariantCultureIgnoreCase):
                            if (fundForecast.PortNav.HasValue)
                                estimatedNav = fundForecast.PortNav;
                            break;
                        case string s when s.Equals("ETFReg", StringComparison.InvariantCultureIgnoreCase):
                            if (fundForecast.ETFNav.HasValue)
                                estimatedNav = fundForecast.ETFNav;
                            break;
                        case string s when s.Equals("ETF Reg", StringComparison.InvariantCultureIgnoreCase):
                            if (fundForecast.ETFNav.HasValue)
                                estimatedNav = fundForecast.ETFNav;
                            break;
                        case string s when s.Equals("Proxy", StringComparison.InvariantCultureIgnoreCase):
                            if (fundForecast.ProxyNav.HasValue)
                                estimatedNav = fundForecast.ProxyNav;
                            break;
                        case string s when s.Equals("AltProxy", StringComparison.InvariantCultureIgnoreCase):
                            if (fundForecast.AltProxyNav.HasValue)
                                estimatedNav = fundForecast.AltProxyNav;
                            break;
                        case string s when s.Equals("Published", StringComparison.InvariantCultureIgnoreCase):
                            if (fundForecast.LastDvdAdjNav.HasValue)
                                estimatedNav = CommonUtils.AddNullableDoubles(fundForecast.LastDvdAdjNav, fundForecast.AI);
                            break;
                    }
                }
            }
            catch (Exception)
            {
            }
            return estimatedNav;
        }
    }
}
