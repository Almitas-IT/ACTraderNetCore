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
    public class EMSXDiscountTargetOrder
    {
        private readonly ILogger<EMSXDiscountTargetOrder> _logger;
        private readonly IConfiguration _configuration;
        private readonly EMSXOrderValidator _emsxOrderValidator;
        private readonly Random RandomNumberGenerator = new Random();

        protected static readonly double DEFAULT_PRICE_CAP = 0.03;

        public EMSXDiscountTargetOrder(ILogger<EMSXDiscountTargetOrder> logger, IConfiguration configuration, EMSXOrderValidator orderValidator)
        {
            this._logger = logger;
            this._configuration = configuration;
            this._emsxOrderValidator = orderValidator;
            _logger.LogInformation("Initializing EMSXDiscountTargetOrder...");
        }

        /// <summary>
        /// Calculates target order price based of discount target and estimated nav
        /// </summary>
        /// <param name="routeStatus"></param>
        public void ProcessDiscountOrder(EMSXRouteStatus routeStatus,
            IDictionary<string, FundForecast> fundForecastDict, double? previousTargetPrice, SecurityPrice securityPrice)
        {
            try
            {
                string symbol = routeStatus.Ticker;
                FundForecast fundForecast = FundForecastOperations.GetFundForecast(symbol, fundForecastDict);
                if (fundForecast != null)
                {
                    double? nav = GetEstimatedNav(routeStatus.EstNavTyp, fundForecast);
                    if (nav.HasValue)
                    {
                        /////////////////////TODO: Added for TESTING only
                        if (_configuration["ConnectionStrings:ENV"].Equals("DEV") && _configuration["ConnectionStrings:EMSX_UAT_ON"].Equals("Y"))
                            nav *= (1.0 + RandomNumberGenerator.NextDouble() / 10.0);

                        //Price Cap and Price Shift Indicator
                        int priceCapShiftInd = routeStatus.RIPrCapShiftInd.GetValueOrDefault();
                        double priceCap = (routeStatus.RIPrCap.GetValueOrDefault() == 0) ? DEFAULT_PRICE_CAP : routeStatus.RIPrCap.GetValueOrDefault();

                        //New Price (based on Estimated Nav and Discount Target)
                        double discountTarget = routeStatus.DscntTgt.GetValueOrDefault() + routeStatus.DscntTgtAdj.GetValueOrDefault();
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

                        routeStatus.EstNav = nav;
                        routeStatus.RIPrChng = priceChange;
                        routeStatus.RIPrChngFinal = finalPriceAdj;
                        routeStatus.RITgtPr = previousTargetPrice.GetValueOrDefault() * (1.0 + finalPriceAdj);

                        //Adjust Final Order Price
                        _emsxOrderValidator.CalculateFinalTargetPrice(routeStatus, securityPrice);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error calculating Discount Change for EMSX Order: "
                    + routeStatus.Ticker + "/"
                    + routeStatus.OrdSeq + "/"
                    + routeStatus.RouteId + "/"
                    + routeStatus.OrdRefId);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="estNavType"></param>
        /// <param name="fundForecast"></param>
        /// <returns></returns>
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