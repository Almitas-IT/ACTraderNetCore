using aCommons;
using aCommons.Trading;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;

namespace aACTrader.Operations.Impl.BatchOrders
{
    public class EMSXRefIndexOrder
    {
        private readonly ILogger<EMSXRefIndexOrder> _logger;
        private readonly IConfiguration _configuration;
        private readonly EMSXOrderValidator _emsxOrderValidator;

        protected static readonly double DEFAULT_PRICE_CAP = 0.03;

        public EMSXRefIndexOrder(ILogger<EMSXRefIndexOrder> logger
            , IConfiguration configuration
            , EMSXOrderValidator orderValidator)
        {
            this._logger = logger;
            this._configuration = configuration;
            this._emsxOrderValidator = orderValidator;
            _logger.LogInformation("Initializing EMSXRefIndexOrder...");
        }

        /// <summary>
        /// Calculates Order price based on change in Reference Index price
        /// Beta Order
        /// <summary>
        /// <param name="routeStatus"></param>
        /// <param name="livePrice"></param>
        /// <param name="previousTargetPrice"></param>
        /// <param name="securityPrice"></param>
        public void ProcessRefIndexBetaOrder(EMSXRouteStatus routeStatus
            , double? livePrice, double? previousTargetPrice, SecurityPrice securityPrice)
        {
            //Calculate price change of (Ref Index) since order submission
            routeStatus.RILivePr = livePrice;
            routeStatus.RIPrChng = (livePrice.GetValueOrDefault() / routeStatus.RILastPr.GetValueOrDefault()) - 1.0;

            //1 = Price Move Up (Ref Index), -1 = Price Move Down (Ref Index)
            int priceMoveType = (routeStatus.RIPrChng.GetValueOrDefault() > 0) ? 1 : -1;

            //Beta adjustment to be applied to Ref Index Price Move
            //0 = Apply for both Up and Down Price Move, 1 = Price Up Move and -1 = Price Down Move
            int betaShiftInd = routeStatus.RIPrBetaShiftInd.GetValueOrDefault();
            int priceCapShiftInd = routeStatus.RIPrCapShiftInd.GetValueOrDefault();

            //if Beta = 0, then set it to 1, this will move the Price by % move of Ref Entity's Price
            double beta = (routeStatus.RIBeta.GetValueOrDefault() == 0) ? 1.0 : routeStatus.RIBeta.GetValueOrDefault();
            double priceCap = (routeStatus.RIPrCap.GetValueOrDefault() == 0) ? DEFAULT_PRICE_CAP : routeStatus.RIPrCap.GetValueOrDefault();

            //If Beta adjustment has to be applied to both Up and Down Price move of the Ref Entity
            double priceAdj = routeStatus.RIPrChng.GetValueOrDefault();
            //Up and Down Move (Both)
            if (betaShiftInd == 0)
                priceAdj = routeStatus.RIPrChng.GetValueOrDefault() * beta;
            //Up Move
            else if (betaShiftInd > 0 && priceMoveType > 0)
                priceAdj = routeStatus.RIPrChng.GetValueOrDefault() * beta;
            //Down Move
            else if (betaShiftInd < 0 && priceMoveType < 0)
                priceAdj = routeStatus.RIPrChng.GetValueOrDefault() * beta;

            //Apply Price Cap
            //If Price Cap has to be applied to both Up and Down Price move of the Ref Entity
            //Up Move
            double finalPriceAdj = priceAdj;
            if (priceCapShiftInd >= 0 && priceMoveType > 0)
                finalPriceAdj = Math.Min(priceCap, priceAdj);
            //Down Move
            else if (priceCapShiftInd <= 0 && priceMoveType < 0)
                finalPriceAdj = Math.Max(-1.0 * priceCap, priceAdj);

            routeStatus.RIPrChngFinal = finalPriceAdj;
            routeStatus.RITgtPr = previousTargetPrice.GetValueOrDefault() * (1.0 + finalPriceAdj);

            //Adjust Final Order Price
            _emsxOrderValidator.CalculateFinalTargetPrice(routeStatus, securityPrice);
        }

        /// <summary>
        /// Calculates Order price based on change in Reference Index/Underlying Security price
        /// Delta Order
        /// </summary>
        /// <param name="routeStatus"></param>
        /// <param name="livePrice"></param>
        /// <param name="previousTargetPrice"></param>
        /// <param name="securityPrice"></param>
        public void ProcessRefIndexDeltaOrder(EMSXRouteStatus routeStatus
            , double? livePrice, double? previousTargetPrice, SecurityPrice securityPrice)
        {
            //Live Price
            routeStatus.RILivePr = livePrice;

            //if Delta = 0, then set it to 1, this will move the Price by % move of Ref Entity's Price
            double delta = (routeStatus.RIBeta.GetValueOrDefault() == 0) ? 1.0 : routeStatus.RIBeta.GetValueOrDefault();
            double priceCap = (routeStatus.RIPrCap.GetValueOrDefault() == 0) ? DEFAULT_PRICE_CAP : routeStatus.RIPrCap.GetValueOrDefault();

            //Calculate Price Change
            routeStatus.RIPrChng = ((livePrice.GetValueOrDefault() * delta) / (routeStatus.RILastPr.GetValueOrDefault() * delta)) - 1.0;

            //1 = Price Move Up (Ref Index), -1 = Price Move Down (Ref Entity)
            int priceMoveType = (routeStatus.RIPrChng.GetValueOrDefault() > 0) ? 1 : -1;

            //0 = Apply for both Up and Down Price Move, 1 = Price Up Move and -1 = Price Down Move
            int priceCapShiftInd = routeStatus.RIPrCapShiftInd.GetValueOrDefault();

            //Apply Price Cap
            //if Price Cap has to be applied to both Up and Down Price move of the Ref Entity
            double priceAdj = routeStatus.RIPrChng.GetValueOrDefault();
            double finalPriceAdj = priceAdj;

            //Up Move
            if (priceCapShiftInd >= 0 && priceMoveType > 0)
                finalPriceAdj = Math.Min(priceCap, priceAdj);
            //Down Move
            else if (priceCapShiftInd <= 0 && priceMoveType < 0)
                finalPriceAdj = Math.Max(-1.0 * priceCap, priceAdj);

            //Final Price Change (after applying Price Cap)
            routeStatus.RIPrChngFinal = finalPriceAdj;
            routeStatus.RITgtPr = previousTargetPrice.GetValueOrDefault() * (1.0 + finalPriceAdj);

            //Adjust Final Order Price
            _emsxOrderValidator.CalculateFinalTargetPrice(routeStatus, securityPrice);
        }

        /// <summary>
        /// Calculates Order price based on change in Reference Index/Underlying Security price
        /// Absolute Order
        /// </summary>
        /// <param name="routeStatus"></param>
        /// <param name="livePrice"></param>
        /// <param name="previousTargetPrice"></param>
        /// <param name="securityPrice"></param>
        public void ProcessRefIndexAbsOrder(EMSXRouteStatus routeStatus
            , double? livePrice, double? previousTargetPrice, SecurityPrice securityPrice)
        {
            //Live Price
            routeStatus.RILivePr = livePrice;

            //Price Diff (Abs)
            double priceDifference = routeStatus.RIBeta.GetValueOrDefault();

            //Calculate Price Change
            routeStatus.RIPrChng = ((livePrice.GetValueOrDefault() + priceDifference)
                / (routeStatus.RILastPr.GetValueOrDefault() + priceDifference)) - 1.0;

            //Apply Price Cap
            //If Price Cap has to be applied to both Up and Down Price move of the Ref Entity
            double priceAdj = routeStatus.RIPrChng.GetValueOrDefault();
            double finalPriceAdj = priceAdj;
            if (routeStatus.RIPrCap.HasValue)
            {
                //Price Cap
                double priceCap = (routeStatus.RIPrCap.GetValueOrDefault() == 0) ? DEFAULT_PRICE_CAP : routeStatus.RIPrCap.GetValueOrDefault();

                //1 = Price Move Up (Ref Index), -1 = Price Move Down (Ref Entity)
                int priceMoveType = (routeStatus.RIPrChng.GetValueOrDefault() > 0) ? 1 : -1;

                //0 = Apply for both Up and Down Price Move, 1 = Price Up Move and -1 = Price Down Move
                int priceCapShiftInd = routeStatus.RIPrCapShiftInd.GetValueOrDefault();

                //Up Move
                if (priceCapShiftInd >= 0 && priceMoveType > 0)
                    finalPriceAdj = Math.Min(priceCap, priceAdj);
                //Down Move
                else if (priceCapShiftInd <= 0 && priceMoveType < 0)
                    finalPriceAdj = Math.Max(-1.0 * priceCap, priceAdj);
            }

            //Final Price Change (after applying Price Cap)
            routeStatus.RIPrChngFinal = finalPriceAdj;
            routeStatus.RITgtPr = livePrice.GetValueOrDefault() + priceDifference;

            //Adjust Final Order Price
            _emsxOrderValidator.CalculateFinalTargetPrice(routeStatus, securityPrice);
        }
    }
}
