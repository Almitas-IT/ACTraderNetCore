using aCommons;
using aCommons.Trading;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;

namespace aACTrader.Operations.Impl.BatchOrders
{
    public class RefIndexOrder
    {
        private readonly ILogger<RefIndexOrder> _logger;
        private readonly IConfiguration _configuration;
        private readonly OrderValidator _orderValidator;

        protected static readonly double DEFAULT_PRICE_CAP = 0.03;

        public RefIndexOrder(ILogger<RefIndexOrder> logger
            , IConfiguration configuration
            , OrderValidator orderValidator)
        {
            this._logger = logger;
            this._configuration = configuration;
            this._orderValidator = orderValidator;
            _logger.LogInformation("Initializing RefIndexOrder...");
        }

        /// <summary>
        /// Calculates Order price based on change in Reference Index price
        /// Beta Order
        /// <summary>
        /// <param name="orderSummary"></param>
        /// <param name="livePrice"></param>
        /// <param name="previousTargetPrice"></param>
        /// <param name="securityPrice"></param>
        public void ProcessRefIndexBetaOrder(OrderSummary orderSummary
            , double? livePrice, double? previousTargetPrice, SecurityPrice securityPrice)
        {
            //Calculate price change of (Ref Index) since order submission
            orderSummary.RILivePr = livePrice;
            orderSummary.RIPrChng = (livePrice.GetValueOrDefault() / orderSummary.RILastPr.GetValueOrDefault()) - 1.0;

            //1 = Price Move Up (Ref Index), -1 = Price Move Down (Ref Index)
            int priceMoveType = (orderSummary.RIPrChng.GetValueOrDefault() > 0) ? 1 : -1;

            //Beta adjustment to be applied to Ref Index Price Move
            //0 = Apply for both Up and Down Price Move, 1 = Price Up Move and -1 = Price Down Move
            int betaShiftInd = orderSummary.RIPrBetaShiftInd.GetValueOrDefault();
            int priceCapShiftInd = orderSummary.RIPrCapShiftInd.GetValueOrDefault();

            //if Beta = 0, then set it to 1, this will move the Price by % move of Ref Entity's Price
            double beta = (orderSummary.RIBeta.GetValueOrDefault() == 0) ? 1.0 : orderSummary.RIBeta.GetValueOrDefault();
            double priceCap = (orderSummary.RIPrCap.GetValueOrDefault() == 0) ? DEFAULT_PRICE_CAP : orderSummary.RIPrCap.GetValueOrDefault();

            //If Beta adjustment has to be applied to both Up and Down Price move of the Ref Entity
            double priceAdj = orderSummary.RIPrChng.GetValueOrDefault();
            //Up and Down Move (Both)
            if (betaShiftInd == 0)
                priceAdj = orderSummary.RIPrChng.GetValueOrDefault() * beta;
            //Up Move
            else if (betaShiftInd > 0 && priceMoveType > 0)
                priceAdj = orderSummary.RIPrChng.GetValueOrDefault() * beta;
            //Down Move
            else if (betaShiftInd < 0 && priceMoveType < 0)
                priceAdj = orderSummary.RIPrChng.GetValueOrDefault() * beta;

            //Apply Price Cap
            //If Price Cap has to be applied to both Up and Down Price move of the Ref Entity
            //Up Move
            double finalPriceAdj = priceAdj;
            if (priceCapShiftInd >= 0 && priceMoveType > 0)
                finalPriceAdj = Math.Min(priceCap, priceAdj);
            //Down Move
            else if (priceCapShiftInd <= 0 && priceMoveType < 0)
                finalPriceAdj = Math.Max(-1.0 * priceCap, priceAdj);

            orderSummary.RIPrChngFinal = finalPriceAdj;
            orderSummary.RITgtPr = previousTargetPrice.GetValueOrDefault() * (1.0 + finalPriceAdj);

            //Adjust Final Order Price
            _orderValidator.CalculateFinalTargetPrice(orderSummary, securityPrice);
        }

        /// <summary>
        /// Calculates Order price based on change in Reference Index/Underlying Security price
        /// Delta Order
        /// </summary>
        /// <param name="orderSummary"></param>
        /// <param name="livePrice"></param>
        /// <param name="previousTargetPrice"></param>
        /// <param name="securityPrice"></param>
        public void ProcessRefIndexDeltaOrder(OrderSummary orderSummary
            , double? livePrice, double? previousTargetPrice, SecurityPrice securityPrice)
        {
            //Live Price
            orderSummary.RILivePr = livePrice;

            //if Delta = 0, then set it to 1, this will move the Price by % move of Ref Entity's Price
            double delta = (orderSummary.RIBeta.GetValueOrDefault() == 0) ? 1.0 : orderSummary.RIBeta.GetValueOrDefault();
            double priceCap = (orderSummary.RIPrCap.GetValueOrDefault() == 0) ? DEFAULT_PRICE_CAP : orderSummary.RIPrCap.GetValueOrDefault();

            //Calculate Price Change
            orderSummary.RIPrChng = ((livePrice.GetValueOrDefault() * delta) / (orderSummary.RILastPr.GetValueOrDefault() * delta)) - 1.0;

            //1 = Price Move Up (Ref Index), -1 = Price Move Down (Ref Entity)
            int priceMoveType = (orderSummary.RIPrChng.GetValueOrDefault() > 0) ? 1 : -1;

            //0 = Apply for both Up and Down Price Move, 1 = Price Up Move and -1 = Price Down Move
            int priceCapShiftInd = orderSummary.RIPrCapShiftInd.GetValueOrDefault();

            //Apply Price Cap
            //if Price Cap has to be applied to both Up and Down Price move of the Ref Entity
            double priceAdj = orderSummary.RIPrChng.GetValueOrDefault();
            double finalPriceAdj = priceAdj;

            //Up Move
            if (priceCapShiftInd >= 0 && priceMoveType > 0)
                finalPriceAdj = Math.Min(priceCap, priceAdj);
            //Down Move
            else if (priceCapShiftInd <= 0 && priceMoveType < 0)
                finalPriceAdj = Math.Max(-1.0 * priceCap, priceAdj);

            //Final Price Change (after applying Price Cap)
            orderSummary.RIPrChngFinal = finalPriceAdj;
            orderSummary.RITgtPr = previousTargetPrice.GetValueOrDefault() * (1.0 + finalPriceAdj);

            //Adjust Final Order Price
            _orderValidator.CalculateFinalTargetPrice(orderSummary, securityPrice);
        }

        /// <summary>
        /// Calculates Order price based on change in Reference Index/Underlying Security price
        /// Absolute Order
        /// </summary>
        /// <param name="orderSummary"></param>
        /// <param name="livePrice"></param>
        /// <param name="previousTargetPrice"></param>
        /// <param name="securityPrice"></param>
        public void ProcessRefIndexAbsOrder(OrderSummary orderSummary
            , double? livePrice, double? previousTargetPrice, SecurityPrice securityPrice)
        {
            //Live Price
            orderSummary.RILivePr = livePrice;

            //Price Diff (Abs)
            double priceDifference = orderSummary.RIBeta.GetValueOrDefault();

            //Calculate Price Change
            orderSummary.RIPrChng = ((livePrice.GetValueOrDefault() + priceDifference)
                / (orderSummary.RILastPr.GetValueOrDefault() + priceDifference)) - 1.0;

            //Apply Price Cap
            //If Price Cap has to be applied to both Up and Down Price move of the Ref Entity
            double priceAdj = orderSummary.RIPrChng.GetValueOrDefault();
            double finalPriceAdj = priceAdj;
            if (orderSummary.RIPrCap.HasValue)
            {
                //Price Cap
                double priceCap = (orderSummary.RIPrCap.GetValueOrDefault() == 0) ? DEFAULT_PRICE_CAP : orderSummary.RIPrCap.GetValueOrDefault();

                //1 = Price Move Up (Ref Index), -1 = Price Move Down (Ref Entity)
                int priceMoveType = (orderSummary.RIPrChng.GetValueOrDefault() > 0) ? 1 : -1;

                //0 = Apply for both Up and Down Price Move, 1 = Price Up Move and -1 = Price Down Move
                int priceCapShiftInd = orderSummary.RIPrCapShiftInd.GetValueOrDefault();

                //Up Move
                if (priceCapShiftInd >= 0 && priceMoveType > 0)
                    finalPriceAdj = Math.Min(priceCap, priceAdj);
                //Down Move
                else if (priceCapShiftInd <= 0 && priceMoveType < 0)
                    finalPriceAdj = Math.Max(-1.0 * priceCap, priceAdj);
            }

            //Final Price Change (after applying Price Cap)
            orderSummary.RIPrChngFinal = finalPriceAdj;
            orderSummary.RITgtPr = livePrice.GetValueOrDefault() + priceDifference;

            //Adjust Final Order Price
            _orderValidator.CalculateFinalTargetPrice(orderSummary, securityPrice);
        }
    }
}
