using aCommons;
using aCommons.Cef;
using aCommons.Utils;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;

namespace aACTrader.Operations.Impl
{
    public class ExpectedAlphaCalculatorNew
    {
        private readonly ILogger<ExpectedAlphaCalculatorNew> _logger;

        public ExpectedAlphaCalculatorNew(ILogger<ExpectedAlphaCalculatorNew> logger)
        {
            _logger = logger;
        }

        public void CalculateExpectedAlpha(string ticker
            , FundMaster fundMaster
            , FundForecast fundForecast
            , IDictionary<string, FundGroupDiscountStats> histDiscountStats
            , IDictionary<string, FundSupplementalData> fundSupplementalDataDict)
        {
            if (fundMaster.CalcExpAlpha == 1)
            {
                if ("United States".Equals(fundMaster.Cntry, StringComparison.CurrentCultureIgnoreCase))
                {
                    double? estimatedNav = FundForecastOperations.GetEstimatedNav(fundForecast);
                    double? currentPrice = fundForecast.LastPrc;
                    double? currentDiscount = fundForecast.PDLastPrc;

                    //Use Nav and Discount Post Rights Offer (if the flag is set)
                    if (fundForecast.RODscntDispFlag == 1)
                    {
                        if (fundForecast.RONav.HasValue)
                            estimatedNav = fundForecast.RONav;
                        if (fundForecast.ROPDLastPrc.HasValue)
                            currentDiscount = fundForecast.ROPDLastPrc;
                    }

                    if (estimatedNav.HasValue
                        && currentDiscount.HasValue
                        && !string.IsNullOrEmpty(fundForecast.FundDscntGrp))
                    {
                        fundForecast.EADscntRtn = null;
                        fundForecast.EALTAvgSectorDscnt = null;
                        fundForecast.EADScoreMvmt = null;
                        fundForecast.EAAScore = null;
                        fundForecast.EABoardVoting = null;
                        fundForecast.EABoardTerm = null;
                        fundForecast.EADscntChng = null;
                        fundForecast.EAExpDscnt = null;
                        fundForecast.EADvdFcst = null;
                        fundForecast.EADvdFcstYld = null;
                        fundForecast.EADvdRtn = null;
                        fundForecast.EAExpDrag = null;
                        fundForecast.EANavAccrShareBB = null;
                        fundForecast.EAAlpha = null;
                        fundForecast.EAFinalAlpha = null;
                        fundForecast.EAFinalAlphaWithShortRebate = null;

                        //Discount Convergence To Long Term Sector Average Discount
                        fundForecast.EALTAvgSectorDscnt = CalculateDiscountConvergenceToSectorAvg(
                            ticker
                            , fundForecast
                            , histDiscountStats
                            , currentDiscount);

                        //Discount Movement Related to D-Scores
                        fundForecast.EADScoreMvmt = CalculateDScoreMovement(ticker, fundForecast);

                        //Activist Score
                        double? rawActivistScore = fundForecast.RawAScore;
                        if (rawActivistScore.HasValue
                            && fundForecast.AScoreMult.HasValue)
                            rawActivistScore *= (fundForecast.AScoreMult / 100.0);
                        fundForecast.EAAScore = rawActivistScore;

                        //Board Vote
                        if (!string.IsNullOrEmpty(fundMaster.BoardVotType))
                        {
                            double? boardVotingTypeAdj = null;
                            if (fundMaster.BoardVotType.StartsWith("Majo", StringComparison.CurrentCultureIgnoreCase))
                                boardVotingTypeAdj = Math.Min(currentDiscount.GetValueOrDefault() * fundForecast.MajVotHaircut.GetValueOrDefault(), fundForecast.MajVotHaircutMin.GetValueOrDefault());
                            fundForecast.EABoardVoting = boardVotingTypeAdj;
                        }

                        //Board Term
                        if (!string.IsNullOrEmpty(fundMaster.BoardVotPeriod))
                        {
                            double? boardTermAdj = null;
                            if (fundMaster.BoardVotPeriod.StartsWith("Ann", StringComparison.CurrentCultureIgnoreCase))
                                boardTermAdj = Math.Max(currentDiscount.GetValueOrDefault() * -1.0 * fundForecast.BTAdjMult.GetValueOrDefault(), fundForecast.BTAdjMin.GetValueOrDefault());
                            fundForecast.EABoardTerm = boardTermAdj;
                        }

                        //Discount Change
                        double?[] dVals = new double?[]{
                                    fundForecast.EALTAvgSectorDscnt
                                    , fundForecast.EADScoreMvmt
                                    , fundForecast.EAAScore
                                    , fundForecast.EABoardVoting
                                    , fundForecast.EABoardTerm };

                        fundForecast.EADscntChng = CommonUtils.AddNullableDoubles(dVals);

                        double? liquidityCost = fundForecast.LiqCost;
                        if (liquidityCost.HasValue)
                            estimatedNav *= (1 - liquidityCost.GetValueOrDefault() / 10000.0);

                        double? shareBuyback = fundForecast.ShareBB;

                        //Current Discount
                        currentDiscount = (currentPrice / estimatedNav) - 1.0;

                        //Expected Discount
                        double? expectedDiscount = CommonUtils.AddNullableDoubles(currentDiscount, fundForecast.EADscntChng);
                        if (!Double.IsInfinity(expectedDiscount.GetValueOrDefault()) && !Double.IsNaN(expectedDiscount.GetValueOrDefault()))
                            fundForecast.EAExpDscnt = expectedDiscount;

                        //Discount Return 
                        double? expectedPrice = estimatedNav * (1.0 + expectedDiscount);
                        double discountReturn = (expectedPrice.GetValueOrDefault() / currentPrice.GetValueOrDefault()) - 1.0;
                        if (!Double.IsInfinity(discountReturn) && !Double.IsNaN(discountReturn))
                            fundForecast.EADscntRtn = discountReturn;

                        //Dividend Return
                        double? forecastedDividends = 0;
                        if (fundForecast.DvdFcst.HasValue)
                            forecastedDividends = fundForecast.DvdFcst;
                        else if (fundMaster.DvdFcst12M.HasValue)
                            forecastedDividends = fundMaster.DvdFcst12M;

                        if (forecastedDividends.HasValue && forecastedDividends > 0)
                        {
                            fundForecast.EADvdFcst = forecastedDividends;

                            double newNav = estimatedNav.GetValueOrDefault() - forecastedDividends.GetValueOrDefault();
                            double newPrice = newNav * (1.0 + currentDiscount.GetValueOrDefault());
                            double dividendReturn = ((newPrice + forecastedDividends.GetValueOrDefault()) / currentPrice.GetValueOrDefault()) - 1.0;
                            if (!Double.IsInfinity(dividendReturn) && !Double.IsNaN(dividendReturn))
                                fundForecast.EADvdRtn = dividendReturn;

                            double dividendYield = forecastedDividends.GetValueOrDefault() / currentPrice.GetValueOrDefault();
                            if (!Double.IsInfinity(dividendYield) && !Double.IsNaN(dividendYield))
                                fundForecast.EADvdFcstYld = dividendYield;
                        }

                        //Expense Drag
                        double? expenseRatio = fundForecast.ExpRatio;
                        if (expenseRatio.HasValue)
                        {
                            double expenseDrag = (-1.0 * expenseRatio.GetValueOrDefault()) / 100.0;
                            if (fundForecast.ExpDragMult.HasValue)
                                expenseDrag *= fundForecast.ExpDragMult.GetValueOrDefault();

                            expenseDrag *= (1.0 + currentDiscount.GetValueOrDefault());
                            if (!Double.IsInfinity(expenseDrag) && !Double.IsNaN(expenseDrag))
                                fundForecast.EAExpDrag = expenseDrag;
                        }

                        //Share Buyback
                        if (shareBuyback.HasValue)
                        {
                            double navAccretionFromBuybacks = (estimatedNav.GetValueOrDefault() - shareBuyback.GetValueOrDefault() * currentPrice.GetValueOrDefault()) / (1 - shareBuyback.GetValueOrDefault());
                            navAccretionFromBuybacks = (navAccretionFromBuybacks / estimatedNav.GetValueOrDefault()) - 1.0;
                            if (!Double.IsInfinity(navAccretionFromBuybacks) && !Double.IsNaN(navAccretionFromBuybacks))
                                fundForecast.EANavAccrShareBB = navAccretionFromBuybacks;
                        }

                        //Expected Alpha
                        double expectedAlpha = fundForecast.EADscntRtn.GetValueOrDefault()
                            + fundForecast.EADvdRtn.GetValueOrDefault()
                            + fundForecast.EAExpDrag.GetValueOrDefault()
                            + fundForecast.EANavAccrShareBB.GetValueOrDefault();

                        if (!Double.IsInfinity(expectedAlpha) && !Double.IsNaN(expectedAlpha))
                            fundForecast.EAAlpha = expectedAlpha;

                        if (fundForecast.EAAlpha.HasValue && fundForecast.ExpAlphaAdjFactor.HasValue)
                            expectedAlpha += fundForecast.ExpAlphaAdjFactor.GetValueOrDefault();

                        //IRR (use Post Tender IRR or Normal IRR)
                        double? irrToLastPrice = null;
                        if (fundForecast.TOIRRLastPrc.HasValue)
                            irrToLastPrice = fundForecast.TOIRRLastPrc;
                        else if (fundForecast.IRRLastPrc.HasValue)
                            irrToLastPrice = fundForecast.IRRLastPrc;

                        //Expected Return - Max(Expected Alpha, IRR)
                        if (fundForecast.EAAlpha.HasValue && !irrToLastPrice.HasValue)
                            fundForecast.EAFinalAlpha = expectedAlpha;
                        else if (irrToLastPrice.HasValue && !fundForecast.EAAlpha.HasValue)
                            fundForecast.EAFinalAlpha = irrToLastPrice;
                        else if (fundForecast.EAAlpha.HasValue && irrToLastPrice.HasValue)
                            fundForecast.EAFinalAlpha = Math.Max(expectedAlpha, irrToLastPrice.GetValueOrDefault());

                        //Add Security Short Rebate Rate if Expected Alpha is -ve
                        if (fundForecast.EAFinalAlpha.HasValue && fundForecast.EAFinalAlpha.GetValueOrDefault() < 0 && fundSupplementalDataDict != null)
                        {
                            FundSupplementalData fundSupplementalData;
                            if (fundSupplementalDataDict.TryGetValue(ticker, out fundSupplementalData)
                                && fundSupplementalData.SecurityRebateRate != null)
                            {
                                double maxRebateRate = Math.Max(
                                    fundSupplementalData.SecurityRebateRate.FidelityRebateRate.GetValueOrDefault(),
                                    fundSupplementalData.SecurityRebateRate.JPMRebateRate.GetValueOrDefault());
                                maxRebateRate /= 100.0;

                                if (maxRebateRate != 0 && fundForecast.EAFinalAlpha.GetValueOrDefault() < maxRebateRate)
                                    fundForecast.EAFinalAlphaWithShortRebate = -1.0 * (fundForecast.EAFinalAlpha.GetValueOrDefault() - maxRebateRate);
                            }
                        }
                    }
                }
                else
                {
                    fundForecast.EAFinalAlpha = null;

                    //IRR (use Post Tender IRR or Normal IRR)
                    double? irrToLastPrice = null;
                    if (fundForecast.TOIRRLastPrc.HasValue)
                        irrToLastPrice = fundForecast.TOIRRLastPrc;
                    else if (fundForecast.IRRLastPrc.HasValue)
                        irrToLastPrice = fundForecast.IRRLastPrc;

                    if (irrToLastPrice.HasValue)
                        fundForecast.EAFinalAlpha = irrToLastPrice;
                }
            }
        }

        /// <summary>
        /// Calculated convergence of security discount to long term average sector discount 
        /// </summary>
        /// <param name="ticker"></param>
        /// <param name="fundForecast"></param>
        /// <param name="historicalDiscountStats"></param>
        /// <param name="currentDiscount"></param>
        /// <returns></returns>
        private Nullable<double> CalculateDiscountConvergenceToSectorAvg(
            string ticker
            , FundForecast fundForecast
            , IDictionary<string, FundGroupDiscountStats> historicalDiscountStats
            , double? currentDiscount)
        {
            double? discountConvergenceToSectorAverage = null;

            try
            {
                double? totalLongTermMedianDiscount = null;
                int discountPeriods = 0;

                //3Yr
                string id = string.Join("|", "CEF", "US", fundForecast.FundDscntGrp, "3Yr");
                FundGroupDiscountStats stats;
                if (historicalDiscountStats.TryGetValue(id, out stats))
                {
                    totalLongTermMedianDiscount = CommonUtils.AddNullableDoubles(totalLongTermMedianDiscount, stats.Median);
                    discountPeriods++;
                }

                //5Yr
                id = string.Join("|", "CEF", "US", fundForecast.FundDscntGrp, "5Yr");
                if (historicalDiscountStats.TryGetValue(id, out stats))
                {
                    totalLongTermMedianDiscount = CommonUtils.AddNullableDoubles(totalLongTermMedianDiscount, stats.Median);
                    discountPeriods++;
                }

                //10Yr
                id = string.Join("|", "CEF", "US", fundForecast.FundDscntGrp, "10Yr");
                if (historicalDiscountStats.TryGetValue(id, out stats))
                {
                    totalLongTermMedianDiscount = CommonUtils.AddNullableDoubles(totalLongTermMedianDiscount, stats.Median);
                    discountPeriods++;
                }

                if (currentDiscount.HasValue && totalLongTermMedianDiscount.HasValue)
                {
                    double longTermAvgDiscount = totalLongTermMedianDiscount.GetValueOrDefault() / (double)discountPeriods;
                    discountConvergenceToSectorAverage = currentDiscount.GetValueOrDefault() - (longTermAvgDiscount / 100.0);
                    if (fundForecast.DscntConvMult.HasValue)
                        discountConvergenceToSectorAverage *= (-1.0 * fundForecast.DscntConvMult.GetValueOrDefault());
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error calculating Discount Convergence to Long Term Sector Average for ticker: " + ticker, ex);
            }
            return discountConvergenceToSectorAverage;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ticker"></param>
        /// <param name="fundForecast"></param>
        /// <returns></returns>
        private Nullable<double> CalculateDScoreMovement(
            string ticker
            , FundForecast fundForecast)
        {
            double? dScoreMovement = null;
            int dScoresCount = 0;

            try
            {
                double?[] dScores = new double?[]{
                    fundForecast.DS6M
                    , fundForecast.DS12M
                    , fundForecast.DS24M
                    , fundForecast.DS36M };

                if (fundForecast.DS6M.HasValue) dScoresCount++;
                if (fundForecast.DS12M.HasValue) dScoresCount++;
                if (fundForecast.DS24M.HasValue) dScoresCount++;
                if (fundForecast.DS36M.HasValue) dScoresCount++;

                double? totalDScores = CommonUtils.AddNullableDoubles(dScores);
                if (totalDScores.HasValue && dScoresCount > 0)
                {
                    dScoreMovement = totalDScores / dScoresCount;
                    if (fundForecast.SecDScoreMult.HasValue)
                        dScoreMovement *= (-1.0 * fundForecast.SecDScoreMult.GetValueOrDefault());
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error calculating D-score movement for ticker: " + ticker, ex);
            }
            return dScoreMovement;
        }
    }
}