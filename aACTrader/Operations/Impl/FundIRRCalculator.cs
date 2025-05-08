using aACTrader.Services.Admin;
using aCommons;
using aCommons.Cef;
using aCommons.Utils;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using static aACTrader.Operations.Impl.XIRR;

namespace aACTrader.Operations.Impl
{
    public class FundIRRCalculator
    {
        private readonly ILogger<FundIRRCalculator> _logger;
        private readonly LogDataService _logDataService;

        public FundIRRCalculator(ILogger<FundIRRCalculator> logger, LogDataService logDataService)
        {
            _logger = logger;
            _logDataService = logDataService;
        }

        /// <summary>
        /// Calculates fund's next redemption, notice and payment dates based on fund redemption schedule.
        /// Calculates days to redemption, notice and payment dates from trading date/settle date for use in IRR calculations 
        /// </summary>
        /// <param name="fundRedemptionDict"></param>
        public void GenerateRedemptionSchedule(IDictionary<string, FundRedemption> fundRedemptionDict)
        {
            DateTime todaysDate = DateTime.Now;
            DateTime settleDate = DateUtils.AddBusinessDays(todaysDate, 2);
            DateTime nextRedemptionDate;
            DateTime nextRedemptionNoticeDate;
            DateTime nextRedemptionPaymentDate;
            DateTime firstRedemptionDate;
            DateTime lastRedemptionDate;

            foreach (KeyValuePair<string, FundRedemption> kvp in fundRedemptionDict)
            {
                string ticker = kvp.Key;
                FundRedemption fundRedemption = kvp.Value;

                nextRedemptionDate = todaysDate;
                nextRedemptionNoticeDate = todaysDate;

                try
                {
                    firstRedemptionDate = fundRedemption.FirstRedemptionDate.GetValueOrDefault();
                    lastRedemptionDate = fundRedemption.LastRedemptionDate.GetValueOrDefault();

                    if ("FTU CT".Equals(ticker, StringComparison.CurrentCultureIgnoreCase))
                        _logger.LogInformation("Ticker: " + ticker);

                    if (todaysDate.Date < firstRedemptionDate.Date)
                    {
                        nextRedemptionDate = firstRedemptionDate;
                        nextRedemptionNoticeDate = DateUtils.AddBusinessDays(nextRedemptionDate, fundRedemption.RedemptionNoticeDays.GetValueOrDefault());
                    }
                    else
                    {
                        //The first day of the month of the redemption month with the first year of redemption which is important for future year redemptions
                        //First redemption date relative to the data entered on the sheet and used in the redemption date calculation
                        DateTime firstRedemptionDateDerivedTemp = DateUtils.AddMonths(firstRedemptionDate, 1);
                        DateTime firstRedemptionDateDerived = new DateTime(firstRedemptionDateDerivedTemp.Year, firstRedemptionDateDerivedTemp.Month, 1);
                        firstRedemptionDateDerived = DateUtils.AddBusinessDays(firstRedemptionDateDerived, fundRedemption.RedemptionDaysFromMonthEnd.GetValueOrDefault());

                        //Calculation for next first redemption date from this year going forward.
                        //DateTime currentYearRedemptionDate = new DateTime(Math.Max(firstRedemptionDateDerived.Year, settleDate.Year), firstRedemptionDateDerived.Month, firstRedemptionDateDerived.Day);
                        DateTime currentYearRedemptionDate = DeriveCurrentYearRedemptionDate(firstRedemptionDateDerived, settleDate, firstRedemptionDate);

                        //The first redemption date starting this year or later
                        DateTime currentYearNotificationDate = DateUtils.AddDays(currentYearRedemptionDate, fundRedemption.RedemptionNoticeDays.GetValueOrDefault());
                        if (!string.IsNullOrEmpty(fundRedemption.RedemptionNoticeDateType) && fundRedemption.RedemptionNoticeDateType.Equals("b", StringComparison.CurrentCultureIgnoreCase))
                            currentYearNotificationDate = DateUtils.AddBusinessDays(currentYearRedemptionDate, fundRedemption.RedemptionNoticeDays.GetValueOrDefault());

                        DateTime nextPeriodNotificationDate = currentYearNotificationDate;
                        if ("Annual".Equals(fundRedemption.RedemptionFrequency, StringComparison.CurrentCultureIgnoreCase))
                        {
                            if (currentYearNotificationDate > settleDate)
                                nextPeriodNotificationDate = currentYearNotificationDate;
                            else
                                nextPeriodNotificationDate = new DateTime(currentYearNotificationDate.Year + 1, currentYearNotificationDate.Month, currentYearNotificationDate.Day);
                        }
                        else if ("Quarterly".Equals(fundRedemption.RedemptionFrequency, StringComparison.CurrentCultureIgnoreCase))
                        {
                            if (currentYearNotificationDate > settleDate)
                                nextPeriodNotificationDate = currentYearNotificationDate;
                            else
                                nextPeriodNotificationDate = new DateTime(currentYearNotificationDate.Year, currentYearNotificationDate.Month + 3, currentYearNotificationDate.Day);
                        }
                        else if ("Monthly".Equals(fundRedemption.RedemptionFrequency, StringComparison.CurrentCultureIgnoreCase))
                        {
                            if (currentYearNotificationDate > settleDate)
                                nextPeriodNotificationDate = currentYearNotificationDate;
                            else
                                nextPeriodNotificationDate = new DateTime(currentYearNotificationDate.Year, currentYearNotificationDate.Month + 1, currentYearNotificationDate.Day);
                        }

                        //next notification date for redemptions
                        nextRedemptionNoticeDate = nextPeriodNotificationDate;

                        //
                        nextRedemptionDate = nextRedemptionNoticeDate;
                        int daysDiff = DateUtils.DaysDiff(currentYearNotificationDate, currentYearRedemptionDate) - 1;
                        nextRedemptionDate = nextRedemptionDate.Add(new TimeSpan(daysDiff, 0, 0, 0));
                    }

                    //next redemption payment date
                    if (!string.IsNullOrEmpty(fundRedemption.PaymentDelayDateType))
                    {
                        DateTime eomNextRedemptionDate = DateUtils.GetEndOfMonth(nextRedemptionDate);
                        if ("b".Equals(fundRedemption.PaymentDelayDateType, StringComparison.CurrentCultureIgnoreCase))
                            nextRedemptionPaymentDate = DateUtils.AddBusinessDays(eomNextRedemptionDate, fundRedemption.PaymentDelay.GetValueOrDefault());
                        else
                            nextRedemptionPaymentDate = DateUtils.AddDays(eomNextRedemptionDate, fundRedemption.PaymentDelay.GetValueOrDefault());
                    }
                    else
                    {
                        nextRedemptionPaymentDate = nextRedemptionDate;
                    }

                    //days from today's/trading date to redemption date and redemption notice date
                    //days from settle date to redemption date and redemption payment date
                    int daysUntilRedemptionPaymentDate = DateUtils.DaysDiff(todaysDate, nextRedemptionPaymentDate);
                    int daysUntilRedemptionNoticeDate = DateUtils.DaysDiff(settleDate, nextRedemptionNoticeDate);
                    int daysFromSettleToRedemptionDate = DateUtils.DaysDiff(settleDate, nextRedemptionDate);
                    int daysFromSettleToRedemptionPaymentDate = DateUtils.DaysDiff(settleDate, nextRedemptionPaymentDate);

                    //populate values
                    fundRedemption.SettlementDate = settleDate;
                    fundRedemption.NextRedemptionDate = nextRedemptionDate;
                    fundRedemption.NextNotificationDate = nextRedemptionNoticeDate;
                    fundRedemption.NextRedemptionPaymentDate = nextRedemptionPaymentDate;
                    fundRedemption.DaysUntilRedemptionPayment = daysUntilRedemptionPaymentDate;
                    fundRedemption.DaysUntilNotification = daysUntilRedemptionNoticeDate;
                    fundRedemption.DaysFromSettleToRedemption = daysFromSettleToRedemptionDate;
                    fundRedemption.DaysFromSettleToRedemptionPayment = daysFromSettleToRedemptionPaymentDate;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error parsing Fund Redemption details for ticker: " + ticker);
                    _logDataService.SaveLog("FundIRRCalculator", "GenerateRedemptionSchedule", ticker, ex.Message, "INFO");
                }
            }
        }

        /// <summary>
        /// Gets next redemption date based on fund redemption frequency 
        /// </summary>
        /// <param name="firstRedemptionDate"></param>
        /// <param name="redemptionFrequency"></param>
        /// <returns></returns>
        private DateTime GetNextRedemptionDate(DateTime firstRedemptionDate, string redemptionFrequency)
        {
            DateTime nextRedemptionDate = firstRedemptionDate;

            if ("Annual".Equals(redemptionFrequency, StringComparison.CurrentCultureIgnoreCase))
                nextRedemptionDate = DateUtils.AddMonths(firstRedemptionDate, 12);
            else if ("Quarterly".Equals(redemptionFrequency, StringComparison.CurrentCultureIgnoreCase))
                nextRedemptionDate = DateUtils.AddMonths(firstRedemptionDate, 3);
            else if ("Monthly".Equals(redemptionFrequency, StringComparison.CurrentCultureIgnoreCase))
                nextRedemptionDate = DateUtils.AddMonths(firstRedemptionDate, 1);
            else //assume annual
                nextRedemptionDate = DateUtils.AddMonths(firstRedemptionDate, 12);

            return nextRedemptionDate;
        }

        /// <summary>
        /// Calculates IRR for funds with redemption schedule (next redemption notice date, redemption date, payment date
        /// Adjusts prices for preferreds and transaction and redemption expenses
        /// Adjusts estimated navs and prices for redemption related expenses
        /// Applies estimated fund returns to full capital strucutre (equity and preferred)
        /// </summary>
        /// <param name="ticker"></param>
        /// <param name="fundMaster"></param>
        /// <param name="fundForecast"></param>
        /// <param name="fundRedemption"></param>
        /// <param name="securityPrice"></param>
        /// <param name="preferredSecurityPrice"></param>
        public void CalculateIRR(string ticker
            , FundMaster fundMaster
            , FundForecast fundForecast
            , FundRedemption fundRedemption
            , SecurityPrice securityPrice
            , SecurityPrice preferredSecurityPrice)
        {
            string irrModel = fundMaster.IRRModel;
            if (!string.IsNullOrEmpty(irrModel) && irrModel.Equals("USCAIRRModel"))
                CalculateUSCAIRR(ticker, fundForecast, securityPrice, preferredSecurityPrice, fundRedemption);
            else if (!string.IsNullOrEmpty(irrModel) && irrModel.Equals("UKIRRModel"))
                CalculateUKIRR(ticker, fundForecast, securityPrice, preferredSecurityPrice, fundRedemption);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ticker"></param>
        /// <param name="fundForecast"></param>
        /// <param name="securityPrice"></param>
        /// <param name="preferredSharePrice"></param>
        /// <param name="fundRedemption"></param>
        public void CalculateUSCAIRR(string ticker
            , FundForecast fundForecast
            , SecurityPrice securityPrice
            , SecurityPrice preferredSharePrice
            , FundRedemption fundRedemption)
        {
            try
            {
                double preferredShareLastPrice = 0.0;
                double preferredShareBidPrice = 0.0;
                double preferredShareAskPrice = 0.0;

                double? estimatedNav = FundForecastOperations.GetEstimatedNav(fundForecast);

                //if use rights offer flag is set then use nav post rights issue
                if (fundForecast.RODscntDispFlag == 1)
                    estimatedNav = fundForecast.RONav;

                if (estimatedNav.GetValueOrDefault() > 0 && securityPrice != null)
                {
                    bool useRedemptionPrice = !string.IsNullOrEmpty(fundRedemption.IsPreferredTakenOutAtRedemptionPrice)
                                && fundRedemption.IsPreferredTakenOutAtRedemptionPrice.Equals("Y", StringComparison.CurrentCultureIgnoreCase);

                    if (preferredSharePrice != null)
                    {
                        if (useRedemptionPrice)
                        {
                            double preferredShareRedemptionValue = fundForecast.PfdRedVal.GetValueOrDefault();
                            preferredShareLastPrice = preferredShareRedemptionValue;
                            preferredShareBidPrice = preferredShareRedemptionValue;
                            preferredShareAskPrice = preferredShareRedemptionValue;
                        }
                        else
                        {
                            double numPreferredSharesPerCommonSplitTrust = fundRedemption.NumPreferredSharesPerCommonSplitTrust.GetValueOrDefault();
                            preferredShareLastPrice = preferredSharePrice.LastPrc.GetValueOrDefault() * numPreferredSharesPerCommonSplitTrust;
                            preferredShareBidPrice = preferredSharePrice.BidPrc.GetValueOrDefault() * numPreferredSharesPerCommonSplitTrust;
                            preferredShareAskPrice = preferredSharePrice.AskPrc.GetValueOrDefault() * numPreferredSharesPerCommonSplitTrust;
                        }
                    }

                    double expenseRatio = fundForecast.ExpRatio.GetValueOrDefault();
                    double expensesToRedemptionTerm = (expenseRatio * fundRedemption.DaysFromSettleToRedemption.GetValueOrDefault()) / 365.0;
                    expensesToRedemptionTerm *= (estimatedNav.GetValueOrDefault() - fundForecast.PfdRedVal.GetValueOrDefault()) / estimatedNav.GetValueOrDefault();

                    //adjust nav for expenses
                    estimatedNav *= ((1.0 - expensesToRedemptionTerm / 100.0) * (1.0 - fundRedemption.RedemptionFeePct.GetValueOrDefault()));
                    estimatedNav -= (fundRedemption.RedemptionFixedFee.GetValueOrDefault() + fundRedemption.PreferredSharedRedemptionFee.GetValueOrDefault());
                    estimatedNav += fundRedemption.PreferredInterestOnRedemptionDate.GetValueOrDefault();

                    fundForecast.NavExclRedExp = estimatedNav;
                    fundForecast.DaysToRedPmt = fundRedemption.DaysUntilRedemptionPayment;
                    fundForecast.PfdAskPrc = preferredShareAskPrice;
                    fundForecast.RedExp = expensesToRedemptionTerm;

                    //calculate IRRs
                    //irr to last price
                    double estimatedNavD = estimatedNav.GetValueOrDefault();
                    double lastPrice = securityPrice.LastPrc.GetValueOrDefault();
                    if (lastPrice > 0)
                    {
                        try
                        {
                            lastPrice += (preferredShareLastPrice + fundRedemption.PerShareCommission.GetValueOrDefault());
                            fundForecast.LastPrcExclRedExp = lastPrice;

                            IEnumerable<CashItem> cfs = new CashItem[] {
                                                new CashItem(fundRedemption.SettlementDate.GetValueOrDefault(), -1.0 * lastPrice),
                                                new CashItem(fundRedemption.NextRedemptionPaymentDate.GetValueOrDefault() , estimatedNavD)};

                            double irrToLastPrice = XIRR.RunScenario(cfs);
                            if (!Double.IsInfinity(irrToLastPrice) && !Double.IsNaN(irrToLastPrice))
                                fundForecast.IRRLastPrc = irrToLastPrice;

                            fundForecast.IRRType = "Normal";
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "Error calculating IRR for last price");
                        }
                    }

                    //irr to bid price
                    double bidPrice = securityPrice.BidPrc.GetValueOrDefault();
                    if (bidPrice > 0)
                    {
                        try
                        {
                            bidPrice += (preferredShareBidPrice + fundRedemption.PerShareCommission.GetValueOrDefault());
                            fundForecast.BidPrcExclRedExp = bidPrice;

                            IEnumerable<CashItem> cfs = new CashItem[] {
                                                new CashItem(fundRedemption.SettlementDate.GetValueOrDefault(), -1.0 * bidPrice),
                                                new CashItem(fundRedemption.NextRedemptionPaymentDate.GetValueOrDefault() , estimatedNavD)};

                            double irrToBidPrice = XIRR.RunScenario(cfs);
                            if (!Double.IsInfinity(irrToBidPrice) && !Double.IsNaN(irrToBidPrice))
                                fundForecast.IRRBidPrc = irrToBidPrice;
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "Error calculating IRR for bid price");
                        }
                    }

                    //irr to ask price
                    double askPrice = securityPrice.AskPrc.GetValueOrDefault();
                    if (askPrice > 0)
                    {
                        try
                        {
                            askPrice += (preferredShareAskPrice + fundRedemption.PerShareCommission.GetValueOrDefault());
                            fundForecast.AskPrcExclRedExp = askPrice;

                            IEnumerable<CashItem> cfs = new CashItem[] {
                                                new CashItem(fundRedemption.SettlementDate.GetValueOrDefault(), -1.0 * askPrice),
                                                new CashItem(fundRedemption.NextRedemptionPaymentDate.GetValueOrDefault() , estimatedNavD)};

                            double irrToAskPrice = XIRR.RunScenario(cfs);
                            if (!Double.IsInfinity(irrToAskPrice) && !Double.IsNaN(irrToAskPrice))
                                fundForecast.IRRAskPrc = irrToAskPrice;
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "Error calculating IRR for ask price");
                        }
                    }

                    //annualized accretion from the dividend paid at par
                    if (fundForecast.EADvdRtn.HasValue)
                    {
                        fundForecast.FIRRLastPrc = CommonUtils.AddNullableDoubles(fundForecast.IRRLastPrc, fundForecast.EADvdRtn);
                        fundForecast.FIRRBidPrc = CommonUtils.AddNullableDoubles(fundForecast.IRRBidPrc, fundForecast.EADvdRtn);
                        fundForecast.FIRRAskPrc = CommonUtils.AddNullableDoubles(fundForecast.IRRAskPrc, fundForecast.EADvdRtn);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error calculating IRRs for ticker: " + ticker);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ticker"></param>
        /// <param name="fundForecast"></param>
        /// <param name="securityPrice"></param>
        /// <param name="preferredSharePrice"></param>
        /// <param name="fundRedemption"></param>
        public void CalculateUKIRR(string ticker
            , FundForecast fundForecast
            , SecurityPrice securityPrice
            , SecurityPrice preferredSharePrice
            , FundRedemption fundRedemption)
        {
            try
            {
                //if ("AEFS LN".Equals(ticker, StringComparison.CurrentCultureIgnoreCase))
                //    _logger.LogInformation("Ticker: " + ticker);

                double? stampTax = fundRedemption.StampTax;
                double? redemptionFeePct = fundRedemption.RedemptionFeePct;
                double? transactionCost = fundRedemption.FundLiquidityTransactionCost;

                //if use right's offer flag is set then use nav post right's issue
                double? estimatedNav = FundForecastOperations.GetEstimatedNav(fundForecast);
                if (fundForecast.RODscntDispFlag == 1)
                    estimatedNav = fundForecast.RONav;

                if (estimatedNav.GetValueOrDefault() > 0)
                {
                    //calculate expenses (expenses should be for equity share class)
                    //adjust for preferred share redemption value
                    double expenseRatio = fundForecast.ExpRatio.GetValueOrDefault();
                    double expensesToRedemptionTerm = (expenseRatio * fundRedemption.DaysFromSettleToRedemption.GetValueOrDefault()) / 365.0;
                    fundForecast.RedExp = expensesToRedemptionTerm;

                    //adjust nav for expenses
                    estimatedNav *= (1 - expensesToRedemptionTerm / 100.0) * (1 - redemptionFeePct.GetValueOrDefault());
                    //estimatedNav -= (fundRedemption.RedemptionFixedFee.GetValueOrDefault() + fundRedemption.PreferredSharedRedemptionFee.GetValueOrDefault());
                    //estimatedNav += fundRedemption.PreferredInterestOnRedemptionDate.GetValueOrDefault();

                    //get security price
                    if (securityPrice != null)
                    {
                        fundForecast.NavExclRedExp = estimatedNav;
                        fundForecast.DaysToRedPmt = fundRedemption.DaysUntilRedemptionPayment;

                        //calculate IRRs
                        //irr to last price
                        double lastPrice = securityPrice.LastPrc.GetValueOrDefault();
                        double estimatedNavD = estimatedNav.GetValueOrDefault();
                        if (lastPrice > 0)
                        {
                            try
                            {
                                if (stampTax.HasValue)
                                    lastPrice *= (1.0 + stampTax.GetValueOrDefault());
                                if (transactionCost.HasValue)
                                    lastPrice *= (1.0 + transactionCost.GetValueOrDefault());

                                fundForecast.LastPrcExclRedExp = lastPrice;

                                IEnumerable<CashItem> cfs = new CashItem[] {
                                                new CashItem(fundRedemption.SettlementDate.GetValueOrDefault(), -1.0 * lastPrice),
                                                new CashItem(fundRedemption.NextRedemptionPaymentDate.GetValueOrDefault() , estimatedNavD)};

                                double irrToLastPrice = XIRR.RunScenario(cfs);
                                if (!Double.IsInfinity(irrToLastPrice) && !Double.IsNaN(irrToLastPrice))
                                    fundForecast.IRRLastPrc = irrToLastPrice;

                                fundForecast.IRRType = "Normal";
                            }
                            catch (Exception ex)
                            {
                                _logger.LogError(ex, "Error calculating IRR for last price");
                            }
                        }

                        //irr to bid price
                        double bidPrice = securityPrice.BidPrc.GetValueOrDefault();
                        if (bidPrice > 0)
                        {
                            try
                            {
                                if (stampTax.HasValue)
                                    bidPrice *= (1.0 + stampTax.GetValueOrDefault());
                                if (transactionCost.HasValue)
                                    bidPrice *= (1.0 + transactionCost.GetValueOrDefault());

                                fundForecast.BidPrcExclRedExp = bidPrice;

                                IEnumerable<CashItem> cfs = new CashItem[] {
                                                new CashItem(fundRedemption.SettlementDate.GetValueOrDefault(), -1.0 * bidPrice),
                                                new CashItem(fundRedemption.NextRedemptionPaymentDate.GetValueOrDefault() , estimatedNavD)};

                                double irrToBidPrice = XIRR.RunScenario(cfs);
                                if (!Double.IsInfinity(irrToBidPrice) && !Double.IsNaN(irrToBidPrice))
                                    fundForecast.IRRBidPrc = irrToBidPrice;
                            }
                            catch (Exception ex)
                            {
                                _logger.LogError(ex, "Error calculating IRR for bid price");
                            }
                        }

                        //irr to ask price
                        double askPrice = securityPrice.AskPrc.GetValueOrDefault();
                        if (askPrice > 0)
                        {
                            try
                            {
                                if (stampTax.HasValue)
                                    askPrice *= (1.0 + stampTax.GetValueOrDefault());
                                if (transactionCost.HasValue)
                                    askPrice *= (1.0 + transactionCost.GetValueOrDefault());

                                fundForecast.AskPrcExclRedExp = askPrice;

                                IEnumerable<CashItem> cfs = new CashItem[] {
                                                new CashItem(fundRedemption.SettlementDate.GetValueOrDefault(), -1.0 * askPrice),
                                                new CashItem(fundRedemption.NextRedemptionPaymentDate.GetValueOrDefault() , estimatedNavD)};

                                double irrToAskPrice = XIRR.RunScenario(cfs);
                                if (!Double.IsInfinity(irrToAskPrice) && !Double.IsNaN(irrToAskPrice))
                                    fundForecast.IRRAskPrc = irrToAskPrice;
                            }
                            catch (Exception ex)
                            {
                                _logger.LogError(ex, "Error calculating IRR for ask price");
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error calculating IRRs for ticker: " + ticker);
            }
        }

        /// <summary>
        /// Calculate next first redemption date from this year going forward.
        /// </summary>
        /// <param name="firstRedemptionDateDerived"></param>
        /// <param name="settleDate"></param>
        /// <param name="firstRedemptionDate"></param>
        /// <returns></returns>
        private DateTime DeriveCurrentYearRedemptionDate(DateTime firstRedemptionDateDerived, DateTime settleDate, DateTime firstRedemptionDate)
        {
            DateTime currentYearRedemptionDate = DateTime.Now;
            try
            {
                currentYearRedemptionDate = new DateTime(Math.Max(firstRedemptionDateDerived.Year, settleDate.Year), firstRedemptionDateDerived.Month, firstRedemptionDateDerived.Day); ;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error calculating Current Year Redemption Date", ex);
                currentYearRedemptionDate = new DateTime(Math.Max(firstRedemptionDateDerived.Year, settleDate.Year), firstRedemptionDateDerived.Month, firstRedemptionDate.Day); ;
            }
            return currentYearRedemptionDate;
        }
    }
}