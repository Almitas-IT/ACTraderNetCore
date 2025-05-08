using aACTrader.Common;
using aCommons;
using aCommons.Cef;
using aCommons.Pfd;
using aCommons.Utils;
using LazyCache;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;

namespace aACTrader.Operations.Impl
{
    public class CashflowGenerator
    {
        private readonly ILogger<CashflowGenerator> _logger;
        private readonly CachingService _cache;

        protected const int NUM_MONTHS = 1200;
        readonly DateTime settleDate = DateUtils.AddBusinessDays(DateTime.Now, 2);
        readonly IDictionary<string, RateIndexCurveMap> IndexMapDict = new Dictionary<string, RateIndexCurveMap>(StringComparer.CurrentCultureIgnoreCase) {
            { "gcan3m", new RateIndexCurveMap( "cadgovt", 3, "cadswap", 1, 0 )},
            { "tbbc3m", new RateIndexCurveMap( "cadgovt", 3, "cadswap", 1, 0 )},
            { "t-bill 3mo", new RateIndexCurveMap( "cadgovt", 3, "cadswap", 1, 0 )},
            { "gcan5yr", new RateIndexCurveMap( "cadgovt", 5, "cadswap", 1, 0 )},
            { "canprm 1mo", new RateIndexCurveMap( "cadswap", 1, "cadswap", 1, 0.0183 )},
            { "canprm 1d", new RateIndexCurveMap( "cadswap", 1, "cadswap", 1, 0.0183 )},
            { "canprm dly", new RateIndexCurveMap( "cadswap", 1, "cadswap", 1, 0.0183 )},
            { "canprm 3mo", new RateIndexCurveMap( "cadswap", 3, "cadswap", 1, 0.0183 )},
            { "usb3myd", new RateIndexCurveMap( "usdgovt", 3, "usdswap", 1, 0 )},
            { "ust3m", new RateIndexCurveMap( "usdgovt", 3, "usdswap", 1, 0 )},
            { "h15t5y", new RateIndexCurveMap( "usdgovt", 5, "usdswap", 1, 0 )},
            { "ht15t5yr", new RateIndexCurveMap( "usdgovt", 5, "usdswap", 1, 0 )},
            { "us0003m", new RateIndexCurveMap( "usdswap", 3, "usdswap", 1, 0 )},
            { "cdor 1m", new RateIndexCurveMap( "cadswap", 1, "cadswap", 1, 0 )},
            { "cdor 3m", new RateIndexCurveMap( "cadswap", 3, "cadswap", 1, 0 )},
            { "unk", new RateIndexCurveMap( "cadswap", 3, "cadswap", 1, 0 )}
        };

        public CashflowGenerator(ILogger<CashflowGenerator> logger, CachingService cache)
        {
            _logger = logger;
            _cache = cache;
        }

        /// <summary>
        /// Pre-calculate cash flows for fixed rate preferreds
        /// For floating rate securities, calculate cash flows based on live interest rate curves 
        /// </summary>
        /// <param name="cache"></param>
        public void GenerateCashFlows()
        {
            IDictionary<string, PfdSecurity> securityDict = _cache.Get<IDictionary<string, PfdSecurity>>(CacheKeys.PFD_SECURITY_MASTER);
            IDictionary<string, PfdSecurityCashFlows> securityCashFlowsDict = new Dictionary<string, PfdSecurityCashFlows>(StringComparer.CurrentCultureIgnoreCase);

            foreach (KeyValuePair<string, PfdSecurity> kvp in securityDict)
            {
                string securityId = kvp.Key;
                PfdSecurity security = kvp.Value;

                //if (security.BBGId.Equals("Alt0004", StringComparison.CurrentCultureIgnoreCase))
                //    _logger.LogDebug("Security Id: " + security.BBGId);

                try
                {
                    CheckSecurityTAndCs(security, settleDate);
                    if (!string.IsNullOrEmpty(security.ResetIndex) && security.NextRefixDt.HasValue)
                        security.IsFloat = true;
                    else
                        security.IsFloat = false;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error processing security: " + securityId);
                }
            }

            ProcessFixedRateSecurities(securityDict, securityCashFlowsDict, settleDate);
            ProcessFloatingRateSecurities(securityDict, securityCashFlowsDict, settleDate);

            //_cache.Remove(CacheKeys.PFD_SECURITY_CASHFLOWS);
            _cache.Add(CacheKeys.PFD_SECURITY_CASHFLOWS, securityCashFlowsDict, DateTimeOffset.MaxValue);
        }

        /// <summary>
        /// Check if security terms and conditions are valid
        /// Check if security is exchangeable
        /// </summary>
        /// <param name="security"></param>
        /// <param name="settleDate"></param>
        private void CheckSecurityTAndCs(PfdSecurity security, DateTime settleDate)
        {
            security.HasValidData = true;

            if (!security.CpnFreq.HasValue)
            {
                security.HasValidData = false;
                security.MissingData = "Missing Cpn Freq";
            }
            else if (!security.PrevCpnDt.HasValue)
            {
                security.HasValidData = false;
                security.MissingData = "Missing Prev Cpn";
            }
            else if (!security.NxtCpnDt.HasValue)
            {
                security.HasValidData = false;
                security.MissingData = "Missing Next Cpn";
            }

            if (security.NxtExchDt.HasValue && !string.IsNullOrEmpty(security.ExchIndex))
            {
                int daysDiff = DateUtils.DaysDiff(settleDate, security.NxtExchDt);
                if (daysDiff > 0)
                    security.IsExchangeable = true;
                else
                    security.IsExchangeable = false;
            }
        }

        /// <summary>
        /// Process fixed rate securities
        /// Generate cash flows
        /// Identify curve to discount the cash flows
        /// </summary>
        /// <param name="securityDict"></param>
        /// <param name="securityCashFlowsDict"></param>
        /// <param name="settleDate"></param>
        private void ProcessFloatingRateSecurities(IDictionary<string, PfdSecurity> securityDict, IDictionary<string, PfdSecurityCashFlows> securityCashFlowsDict, DateTime settleDate)
        {
            IDictionary<string, RateIndexProxy> rateProxyDict = _cache.Get<IDictionary<string, RateIndexProxy>>(CacheKeys.PFD_INDEX_PROXIES);

            foreach (KeyValuePair<string, PfdSecurity> kvp in securityDict)
            {
                string securityId = kvp.Key;
                PfdSecurity security = kvp.Value;

                try
                {
                    if (security.IsFloat && security.HasValidData)
                    {
                        //if (security.BBGId.Equals("Alt0004", StringComparison.CurrentCultureIgnoreCase))
                        //	_logger.LogDebug("Security Id: " + security.BBGId);

                        //get index proxy
                        RateIndexProxy indexProxy = null;
                        string proxyIndex = security.ResetIndex;
                        if (rateProxyDict.ContainsKey(security.ResetIndex))
                        {
                            indexProxy = rateProxyDict[security.ResetIndex];
                            proxyIndex = indexProxy.ProxyIndex;
                        }

                        //get interest rate curve
                        RateIndexCurveMap rateIndexCurveMap;
                        if (IndexMapDict.TryGetValue(proxyIndex, out rateIndexCurveMap))
                        {
                            PfdSecurityCashFlows securityCashFlows;
                            if (!securityCashFlowsDict.TryGetValue(securityId, out securityCashFlows))
                            {
                                securityCashFlows = new PfdSecurityCashFlows
                                {
                                    BBGId = securityId,
                                    RateIndexCurveMap = rateIndexCurveMap,
                                    RateIndexProxy = indexProxy
                                };

                                securityCashFlowsDict.Add(securityId, securityCashFlows);

                                if (security.IsExchangeable)
                                {
                                    RateIndexProxy exchIndexProxy = null;
                                    string proxyExchIndex = security.ExchIndex;
                                    if (rateProxyDict.ContainsKey(security.ExchIndex))
                                    {
                                        exchIndexProxy = rateProxyDict[security.ExchIndex];
                                        securityCashFlows.ExchangeableRateIndexProxy = exchIndexProxy;
                                        proxyExchIndex = exchIndexProxy.ProxyIndex;
                                    }

                                    RateIndexCurveMap exchRateIndexCurveMap;
                                    if (IndexMapDict.TryGetValue(proxyExchIndex, out exchRateIndexCurveMap))
                                    {
                                        securityCashFlows.ExchangeableRateIndexCurveMap = exchRateIndexCurveMap;
                                    }
                                }

                                IDictionary<string, IRCurve> rateCurveDict = null;

                                //for normal securities
                                if (security.MaturityDt.HasValue)
                                {
                                    rateCurveDict = _cache.Get<IDictionary<string, IRCurve>>(CacheKeys.PFD_FWD_RATE_CURVES);
                                    FloatingRateCashFlowGeneratorWithMaturity(security, settleDate, securityCashFlows, rateCurveDict, GlobalConstants.RATE_SCENARIO_BASE);

                                    rateCurveDict = _cache.Get<IDictionary<string, IRCurve>>(CacheKeys.PFD_FWD_RATE_CURVES_UP);
                                    FloatingRateCashFlowGeneratorWithMaturity(security, settleDate, securityCashFlows, rateCurveDict, GlobalConstants.RATE_SCENARIO_UP);

                                    rateCurveDict = _cache.Get<IDictionary<string, IRCurve>>(CacheKeys.PFD_FWD_RATE_CURVES_DOWN);
                                    FloatingRateCashFlowGeneratorWithMaturity(security, settleDate, securityCashFlows, rateCurveDict, GlobalConstants.RATE_SCENARIO_DOWN);
                                }
                                else
                                {
                                    rateCurveDict = _cache.Get<IDictionary<string, IRCurve>>(CacheKeys.PFD_FWD_RATE_CURVES);
                                    FloatingRateCashFlowGeneratorPerpetual(security, settleDate, securityCashFlows, rateCurveDict, GlobalConstants.RATE_SCENARIO_BASE);

                                    rateCurveDict = _cache.Get<IDictionary<string, IRCurve>>(CacheKeys.PFD_FWD_RATE_CURVES_UP);
                                    FloatingRateCashFlowGeneratorPerpetual(security, settleDate, securityCashFlows, rateCurveDict, GlobalConstants.RATE_SCENARIO_UP);

                                    rateCurveDict = _cache.Get<IDictionary<string, IRCurve>>(CacheKeys.PFD_FWD_RATE_CURVES_DOWN);
                                    FloatingRateCashFlowGeneratorPerpetual(security, settleDate, securityCashFlows, rateCurveDict, GlobalConstants.RATE_SCENARIO_DOWN);
                                }

                                //for exchangeable securities
                                if (security.IsExchangeable)
                                {
                                    rateCurveDict = _cache.Get<IDictionary<string, IRCurve>>(CacheKeys.PFD_FWD_RATE_CURVES);
                                    FloatingRateExchangeableCashFlowGenerator(security, settleDate, securityCashFlows, rateCurveDict, GlobalConstants.RATE_SCENARIO_BASE);

                                    rateCurveDict = _cache.Get<IDictionary<string, IRCurve>>(CacheKeys.PFD_FWD_RATE_CURVES_UP);
                                    FloatingRateExchangeableCashFlowGenerator(security, settleDate, securityCashFlows, rateCurveDict, GlobalConstants.RATE_SCENARIO_UP);

                                    rateCurveDict = _cache.Get<IDictionary<string, IRCurve>>(CacheKeys.PFD_FWD_RATE_CURVES_DOWN);
                                    FloatingRateExchangeableCashFlowGenerator(security, settleDate, securityCashFlows, rateCurveDict, GlobalConstants.RATE_SCENARIO_DOWN);
                                }
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error generating cashflows for security: " + securityId);
                }
            }
        }

        /// <summary>
        /// Process fixed rate securities
        /// Generate cash flows
        /// Identify curve to discount the cash flows
        /// </summary>
        /// <param name="securityDict"></param>
        /// <param name="securityCashFlowsDict"></param>
        /// <param name="settleDate"></param>
        private void ProcessFixedRateSecurities(IDictionary<string, PfdSecurity> securityDict, IDictionary<string, PfdSecurityCashFlows> securityCashFlowsDict, DateTime settleDate)
        {
            foreach (KeyValuePair<string, PfdSecurity> kvp in securityDict)
            {
                string securityId = kvp.Key;
                PfdSecurity security = kvp.Value;

                try
                {
                    if (!security.IsFloat && security.HasValidData)
                    {
                        //if (security.BBGId.Equals("EP0481630", StringComparison.CurrentCultureIgnoreCase))
                        //	_logger.LogDebug("Security Id: " + security.BBGId);

                        //get interest rate curve to discount expected cashflows
                        string discountCurveIndex = "us0003m";
                        if (!string.IsNullOrEmpty(security.Currency) && security.Currency.Equals("CAD", StringComparison.CurrentCultureIgnoreCase))
                        {
                            discountCurveIndex = "cdor 3m";
                        }
                        else if (!string.IsNullOrEmpty(security.Currency) && security.Currency.Equals("USD", StringComparison.CurrentCultureIgnoreCase))
                        {
                            discountCurveIndex = "us0003m";
                        }

                        RateIndexCurveMap rateIndexCurveMap;
                        if (IndexMapDict.TryGetValue(discountCurveIndex, out rateIndexCurveMap))
                        {
                            PfdSecurityCashFlows securityCashFlows;
                            if (!securityCashFlowsDict.TryGetValue(securityId, out securityCashFlows))
                            {
                                securityCashFlows = new PfdSecurityCashFlows
                                {
                                    BBGId = securityId,
                                    RateIndexCurveMap = rateIndexCurveMap
                                };
                                securityCashFlowsDict.Add(securityId, securityCashFlows);

                                if (security.MaturityDt.HasValue)
                                {
                                    FixedRateCashFlowGeneratorWithMaturity(security, settleDate, securityCashFlows);
                                }
                                else
                                {
                                    FixedRateCashFlowGeneratorPerpetual(security, settleDate, securityCashFlows);
                                }
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error generating cashflows for security: " + securityId);
                }
            }
        }

        /**
		 * Pre-compute cash flows for fixed rate securities
		 * 
		 * TODO: implement day count convention as per security details 
		 */
        private void FixedRateCashFlowGeneratorPerpetual(PfdSecurity security, DateTime settleDate, PfdSecurityCashFlows securityCashFlows)
        {
            try
            {
                //if (security.BBGId.Equals("EP0289082", StringComparison.CurrentCultureIgnoreCase))
                //	_logger.LogDebug("Security Id: " + security.BBGId);

                int cpnFrequency = 12 / security.CpnFreq.GetValueOrDefault();
                double currentCpn = security.Cpn.GetValueOrDefault() / 100.0;
                DateTime previousCpnDate = security.PrevCpnDt.GetValueOrDefault();
                DateTime nextCpnDate = security.NxtCpnDt.GetValueOrDefault();
                double parAmt = security.Par.GetValueOrDefault();

                //populate accrued coupon
                double accruedCpn = security.IntAcc.GetValueOrDefault();
                int accruedDays = security.IntAccDays.GetValueOrDefault();
                security.CalcIntAcc = accruedCpn;

                //calculate next coupon date
                //TODO: check if the below condition should be less than equal to or equal to
                if (nextCpnDate.Date < settleDate.Date)
                    nextCpnDate = CalculateNextDate(settleDate, nextCpnDate, cpnFrequency);

                int daysToNextCpn = 0;
                int period = 0;
                double cashflow = 0;
                int numPeriods = NUM_MONTHS;

                //populate stub coupon
                int stubDays = DateUtils.DaysDiff(settleDate, nextCpnDate);
                int monthsToNextCpn = DateUtils.MonthsDiff(settleDate, nextCpnDate) + 1;
                double stubCpn = parAmt * currentCpn * (stubDays / 365.0);

                //known cashflow (coupon payment)
                int numCashflows = Convert.ToInt32(numPeriods / cpnFrequency);
                CashFlows cashFlows = new CashFlows(numCashflows + 1);
                cashFlows.Period[period] = monthsToNextCpn;
                cashFlows.TimeInYears[period] = stubDays / 365.0;
                cashFlows.CashFlow[period] = stubCpn;
                cashFlows.AccrualStartDate[period] = settleDate;
                cashFlows.AccrualEndDate[period] = nextCpnDate;
                cashFlows.Coupon[period] = currentCpn;

                //populate future cashflows (coupon payments)
                previousCpnDate = nextCpnDate;
                for (int i = 1; i <= numPeriods; i++)
                {
                    if (i % cpnFrequency == 0)
                    {
                        DateTime nextDt = DateUtils.AddMonths(nextCpnDate, i);
                        monthsToNextCpn = DateUtils.MonthsDiff(settleDate, nextDt) + 1;
                        daysToNextCpn = DateUtils.DaysDiff(settleDate, nextDt);
                        cashflow += parAmt * currentCpn / 12.0;

                        period++;
                        cashFlows.Period[period] = monthsToNextCpn;
                        cashFlows.TimeInYears[period] = daysToNextCpn / 365.0;
                        cashFlows.CashFlow[period] = cashflow;
                        cashFlows.AccrualStartDate[period] = previousCpnDate;
                        cashFlows.AccrualEndDate[period] = nextDt;
                        cashFlows.Coupon[period] = currentCpn;
                        previousCpnDate = nextDt;

                        cashflow = 0; //reset cashflow
                    }
                    else
                    {
                        cashflow += parAmt * currentCpn / 12.0; //accrued coupon between cashflow dates
                    }
                }

                //populate cashflows
                securityCashFlows.AccDays = accruedDays;
                securityCashFlows.StubDays = stubDays;
                securityCashFlows.AccCpn = accruedCpn;
                securityCashFlows.StubCpn = stubCpn;
                securityCashFlows.SettleDate = settleDate;
                securityCashFlows.CashFlows = cashFlows;
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error generating cashflows for security: " + security.BBGId);
            }
        }

        /**
		 * Pre-compute cash flows for fixed rate securities
		 * 
		 * TODO: implement day count convention as per security details 
		 */
        private void FixedRateCashFlowGeneratorWithMaturity(PfdSecurity security, DateTime settleDate, PfdSecurityCashFlows securityCashFlows)
        {
            try
            {
                //if (security.BBGId.Equals("EP0289082", StringComparison.CurrentCultureIgnoreCase) ||
                //	security.BBGId.Equals("EP0174581", StringComparison.CurrentCultureIgnoreCase) ||
                //	security.BBGId.Equals("EP0481630", StringComparison.CurrentCultureIgnoreCase))
                //{
                //	_logger.LogDebug("Security Id: " + security.BBGId);
                //}

                int cpnFrequency = 12 / security.CpnFreq.GetValueOrDefault();
                double currentCpn = security.Cpn.GetValueOrDefault() / 100.0;
                double parAmt = security.Par.GetValueOrDefault();
                DateTime previousCpnDate = security.PrevCpnDt.GetValueOrDefault();
                DateTime nextCpnDate = security.NxtCpnDt.GetValueOrDefault();
                DateTime maturityDate = security.MaturityDt.GetValueOrDefault();

                //populate accrued coupon
                int accruedDays = security.IntAccDays.GetValueOrDefault();
                double accruedCpn = security.IntAcc.GetValueOrDefault();
                security.CalcIntAcc = accruedCpn;

                //calculate next coupon date
                //TODO: check if the below condition should be less than equal to or equal to
                if (nextCpnDate.Date < settleDate.Date)
                    nextCpnDate = CalculateNextDate(settleDate, nextCpnDate, cpnFrequency);

                if (nextCpnDate.Date > security.MaturityDt.Value.Date)
                    nextCpnDate = security.MaturityDt.GetValueOrDefault();

                //identify number of cashflow periods
                int numCashflows = 0;
                DateTime nextCashflowDate = settleDate;
                while (nextCashflowDate <= maturityDate)
                {
                    numCashflows++;
                    nextCashflowDate = DateUtils.AddMonths(nextCashflowDate, cpnFrequency);
                    if ((nextCashflowDate.Year == maturityDate.Year) && (nextCashflowDate.Month == maturityDate.Month))
                        break;
                }

                int numPeriods = DateUtils.MonthsDiff(settleDate, security.MaturityDt.GetValueOrDefault());
                numPeriods = numPeriods < 1 ? 1 : numPeriods;

                //calculate cashflows
                int daysToNextCpn = 0;
                int period = 0;
                int stubDays = 0;
                int monthsToNextCpn = 0;
                double cashflow = 0;
                double stubCpn = 0;

                CashFlows cashFlows = null;
                if (numCashflows == 1)
                {
                    cashFlows = new CashFlows(numCashflows);

                    monthsToNextCpn = DateUtils.MonthsDiff(settleDate, nextCpnDate) + 1;
                    stubDays = DateUtils.DaysDiff(settleDate, nextCpnDate);
                    stubCpn = parAmt * currentCpn * (stubDays / 365.0);

                    cashFlows.Period[period] = monthsToNextCpn;
                    cashFlows.TimeInYears[period] = stubDays / 365.0;
                    cashFlows.CashFlow[period] = stubCpn + security.Par.GetValueOrDefault();
                    cashFlows.AccrualStartDate[period] = settleDate;
                    cashFlows.AccrualEndDate[period] = maturityDate;
                    cashFlows.Coupon[period] = currentCpn;
                }
                else
                {
                    stubDays = DateUtils.DaysDiff(settleDate, nextCpnDate);
                    monthsToNextCpn = DateUtils.MonthsDiff(settleDate, nextCpnDate) + 1;
                    stubCpn = parAmt * currentCpn * (stubDays / 365.0);

                    cashFlows = new CashFlows(numCashflows + 1);
                    cashFlows.Period[period] = monthsToNextCpn;
                    cashFlows.TimeInYears[period] = stubDays / 365.0;
                    cashFlows.CashFlow[period] = stubCpn;
                    cashFlows.AccrualStartDate[period] = settleDate;
                    cashFlows.AccrualEndDate[period] = nextCpnDate;
                    cashFlows.Coupon[period] = currentCpn;

                    //populate future cashflows (coupon payments)
                    //if maturity date is provided, add par amount at maturity date
                    previousCpnDate = nextCpnDate;
                    for (int i = 1; i < numPeriods; i++)
                    {
                        if (i % cpnFrequency == 0)
                        {
                            DateTime nextDt = DateUtils.AddMonths(nextCpnDate, i);
                            monthsToNextCpn = DateUtils.MonthsDiff(settleDate, nextDt) + 1;
                            daysToNextCpn = DateUtils.DaysDiff(settleDate, nextDt);
                            cashflow += parAmt * currentCpn / 12.0;

                            period++;
                            cashFlows.Period[period] = monthsToNextCpn;
                            cashFlows.TimeInYears[period] = daysToNextCpn / 365.0;
                            cashFlows.CashFlow[period] = cashflow;
                            cashFlows.AccrualStartDate[period] = previousCpnDate;
                            cashFlows.AccrualEndDate[period] = nextDt;
                            cashFlows.Coupon[period] = currentCpn;
                            previousCpnDate = nextDt;

                            cashflow = 0; //reset cashflow
                        }
                        else
                        {
                            cashflow += parAmt * currentCpn / 12.0; //accrued coupon between cashflow dates
                        }
                    }

                    //last cashflow
                    stubDays = DateUtils.DaysDiff(previousCpnDate, maturityDate);
                    monthsToNextCpn = DateUtils.MonthsDiff(settleDate, maturityDate) + 1;
                    daysToNextCpn = DateUtils.DaysDiff(settleDate, security.MaturityDt);
                    cashflow = parAmt * currentCpn * (stubDays / 365.0); // cashflow from last coupon date to maturity date
                    cashflow += parAmt; // cashflow at maturity

                    period++;
                    cashFlows.Period[period] = monthsToNextCpn;
                    cashFlows.TimeInYears[period] = daysToNextCpn / 365.0;
                    cashFlows.CashFlow[period] = cashflow;
                    cashFlows.AccrualStartDate[period] = previousCpnDate;
                    cashFlows.AccrualEndDate[period] = maturityDate;
                    cashFlows.Coupon[period] = currentCpn;
                }

                //populate cashflows
                securityCashFlows.AccDays = accruedDays;
                securityCashFlows.StubDays = stubDays;
                securityCashFlows.AccCpn = accruedCpn;
                securityCashFlows.StubCpn = stubCpn;
                securityCashFlows.SettleDate = settleDate;
                securityCashFlows.CashFlows = cashFlows;
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error generating cashflows for security: " + security.BBGId);
            }
        }


        /**
		 * Calculate cashflows for floating rate securities
		 * 
		 * TODO: implement day count convention as per security details 
		 */
        private void FloatingRateCashFlowGeneratorPerpetual(PfdSecurity security, DateTime settleDate, PfdSecurityCashFlows securityCashFlows, IDictionary<string, IRCurve> rateCurveDict, string scenarioName)
        {
            try
            {
                //if (security.BBGId.Equals("Alt0004", StringComparison.CurrentCultureIgnoreCase))
                //{
                //	_logger.LogDebug("Security Id: " + security.BBGId);
                //}

                //calculate coupon rates based on reset index and reset frequency
                IRCurve interestRateCurve = rateCurveDict[securityCashFlows.RateIndexCurveMap.RateResetCurveName];
                RateReset rateReset = CalculateRateResetSchedule(security, settleDate, interestRateCurve, securityCashFlows);

                int cpnFrequency = 12 / security.CpnFreq.GetValueOrDefault();
                double currentCpn = security.Cpn.GetValueOrDefault() / 100.0;
                DateTime previousCpnDate = security.PrevCpnDt.GetValueOrDefault();
                DateTime nextCpnDate = security.NxtCpnDt.GetValueOrDefault();
                double parAmt = security.Par.GetValueOrDefault();

                //calculate next coupon date
                //TODO: check if the below condition should be less than or less than equal to
                if (nextCpnDate.Date < settleDate.Date)
                    nextCpnDate = CalculateNextDate(settleDate, nextCpnDate, cpnFrequency);

                int stubDays = DateUtils.DaysDiff(settleDate, nextCpnDate);
                int monthsToNextCpn = DateUtils.MonthsDiff(settleDate, nextCpnDate) + 1;
                double stubCpn = parAmt * currentCpn * (stubDays / 365.0);

                //populate accrued coupon
                int accruedDays = security.IntAccDays.GetValueOrDefault();
                double accruedCpn = security.IntAcc.GetValueOrDefault();
                security.CalcIntAcc = accruedCpn;

                int daysToNextCpn = 0;
                int period = 0;
                double cashflow = 0;

                //add known cashflow (accrued coupon)
                int numPeriods = NUM_MONTHS;
                int numCashflows = Convert.ToInt32(numPeriods / cpnFrequency);

                CashFlows cashFlows = new CashFlows(numCashflows + 1);
                cashFlows.Period[period] = monthsToNextCpn;
                cashFlows.TimeInYears[period] = stubDays / 365.0;
                cashFlows.CashFlow[period] = stubCpn;
                cashFlows.AccrualStartDate[period] = settleDate;
                cashFlows.AccrualEndDate[period] = nextCpnDate;
                cashFlows.Coupon[period] = currentCpn;
                cashFlows.ResetDate[period] = null;
                cashFlows.ForwardRate[period] = rateReset.ForwardRate[period];
                cashFlows.AdjForwardRate[period] = rateReset.AdjForwardRate[period];

                //populate future cashflows
                previousCpnDate = nextCpnDate;
                int resetDay = security.DerivedNextResetDate.Day;
                double cpnRate = currentCpn;
                int rateLookupPeriod = 0;
                for (int i = 1; i <= numPeriods; i++)
                {
                    if (i % cpnFrequency == 0)
                    {
                        DateTime nextDate = DateUtils.AddMonths(nextCpnDate, i);
                        monthsToNextCpn = DateUtils.MonthsDiff(settleDate, nextDate) + 1;
                        daysToNextCpn = DateUtils.DaysDiff(settleDate, nextDate);
                        cashflow += parAmt * cpnRate * (resetDay / 365.0);

                        rateLookupPeriod = (monthsToNextCpn - 1) >= NUM_MONTHS ? NUM_MONTHS - 1 : (monthsToNextCpn - 1);
                        cpnRate = rateReset.CouponResetRate[rateLookupPeriod];

                        period++;
                        cashFlows.Period[period] = monthsToNextCpn;
                        cashFlows.TimeInYears[period] = daysToNextCpn / 365.0;
                        cashFlows.CashFlow[period] = cashflow;
                        cashFlows.AccrualStartDate[period] = previousCpnDate;
                        cashFlows.AccrualEndDate[period] = nextDate;
                        cashFlows.Coupon[period] = cpnRate;
                        cashFlows.ResetDate[period] = rateReset.CouponResetDate[rateLookupPeriod];
                        cashFlows.ForwardRate[period] = rateReset.ForwardRate[rateLookupPeriod];
                        cashFlows.AdjForwardRate[period] = rateReset.AdjForwardRate[rateLookupPeriod];
                        previousCpnDate = nextDate;

                        //accrued coupon
                        int daysInMonth = DateTime.DaysInMonth(nextDate.Year, nextDate.Month);
                        int tailDays = daysInMonth - resetDay;
                        cashflow = parAmt * cpnRate * (tailDays / 365.0);
                    }
                    else
                    {
                        cashflow += parAmt * cpnRate / 12.0; //accrued coupon between cash flow dates
                        cashFlows.ResetDate[period] = rateReset.CouponResetDate[i];
                    }
                }

                //populate cashflows
                if (scenarioName.Equals(GlobalConstants.RATE_SCENARIO_BASE, StringComparison.CurrentCultureIgnoreCase))
                {
                    securityCashFlows.AccDays = accruedDays;
                    securityCashFlows.StubDays = stubDays;
                    securityCashFlows.AccCpn = accruedCpn;
                    securityCashFlows.StubCpn = stubCpn;
                    securityCashFlows.SettleDate = settleDate;
                    securityCashFlows.CashFlows = cashFlows;
                }
                else if (scenarioName.Equals(GlobalConstants.RATE_SCENARIO_UP, StringComparison.CurrentCultureIgnoreCase))
                {
                    securityCashFlows.CashFlowsRateUpScenario = cashFlows;
                }
                else if (scenarioName.Equals(GlobalConstants.RATE_SCENARIO_DOWN, StringComparison.CurrentCultureIgnoreCase))
                {
                    securityCashFlows.CashFlowsRateDownScenario = cashFlows;
                }
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error generating cashflows for security: " + security.BBGId);
            }
        }

        /**
		 * Calculate cashflows for floating rate securities
		 * TODO: implement day count convention as per security details
		 */
        private void FloatingRateCashFlowGeneratorWithMaturity(PfdSecurity security, DateTime settleDate, PfdSecurityCashFlows securityCashFlows, IDictionary<string, IRCurve> rateCurveDict, string scenarioName)
        {
            try
            {
                //if (security.BBGId.Equals("Alt0004", StringComparison.CurrentCultureIgnoreCase))
                //{
                //    _logger.LogDebug("Security Id: " + security.BBGId);
                //}

                //calculate coupon rates based on reset index and reset frequency
                IRCurve interestRateCurve = rateCurveDict[securityCashFlows.RateIndexCurveMap.RateResetCurveName];
                RateReset rateReset = CalculateRateResetSchedule(security, settleDate, interestRateCurve, securityCashFlows);

                int cpnFrequency = 12 / security.CpnFreq.GetValueOrDefault();
                double currentCpn = security.Cpn.GetValueOrDefault() / 100.0;
                DateTime previousCpnDate = security.PrevCpnDt.GetValueOrDefault();
                DateTime nextCpnDate = security.NxtCpnDt.GetValueOrDefault();
                double parAmt = security.Par.GetValueOrDefault();
                DateTime maturityDate = security.MaturityDt.GetValueOrDefault();

                //populate accrued coupon
                int accruedDays = security.IntAccDays.GetValueOrDefault();
                double accruedCpn = security.IntAcc.GetValueOrDefault();
                security.CalcIntAcc = accruedCpn;

                //calculate next coupon date
                //TODO: check if the below condition should be less than or less than equal to
                if (nextCpnDate.Date < settleDate.Date)
                    nextCpnDate = CalculateNextDate(settleDate, nextCpnDate, cpnFrequency);

                if (nextCpnDate.Date > maturityDate.Date)
                    nextCpnDate = maturityDate;

                //identify number of cashflow periods
                int numCashflows = 0;
                DateTime nextCashflowDate = settleDate;
                while (nextCashflowDate <= maturityDate)
                {
                    numCashflows++;
                    nextCashflowDate = DateUtils.AddMonths(nextCashflowDate, cpnFrequency);
                    if ((nextCashflowDate.Year == maturityDate.Year) && (nextCashflowDate.Month == maturityDate.Month))
                        break;
                }

                int numPeriods = DateUtils.MonthsDiff(settleDate, security.MaturityDt.GetValueOrDefault());
                numPeriods = numPeriods < 1 ? 1 : numPeriods;

                int daysToNextCpn = 0;
                int period = 0;
                int stubDays = 0;
                int monthsToNextCpn = 0;
                double cashflow = 0;
                double stubCpn = 0;

                //populate cash flows
                CashFlows cashFlows = null;
                if (numCashflows == 1)
                {
                    cashFlows = new CashFlows(numCashflows);

                    stubDays = DateUtils.DaysDiff(settleDate, nextCpnDate);
                    monthsToNextCpn = DateUtils.MonthsDiff(settleDate, nextCpnDate) + 1;
                    stubCpn = parAmt * currentCpn * (stubDays / 365.0);

                    cashFlows.Period[period] = monthsToNextCpn;
                    cashFlows.TimeInYears[period] = stubDays / 365.0;
                    cashFlows.CashFlow[period] = stubCpn;
                    cashFlows.AccrualStartDate[period] = settleDate;
                    cashFlows.AccrualEndDate[period] = nextCpnDate;
                    cashFlows.Coupon[period] = currentCpn;
                    cashFlows.ResetDate[period] = null;
                    cashFlows.ForwardRate[period] = rateReset.ForwardRate[period];
                    cashFlows.AdjForwardRate[period] = rateReset.AdjForwardRate[period];
                }
                else
                {
                    stubDays = DateUtils.DaysDiff(settleDate, nextCpnDate);
                    monthsToNextCpn = DateUtils.MonthsDiff(settleDate, nextCpnDate) + 1;
                    stubCpn = parAmt * currentCpn * (stubDays / 365.0);

                    cashFlows = new CashFlows(numCashflows + 1);
                    cashFlows.Period[period] = monthsToNextCpn;
                    cashFlows.TimeInYears[period] = stubDays / 365.0;
                    cashFlows.CashFlow[period] = stubCpn;
                    cashFlows.AccrualStartDate[period] = settleDate;
                    cashFlows.AccrualEndDate[period] = nextCpnDate;
                    cashFlows.Coupon[period] = currentCpn;
                    cashFlows.ResetDate[period] = null;
                    cashFlows.ForwardRate[period] = rateReset.ForwardRate[period];
                    cashFlows.AdjForwardRate[period] = rateReset.AdjForwardRate[period];

                    //populate future cashflows
                    previousCpnDate = nextCpnDate;
                    int resetDay = security.DerivedNextResetDate.Day;
                    double cpnRate = currentCpn;
                    int rateLookupPeriod = 0;
                    for (int i = 1; i < numPeriods; i++)
                    {
                        if (i % cpnFrequency == 0)
                        {
                            DateTime nextDate = DateUtils.AddMonths(nextCpnDate, i);
                            monthsToNextCpn = DateUtils.MonthsDiff(settleDate, nextDate) + 1;
                            daysToNextCpn = DateUtils.DaysDiff(settleDate, nextDate);
                            cashflow += parAmt * cpnRate * (resetDay / 365.0);

                            rateLookupPeriod = (monthsToNextCpn - 1) >= NUM_MONTHS ? NUM_MONTHS - 1 : (monthsToNextCpn - 1);
                            cpnRate = rateReset.CouponResetRate[rateLookupPeriod];

                            period++;
                            cashFlows.Period[period] = monthsToNextCpn;
                            cashFlows.TimeInYears[period] = daysToNextCpn / 365.0;
                            cashFlows.CashFlow[period] = cashflow;
                            cashFlows.AccrualStartDate[period] = previousCpnDate;
                            cashFlows.AccrualEndDate[period] = nextDate;
                            cashFlows.Coupon[period] = cpnRate;
                            cashFlows.ResetDate[period] = rateReset.CouponResetDate[rateLookupPeriod];
                            cashFlows.ForwardRate[period] = rateReset.ForwardRate[rateLookupPeriod];
                            cashFlows.AdjForwardRate[period] = rateReset.AdjForwardRate[rateLookupPeriod];

                            previousCpnDate = nextDate;

                            //accrued coupon
                            int daysInMonth = DateTime.DaysInMonth(nextDate.Year, nextDate.Month);
                            int tailDays = daysInMonth - resetDay;
                            cashflow = parAmt * cpnRate * (tailDays / 365.0);
                        }
                        else
                        {
                            cashflow += parAmt * cpnRate / 12.0; //accrued coupon between cash flow dates
                            cashFlows.ResetDate[period] = rateReset.CouponResetDate[i];
                        }
                    }

                    //last cashflow
                    stubDays = DateUtils.DaysDiff(previousCpnDate, maturityDate);
                    monthsToNextCpn = DateUtils.MonthsDiff(settleDate, maturityDate) + 1;
                    daysToNextCpn = DateUtils.DaysDiff(settleDate, security.MaturityDt);
                    cashflow = parAmt * currentCpn * (stubDays / 365.0); // cashflow from last coupon date to maturity date
                    cashflow += parAmt; // cashflow at maturity

                    period++;
                    cashFlows.Period[period] = monthsToNextCpn;
                    cashFlows.TimeInYears[period] = daysToNextCpn / 365.0;
                    cashFlows.CashFlow[period] = cashflow;
                    cashFlows.AccrualStartDate[period] = previousCpnDate;
                    cashFlows.AccrualEndDate[period] = maturityDate;
                    cashFlows.Coupon[period] = currentCpn;
                }

                //populate cashflows
                if (scenarioName.Equals(GlobalConstants.RATE_SCENARIO_BASE, StringComparison.CurrentCultureIgnoreCase))
                {
                    securityCashFlows.AccDays = accruedDays;
                    securityCashFlows.StubDays = stubDays;
                    securityCashFlows.AccCpn = accruedCpn;
                    securityCashFlows.StubCpn = stubCpn;
                    securityCashFlows.SettleDate = settleDate;
                    securityCashFlows.CashFlows = cashFlows;
                }
                else if (scenarioName.Equals(GlobalConstants.RATE_SCENARIO_UP, StringComparison.CurrentCultureIgnoreCase))
                {
                    securityCashFlows.CashFlowsRateUpScenario = cashFlows;
                }
                else if (scenarioName.Equals(GlobalConstants.RATE_SCENARIO_DOWN, StringComparison.CurrentCultureIgnoreCase))
                {
                    securityCashFlows.CashFlowsRateDownScenario = cashFlows;
                }
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error generating cashflows for security: " + security.BBGId);
            }
        }

        /**
		 * Calculate cashflows for floating rate securities
		 * TODO: implement day count convention as per security details
		 */
        private void FloatingRateExchangeableCashFlowGenerator(PfdSecurity security, DateTime settleDate, PfdSecurityCashFlows securityCashFlows, IDictionary<string, IRCurve> rateCurveDict, string scenarioName)
        {
            try
            {
                //if (security.BBGId.Equals("EP0052787", StringComparison.CurrentCultureIgnoreCase))
                //{
                //	_logger.LogDebug("Security Id: " + security.BBGId);
                //}

                //calculate coupon rates based on reset index and reset frequency
                IRCurve interestRateCurve = rateCurveDict[securityCashFlows.RateIndexCurveMap.RateResetCurveName];
                RateReset rateReset = CalculateRateResetSchedule(security, settleDate, interestRateCurve, securityCashFlows);

                //calculate coupon rates based on reset index and reset frequency for exchangeable security
                RateReset exchRateReset = rateReset;
                if (security.IsExchangeable)
                {
                    IRCurve exchInterestRateCurve = rateCurveDict[securityCashFlows.ExchangeableRateIndexCurveMap.RateResetCurveName];
                    exchRateReset = CalculateExchangeRateResetSchedule(security, settleDate, exchInterestRateCurve, securityCashFlows);
                }

                int cpnFrequency = 12 / security.CpnFreq.GetValueOrDefault();
                DateTime previousCpnDate = security.PrevCpnDt.GetValueOrDefault();
                DateTime nextCpnDate = security.NxtCpnDt.GetValueOrDefault();
                double parAmt = security.Par.GetValueOrDefault();
                double currentCpn = security.Cpn.GetValueOrDefault() / 100.0;
                double endingParAmt = 0;

                //calculate next coupon date
                //TODO: check if the below condition should be less than or less than equal to
                if (nextCpnDate.Date < settleDate.Date)
                    nextCpnDate = CalculateNextDate(settleDate, nextCpnDate, cpnFrequency);

                int stubDays = DateUtils.DaysDiff(settleDate, nextCpnDate);
                int monthsToNextCpn = DateUtils.MonthsDiff(settleDate, nextCpnDate) + 1;
                int accruedDays = security.IntAccDays.GetValueOrDefault();
                double accruedCpn = security.IntAcc.GetValueOrDefault();
                double stubCpn = parAmt * currentCpn * (stubDays / 365.0);

                int numPeriods = NUM_MONTHS;
                if (security.MaturityDt.HasValue)
                {
                    numPeriods = DateUtils.MonthsDiff(nextCpnDate, security.MaturityDt.GetValueOrDefault());
                    numPeriods = numPeriods < 1 ? 1 : numPeriods;
                    endingParAmt = parAmt;
                }

                int daysToNextCpn = 0;
                int period = 0;
                int lastCashFlowPeriod = 0;
                double cashflow = 0;

                //add known cashflow (accrued coupon)
                int numCashflows = Convert.ToInt32(numPeriods / cpnFrequency);

                CashFlows cashFlows = new CashFlows(numCashflows + 1);
                cashFlows.Period[period] = monthsToNextCpn;
                cashFlows.TimeInYears[period] = stubDays / 365.0;
                cashFlows.CashFlow[period] = stubCpn;
                cashFlows.AccrualStartDate[period] = settleDate;
                cashFlows.AccrualEndDate[period] = nextCpnDate;
                cashFlows.Coupon[period] = currentCpn;
                cashFlows.ResetDate[period] = null;
                cashFlows.ForwardRate[period] = rateReset.ForwardRate[period];
                cashFlows.AdjForwardRate[period] = rateReset.AdjForwardRate[period];

                //populate future cashflows
                int resetDay = security.DerivedNextResetDate.Day;
                double cpnRate = currentCpn;
                Nullable<DateTime> resetDate = nextCpnDate;

                double forwardRate = 0, adjForwardRate = 0;
                int rateLookupPeriod = 0;
                for (int i = 1; i <= numPeriods; i++)
                {
                    if (i % cpnFrequency == 0)
                    {
                        DateTime nextDate = DateUtils.AddMonths(nextCpnDate, i);
                        monthsToNextCpn = DateUtils.MonthsDiff(settleDate, nextDate) + 1;
                        daysToNextCpn = DateUtils.DaysDiff(settleDate, nextDate);

                        rateLookupPeriod = (monthsToNextCpn - 1) >= NUM_MONTHS ? NUM_MONTHS - 1 : (monthsToNextCpn - 1);
                        //if (nextDate.Date <= security.NxtExchDt.GetValueOrDefault().Date)
                        if (nextDate.Date < security.NxtExchDt.GetValueOrDefault().Date)
                        {
                            cpnRate = rateReset.CouponResetRate[rateLookupPeriod];
                            resetDate = rateReset.CouponResetDate[rateLookupPeriod];
                            forwardRate = rateReset.ForwardRate[rateLookupPeriod];
                            adjForwardRate = rateReset.AdjForwardRate[rateLookupPeriod];
                        }
                        else
                        {
                            cpnRate = exchRateReset.CouponResetRate[rateLookupPeriod];
                            resetDate = exchRateReset.CouponResetDate[rateLookupPeriod];
                            forwardRate = exchRateReset.ForwardRate[rateLookupPeriod];
                            adjForwardRate = exchRateReset.AdjForwardRate[rateLookupPeriod];
                        }

                        cashflow += parAmt * cpnRate * (resetDay / 365.0);

                        period++;
                        cashFlows.Period[period] = monthsToNextCpn;
                        cashFlows.TimeInYears[period] = daysToNextCpn / 365.0;
                        cashFlows.CashFlow[period] = cashflow;
                        cashFlows.AccrualStartDate[period] = previousCpnDate;
                        cashFlows.AccrualEndDate[period] = nextDate;
                        cashFlows.Coupon[period] = cpnRate;
                        cashFlows.ResetDate[period] = resetDate;
                        cashFlows.ForwardRate[period] = forwardRate;
                        cashFlows.AdjForwardRate[period] = adjForwardRate;
                        previousCpnDate = nextDate;
                        lastCashFlowPeriod = period;

                        //accrued coupon between cash flow dates
                        int daysInMonth = DateTime.DaysInMonth(nextDate.Year, nextDate.Month);
                        int tailDays = daysInMonth - resetDay;
                        cashflow = parAmt * cpnRate * (tailDays / 365.0);
                    }
                    else
                    {
                        cashflow += parAmt * cpnRate / 12.0; //accrued coupon
                    }
                }

                //add ending par in case of scheduled maturity
                cashFlows.CashFlow[lastCashFlowPeriod] += endingParAmt;

                //populate cashflows
                //securityCashFlows.AccDays = accruedDays;
                //securityCashFlows.StubDays = stubDays;
                //securityCashFlows.AccCpn = accruedCpn;
                //securityCashFlows.StubCpn = stubCpn;
                //securityCashFlows.SettleDate = settleDate;

                if (scenarioName.Equals(GlobalConstants.RATE_SCENARIO_BASE, StringComparison.CurrentCultureIgnoreCase))
                {
                    securityCashFlows.ExchangeableCashFlows = cashFlows;
                }
                else if (scenarioName.Equals(GlobalConstants.RATE_SCENARIO_UP, StringComparison.CurrentCultureIgnoreCase))
                {
                    securityCashFlows.ExchangeableCashFlowsRateUpScenario = cashFlows;
                }
                else if (scenarioName.Equals(GlobalConstants.RATE_SCENARIO_DOWN, StringComparison.CurrentCultureIgnoreCase))
                {
                    securityCashFlows.ExchangeableCashFlowsRateDownScenario = cashFlows;
                }
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error generating cashflows for exchangeable security: " + security.BBGId);
            }
        }

        /*
		 * Calculates coupon rates based on the rate reset schedule
		 * If proxy index is specified, then proxy index curve is used
		 * The curve is further adjusted based on adjustment factors
		 * Beta/Multiplier - applied to base curve
		 * Margin - margin added to base curve
		 * Ex:- { "canprm 1mo", new RateIndexCurveMap( "cadswap", 1, "cadswap", 1, 0.0183 )}
		 */
        private RateReset CalculateRateResetSchedule(PfdSecurity security, DateTime settleDate, IRCurve interestRateCurve, PfdSecurityCashFlows securityCashFlows)
        {
            RateReset rateReset = new RateReset(NUM_MONTHS + 1);

            try
            {
                double cpnRate = security.Cpn.GetValueOrDefault() / 100.0;
                double resetMultiplier = security.Multiplier.GetValueOrDefault();
                double resetMargin = security.FloatSpread.GetValueOrDefault() / 10000.0;
                int resetFrequency = security.RefixFreq.GetValueOrDefault();
                DateTime nextResetDate = security.NextRefixDt.GetValueOrDefault();

                //if next reset date is less than settle date then get the next reset date based on reset frequency
                //TODO: check the condition when next reset date is on same day as settle date
                if (nextResetDate.Date < settleDate.Date)
                {
                    nextResetDate = CalculateNextDate(settleDate, nextResetDate, resetFrequency);
                    security.DerivedNextResetDate = nextResetDate;
                }

                //get reset day
                DateTime nextMonth = DateUtils.AddMonths(DateTime.Now, 1);
                DateTime nextCpnDate = new DateTime(nextMonth.Year, nextMonth.Month, 1);
                nextCpnDate = DateUtils.AddDays(nextCpnDate, nextResetDate.Day - 1);

                //get forward curve
                int forwardCurveTenor = securityCashFlows.RateIndexCurveMap.RateResetCurveTenor.GetValueOrDefault();
                double[] forwardCurve = new double[interestRateCurve.Size];
                if (forwardCurveTenor == 1)
                    forwardCurve = interestRateCurve.ForwardRate1M;
                else if (forwardCurveTenor == 3)
                    forwardCurve = interestRateCurve.ForwardRate3M;
                else if (forwardCurveTenor == 5)
                    forwardCurve = interestRateCurve.ForwardRate5YR;

                //use index proxy if specified
                RateIndexProxy indexProxy = securityCashFlows.RateIndexProxy;

                double resetCpnRate = cpnRate;
                rateReset.CouponResetRate[0] = resetCpnRate;
                rateReset.CouponResetDate[0] = null;
                rateReset.ForwardRate[0] = forwardCurve[0];
                rateReset.AdjForwardRate[0] = forwardCurve[0];

                int period = 0;
                for (int i = 1; i <= NUM_MONTHS; i++)
                {
                    period = i >= NUM_MONTHS ? NUM_MONTHS - 1 : i;

                    double forwardRate = forwardCurve[period];
                    double adjForwardRate = forwardRate;
                    if (indexProxy != null)
                        adjForwardRate = (adjForwardRate * indexProxy.Multiplier.GetValueOrDefault()) + (indexProxy.Margin.GetValueOrDefault() / 10000.0);
                    adjForwardRate = (adjForwardRate * securityCashFlows.RateIndexCurveMap.Beta.GetValueOrDefault()) + (securityCashFlows.RateIndexCurveMap.Margin.GetValueOrDefault());

                    rateReset.CouponResetDate[i] = null;
                    rateReset.ForwardRate[i] = forwardRate;
                    rateReset.AdjForwardRate[i] = adjForwardRate;

                    int monthsDiff = DateUtils.MonthsDiff(nextResetDate, nextCpnDate);
                    if (monthsDiff % resetFrequency == 0)
                    {
                        if (CheckForResetDate(nextCpnDate, nextResetDate))
                        {
                            if (nextCpnDate.Month < nextResetDate.Month && nextCpnDate.Year < nextResetDate.Year)
                                _logger.LogDebug("Month and Year Mismatch => " + security.BBGId + " ; " + nextResetDate + " ; " + nextCpnDate);

                            if (nextCpnDate.Year == nextResetDate.Year && nextCpnDate.Month < nextResetDate.Month)
                                _logger.LogDebug("Month Mismatch => " + security.BBGId + " ; " + nextResetDate + " ; " + nextCpnDate);

                            if (nextCpnDate.Year < nextResetDate.Year && nextCpnDate.Month == nextResetDate.Month)
                                _logger.LogDebug("Year Mismatch => " + security.BBGId + " ; " + nextResetDate + " ; " + nextCpnDate);

                            resetCpnRate = forwardRate;

                            //adjust for index proxy
                            if (indexProxy != null)
                                resetCpnRate = (resetCpnRate * indexProxy.Multiplier.GetValueOrDefault()) + (indexProxy.Margin.GetValueOrDefault() / 10000.0);

                            //adjust for index beta and margin
                            resetCpnRate = (resetCpnRate * securityCashFlows.RateIndexCurveMap.Beta.GetValueOrDefault()) + (securityCashFlows.RateIndexCurveMap.Margin.GetValueOrDefault());

                            //adjust for rate reset
                            resetCpnRate = (resetCpnRate * resetMultiplier) + resetMargin;
                            rateReset.CouponResetDate[i] = nextCpnDate;
                        }
                    }

                    rateReset.CouponResetRate[i] = resetCpnRate;
                    nextCpnDate = DateUtils.AddMonths(nextCpnDate, 1);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error calculating cash flow schedule for security: " + security.BBGId);
            }

            return rateReset;
        }

        /*
		 * Calculates coupon rates based on rate reset schedule for exchangeable securities
		 * 
		 */
        private RateReset CalculateExchangeRateResetSchedule(PfdSecurity security, DateTime settleDate, IRCurve interestRateCurve, PfdSecurityCashFlows securityCashFlows)
        {
            RateReset rateReset = new RateReset(NUM_MONTHS + 1);

            try
            {
                double cpnRate = 0;
                double resetMultiplier = 0;
                double resetMargin = 0;
                int resetFrequency = 0;

                //TODO: check if exchange coupon or original coupon should be used
                if (security.ExchCpn.HasValue)
                    cpnRate = security.ExchCpn.GetValueOrDefault() / 100.0;
                else
                    cpnRate = security.Cpn.GetValueOrDefault() / 100.0;

                if (security.ExchMultiplier.HasValue)
                    resetMultiplier = security.ExchMultiplier.GetValueOrDefault();
                else
                    resetMultiplier = security.Multiplier.GetValueOrDefault();

                if (security.ExchSpread.HasValue)
                    resetMargin = security.ExchSpread.GetValueOrDefault() / 10000.0;

                if (security.ExchRefixFreq.HasValue)
                    resetFrequency = security.ExchRefixFreq.GetValueOrDefault();
                else
                    resetFrequency = security.RefixFreq.GetValueOrDefault();

                //if next reset date is less than settle date then get the next reset date based on reset frequency
                //TODO: check the condition when next reset rate is on settle date
                DateTime nextResetDate = security.NxtExchDt.GetValueOrDefault();
                if (nextResetDate.Date < settleDate.Date)
                    nextResetDate = CalculateNextDate(settleDate, nextResetDate, security.ExchRefixFreq.GetValueOrDefault());

                //get reset day
                DateTime nextMonth = DateUtils.AddMonths(DateTime.Now, 1);
                DateTime nextCpnDate = new DateTime(nextMonth.Year, nextMonth.Month, 1);
                nextCpnDate = DateUtils.AddDays(nextCpnDate, nextResetDate.Day - 1);

                //get forward curve
                int forwardCurveTenor = securityCashFlows.ExchangeableRateIndexCurveMap.RateResetCurveTenor.GetValueOrDefault();
                double[] forwardCurve = new double[interestRateCurve.Size];
                if (forwardCurveTenor == 1)
                    forwardCurve = interestRateCurve.ForwardRate1M;
                else if (forwardCurveTenor == 3)
                    forwardCurve = interestRateCurve.ForwardRate3M;
                else if (forwardCurveTenor == 5)
                    forwardCurve = interestRateCurve.ForwardRate5YR;

                //index proxy
                RateIndexProxy indexProxy = securityCashFlows.ExchangeableRateIndexProxy;

                double resetCpnRate = cpnRate;
                rateReset.CouponResetRate[0] = resetCpnRate;
                rateReset.CouponResetDate[0] = nextCpnDate;

                int period = 0;
                for (int i = 1; i <= NUM_MONTHS; i++)
                {
                    period = i >= NUM_MONTHS ? NUM_MONTHS - 1 : i;

                    double adjForwardRate = forwardCurve[period];
                    if (indexProxy != null)
                        adjForwardRate = (adjForwardRate * indexProxy.Multiplier.GetValueOrDefault()) + (indexProxy.Margin.GetValueOrDefault() / 10000.0);
                    adjForwardRate = (adjForwardRate * securityCashFlows.RateIndexCurveMap.Beta.GetValueOrDefault()) + (securityCashFlows.RateIndexCurveMap.Margin.GetValueOrDefault());

                    rateReset.CouponResetDate[i] = null;
                    rateReset.ForwardRate[i] = forwardCurve[period];
                    rateReset.AdjForwardRate[i] = adjForwardRate;

                    int daysDiff = DateUtils.MonthsDiff(nextResetDate, nextCpnDate);
                    if (daysDiff % resetFrequency == 0)
                    {
                        resetCpnRate = forwardCurve[period];

                        //adjust for index proxy
                        if (indexProxy != null)
                            resetCpnRate = (resetCpnRate * indexProxy.Multiplier.GetValueOrDefault()) + (indexProxy.Margin.GetValueOrDefault() / 10000.0);

                        //adjust for index beta and margin
                        resetCpnRate = (resetCpnRate * securityCashFlows.ExchangeableRateIndexCurveMap.Beta.GetValueOrDefault()) + (securityCashFlows.ExchangeableRateIndexCurveMap.Margin.GetValueOrDefault());

                        //adjust for rate reset
                        resetCpnRate = (resetCpnRate * resetMultiplier) + resetMargin;
                        rateReset.CouponResetDate[i] = nextCpnDate;
                    }
                    rateReset.CouponResetRate[i] = resetCpnRate;
                    nextCpnDate = DateUtils.AddMonths(nextCpnDate, 1);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error calculating rate reset schedule for exchangeable security: " + security.BBGId);
            }

            return rateReset;
        }

        /**
		 * Calculate PV given a set of cashflows, spread and price
		 */
        protected double CalculatePV(CashFlows cashFlows, IRCurve curve, double spread, double price)
        {
            double pv = price;
            double[] discountRates = curve.DiscountRate;
            for (int i = 0; i < cashFlows.Size; i++)
            {
                double cashFlow = cashFlows.CashFlow[i];
                int period = cashFlows.Period[i];
                period = period >= NUM_MONTHS ? NUM_MONTHS - 1 : period;
                double discountRate = discountRates[period] + spread;
                double timeInYears = cashFlows.TimeInYears[i];
                double pvCashFlow = cashFlow * Math.Pow(1 + discountRate, -1 * timeInYears);
                pv += pvCashFlow;
            }

            return pv;
        }

        protected double CalculatePrice(CashFlows cashFlows, double yield)
        {
            double pv = 0;
            for (int i = 0; i < cashFlows.Size; i++)
            {
                double cashFlow = cashFlows.CashFlow[i];
                double discountRate = yield;
                double timeInYears = cashFlows.TimeInYears[i];
                double pvCashFlow = cashFlow * Math.Pow(1 + discountRate, -1 * timeInYears);
                pv += pvCashFlow;
            }

            return pv;
        }

        public double CalculatePriceGivenIRR(string ticker, double irr)
        {
            double pv = 0;
            IDictionary<string, FundForecast> fundForecastDict = _cache.Get<IDictionary<string, FundForecast>>(CacheKeys.FUND_FORECASTS);

            FundForecast fundForecast;
            if (fundForecastDict.TryGetValue(ticker, out fundForecast)
                && fundForecast.LastPrcExclRedExp.HasValue)
            {
                CashFlows cashFlows = new CashFlows(2);
                cashFlows.CashFlow[0] = fundForecast.NavExclRedExp.GetValueOrDefault();
                cashFlows.TimeInYears[0] = fundForecast.DaysToRedPmt.GetValueOrDefault();

                for (int i = 0; i < cashFlows.Size; i++)
                {
                    double cashFlow = cashFlows.CashFlow[i];
                    double discountRate = irr;
                    double timeInYears = cashFlows.TimeInYears[i];
                    double pvCashFlow = cashFlow * Math.Pow(1 + discountRate, -1 * timeInYears);
                    pv += pvCashFlow;
                }
            }

            return pv;
        }

        protected double CalculatePV(CashFlows cashFlows, double yield, double price)
        {
            double pv = price;
            for (int i = 0; i < cashFlows.Size; i++)
            {
                double cashFlow = cashFlows.CashFlow[i];
                double timeInYears = cashFlows.TimeInYears[i];
                double pvCashFlow = cashFlow * Math.Pow(1 + yield, -1 * timeInYears);
                pv += pvCashFlow;
            }

            return pv;
        }

        protected double CalculatePV(CashFlows cashFlows, double yield, double price, double callPrice, DateTime nextCallDate)
        {
            double pv = price;
            double timeInYears, pvCashFlow;
            for (int i = 0; i < cashFlows.Size; i++)
            {
                DateTime cashFlowDate = cashFlows.AccrualEndDate[i];
                if (cashFlowDate <= nextCallDate)
                {
                    double cashFlow = cashFlows.CashFlow[i];
                    timeInYears = cashFlows.TimeInYears[i];
                    pvCashFlow = cashFlow * Math.Pow(1 + yield, -1 * timeInYears);
                    pv += pvCashFlow;
                }
            }

            //add call price
            timeInYears = DateUtils.DaysDiff(settleDate, nextCallDate) / 365.0;
            pvCashFlow = callPrice * Math.Pow(1 + yield, -1 * timeInYears);
            pv += pvCashFlow;

            return pv;
        }

        /**
		 * Calculated spread given a set of cashflows, discount curve and price
		 */
        public double? CalculateSpread(PfdSecurityCashFlows securityCashFlows, IRCurve interestRateCurve, double price, bool isExchangeable)
        {
            double? calculatedSpread = null;

            try
            {
                CashFlows cashFlows = null;

                if (isExchangeable)
                    cashFlows = securityCashFlows.ExchangeableCashFlows;
                else
                    cashFlows = securityCashFlows.CashFlows;

                if (cashFlows != null)
                {
                    //calculate initial spread range
                    double cleanPrice = -(price - securityCashFlows.AccCpn);
                    double cashFlowIn = 0, cashFlowOut = cleanPrice;

                    for (int i = 0; i < cashFlows.Size; i++)
                    {
                        double cashFlow = cashFlows.CashFlow[i];
                        if (cashFlow > 0)
                            cashFlowIn += cashFlow;
                        else
                            cashFlowOut += cashFlow;
                    }

                    double xa = -0.99;
                    double xb = cashFlowIn / (-cashFlowOut);

                    double ya = CalculatePV(cashFlows, interestRateCurve, xa, cleanPrice);
                    double yb = CalculatePV(cashFlows, interestRateCurve, xb, cleanPrice);

                    if ((ya >= 0) == (yb >= 0))
                    {
                        calculatedSpread = null;
                    }
                    else if (ya == 0)
                    {
                        calculatedSpread = xa;
                    }
                    else if (yb == 0)
                    {
                        calculatedSpread = xb;
                    }
                    else
                    {
                        double increment = 0.618;
                        double delta = 0.00001;
                        double inputSpread = 0;
                        double pv = 0;
                        bool solved = false;
                        int iterations = 0;

                        do
                        {
                            iterations++;
                            inputSpread = xa + increment * (xb - xa);
                            pv = CalculatePV(cashFlows, interestRateCurve, inputSpread, cleanPrice);
                            solved = Math.Abs(pv) <= delta ? true : false;

                            if (Math.Sign(pv) == Math.Sign(yb))
                                xb = inputSpread;
                            else
                                xa = inputSpread;
                        } while (!solved && iterations < 10000);

                        if (solved)
                            calculatedSpread = inputSpread;
                    }
                }
            }
            catch (Exception ex)
            {
                //_logger.LogError(ex, "Error calculating spread for security: " + security.BBGId);
            }

            return calculatedSpread;
        }

        /**
		 * Calculate price given cashflows, discount curve and spread
		 */
        public double? CalculatePrice(PfdSecurity security, PfdSecurityCashFlows securityCashFlows, IRCurve interestRateCurve, double spread, bool isExchangeable)
        {
            double? calculatedPrice = null;

            try
            {
                CashFlows cashFlows = null;

                if (isExchangeable)
                    cashFlows = securityCashFlows.ExchangeableCashFlows;
                else
                    cashFlows = securityCashFlows.CashFlows;

                if (cashFlows != null)
                {
                    calculatedPrice = CalculatePV(cashFlows, interestRateCurve, spread, 0d);
                    calculatedPrice += securityCashFlows.AccCpn;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error calculating price for security: " + security.BBGId);
            }

            return calculatedPrice;
        }

        public double? CalculatePriceByScenario(PfdSecurity security, PfdSecurityCashFlows securityCashFlows, IRCurve interestRateCurve, double spread, bool isExchangeable, string scenarioName)
        {
            double? calculatedPrice = null;

            try
            {
                CashFlows cashFlows = null;

                if (isExchangeable)
                {
                    if (scenarioName.Equals(GlobalConstants.RATE_SCENARIO_BASE, StringComparison.CurrentCultureIgnoreCase))
                        cashFlows = securityCashFlows.ExchangeableCashFlows;
                    else if (scenarioName.Equals(GlobalConstants.RATE_SCENARIO_UP, StringComparison.CurrentCultureIgnoreCase))
                        cashFlows = securityCashFlows.ExchangeableCashFlowsRateUpScenario;
                    else if (scenarioName.Equals(GlobalConstants.RATE_SCENARIO_DOWN, StringComparison.CurrentCultureIgnoreCase))
                        cashFlows = securityCashFlows.ExchangeableCashFlowsRateDownScenario;
                }
                else
                {
                    if (!security.IsFloat && security.HasValidData)
                    {
                        cashFlows = securityCashFlows.CashFlows;
                    }
                    else
                    {
                        if (scenarioName.Equals(GlobalConstants.RATE_SCENARIO_BASE, StringComparison.CurrentCultureIgnoreCase))
                            cashFlows = securityCashFlows.CashFlows;
                        else if (scenarioName.Equals(GlobalConstants.RATE_SCENARIO_UP, StringComparison.CurrentCultureIgnoreCase))
                            cashFlows = securityCashFlows.CashFlowsRateUpScenario;
                        else if (scenarioName.Equals(GlobalConstants.RATE_SCENARIO_DOWN, StringComparison.CurrentCultureIgnoreCase))
                            cashFlows = securityCashFlows.CashFlowsRateDownScenario;
                    }
                }

                if (cashFlows != null)
                {
                    calculatedPrice = CalculatePV(cashFlows, interestRateCurve, spread, 0d);
                    calculatedPrice += securityCashFlows.AccCpn;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error calculating price for security: " + security.BBGId);
            }

            return calculatedPrice;
        }

        /**
		 * Calculate price given cashflows, discount curve and spread
		 */
        public double? CalculatePriceGivenYield(PfdSecurity security, PfdSecurityCashFlows securityCashFlows, bool isExchangeable, double yield)
        {
            double? calculatedPrice = null;

            try
            {
                CashFlows cashFlows = null;

                if (isExchangeable)
                    cashFlows = securityCashFlows.ExchangeableCashFlows;
                else
                    cashFlows = securityCashFlows.CashFlows;

                if (cashFlows != null)
                {
                    calculatedPrice = CalculatePrice(cashFlows, yield);
                    calculatedPrice += securityCashFlows.AccCpn;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error calculating price for security: " + security.BBGId);
            }

            return calculatedPrice;
        }

        /**
		 * Calculate next reset date or next coupon date based on reset/coupon frequency
		 */
        protected DateTime CalculateNextDate(DateTime settleDate, DateTime nextDate, int frequency)
        {
            DateTime nextResetDate = nextDate;
            do
            {
                nextResetDate = DateUtils.AddMonths(nextResetDate, frequency);
            } while (nextResetDate < settleDate);

            return nextResetDate;
        }

        /**
		 * Calculated yield given a set of cashflows and price
		 */
        public double? CalculateYield(PfdSecurityCashFlows securityCashFlows, bool isExchangeable, double price)
        {
            double? calculatedYield = null;

            try
            {
                CashFlows cashFlows = null;

                if (isExchangeable)
                    cashFlows = securityCashFlows.ExchangeableCashFlows;
                else
                    cashFlows = securityCashFlows.CashFlows;

                if (cashFlows != null)
                {
                    //calculate initial yield range
                    double cleanPrice = -(price - securityCashFlows.AccCpn);
                    double cashFlowIn = 0, cashFlowOut = cleanPrice;

                    for (int i = 0; i < cashFlows.Size; i++)
                    {
                        double cashFlow = cashFlows.CashFlow[i];
                        if (cashFlow > 0)
                            cashFlowIn += cashFlow;
                        else
                            cashFlowOut += cashFlow;
                    }

                    double xa = -0.99;
                    double xb = cashFlowIn / (-cashFlowOut);

                    double ya = CalculatePV(cashFlows, xa, cleanPrice);
                    double yb = CalculatePV(cashFlows, xb, cleanPrice);

                    if ((ya >= 0) == (yb >= 0))
                    {
                        calculatedYield = null;
                    }
                    else if (ya == 0)
                    {
                        calculatedYield = xa;
                    }
                    else if (yb == 0)
                    {
                        calculatedYield = xb;
                    }
                    else
                    {
                        double increment = 0.618;
                        double delta = 0.00001;
                        double inputYield = 0;
                        double pv = 0;
                        bool solved = false;
                        int iterations = 0;

                        do
                        {
                            iterations++;
                            inputYield = xa + increment * (xb - xa);
                            pv = CalculatePV(cashFlows, inputYield, cleanPrice);
                            solved = Math.Abs(pv) <= delta ? true : false;

                            if (Math.Sign(pv) == Math.Sign(yb))
                                xb = inputYield;
                            else
                                xa = inputYield;
                        } while (!solved && iterations < 10000);

                        if (solved)
                            calculatedYield = inputYield;
                    }
                }
            }
            catch (Exception ex)
            {
                //_logger.LogError(ex, "Error calculating yield for security: " + security.BBGId);
            }

            return calculatedYield;
        }

        public double? CalculateYieldToCall(PfdSecurity security, PfdSecurityCashFlows securityCashFlows, bool isExchangeable, double price)
        {
            double? calculatedYield = null;

            try
            {
                CashFlows cashFlows = null;

                if (isExchangeable)
                    cashFlows = securityCashFlows.ExchangeableCashFlows;
                else
                    cashFlows = securityCashFlows.CashFlows;

                if (cashFlows != null)
                {
                    //calculate initial yield range
                    double cleanPrice = -(price - securityCashFlows.AccCpn);
                    double cashFlowIn = 0, cashFlowOut = cleanPrice;

                    for (int i = 0; i < cashFlows.Size; i++)
                    {
                        double cashFlow = cashFlows.CashFlow[i];
                        if (cashFlow > 0)
                            cashFlowIn += cashFlow;
                        else
                            cashFlowOut += cashFlow;
                    }

                    double xa = -0.99;
                    double xb = cashFlowIn / (-cashFlowOut);

                    double ya = CalculatePV(cashFlows, xa, cleanPrice, security.NxtCallPrice.GetValueOrDefault(), security.NxtCallDt.GetValueOrDefault());
                    double yb = CalculatePV(cashFlows, xb, cleanPrice, security.NxtCallPrice.GetValueOrDefault(), security.NxtCallDt.GetValueOrDefault());

                    if ((ya >= 0) == (yb >= 0))
                    {
                        calculatedYield = null;
                    }
                    else if (ya == 0)
                    {
                        calculatedYield = xa;
                    }
                    else if (yb == 0)
                    {
                        calculatedYield = xb;
                    }
                    else
                    {
                        double increment = 0.618;
                        double delta = 0.00001;
                        double inputYield = 0;
                        double pv = 0;
                        bool solved = false;
                        int iterations = 0;

                        do
                        {
                            iterations++;
                            inputYield = xa + increment * (xb - xa);
                            pv = CalculatePV(cashFlows, inputYield, cleanPrice, security.NxtCallPrice.GetValueOrDefault(), security.NxtCallDt.GetValueOrDefault());
                            solved = Math.Abs(pv) <= delta ? true : false;

                            if (Math.Sign(pv) == Math.Sign(yb))
                                xb = inputYield;
                            else
                                xa = inputYield;
                        } while (!solved && iterations < 10000);

                        if (solved)
                            calculatedYield = inputYield;
                    }
                }
            }
            catch (Exception)
            {
                //_logger.LogError(ex, "Error calculating yield for security: " + security.BBGId);
            }

            return calculatedYield;
        }

        private bool CheckForResetDate(DateTime nextCpnDate, DateTime nextResetDate)
        {
            bool result = true;

            try
            {
                if (nextCpnDate.Year < nextResetDate.Year)
                    return false;

                if (nextCpnDate.Year == nextResetDate.Year && nextCpnDate.Month < nextResetDate.Month)
                    return false;
            }
            catch (Exception)
            {
            }

            return result;
        }
    }
}