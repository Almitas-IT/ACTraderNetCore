using aACTrader.Common;
using aACTrader.DAO.Repository;
using aACTrader.Model;
using aCommons;
using aCommons.Cef;
using aCommons.Pfd;
using aCommons.Utils;
using LazyCache;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;

namespace aACTrader.Operations.Impl
{
    public class PfdCommonOperations
    {
        private readonly ILogger<PfdCommonOperations> _logger;
        private readonly PfdBaseDao _pfdBaseDao;
        private readonly CachingService _cache;
        private readonly CashflowGenerator _cashflowGenerator;

        protected const int NUM_MONTHS = 1200;

        private static readonly DateTime TodaysDate = DateTime.Now.Date;

        public PfdCommonOperations(ILogger<PfdCommonOperations> logger
            , PfdBaseDao pfdBaseDao
            , CachingService cache
            , CashflowGenerator cashflowGenerator)
        {
            _logger = logger;
            _pfdBaseDao = pfdBaseDao;
            _cache = cache;
            _cashflowGenerator = cashflowGenerator;
        }

        public void Start()
        {
            try
            {
                _logger.LogInformation("Starting Pfd Common Operations...");

                _logger.LogInformation("Populating Pfd Security Master...");
                IDictionary<string, PfdSecurity> securityMasterDict = _pfdBaseDao.GetPfdSecurityMaster();
                _cache.Add(CacheKeys.PFD_SECURITY_MASTER, securityMasterDict, DateTimeOffset.MaxValue);

                _logger.LogInformation("Populating Issuer Security Map...");
                PopulateIssuerSecurityMap();

                _logger.LogInformation("Populating Interest Rate Curves...");
                IDictionary<string, IRCurve> baseForwardRateCurveDict = _pfdBaseDao.GetInterestRateCurvesByScenario(GlobalConstants.RATE_SCENARIO_BASE);
                ExtendRateCurves(baseForwardRateCurveDict);
                _cache.Add(CacheKeys.PFD_FWD_RATE_CURVES, baseForwardRateCurveDict, DateTimeOffset.MaxValue);

                IDictionary<string, IRCurve> curveUpForwardRateCurveDict = _pfdBaseDao.GetInterestRateCurvesByScenario(GlobalConstants.RATE_SCENARIO_UP);
                ExtendRateCurves(curveUpForwardRateCurveDict);
                _cache.Add(CacheKeys.PFD_FWD_RATE_CURVES_UP, curveUpForwardRateCurveDict, DateTimeOffset.MaxValue);

                IDictionary<string, IRCurve> curveDownForwardRateCurveDict = _pfdBaseDao.GetInterestRateCurvesByScenario(GlobalConstants.RATE_SCENARIO_DOWN);
                ExtendRateCurves(curveDownForwardRateCurveDict);
                _cache.Add(CacheKeys.PFD_FWD_RATE_CURVES_DOWN, curveDownForwardRateCurveDict, DateTimeOffset.MaxValue);

                _logger.LogInformation("Populating NEW Interest Rate Curves...");
                IDictionary<string, IRCurve> interestRateCurveDict = _pfdBaseDao.GetInterestRateCurvesNew();
                ExtendRateCurves(interestRateCurveDict);
                _cache.Add(CacheKeys.PFD_FWD_RATE_CURVES_NEW, interestRateCurveDict, DateTimeOffset.MaxValue);

                _logger.LogInformation("Populating Index Rate Proxies...");
                IDictionary<string, RateIndexProxy> indexProxyDict = _pfdBaseDao.GetIndexProxies();
                _cache.Add(CacheKeys.PFD_INDEX_PROXIES, indexProxyDict, DateTimeOffset.MaxValue);

                _logger.LogInformation("Populating Spot Rates...");
                IDictionary<string, IDictionary<int, SpotRate>> spotRatesDict = _pfdBaseDao.GetSpotRates();
                _cache.Add(CacheKeys.PFD_SPOT_RATES, spotRatesDict, DateTimeOffset.MaxValue);

                _logger.LogInformation("Populating NEW Spot Rates...");
                IDictionary<string, IDictionary<int, SpotRate>> spotRatesDictNew = _pfdBaseDao.GetSpotRatesNew();
                _cache.Add(CacheKeys.PFD_SPOT_RATES_NEW, spotRatesDictNew, DateTimeOffset.MaxValue);

                _logger.LogInformation("Generating Cashflows...");
                GenerateCashFlows();

                _logger.LogInformation("Populating Ticker Blooomberg Code Map...");
                PopulatePfdTickerBloombergCoderMap();

                _logger.LogInformation("Generated Cashflows... - DONE");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error initializing");
                throw;
            }
        }

        private void PopulateIssuerSecurityMap()
        {
            IDictionary<string, PfdSecurity> securityMasterDict = _cache.Get<IDictionary<string, PfdSecurity>>(CacheKeys.PFD_SECURITY_MASTER);
            IDictionary<string, IList<string>> issuerSecurityDict = new Dictionary<string, IList<string>>();

            foreach (KeyValuePair<string, PfdSecurity> kvp in securityMasterDict)
            {
                PfdSecurity security = kvp.Value;

                if (!issuerSecurityDict.TryGetValue(security.IssuerTicker, out IList<string> securityList))
                {
                    securityList = new List<string>();
                    issuerSecurityDict.Add(security.IssuerTicker, securityList);
                }

                securityList.Add(security.BBGId);
            }

            _cache.Remove(CacheKeys.PFD_ISSUER_SECURITY_MAP);
            _cache.Add(CacheKeys.PFD_ISSUER_SECURITY_MAP, issuerSecurityDict, DateTimeOffset.MaxValue);
        }

        public void UpdatePfdSecurityMaster()
        {
            try
            {
                _logger.LogInformation("Updating Pfd Common Operations...");

                _logger.LogInformation("Populating Pfd Security Master...");
                IDictionary<string, PfdSecurity> securityMasterDict = _pfdBaseDao.GetPfdSecurityMaster();
                _cache.Remove(CacheKeys.PFD_SECURITY_MASTER);
                _cache.Add(CacheKeys.PFD_SECURITY_MASTER, securityMasterDict, DateTimeOffset.MaxValue);

                _logger.LogInformation("Populating Issuer Security Map...");
                PopulateIssuerSecurityMap();

                _logger.LogInformation("Generating Cashflows...");
                GenerateCashFlows();
                _logger.LogInformation("Generated Cashflows...");

                _logger.LogInformation("Populating Pfd Ticker Blooomberg Code Map...");
                PopulatePfdTickerBloombergCoderMap();

                _logger.LogInformation("Updating Pfd Common Operations... - DONE");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error updating pfd security details and cashflows");
                throw;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void UpdatePfdDataCache()
        {
            try
            {
                _logger.LogInformation("Updating Pfd Data Cache...");
                UpdatePfdInterestRateCache();
                GenerateCashFlows();
                _logger.LogInformation("Updating Pfd Data Cache... - DONE");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error Updating Pfd Data Cache", ex);
                throw;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        private void UpdatePfdInterestRateCache()
        {
            _logger.LogInformation("Updating Pfd Interest Rate Curves - STARTED");

            try
            {
                IDictionary<string, IRCurve> baseForwardRateCurveDict = _pfdBaseDao.GetInterestRateCurvesByScenario(GlobalConstants.RATE_SCENARIO_BASE);
                if (baseForwardRateCurveDict != null && baseForwardRateCurveDict.Count > 0)
                {
                    ExtendRateCurves(baseForwardRateCurveDict);
                    _cache.Remove(CacheKeys.PFD_FWD_RATE_CURVES);
                    _cache.Add(CacheKeys.PFD_FWD_RATE_CURVES, baseForwardRateCurveDict, DateTimeOffset.MaxValue);
                }

                IDictionary<string, IRCurve> curveUpForwardRateCurveDict = _pfdBaseDao.GetInterestRateCurvesByScenario(GlobalConstants.RATE_SCENARIO_UP);
                if (curveUpForwardRateCurveDict != null && curveUpForwardRateCurveDict.Count > 0)
                {
                    ExtendRateCurves(curveUpForwardRateCurveDict);
                    _cache.Remove(CacheKeys.PFD_FWD_RATE_CURVES_UP);
                    _cache.Add(CacheKeys.PFD_FWD_RATE_CURVES_UP, curveUpForwardRateCurveDict, DateTimeOffset.MaxValue);
                }

                IDictionary<string, IRCurve> curveDownForwardRateCurveDict = _pfdBaseDao.GetInterestRateCurvesByScenario(GlobalConstants.RATE_SCENARIO_DOWN);
                if (curveDownForwardRateCurveDict != null && curveDownForwardRateCurveDict.Count > 0)
                {
                    ExtendRateCurves(curveDownForwardRateCurveDict);
                    _cache.Remove(CacheKeys.PFD_FWD_RATE_CURVES_DOWN);
                    _cache.Add(CacheKeys.PFD_FWD_RATE_CURVES_DOWN, curveDownForwardRateCurveDict, DateTimeOffset.MaxValue);
                }

                IDictionary<string, IRCurve> interestRateCurveDict = _pfdBaseDao.GetInterestRateCurvesNew();
                if (interestRateCurveDict != null && interestRateCurveDict.Count > 0)
                {
                    ExtendRateCurves(interestRateCurveDict);
                    _cache.Remove(CacheKeys.PFD_FWD_RATE_CURVES_NEW);
                    _cache.Add(CacheKeys.PFD_FWD_RATE_CURVES_NEW, interestRateCurveDict, DateTimeOffset.MaxValue);
                }

                _logger.LogInformation("Updating Pfd Interest Rate Curves - DONE");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error updating Interest rate curves");
            }

            try
            {
                IDictionary<string, IDictionary<int, SpotRate>> spotRatesDict = _pfdBaseDao.GetSpotRates();
                _cache.Remove(CacheKeys.PFD_SPOT_RATES);
                _cache.Add(CacheKeys.PFD_SPOT_RATES, spotRatesDict, DateTimeOffset.MaxValue);

                IDictionary<string, IDictionary<int, SpotRate>> spotRatesDictNew = _pfdBaseDao.GetSpotRatesNew();
                _cache.Remove(CacheKeys.PFD_SPOT_RATES_NEW);
                _cache.Add(CacheKeys.PFD_SPOT_RATES_NEW, spotRatesDictNew, DateTimeOffset.MaxValue);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error updating Spot rates");
            }
        }

        /// <summary>
        /// Generates security cash flow for floating rate securities
        /// The cash flows are pre-computed based on changes to interest rate curves and used to spread/yield/price
        /// </summary>
        /// <param name="cache"></param>
        public void GenerateCashFlows()
        {
            _logger.LogInformation("Updating Pfd Cash Flows - STARTED");
            _cashflowGenerator.GenerateCashFlows();
            _logger.LogInformation("Updating Pfd Cash Flows - DONE");
        }

        /// <summary>
        /// Extends the curve period to max period of 1200 months (basically 100 years)
        /// This is needed to estimate cash flows for perpetual bonds in Canada upto 100 years
        /// Takes the last curve point and uses it for remaining periods
        /// 
        /// TODO:- Ideally the curve should be estimated beyond the last spot rate using some model
        /// </summary>
        /// <param name="forwardRateCurveDict"></param>
        public void ExtendRateCurves(IDictionary<string, IRCurve> forwardRateCurveDict)
        {
            foreach (KeyValuePair<string, IRCurve> kvp in forwardRateCurveDict)
            {
                IRCurve rateCurve = kvp.Value;
                int maxPeriod = rateCurve.Period.Max();
                for (int i = maxPeriod; i < GlobalConstants.NUM_PERIODS; i++)
                {
                    rateCurve.Period[i] = rateCurve.Period[i - 1] + 1;
                    rateCurve.DiscountRate[i] = rateCurve.DiscountRate[i - 1];
                    rateCurve.ForwardRate1M[i] = rateCurve.ForwardRate1M[i - 1];
                    rateCurve.ForwardRate3M[i] = rateCurve.ForwardRate3M[i - 1];
                    rateCurve.ForwardRate5YR[i] = rateCurve.ForwardRate5YR[i - 1];
                }
            }
        }

        public double? Solve(string outputType, string securityId, double inputPrice, double inputSpread, double inputYield, string exchangeable, string scenarioName)
        {
            double? result = null;

            try
            {
                IDictionary<string, PfdSecurity> securityDict = _cache.Get<IDictionary<string, PfdSecurity>>(CacheKeys.PFD_SECURITY_MASTER);
                IDictionary<string, PfdSecurityCashFlows> securityCashFlowsDict = _cache.Get<IDictionary<string, PfdSecurityCashFlows>>(CacheKeys.PFD_SECURITY_CASHFLOWS);
                IDictionary<string, IRCurve> rateCurveDict = _cache.Get<IDictionary<string, IRCurve>>(CacheKeys.PFD_FWD_RATE_CURVES);

                if (securityDict.TryGetValue(securityId, out PfdSecurity security))
                {
                    if (securityCashFlowsDict.TryGetValue(securityId, out PfdSecurityCashFlows securityCashFlows))
                    {
                        if (rateCurveDict.TryGetValue(securityCashFlows.RateIndexCurveMap.DiscountCurveName, out IRCurve interestRateCurve))
                        {
                            if (outputType.Equals("SPREAD", StringComparison.CurrentCultureIgnoreCase))
                            {
                                if ("N".Equals(exchangeable, StringComparison.CurrentCultureIgnoreCase))
                                {
                                    result = _cashflowGenerator.CalculateSpread(securityCashFlows, interestRateCurve, inputPrice, false);
                                }
                                else if ("Y".Equals(exchangeable, StringComparison.CurrentCultureIgnoreCase))
                                {
                                    if (security.IsExchangeable)
                                    {
                                        rateCurveDict.TryGetValue(securityCashFlows.ExchangeableRateIndexCurveMap.DiscountCurveName, out interestRateCurve);
                                        result = _cashflowGenerator.CalculateSpread(securityCashFlows, interestRateCurve, inputPrice, true);
                                    }
                                }
                            }
                            else if (outputType.Equals("PRICE", StringComparison.CurrentCultureIgnoreCase))
                            {
                                if ("N".Equals(exchangeable, StringComparison.CurrentCultureIgnoreCase))
                                {
                                    result = _cashflowGenerator.CalculatePrice(security, securityCashFlows, interestRateCurve, inputSpread, false);
                                }
                                else if ("Y".Equals(exchangeable, StringComparison.CurrentCultureIgnoreCase))
                                {
                                    if (security.IsExchangeable)
                                    {
                                        result = _cashflowGenerator.CalculatePrice(security, securityCashFlows, interestRateCurve, inputSpread, true);
                                    }
                                }
                            }
                            else if (outputType.Equals("YIELD", StringComparison.CurrentCultureIgnoreCase))
                            {
                                if ("N".Equals(exchangeable, StringComparison.CurrentCultureIgnoreCase))
                                {
                                    result = _cashflowGenerator.CalculateYield(securityCashFlows, false, inputPrice);
                                }
                                else if ("Y".Equals(exchangeable, StringComparison.CurrentCultureIgnoreCase))
                                {
                                    if (security.IsExchangeable)
                                    {
                                        result = _cashflowGenerator.CalculateYield(securityCashFlows, true, inputPrice);
                                    }
                                }
                            }
                            else if (outputType.Equals("YIELD_TO_CALL", StringComparison.CurrentCultureIgnoreCase))
                            {
                                if (security.NxtCallDt.HasValue && security.NxtCallPrice.HasValue && security.NxtCallDt > TodaysDate)
                                {
                                    if ("N".Equals(exchangeable, StringComparison.CurrentCultureIgnoreCase))
                                    {
                                        result = _cashflowGenerator.CalculateYieldToCall(security, securityCashFlows, false, inputPrice);
                                    }
                                    else if ("Y".Equals(exchangeable, StringComparison.CurrentCultureIgnoreCase))
                                    {
                                        if (security.IsExchangeable)
                                        {
                                            result = _cashflowGenerator.CalculateYieldToCall(security, securityCashFlows, true, inputPrice);
                                        }
                                    }
                                }
                            }
                            else if (outputType.Equals("YIELD_TO_PRICE", StringComparison.CurrentCultureIgnoreCase))
                            {
                                if ("N".Equals(exchangeable, StringComparison.CurrentCultureIgnoreCase))
                                {
                                    result = _cashflowGenerator.CalculatePriceGivenYield(security, securityCashFlows, false, inputYield);
                                }
                                else if ("Y".Equals(exchangeable, StringComparison.CurrentCultureIgnoreCase))
                                {
                                    if (security.IsExchangeable)
                                    {
                                        result = _cashflowGenerator.CalculatePriceGivenYield(security, securityCashFlows, true, inputYield);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception)
            {
            }

            return result;
        }

        public double? SolveByScenario(string outputType, string securityId, double inputPrice, double inputSpread, double inputYield, string exchangeable, string scenarioName)
        {
            double? result = null;

            try
            {
                IDictionary<string, PfdSecurity> securityDict = _cache.Get<IDictionary<string, PfdSecurity>>(CacheKeys.PFD_SECURITY_MASTER);
                IDictionary<string, PfdSecurityCashFlows> securityCashFlowsDict = _cache.Get<IDictionary<string, PfdSecurityCashFlows>>(CacheKeys.PFD_SECURITY_CASHFLOWS);
                IDictionary<string, IRCurve> rateCurveDict = null;

                if (GlobalConstants.RATE_SCENARIO_BASE.Equals(scenarioName, StringComparison.CurrentCultureIgnoreCase))
                    rateCurveDict = _cache.Get<IDictionary<string, IRCurve>>(CacheKeys.PFD_FWD_RATE_CURVES);
                else if (GlobalConstants.RATE_SCENARIO_UP.Equals(scenarioName, StringComparison.CurrentCultureIgnoreCase))
                    rateCurveDict = _cache.Get<IDictionary<string, IRCurve>>(CacheKeys.PFD_FWD_RATE_CURVES_UP);
                else if (GlobalConstants.RATE_SCENARIO_DOWN.Equals(scenarioName, StringComparison.CurrentCultureIgnoreCase))
                    rateCurveDict = _cache.Get<IDictionary<string, IRCurve>>(CacheKeys.PFD_FWD_RATE_CURVES_DOWN);

                if (securityDict.TryGetValue(securityId, out PfdSecurity security))
                {
                    if (securityCashFlowsDict.TryGetValue(securityId, out PfdSecurityCashFlows securityCashFlows))
                    {
                        if (rateCurveDict.TryGetValue(securityCashFlows.RateIndexCurveMap.DiscountCurveName, out IRCurve interestRateCurve))
                        {
                            if (outputType.Equals("PRICE_BY_SCENARIO", StringComparison.CurrentCultureIgnoreCase))
                            {
                                if ("N".Equals(exchangeable, StringComparison.CurrentCultureIgnoreCase))
                                {
                                    result = _cashflowGenerator.CalculatePriceByScenario(security, securityCashFlows, interestRateCurve, inputSpread, false, scenarioName);
                                }
                                else if ("Y".Equals(exchangeable, StringComparison.CurrentCultureIgnoreCase))
                                {
                                    if (security.IsExchangeable)
                                    {
                                        result = _cashflowGenerator.CalculatePriceByScenario(security, securityCashFlows, interestRateCurve, inputSpread, true, scenarioName);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception)
            {
            }

            return result;
        }

        public Nullable<double> CalculatePriceGivenIRR(string ticker, double irr)
        {
            try
            {
                return _cashflowGenerator.CalculatePriceGivenIRR(ticker, irr);
            }
            catch (Exception)
            {
                return null;
            }
        }

        public IList<PeriodicCashFlow> GetPfdSecurityCashFlows(string ticker, PfdSecurity pfdSecurity, PfdSecurityCashFlows pfdSecurityCashFlows, IDictionary<string, IRCurve> rateCurveDict, CashFlows cashflows, CashFlows exchCashflows)
        {
            IList<PeriodicCashFlow> periodicCashflows = new List<PeriodicCashFlow>();

            if (cashflows != null)
            {
                rateCurveDict.TryGetValue(pfdSecurityCashFlows.RateIndexCurveMap.DiscountCurveName, out IRCurve interestRateCurve);
                double[] discountRates = interestRateCurve.DiscountRate;
                double[] exchDiscountRates = interestRateCurve.DiscountRate;

                if (exchCashflows != null)
                {
                    IRCurve exchInterestRateCurve;
                    rateCurveDict.TryGetValue(pfdSecurityCashFlows.ExchangeableRateIndexCurveMap.DiscountCurveName, out exchInterestRateCurve);
                    exchDiscountRates = exchInterestRateCurve.DiscountRate;
                }

                for (int i = 0; i < cashflows.Size; i++)
                {
                    PeriodicCashFlow cashflow = new PeriodicCashFlow
                    {
                        Period = cashflows.Period[i],
                        TimeInYears = cashflows.TimeInYears[i],
                        CashFlowAmt = cashflows.CashFlow[i],
                        Coupon = cashflows.Coupon[i],
                        AccrualStartDate = DateUtils.ConvertDate(cashflows.AccrualStartDate[i], "yyyy-MM-dd"),
                        AccrualEndDate = DateUtils.ConvertDate(cashflows.AccrualEndDate[i], "yyyy-MM-dd")
                    };
                    cashflow.PaymentDate = cashflow.AccrualEndDate;
                    cashflow.ForwardRate = cashflows.ForwardRate[i];
                    cashflow.AdjForwardRate = cashflows.AdjForwardRate[i];

                    if (cashflows.ResetDate[i] != null)
                        cashflow.ResetDate = DateUtils.ConvertDate(cashflows.ResetDate[i], "yyyy-MM-dd");

                    double cashFlow = cashflows.CashFlow[i];
                    int period = cashflows.Period[i];
                    period = period >= NUM_MONTHS ? NUM_MONTHS - 1 : period;
                    double discountRate = discountRates[period];
                    double timeInYears = cashflows.TimeInYears[i];
                    double discountFactor = Math.Pow(1 + discountRate, -1 * timeInYears);
                    double pvCashFlow = cashFlow * discountFactor;

                    cashflow.ZeroRate = discountRate;
                    cashflow.DiscountFactor = discountFactor;
                    cashflow.PresentValue = pvCashFlow;

                    //cash flow if the bond is called
                    if (pfdSecurity.NxtCallDt.HasValue && cashflows.AccrualEndDate[i] <= pfdSecurity.NxtCallDt)
                        cashflow.CallCashFlowAmt = cashflow.CashFlowAmt;

                    if (pfdSecurity.NxtCallDt.HasValue && DateUtils.CompareMonthAndYear(cashflows.AccrualEndDate[i], pfdSecurity.NxtCallDt.GetValueOrDefault()))
                        cashflow.CallCashFlowAmt += pfdSecurity.NxtCallPrice.GetValueOrDefault();

                    //exchageable cash flows
                    if (exchCashflows != null)
                    {
                        cashflow.ExchZeroRate = exchCashflows.DiscountRate[i];
                        cashflow.ExchCashFlowAmt = exchCashflows.CashFlow[i];
                        cashflow.ExchCoupon = exchCashflows.Coupon[i];
                        cashflow.ExchAccrualStartDate = DateUtils.ConvertDate(exchCashflows.AccrualStartDate[i], "yyyy-MM-dd");
                        cashflow.ExchAccrualEndDate = DateUtils.ConvertDate(exchCashflows.AccrualEndDate[i], "yyyy-MM-dd");
                        cashflow.ExchPaymentDate = cashflow.ExchAccrualEndDate;
                        cashflow.ExchForwardRate = exchCashflows.ForwardRate[i];
                        cashflow.ExchAdjForwardRate = exchCashflows.AdjForwardRate[i];

                        if (exchCashflows.ResetDate[i] != null && exchCashflows.ResetDate[i] != DateTime.MinValue)
                            cashflow.ExchResetDate = DateUtils.ConvertDate(exchCashflows.ResetDate[i], "yyyy-MM-dd");

                        cashFlow = exchCashflows.CashFlow[i];
                        period = exchCashflows.Period[i];
                        period = period >= NUM_MONTHS ? NUM_MONTHS - 1 : period;
                        discountRate = exchDiscountRates[period];
                        timeInYears = exchCashflows.TimeInYears[i];
                        discountFactor = Math.Pow(1 + discountRate, -1 * timeInYears);
                        pvCashFlow = cashFlow * discountFactor;

                        cashflow.ExchZeroRate = discountRate;
                        cashflow.ExchDiscountFactor = discountFactor;
                        cashflow.ExchPresentValue = pvCashFlow;

                        //cash flows if the bond is called
                        if (pfdSecurity.NxtCallDt.HasValue && cashflows.AccrualEndDate[i] <= pfdSecurity.NxtCallDt)
                            cashflow.ExchCallCashFlowAmt = cashflow.ExchCashFlowAmt;

                        if (pfdSecurity.NxtCallDt.HasValue && DateUtils.CompareMonthAndYear(cashflows.AccrualEndDate[i], pfdSecurity.NxtCallDt.GetValueOrDefault()))
                            cashflow.ExchCallCashFlowAmt += pfdSecurity.NxtCallPrice.GetValueOrDefault();
                    }

                    periodicCashflows.Add(cashflow);
                }
            }
            return periodicCashflows;
        }

        public PeriodicCashFlowSummary GetPfdSecurityCashFlowsSummary(string ticker)
        {
            PeriodicCashFlowSummary cashFlowSummary = new PeriodicCashFlowSummary();

            IDictionary<string, PfdSecurity> securityDict = _cache.Get<IDictionary<string, PfdSecurity>>(CacheKeys.PFD_SECURITY_MASTER);

            if (securityDict.TryGetValue(ticker, out PfdSecurity security))
            {
                IDictionary<string, PfdSecurityCashFlows> securityCashFlowsDict = _cache.Get<IDictionary<string, PfdSecurityCashFlows>>(CacheKeys.PFD_SECURITY_CASHFLOWS);

                PfdSecurityCashFlows securityCashFlows;
                securityCashFlowsDict.TryGetValue(ticker, out securityCashFlows);

                //base curve
                IDictionary<string, IRCurve> rateCurveDict = _cache.Get<IDictionary<string, IRCurve>>(CacheKeys.PFD_FWD_RATE_CURVES);
                CashFlows cashFlows = securityCashFlows.CashFlows;
                CashFlows exchCashFlows = securityCashFlows.ExchangeableCashFlows;
                IList<PeriodicCashFlow> periodicCashFlow = GetPfdSecurityCashFlows(ticker, security, securityCashFlows, rateCurveDict, cashFlows, exchCashFlows);
                cashFlowSummary.PeriodicCashFlowBase = periodicCashFlow;

                //curve+10bp
                rateCurveDict = _cache.Get<IDictionary<string, IRCurve>>(CacheKeys.PFD_FWD_RATE_CURVES_UP);
                cashFlows = securityCashFlows.CashFlowsRateUpScenario;
                exchCashFlows = securityCashFlows.ExchangeableCashFlowsRateUpScenario;
                periodicCashFlow = GetPfdSecurityCashFlows(ticker, security, securityCashFlows, rateCurveDict, cashFlows, exchCashFlows);
                cashFlowSummary.PeriodicCashFlowCurveUp = periodicCashFlow;

                //curve-10bp
                rateCurveDict = _cache.Get<IDictionary<string, IRCurve>>(CacheKeys.PFD_FWD_RATE_CURVES_DOWN);
                cashFlows = securityCashFlows.CashFlowsRateDownScenario;
                exchCashFlows = securityCashFlows.ExchangeableCashFlowsRateDownScenario;
                periodicCashFlow = GetPfdSecurityCashFlows(ticker, security, securityCashFlows, rateCurveDict, cashFlows, exchCashFlows);
                cashFlowSummary.PeriodicCashFlowCurveDown = periodicCashFlow;
            }

            return cashFlowSummary;
        }

        public RateResetTO GetPfdRateResetDetails(string ticker)
        {
            RateResetTO rateResetData = new RateResetTO();

            IDictionary<string, PfdSecurityCashFlows> dict = _cache.Get<IDictionary<string, PfdSecurityCashFlows>>(CacheKeys.PFD_SECURITY_CASHFLOWS);
            PfdSecurityCashFlows pfdSecurityCashFlows;
            if (dict.TryGetValue(ticker, out pfdSecurityCashFlows))
            {
                //main security
                if (pfdSecurityCashFlows.RateIndexCurveMap != null)
                {
                    rateResetData.RateResetCurveName = pfdSecurityCashFlows.RateIndexCurveMap.RateResetCurveName;
                    rateResetData.RateResetCurveTenor = pfdSecurityCashFlows.RateIndexCurveMap.RateResetCurveTenor;
                    rateResetData.DiscountCurveName = pfdSecurityCashFlows.RateIndexCurveMap.DiscountCurveName;
                    rateResetData.Beta = pfdSecurityCashFlows.RateIndexCurveMap.Beta;
                    rateResetData.Margin = pfdSecurityCashFlows.RateIndexCurveMap.Margin;
                }

                if (pfdSecurityCashFlows.RateIndexProxy != null)
                {
                    rateResetData.Index = pfdSecurityCashFlows.RateIndexProxy.Index;
                    rateResetData.ProxyIndex = pfdSecurityCashFlows.RateIndexProxy.ProxyIndex;
                    rateResetData.ProxyIndexMultiplier = pfdSecurityCashFlows.RateIndexProxy.Multiplier;
                    rateResetData.ProxyIndexMargin = pfdSecurityCashFlows.RateIndexProxy.Margin;
                }

                //exchangeable security
                if (pfdSecurityCashFlows.ExchangeableRateIndexCurveMap != null)
                {
                    rateResetData.ExchRateResetCurveName = pfdSecurityCashFlows.ExchangeableRateIndexCurveMap.RateResetCurveName;
                    rateResetData.ExchRateResetCurveTenor = pfdSecurityCashFlows.ExchangeableRateIndexCurveMap.RateResetCurveTenor;
                    rateResetData.ExchDiscountCurveName = pfdSecurityCashFlows.ExchangeableRateIndexCurveMap.DiscountCurveName;
                    rateResetData.ExchBeta = pfdSecurityCashFlows.ExchangeableRateIndexCurveMap.Beta;
                    rateResetData.ExchMargin = pfdSecurityCashFlows.ExchangeableRateIndexCurveMap.Margin;
                }

                if (pfdSecurityCashFlows.ExchangeableRateIndexProxy != null)
                {
                    rateResetData.ExchIndex = pfdSecurityCashFlows.ExchangeableRateIndexProxy.Index;
                    rateResetData.ExchProxyIndex = pfdSecurityCashFlows.ExchangeableRateIndexProxy.ProxyIndex;
                    rateResetData.ExchProxyIndexMultiplier = pfdSecurityCashFlows.ExchangeableRateIndexProxy.Multiplier;
                    rateResetData.ExchProxyIndexMargin = pfdSecurityCashFlows.ExchangeableRateIndexProxy.Margin;
                }
            }

            return rateResetData;
        }

        public IList<RateCurveTO> GetPfdRateCurve(string curveName)
        {
            IList<RateCurveTO> rateCurveList = new List<RateCurveTO>();

            IDictionary<string, IRCurve> rateCurveDict = _cache.Get<IDictionary<string, IRCurve>>(CacheKeys.PFD_FWD_RATE_CURVES);
            IDictionary<string, IDictionary<int, SpotRate>> spotRatesDict = _cache.Get<IDictionary<string, IDictionary<int, SpotRate>>>(CacheKeys.PFD_SPOT_RATES);

            IDictionary<int, SpotRate> spotRates;
            spotRatesDict.TryGetValue(curveName, out spotRates);

            if (rateCurveDict.TryGetValue(curveName, out IRCurve curve))
            {
                for (int i = 0; i < curve.Size; i++)
                {
                    RateCurveTO data = new RateCurveTO
                    {
                        Period = curve.Period[i]
                    };

                    //spot rates
                    SpotRate spotRate;
                    if (spotRates != null && spotRates.TryGetValue(data.Period, out spotRate))
                    {
                        data.FIGI = spotRate.FIGI;
                        data.Tenor = spotRate.Tenor;
                        data.SpotRate = spotRate.Value;
                    }

                    //spot/zero curve
                    double discountFactor = Math.Pow(1 + curve.DiscountRate[i], -1 * curve.Period[i] / 12.0);
                    data.DiscountFactor = discountFactor;
                    data.DiscountRate = curve.DiscountRate[i] * 100.0;

                    //forward curve
                    data.ForwardRate1M = curve.ForwardRate1M[i] * 100.0;
                    data.ForwardRate3M = curve.ForwardRate3M[i] * 100.0;
                    data.ForwardRate5YR = curve.ForwardRate5YR[i] * 100.0;

                    rateCurveList.Add(data);
                }
            }
            return rateCurveList;
        }

        public IList<RateCurveTO> GetPfdRateCurveNew(string curveName)
        {
            IList<RateCurveTO> rateCurveList = new List<RateCurveTO>();

            IDictionary<string, IRCurve> rateCurveDict = _cache.Get<IDictionary<string, IRCurve>>(CacheKeys.PFD_FWD_RATE_CURVES_NEW);
            IDictionary<string, IDictionary<int, SpotRate>> spotRatesDict = _cache.Get<IDictionary<string, IDictionary<int, SpotRate>>>(CacheKeys.PFD_SPOT_RATES_NEW);

            IDictionary<int, SpotRate> spotRates;
            spotRatesDict.TryGetValue(curveName, out spotRates);

            IRCurve curve;
            if (rateCurveDict.TryGetValue(curveName, out curve))
            {
                for (int i = 0; i < curve.Size; i++)
                {
                    RateCurveTO data = new RateCurveTO
                    {
                        Period = curve.Period[i]
                    };

                    //spot rates
                    SpotRate spotRate;
                    if (spotRates != null && spotRates.TryGetValue(data.Period, out spotRate))
                    {
                        data.FIGI = spotRate.FIGI;
                        data.Tenor = spotRate.Tenor;
                        data.SpotRate = spotRate.Value;
                    }

                    //spot/zero curve
                    double discountFactor = Math.Pow(1 + curve.DiscountRate[i], -1 * curve.Period[i] / 12.0);
                    data.DiscountFactor = discountFactor;
                    data.DiscountRate = curve.DiscountRate[i] * 100.0;

                    //forward curve
                    data.ForwardRate1M = curve.ForwardRate1M[i] * 100.0;
                    data.ForwardRate3M = curve.ForwardRate3M[i] * 100.0;
                    data.ForwardRate5YR = curve.ForwardRate5YR[i] * 100.0;

                    rateCurveList.Add(data);
                }
            }
            return rateCurveList;
        }

        public void SaveSecurityDetails(IList<PfdSecurity> overrides)
        {
            if (overrides != null && overrides.Count > 0)
            {
                foreach (PfdSecurity data in overrides)
                {
                    if (!string.IsNullOrEmpty(data.IssueDtAsString))
                    {
                        DateTime? issueDt = DateUtils.ConvertToDate(data.IssueDtAsString, "yyyy-MM-dd");
                        data.IssueDt = issueDt;
                    }

                    if (!string.IsNullOrEmpty(data.MaturityDtAsString))
                    {
                        DateTime? maturityDt = DateUtils.ConvertToDate(data.MaturityDtAsString, "yyyy-MM-dd");
                        data.MaturityDt = maturityDt;
                    }

                    if (!string.IsNullOrEmpty(data.NextRefixDtAsString))
                    {
                        DateTime? nextRefixDt = DateUtils.ConvertToDate(data.NextRefixDtAsString, "yyyy-MM-dd");
                        data.NextRefixDt = nextRefixDt;
                    }

                    if (!string.IsNullOrEmpty(data.PrevCpnDtAsString))
                    {
                        DateTime? prevCpnDt = DateUtils.ConvertToDate(data.PrevCpnDtAsString, "yyyy-MM-dd");
                        data.PrevCpnDt = prevCpnDt;
                    }

                    if (!string.IsNullOrEmpty(data.NxtCpnDtAsString))
                    {
                        DateTime? nextCpnDt = DateUtils.ConvertToDate(data.NxtCpnDtAsString, "yyyy-MM-dd");
                        data.NxtCpnDt = nextCpnDt;
                    }

                    if (!string.IsNullOrEmpty(data.NxtExchDtAsString))
                    {
                        DateTime? nextExchDt = DateUtils.ConvertToDate(data.NxtExchDtAsString, "yyyy-MM-dd");
                        data.NxtExchDt = nextExchDt;
                    }

                    if (!string.IsNullOrEmpty(data.ExchNxtRefixDtAsString))
                    {
                        DateTime? exchNextRefixDt = DateUtils.ConvertToDate(data.ExchNxtRefixDtAsString, "yyyy-MM-dd");
                        data.ExchNxtRefixDt = exchNextRefixDt;
                    }

                    if (!string.IsNullOrEmpty(data.NxtCallDtAsString))
                    {
                        DateTime? nextCallDt = DateUtils.ConvertToDate(data.NxtCallDtAsString, "yyyy-MM-dd");
                        data.NxtCallDt = nextCallDt;
                    }
                }

                //save to database
                _pfdBaseDao.SavePfdSecurityMasterOverrides(overrides);

                //refresh cache
                UpdatePfdSecurityMaster();
            }
        }

        public void SaveInterestRateCurveMembers(IList<IRCurveMember> curveMembers)
        {
            if (curveMembers != null && curveMembers.Count > 0)
            {
                _logger.LogInformation("Saving interest rate curve members...");
                _pfdBaseDao.SaveIRCurveRates(curveMembers);

                _logger.LogInformation("Generating forward rate curves..");
                GenerateForwardCurves();

                _logger.LogInformation("Updating spot rates in cache..");
                UpdatePfdSpotRates();

                _logger.LogInformation("Updating forward rate curves in cache..");
                UpdatePfdInterestRateCurves();

                _logger.LogInformation("Updating security cash flows..");
                UpdatePfdInterestRateCurves();
            }
        }

        public void GenerateForwardCurves()
        {
            try
            {
                int exitCode;
                ProcessStartInfo processInfo;
                Process process;

                processInfo = new ProcessStartInfo("cmd.exe", "/c " + ConfigurationManager.AppSettings["BLOOMBERG_SERVICE_BATCH_FILE"])
                {
                    CreateNoWindow = true,
                    UseShellExecute = false,
                    WorkingDirectory = ConfigurationManager.AppSettings["BLOOMBERG_SERVICE_PATH"],

                    // *** Redirect the output ***
                    RedirectStandardError = true,
                    RedirectStandardOutput = true
                };

                process = Process.Start(processInfo);
                process.WaitForExit();

                // *** Read the streams ***
                string output = process.StandardOutput.ReadToEnd();
                string error = process.StandardError.ReadToEnd();

                exitCode = process.ExitCode;

                _logger.LogInformation("output>>" + (String.IsNullOrEmpty(output) ? "(none)" : output));
                _logger.LogInformation("error>>" + (String.IsNullOrEmpty(error) ? "(none)" : error));
                _logger.LogInformation("Exit Code: " + exitCode.ToString());
                process.Close();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error generating forward rate curves");
            }
        }

        private void UpdatePfdSpotRates()
        {
            try
            {
                IDictionary<string, IDictionary<int, SpotRate>> spotRatesDict = _pfdBaseDao.GetSpotRatesNew();
                _cache.Remove(CacheKeys.PFD_SPOT_RATES_NEW);
                _cache.Add(CacheKeys.PFD_SPOT_RATES_NEW, spotRatesDict, DateTimeOffset.MaxValue);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error updating spot rates");
            }
        }

        private void UpdatePfdInterestRateCurves()
        {
            try
            {
                IDictionary<string, IRCurve> rateCurveDict = _pfdBaseDao.GetInterestRateCurvesNew();
                ExtendRateCurves(rateCurveDict);
                _cache.Remove(CacheKeys.PFD_FWD_RATE_CURVES_NEW);
                _cache.Add(CacheKeys.PFD_FWD_RATE_CURVES_NEW, rateCurveDict, DateTimeOffset.MaxValue);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error updating forward rate curves");
            }
        }

        /// <summary>
        /// Gets list of tickers matching input criteria (filters selected by user)
        /// </summary>
        /// <param name="parameters"></param>
        /// <returns></returns>
        public IList<FundGroup> GetFundTickers(InputParameters parameters)
        {
            IDictionary<string, PfdSecurity> securityDict = _cache.Get<IDictionary<string, PfdSecurity>>(CacheKeys.PFD_SECURITY_MASTER);

            IList<PfdSecurity> filteredSecurityList = new List<PfdSecurity>();
            IList<FundGroup> fundGroupList = new List<FundGroup>();
            IList<FundGroup> fundGroupListSorted = new List<FundGroup>();

            //filter securities
            foreach (KeyValuePair<string, PfdSecurity> kvp in securityDict)
            {
                PfdSecurity security = kvp.Value;
                if (ApplyCountryFilter(security, parameters.Country))
                {
                    if (ApplyAssetTypeFilter(security, parameters.AssetType))
                    {
                        filteredSecurityList.Add(security);
                    }
                }
            }

            //group securities
            foreach (PfdSecurity pfdSecurity in filteredSecurityList)
            {
                FundGroup fundGroup = new FundGroup
                {
                    Ticker = pfdSecurity.BBGId,
                    SortFieldValueString = pfdSecurity.CompanyNameLong,
                    SortFieldValue = pfdSecurity.AmountOutstanding
                };

                //group by column
                if (!string.IsNullOrEmpty(parameters.GroupBy))
                    GroupFunds(fundGroup, pfdSecurity, parameters.GroupBy, "GroupBy1");

                //default group
                if (string.IsNullOrEmpty(fundGroup.GroupName))
                    fundGroup.GroupName = "All";

                fundGroupList.Add(fundGroup);
            }

            fundGroupListSorted = fundGroupList
                        .OrderBy(x => x.GroupByColumn1)
                        .ThenBy(x => x.SortFieldValueString)
                        .ThenBy(x => x.SortFieldValue)
                        // .ThenBy(x => x.Ticker)
                        .ToList<FundGroup>();

            return fundGroupListSorted;
        }

        /// <summary>
        /// Apply Country filter
        /// </summary>
        /// <param name="security"></param>
        /// <param name="country"></param>
        /// <returns></returns>
        private bool ApplyCountryFilter(PfdSecurity security, string country)
        {
            bool countryFilter = false;

            try
            {
                if (string.IsNullOrEmpty(country))
                    countryFilter = true;
                else if (country.Equals("All", StringComparison.CurrentCultureIgnoreCase))
                    countryFilter = true;
                else if (country.Equals("United States", StringComparison.CurrentCultureIgnoreCase) &&
                    security.CountryCode.Equals("United States", StringComparison.CurrentCultureIgnoreCase))
                    countryFilter = true;
                else if (country.Equals("Canada", StringComparison.CurrentCultureIgnoreCase) &&
                    security.CountryCode.Equals("Canada", StringComparison.CurrentCultureIgnoreCase))
                    countryFilter = true;
            }
            catch (Exception)
            {
            }
            return countryFilter;
        }

        /// <summary>
        /// Apply Asset Type filter
        /// </summary>
        /// <param name="security"></param>
        /// <param name="assetType"></param>
        /// <returns></returns>
        private bool ApplyAssetTypeFilter(PfdSecurity security, string assetType)
        {
            bool assetTypeFilter = false;

            try
            {
                if (string.IsNullOrEmpty(assetType))
                    assetTypeFilter = true;
                if (assetType.Equals("All", StringComparison.CurrentCultureIgnoreCase))
                    assetTypeFilter = true;
                else if (assetType.Equals("Fixed", StringComparison.CurrentCultureIgnoreCase) &&
                    security.FixedFloat.Equals("fixed", StringComparison.CurrentCultureIgnoreCase))
                    assetTypeFilter = true;
                else if (assetType.Equals("Float", StringComparison.CurrentCultureIgnoreCase) &&
                    security.FixedFloat.Equals("float", StringComparison.CurrentCultureIgnoreCase))
                    assetTypeFilter = true;
            }
            catch (Exception)
            {
            }
            return assetTypeFilter;
        }

        /// <summary>
        /// Group funds by selected grouping (geo levels, asset class levels, fund category)
        /// </summary>
        /// <param name="fundGroup"></param>
        /// <param name="pfdSecurity"></param>
        /// <param name="selectedGroup"></param>
        /// <param name="groupByColumn"></param>
        private void GroupFunds(FundGroup fundGroup, PfdSecurity pfdSecurity, string selectedGroup, string groupByColumn)
        {
            string groupName = string.Empty;

            if ("Country".Equals(selectedGroup, StringComparison.CurrentCultureIgnoreCase))
                groupName = pfdSecurity.Country;
            else if ("Currency".Equals(selectedGroup, StringComparison.CurrentCultureIgnoreCase))
                groupName = pfdSecurity.Currency;
            else if ("Fund Level 1".Equals(selectedGroup, StringComparison.CurrentCultureIgnoreCase))
                groupName = pfdSecurity.AssetClassLevel1;
            else if ("Fund Level 2".Equals(selectedGroup, StringComparison.CurrentCultureIgnoreCase))
                groupName = pfdSecurity.AssetClassLevel2;
            else if ("Fund Level 3".Equals(selectedGroup, StringComparison.CurrentCultureIgnoreCase))
                groupName = pfdSecurity.AssetClassLevel3;
            else if ("Geo Level 1".Equals(selectedGroup, StringComparison.CurrentCultureIgnoreCase))
                groupName = pfdSecurity.GeoLevel1;
            else if ("Geo Level 2".Equals(selectedGroup, StringComparison.CurrentCultureIgnoreCase))
                groupName = pfdSecurity.GeoLevel2;
            else if ("Geo Level 3".Equals(selectedGroup, StringComparison.CurrentCultureIgnoreCase))
                groupName = pfdSecurity.GeoLevel3;
            else if ("Fund Category".Equals(selectedGroup, StringComparison.CurrentCultureIgnoreCase))
                groupName = pfdSecurity.FundCategory;

            if ("GroupBy1".Equals(groupByColumn, StringComparison.CurrentCultureIgnoreCase))
                fundGroup.GroupByColumn1 = groupName;
            else if ("GroupBy2".Equals(groupByColumn, StringComparison.CurrentCultureIgnoreCase))
                fundGroup.GroupByColumn2 = groupName;
            else if ("GroupBy3".Equals(groupByColumn, StringComparison.CurrentCultureIgnoreCase))
                fundGroup.GroupByColumn3 = groupName;
            else if ("GroupBy4".Equals(groupByColumn, StringComparison.CurrentCultureIgnoreCase))
                fundGroup.GroupByColumn4 = groupName;

            if (string.IsNullOrEmpty(fundGroup.GroupName))
                fundGroup.GroupName = groupName;
            else
                fundGroup.GroupName += "/" + groupName;
        }

        public void SaveSecurityAnalytics()
        {
            try
            {
                IDictionary<string, PfdSecurityAnalytic> securityAnalyticDict = new Dictionary<string, PfdSecurityAnalytic>();

                IDictionary<string, PfdSecurity> securityDict = _cache.Get<IDictionary<string, PfdSecurity>>(CacheKeys.PFD_SECURITY_MASTER);
                IDictionary<string, SecurityPrice> securityPriceDict = _cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);
                IDictionary<string, string> priceTickerMap = _cache.Get<IDictionary<string, string>>(CacheKeys.PRICE_TICKER_MAP);

                IDictionary<string, PfdSecurityExt> securityExtDict = _pfdBaseDao.GetPfdSecurityExtData();

                DateTime effectiveDate = DateTime.Today;
                string effectiveDateAsString = DateUtils.ConvertDate(effectiveDate, "yyyy-MM-dd");

                //calculate analytics (spreads)
                foreach (KeyValuePair<string, PfdSecurity> kvp in securityDict)
                {
                    string securityId = kvp.Key;
                    PfdSecurity security = kvp.Value;

                    try
                    {
                        PfdSecurityAnalytic securityAnalytic = new PfdSecurityAnalytic
                        {
                            BBGId = security.BBGId,
                            Figi = security.Figi,
                            EffectiveDate = effectiveDate,
                            EffectiveDateAsString = effectiveDateAsString
                        };

                        //get security price
                        SecurityPrice securityPrice = SecurityPriceLookupOperations.GetSecurityPrice(securityId, priceTickerMap, securityPriceDict);
                        if (securityPrice != null)
                        {
                            securityAnalytic.LastPrice = securityPrice.LastPrc;
                            securityAnalytic.BidPrice = securityPrice.BidPrc;
                            securityAnalytic.AskPrice = securityPrice.AskPrc;

                            PfdSecurityExt securityExt;
                            securityExtDict.TryGetValue(securityId, out securityExt);

                            //spread to last price
                            double? spreadToLastPrice = Solve("SPREAD", securityId, securityPrice.LastPrc.GetValueOrDefault(), 0d, 0d, "N", null);
                            double? maxSpreadToLastPrice = spreadToLastPrice;
                            if ("Y".Equals(security.Exchangeable, StringComparison.CurrentCultureIgnoreCase))
                            {
                                double? exchSpreadToLastPrice = Solve("SPREAD", securityId, securityPrice.LastPrc.GetValueOrDefault(), 0d, 0d, "Y", null);
                                maxSpreadToLastPrice = Math.Max(spreadToLastPrice.GetValueOrDefault(), exchSpreadToLastPrice.GetValueOrDefault());
                            }

                            //spread to bid price
                            double? spreadToBidPrice = Solve("SPREAD", securityId, securityPrice.BidPrc.GetValueOrDefault(), 0d, 0d, "N", null);
                            double? maxSpreadToBidPrice = spreadToBidPrice;
                            if ("Y".Equals(security.Exchangeable, StringComparison.CurrentCultureIgnoreCase))
                            {
                                double? exchSpreadToBidPrice = Solve("SPREAD", securityId, securityPrice.BidPrc.GetValueOrDefault(), 0d, 0d, "Y", null);
                                maxSpreadToBidPrice = Math.Max(spreadToBidPrice.GetValueOrDefault(), exchSpreadToBidPrice.GetValueOrDefault());
                            }

                            //spread to ask price
                            double? spreadToAskPrice = Solve("SPREAD", securityId, securityPrice.AskPrc.GetValueOrDefault(), 0d, 0d, "N", null);
                            double? maxSpreadToAskPrice = spreadToAskPrice;
                            if ("Y".Equals(security.Exchangeable, StringComparison.CurrentCultureIgnoreCase))
                            {
                                double? exchSpreadToAskPrice = Solve("SPREAD", securityId, securityPrice.AskPrc.GetValueOrDefault(), 0d, 0d, "Y", null);
                                maxSpreadToAskPrice = Math.Max(spreadToAskPrice.GetValueOrDefault(), exchSpreadToAskPrice.GetValueOrDefault());
                            }

                            double spreadAdjustment = 0;
                            if (securityExt != null && securityExt.SpreadAdj.HasValue)
                                spreadAdjustment = securityExt.SpreadAdj.GetValueOrDefault();

                            securityAnalytic.SpreadLastPrice = maxSpreadToLastPrice * 10000.0;
                            securityAnalytic.SpreadBidPrice = maxSpreadToBidPrice * 10000.0;
                            securityAnalytic.SpreadAskPrice = maxSpreadToAskPrice * 10000.0;
                            securityAnalytic.AdjSpreadLastPrice = securityAnalytic.SpreadLastPrice - spreadAdjustment;
                            securityAnalytic.AdjSpreadBidPrice = securityAnalytic.SpreadBidPrice - spreadAdjustment;
                            securityAnalytic.AdjSpreadAskPrice = securityAnalytic.SpreadAskPrice - spreadAdjustment;
                        }

                        if (!securityAnalyticDict.ContainsKey(securityId))
                            securityAnalyticDict.Add(securityId, securityAnalytic);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error calculating analytics for ticker: " + securityId);
                    }
                }

                //save analytics
                IList<PfdSecurityAnalytic> list = securityAnalyticDict.Values.ToList<PfdSecurityAnalytic>();
                _pfdBaseDao.SavePfdSecurityAnalyticsNew(list);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving pfd security analytics");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        private void PopulatePfdTickerBloombergCoderMap()
        {
            IDictionary<string, PfdSecurityMap> pdSecurityMap = _pfdBaseDao.GetPfdTickerBloombergCodeMap();
            _cache.Remove(CacheKeys.PFD_TICKER_BLOOMBERG_CODE_MAP);
            _cache.Add(CacheKeys.PFD_TICKER_BLOOMBERG_CODE_MAP, pdSecurityMap, DateTimeOffset.MaxValue);
        }
    }
}