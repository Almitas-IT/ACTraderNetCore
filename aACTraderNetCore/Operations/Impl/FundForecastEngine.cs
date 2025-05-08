using aACTrader.Operations.Impl.NavEstimation;
using aCommons;
using aCommons.Cef;
using aCommons.Crypto;
using aCommons.DTO;
using aCommons.Utils;
using LazyCache;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using static aACTrader.Operations.Impl.XIRR;

namespace aACTrader.Operations.Impl
{
    public class FundForecastEngine
    {
        private readonly ILogger<FundForecastEngine> _logger;

        private static readonly DateTime TodaysDate = DateTime.Now.Date;

        private readonly CachingService _cache;
        private readonly ExpressionEvaluator _expressionEvaluator;
        private readonly NavProxyEvaluator _navProxyEvaluator;
        private readonly AltNavProxyEvaluator _altNavProxyEvaluator;
        private readonly PortProxyEvaluator _portProxyEvaluator;
        private readonly FundIRRCalculator _fundIRRCalculator;
        private readonly ExpectedAlphaCalculatorNew _expectedAlphaCalculator;
        private readonly ConditionalProxyEvaluator _conditionalProxyEvaluator;

        public FundForecastEngine(ILogger<FundForecastEngine> logger
            , CachingService cache
            , ExpressionEvaluator expressionEvaluator
            , NavProxyEvaluator navProxyEvaluator
            , AltNavProxyEvaluator altNavProxyEvaluator
            , PortProxyEvaluator portProxyEvaluator
            , FundIRRCalculator fundIRRCalculator
            , ExpectedAlphaCalculatorNew expectedAlphaCalculatorNew
            , ConditionalProxyEvaluator conditionalProxyEvaluator)
        {
            _logger = logger;
            _cache = cache;
            _expressionEvaluator = expressionEvaluator;
            _navProxyEvaluator = navProxyEvaluator;
            _altNavProxyEvaluator = altNavProxyEvaluator;
            _portProxyEvaluator = portProxyEvaluator;
            _fundIRRCalculator = fundIRRCalculator;
            _expectedAlphaCalculator = expectedAlphaCalculatorNew;
            _conditionalProxyEvaluator = conditionalProxyEvaluator;
        }

        public void Start()
        {
            _logger.LogInformation("Fund Forecast Engine - STARTED");

            IDictionary<string, FundMaster> fundMasterDict = _cache.Get<IDictionary<string, FundMaster>>(CacheKeys.FUND_MASTER);
            IDictionary<string, FundForecast> fundForecastDict = _cache.Get<IDictionary<string, FundForecast>>(CacheKeys.FUND_FORECASTS);
            IDictionary<string, SecurityPrice> securityPriceDict = _cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);
            IDictionary<string, string> priceTickerMap = _cache.Get<IDictionary<string, string>>(CacheKeys.PRICE_TICKER_MAP);
            IDictionary<string, string> pfdCommonShareTickerMap = _cache.Get<IDictionary<string, string>>(CacheKeys.PFD_COMMON_SHARE_MAP);
            IDictionary<string, FundNav> fundNavDict = _cache.Get<IDictionary<string, FundNav>>(CacheKeys.FUND_NAVS);
            IDictionary<string, FundRedemption> fundRedemptionDict = _cache.Get<IDictionary<string, FundRedemption>>(CacheKeys.FUND_REDEMPTIONS);
            IDictionary<string, FXRate> fxRateDict = _cache.Get<IDictionary<string, FXRate>>(CacheKeys.FX_RATES);

            foreach (KeyValuePair<string, FundMaster> kvp in fundMasterDict)
            {
                string ticker = kvp.Key;
                FundMaster fundMaster = kvp.Value;

                if (fundForecastDict.TryGetValue(ticker, out FundForecast fundForecast))
                {
                    //Get Fund Redemption Details
                    fundRedemptionDict.TryGetValue(ticker, out FundRedemption fundRedemption);

                    //Reset Fund Forecast Values
                    //Calculate Live Discounts to Common Share Price
                    SecurityPrice securityPrice = SecurityPriceLookupOperations.GetSecurityPrice(ticker, priceTickerMap, securityPriceDict);
                    Reset(ticker
                        , fundMaster
                        , fundForecast
                        , securityPrice
                        , fundNavDict
                        , fundRedemption
                        , fxRateDict);

                    //South Korean Pfds
                    string country = fundMaster.Cntry;
                    string paymentRank = fundMaster.PayRank;
                    if (!string.IsNullOrEmpty(country)
                        && country.Equals("SOUTH KOREA", StringComparison.CurrentCultureIgnoreCase)
                        && !string.IsNullOrEmpty(paymentRank)
                        && paymentRank.Equals("Preferred", StringComparison.CurrentCultureIgnoreCase))
                    {
                        CalculateDiscountsForSouthKoreaPfds(
                            ticker
                            , fundForecast
                            , securityPriceDict
                            , pfdCommonShareTickerMap
                            , priceTickerMap);
                    }

                    fundForecast.LastUpdTime = DateTime.Now.ToString("HH:mm:ss");
                }
            }

            _logger.LogInformation("Fund Forecast Engine - DONE");
        }

        /// <summary>
        /// 
        /// </summary>
        public void Calculate()
        {
            IDictionary<string, FundMaster> fundMasterDict = _cache.Get<IDictionary<string, FundMaster>>(CacheKeys.FUND_MASTER);
            IDictionary<string, FundForecast> fundForecastDict = _cache.Get<IDictionary<string, FundForecast>>(CacheKeys.FUND_FORECASTS);
            IDictionary<string, SecurityPrice> securityPriceDict = _cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);
            IDictionary<string, HashSet<string>> fundETFTickersDict = _cache.Get<IDictionary<string, HashSet<string>>>(CacheKeys.ETF_TICKERS);
            IDictionary<string, FundETFReturn> etfHistReturnsDict = _cache.Get<IDictionary<string, FundETFReturn>>(CacheKeys.ETF_RETURNS);

            IDictionary<string, FundRedemption> fundRedemptionDict = _cache.Get<IDictionary<string, FundRedemption>>(CacheKeys.FUND_REDEMPTIONS);
            IDictionary<string, FundNav> fundNavDict = _cache.Get<IDictionary<string, FundNav>>(CacheKeys.FUND_NAVS);
            IDictionary<string, string> priceTickerMap = _cache.Get<IDictionary<string, string>>(CacheKeys.PRICE_TICKER_MAP);
            IDictionary<string, FXRate> fxRateDict = _cache.Get<IDictionary<string, FXRate>>(CacheKeys.FX_RATES);

            IDictionary<string, FundRightsOffer> fundRightsOffersDict = _cache.Get<IDictionary<string, FundRightsOffer>>(CacheKeys.FUND_RIGHTS_OFFERS);
            IDictionary<string, FundHistStats> fundHistStatsDict = _cache.Get<IDictionary<string, FundHistStats>>(CacheKeys.FUND_STATS);
            IDictionary<string, FundTenderOffer> fundTenderOffersDict = _cache.Get<IDictionary<string, FundTenderOffer>>(CacheKeys.FUND_TENDER_OFFERS);
            IDictionary<string, FundGroupDiscountStats> histDiscountStats = _cache.Get<IDictionary<string, FundGroupDiscountStats>>(CacheKeys.HIST_DISCOUNT_STATS);
            IDictionary<string, FundSupplementalData> fundSupplementalDataDict = _cache.Get<IDictionary<string, FundSupplementalData>>(CacheKeys.FUND_SUPPLEMENTAL_DETAILS);

            //Proxy Formulas
            IDictionary<string, FundProxyFormula> fundProxyFormulaDict = _cache.Get<IDictionary<string, FundProxyFormula>>(CacheKeys.PROXY_FORMULAS);
            IDictionary<string, FundETFReturn> proxyETFHistReturnsDict = _cache.Get<IDictionary<string, FundETFReturn>>(CacheKeys.PROXY_ETF_RETURNS);

            //Alternate Proxy Formulas
            IDictionary<string, FundProxyFormula> fundAltProxyFormulaDict = _cache.Get<IDictionary<string, FundProxyFormula>>(CacheKeys.ALT_PROXY_FORMULAS);
            IDictionary<string, FundETFReturn> altProxyETFHistReturnsDict = _cache.Get<IDictionary<string, FundETFReturn>>(CacheKeys.ALT_PROXY_ETF_RETURNS);

            //Port Proxy Formulas (Proxy for Fixed Income)
            IDictionary<string, FundProxyFormula> fundPortProxyFormulaDict = _cache.Get<IDictionary<string, FundProxyFormula>>(CacheKeys.PORT_PROXY_FORMULAS);
            IDictionary<string, FundETFReturn> portProxyETFHistReturnsDict = _cache.Get<IDictionary<string, FundETFReturn>>(CacheKeys.PORT_PROXY_ETF_RETURNS);

            //Conditional Proxy
            IDictionary<string, FundCondProxy> fundCondProxyDict = _cache.Get<IDictionary<string, FundCondProxy>>(CacheKeys.CONDITIONAL_PROXY);

            foreach (KeyValuePair<string, FundMaster> kvp in fundMasterDict)
            {
                string ticker = kvp.Key;

                try
                {
                    FundMaster fundMaster = kvp.Value;
                    if (fundForecastDict.TryGetValue(ticker, out FundForecast fundForecast))
                    {
                        //Get Fund Redemption Details
                        fundRedemptionDict.TryGetValue(ticker, out FundRedemption fundRedemption);

                        //Security Price (Common and Preferred)
                        SecurityPrice securityPrice = SecurityPriceLookupOperations.GetSecurityPrice(ticker, priceTickerMap, securityPriceDict);
                        SecurityPrice preferredSecurityPrice = null;
                        if (fundRedemption != null && !string.IsNullOrEmpty(fundRedemption.PreferredShareTicker))
                            preferredSecurityPrice = SecurityPriceLookupOperations.GetSecurityPrice(fundRedemption.PreferredShareTicker, priceTickerMap, securityPriceDict);

                        //Reset Fund Forecast Values
                        Reset(ticker
                            , fundMaster
                            , fundForecast
                            , securityPrice
                            , fundNavDict
                            , fundRedemption
                            , fxRateDict);

                        //Accrued Interest (BDCs)
                        if ((!string.IsNullOrEmpty(fundMaster.AssetTyp)
                            && "BDC".Equals(fundMaster.AssetTyp, StringComparison.CurrentCultureIgnoreCase))
                            || fundForecast.AccrRate.HasValue)
                            CalculateAccruedInterest(fundMaster, fundForecast);

                        //Holding Companies - special calculation for Empire Life asset in ELF CT fund
                        if (ticker.Equals("ELF CT", StringComparison.CurrentCultureIgnoreCase))
                            ProcessHoldingCompanies(ticker, fundForecast, securityPriceDict);

                        //Calculate ETF Returns
                        if (fundForecast.LastDvdAdjNav.HasValue && fundForecast.HasRC == 1)
                        {
                            _expressionEvaluator.CalculateETFNav(ticker
                            , fundMaster
                            , fundForecast
                            , etfHistReturnsDict
                            , fundETFTickersDict
                            , priceTickerMap
                            , fxRateDict
                            , securityPriceDict);
                        }

                        //Proxy Returns
                        if (fundProxyFormulaDict.TryGetValue(ticker, out FundProxyFormula fundProxyFormula))
                        {
                            if (!string.IsNullOrEmpty(fundProxyFormula.ProxyFormula)
                                && fundProxyFormula.ProxyTickersWithCoefficients.Count > 0)
                            {
                                _navProxyEvaluator.Calculate(ticker
                                , fundMaster
                                , fundForecast
                                , fundProxyFormula
                                , proxyETFHistReturnsDict
                                , fxRateDict
                                , securityPriceDict
                                , priceTickerMap);
                            }
                        }

                        //Alternate Proxy Returns
                        if (fundAltProxyFormulaDict.TryGetValue(ticker, out fundProxyFormula))
                        {
                            if (!string.IsNullOrEmpty(fundProxyFormula.ProxyFormula)
                                && fundProxyFormula.ProxyTickersWithCoefficients.Count > 0)
                            {
                                _altNavProxyEvaluator.Calculate(ticker
                                , fundMaster
                                , fundForecast
                                , fundProxyFormula
                                , altProxyETFHistReturnsDict
                                , fxRateDict
                                , securityPriceDict
                                , priceTickerMap);
                            }
                        }

                        //Port Proxy Returns
                        if (fundPortProxyFormulaDict.TryGetValue(ticker, out fundProxyFormula))
                        {
                            if (!string.IsNullOrEmpty(fundProxyFormula.ProxyFormula)
                                && fundProxyFormula.ProxyTickersWithCoefficients.Count > 0)
                            {
                                _portProxyEvaluator.Calculate(ticker
                                    , fundMaster
                                    , fundForecast
                                    , fundProxyFormula
                                    , portProxyETFHistReturnsDict
                                    , fxRateDict
                                    , securityPriceDict
                                    , priceTickerMap);
                            }
                        }

                        //Conditional Proxy Returns
                        if (fundCondProxyDict.TryGetValue(ticker, out FundCondProxy fundCondProxy))
                        {
                            _conditionalProxyEvaluator.Process(ticker
                                , fundForecast
                                , fundCondProxy
                                , fxRateDict
                                , securityPriceDict
                                , priceTickerMap);
                        }

                        //Apply Nav Hierarchy
                        if (!string.IsNullOrEmpty(fundForecast.NavEstMthd))
                            ApplyNavHierarchy(ticker, fundForecast, fundNavDict);

                        //Process Rights Offers
                        FundRightsOffer fundRightsOffer = null;
                        if (fundForecast.CalcRO == 1)
                        {
                            fundRightsOffersDict.TryGetValue(ticker, out fundRightsOffer);

                            ProcessRightsOffers(ticker
                                , fundForecast
                                , fundRightsOffer
                                , securityPrice
                                , preferredSecurityPrice
                                , fundRedemption);
                        }

                        //Calculate Live Discounts to Published Nav (only for BDCs)
                        if ((!string.IsNullOrEmpty(fundMaster.AssetTyp)
                            && "BDC".Equals(fundMaster.AssetTyp, StringComparison.CurrentCultureIgnoreCase))
                            || "RMRM US".Equals(fundMaster.Ticker))
                            CalculatePublishedDiscountsForBDCs(fundForecast, securityPrice);

                        //Calculate Live Discounts to Estimated/Published Nav
                        CalculateLiveDiscounts(fundForecast
                            , fundMaster
                            , securityPrice
                            , preferredSecurityPrice
                            , fundRightsOffer
                            , fundRedemption);

                        //Calculate Z and D Scores
                        CalculateZDScores(ticker
                            , fundForecast
                            , fundRightsOffer
                            , fundHistStatsDict);

                        if (fundRedemption != null && fundRedemption.NextRedemptionDate.HasValue)
                        {
                            _fundIRRCalculator.CalculateIRR(ticker
                                , fundMaster
                                , fundForecast
                                , fundRedemption
                                , securityPrice
                                , preferredSecurityPrice);
                        }

                        //Process Tender Offers
                        if (fundForecast.CalcTO == 1)
                        {
                            fundTenderOffersDict.TryGetValue(ticker, out FundTenderOffer fundTenderOffer);
                            ProcessTenderOffers(ticker
                                , fundForecast
                                , fundTenderOffer
                                , securityPrice);
                        }

                        //Expected Alpha Calculator
                        _expectedAlphaCalculator.CalculateExpectedAlpha(ticker
                            , fundMaster
                            , fundForecast
                            , histDiscountStats
                            , fundSupplementalDataDict);

                        fundForecast.LastUpdTime = DateTime.Now.ToString("HH:mm:ss");
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error calculating Navs for ticker: " + ticker);
                }
            }

            try
            {
                IDictionary<string, BatchMonitorTO> dict = _cache.Get<Dictionary<string, BatchMonitorTO>>(CacheKeys.BATCH_MONITOR);
                dict["Fund Estimated Nav Update"].LastUpdate = DateTime.Now;
            }
            catch (Exception)
            {
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ticker"></param>
        /// <param name="fundMaster"></param>
        /// <param name="fundForecast"></param>
        /// <param name="securityPrice"></param>
        /// <param name="fundNavDict"></param>
        /// <param name="fundRedemption"></param>
        /// <param name="fxRateDict"></param>
        private void Reset(
            string ticker
            , FundMaster fundMaster
            , FundForecast fundForecast
            , SecurityPrice securityPrice
            , IDictionary<string, FundNav> fundNavDict
            , FundRedemption fundRedemption
            , IDictionary<string, FXRate> fxRateDict)
        {
            FundNav fundNav;
            if (fundNavDict.TryGetValue(ticker, out fundNav))
            {
                fundForecast.LastNav = fundNav.LastNav;
                fundForecast.LastDvdAdjNav = FundForecastOperations.GetLastNav(fundNav);
                fundForecast.LastNavDt = FundForecastOperations.GetLastNavDate(fundNav);
                fundForecast.NavUpdTm = fundNav.NavUpdateTime;
                fundForecast.LastPD = fundNav.LastPctPremium;
                fundForecast.LastNavSrc = fundNav.LastNavSource;
                fundForecast.ShOut = fundNav.EquitySharesOutstanding;
                fundForecast.ExDvdDates = fundNav.ExDvdDates;
                fundForecast.IntrVal = fundNav.IntrinsicValue;
                fundForecast.IntrValFlag = "N";
                fundForecast.OvrLastNav = fundNav.OvrLastNav;
                fundForecast.OvrEstNav = fundNav.OvrEstimatedNav;

                //adjust nav for preferred shares redemption value
                //include preferred share redemption value in nav if it is not already included
                if (fundRedemption != null)
                {
                    fundForecast.PfdRedVal =
                        fundRedemption.PreferredShareRedemptionValue.GetValueOrDefault() *
                        fundRedemption.NumPreferredSharesPerCommonSplitTrust.GetValueOrDefault();

                    string pfdRedemptionValueFlag = fundRedemption.IsPreferredShareRedemptionValueIncludedInNav;
                    if (!string.IsNullOrEmpty(pfdRedemptionValueFlag) &&
                        "N".Equals(pfdRedemptionValueFlag, StringComparison.CurrentCultureIgnoreCase))
                        fundForecast.LastDvdAdjNav += fundForecast.PfdRedVal;
                }

                //reported nav adjusted for ex dividends since reported nav date
                if (fundForecast.LastDvdAdjNav.HasValue && fundNav.DvdFromLastNavDate.HasValue)
                {
                    fundForecast.DvdFromLastNavDt = fundNav.DvdFromLastNavDate;
                    fundForecast.LastDvdAdjNav -= fundNav.DvdFromLastNavDate;
                }

                //UNII balance relative to nav
                if (fundMaster.UNII.HasValue
                    && fundForecast.LastDvdAdjNav.GetValueOrDefault() > 0)
                    fundForecast.UNIINavPct = fundMaster.UNII.GetValueOrDefault() / fundForecast.LastDvdAdjNav.GetValueOrDefault();
            }

            //convert fund market value (from fund's base currency) to USD
            if (fundForecast.ShOut.HasValue && securityPrice != null)
            {
                double lastPrice = securityPrice.LastPrc.GetValueOrDefault();
                if (lastPrice > 0)
                {
                    double fundMarketValue =
                        (fundForecast.ShOut.GetValueOrDefault() * lastPrice) / 1000000.0;

                    string currency = fundForecast.Curr;
                    if (!(currency.Equals("USD", StringComparison.CurrentCultureIgnoreCase)))
                    {
                        if ("GBp".Equals(currency))
                        {
                            fundMarketValue /= 100.0;
                            currency = "GBP";
                        }

                        if (fxRateDict.TryGetValue(currency, out FXRate fxRateFund))
                            fundMarketValue *= fxRateFund.FXRateLatest.GetValueOrDefault();
                    }

                    fundForecast.MV = fundMarketValue;
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ticker"></param>
        /// <param name="fundForecast"></param>
        /// <param name="securityPriceDict"></param>
        private void ProcessHoldingCompanies(string ticker
            , FundForecast fundForecast
            , IDictionary<string, SecurityPrice> securityPriceDict)
        {
            try
            {
                if (fundForecast.LastNavDt.HasValue &&
                    securityPriceDict.TryGetValue("Empire Life", out SecurityPrice securityPrice))
                {
                    DateTime navDate = fundForecast.LastNavDt.GetValueOrDefault();
                    int daysDiff = DateUtils.DaysDiff(navDate, TodaysDate);
                    double navReturn = 0.07 * (daysDiff / 365.0);

                    securityPrice.LastPrc = 0.000001;
                    securityPrice.PrcRtn = navReturn;
                    securityPrice.PrcChng = navReturn;
                }
            }
            catch (Exception ex)
            {
                _logger.LogInformation(ex, "Error Pre-Processing Holding Companies");
            }
        }

        /// <summary>
        /// Calculates accrued interest for BDCs
        /// </summary>
        /// <param name="fundMaster"></param>
        /// <param name="fundForecast"></param>
        private void CalculateAccruedInterest(FundMaster fundMaster, FundForecast fundForecast)
        {
            string ticker = fundMaster.Ticker;

            try
            {
                if (fundForecast.LastNav.HasValue && fundForecast.LastNavDt.HasValue)
                {
                    DateTime? lastDividendDate = fundForecast.LastNavDt;
                    if (fundForecast.AccrRate.HasValue)
                    {
                        int daysSinceLastDividendDate = DateUtils.DaysDiff(lastDividendDate, TodaysDate);
                        double accruedInterest = (daysSinceLastDividendDate / 366.0)
                            * fundForecast.LastNav.GetValueOrDefault()
                            * fundForecast.AccrRate.GetValueOrDefault();
                        fundForecast.AI = accruedInterest;
                    }
                    else if (fundMaster.ShOut.HasValue && fundMaster.NII.HasValue)
                    {
                        double bookValue = fundMaster.ShOut.GetValueOrDefault() * fundForecast.LastNav.GetValueOrDefault();
                        double netIncome = fundMaster.NII.GetValueOrDefault() * 4.0;
                        double accrualRate = netIncome / bookValue;

                        int daysSinceLastNavDate = DateUtils.DaysDiff(lastDividendDate, TodaysDate);
                        double accruedInterest = (daysSinceLastNavDate / 366.0)
                            * fundForecast.LastNav.GetValueOrDefault()
                            * accrualRate;
                        fundForecast.AI = accruedInterest;
                        fundForecast.AccrRate = accrualRate;
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error calculating Accrued Interest for ticker: " + ticker);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ticker"></param>
        /// <param name="fundForecast"></param>
        /// <param name="fundNavDict"></param>
        private void ApplyNavHierarchy(string ticker
            , FundForecast fundForecast
            , IDictionary<string, FundNav> fundNavDict)
        {
            string navEstimationMethod = fundForecast.NavEstMthd;

            if (!"CryptoNav".Equals(navEstimationMethod))
            {
                fundForecast.EstNav = null;
                fundForecast.EstRtn = null;
                fundForecast.UnAdjEstNav = null;
            }

            if (fundForecast.OvrEstNav.HasValue)
            {
                fundForecast.EstNav = fundForecast.OvrEstNav;
                fundForecast.NavEstMthd = "User Override";
            }
            // 2/4/2025 - commented this code as estimated nav are being overriden with published navs
            //else if (fundForecast.LastNavDt.GetValueOrDefault().Date.CompareTo(TodaysDate.Date) == 0)
            //{
            //    fundForecast.EstNav = fundForecast.LastNav;
            //}
            else
            {
                if ("Holdings".Equals(navEstimationMethod, StringComparison.CurrentCultureIgnoreCase))
                {
                    fundForecast.EstNav = fundForecast.PortNav;
                    fundForecast.EstRtn = fundForecast.AdjPortRtn;
                    //returns using proxy securities
                    fundForecast.EstNavProxy = fundForecast.PortNavProxy;
                    fundForecast.EstRtnProxy = fundForecast.AdjPortRtnProxy;
                }
                else if ("ETF Reg".Equals(navEstimationMethod, StringComparison.CurrentCultureIgnoreCase))
                {
                    fundForecast.EstNav = fundForecast.ETFNav;
                    fundForecast.EstRtn = fundForecast.ETFRtn;
                }
                else if ("Proxy".Equals(navEstimationMethod, StringComparison.CurrentCultureIgnoreCase))
                {
                    fundForecast.EstNav = fundForecast.ProxyNav;
                    fundForecast.EstRtn = fundForecast.AdjProxyRtn;
                }
                else if ("CondProxy".Equals(navEstimationMethod, StringComparison.CurrentCultureIgnoreCase))
                {
                    if (!string.IsNullOrEmpty(fundForecast.Cond1ProxyFlag) && fundForecast.Cond1ProxyFlag.Equals("Y"))
                    {
                        fundForecast.EstNav = fundForecast.Cond1ProxyNav;
                        fundForecast.EstRtn = fundForecast.Cond1ProxyRtn;
                        fundForecast.CondProxyFlag = "Condition1 Proxy";
                    }
                    else
                    {
                        fundForecast.EstNav = fundForecast.GenProxyNav;
                        fundForecast.EstRtn = fundForecast.GenProxyRtn;
                        fundForecast.CondProxyFlag = "General Proxy";
                    }
                }
                else if ("NumisEstNav".Equals(navEstimationMethod, StringComparison.CurrentCultureIgnoreCase))
                {
                    if (fundNavDict.TryGetValue(ticker, out FundNav fundNav))
                        fundForecast.EstNav = fundNav.NumisEstimatedNavAdjusted;
                }
                else if ("Published".Equals(navEstimationMethod, StringComparison.CurrentCultureIgnoreCase))
                {
                    fundForecast.EstNav = CommonUtils.AddNullableDoubles(fundForecast.LastDvdAdjNav, fundForecast.AI);
                }
                else if ("Alt Proxy".Equals(navEstimationMethod, StringComparison.CurrentCultureIgnoreCase) ||
                    "AltProxy".Equals(navEstimationMethod, StringComparison.CurrentCultureIgnoreCase))
                {
                    fundForecast.EstNav = fundForecast.AltProxyNav;
                    fundForecast.EstRtn = fundForecast.AltAdjProxyRtn;
                }
            }

            //Apply Nav Estimation Hierarchy
            //Use ETF Regression Nav if Estimated Nav is missing
            //20201215: Added to handle scenario where port holdings are missing but ETF reg navs are available
            if (!"Published".Equals(navEstimationMethod, StringComparison.CurrentCultureIgnoreCase)
                && !fundForecast.EstNav.HasValue
                && fundForecast.ETFNav.HasValue)
            {
                fundForecast.EstNav = fundForecast.ETFNav;
                fundForecast.EstRtn = fundForecast.ETFRtn;
                //_logger.LogInformation("Using ETF Reg Navs for: " + fundForecast.Ticker);
            }

            //Apply Fees
            ApplyFees(fundForecast);

            //Adjust Estimated Nav
            //UnAdjusted Estimated Nav is used for calculating Z and D Scores without Nav Adjustment
            //2020/10/06 - Per William's request
            if (fundForecast.EstNav.HasValue)
            {
                fundForecast.UnAdjEstNav = fundForecast.EstNav;
                if (fundForecast.AdjEstNav.HasValue)
                {
                    fundForecast.EstNav += fundForecast.AdjEstNav.GetValueOrDefault();
                    fundForecast.EstNavProxy += fundForecast.AdjEstNav.GetValueOrDefault();
                }
            }
        }

        /// <summary>
        /// Apply Management Fees and Tax Liability  
        /// </summary>
        /// <param name="fundForecast"></param>
        private void ApplyFees(FundForecast fundForecast)
        {
            //Tax Liability (applies if there is a +ve return)
            if (fundForecast.TaxLiability.HasValue && fundForecast.EstRtn > 0)
            {
                fundForecast.EstRtn *= (1.0 - fundForecast.TaxLiability.GetValueOrDefault());
                fundForecast.EstNav = fundForecast.LastDvdAdjNav * (1.0 + fundForecast.EstRtn);
            }

            //Management Fees
            if (fundForecast.MgmtFee.HasValue && fundForecast.EstRtn > 0)
            {
                int daysDiff = DateUtils.DaysDiff(fundForecast.LastNavDt, TodaysDate);
                double managementFee = fundForecast.MgmtFee.GetValueOrDefault() * daysDiff / 365.0;
                fundForecast.EstRtn -= managementFee;
                fundForecast.EstNav = fundForecast.LastDvdAdjNav * (1.0 + fundForecast.EstRtn);
            }
        }

        /// <summary>
        /// Calculates Live Discounts
        /// </summary>
        /// <param name="fundForecast"></param>
        /// <param name="securityPrice"></param>
        /// <param name="preferredSharePrice"></param>
        private void CalculateLiveDiscounts(
            FundForecast fundForecast
            , FundMaster fundMaster
            , SecurityPrice securityPrice
            , SecurityPrice preferredSharePrice
            , FundRightsOffer fundRightsOffer
            , FundRedemption fundRedemption)
        {
            double preferredShareLastPrice = 0.0;
            double preferredShareBidPrice = 0.0;
            double preferredShareAskPrice = 0.0;
            double numPreferredSharesPerCommonSplitTrust = 0.0;

            if (fundRedemption != null)
                numPreferredSharesPerCommonSplitTrust = fundRedemption.NumPreferredSharesPerCommonSplitTrust.GetValueOrDefault();

            if (preferredSharePrice != null)
            {
                preferredShareLastPrice = preferredSharePrice.LastPrc.GetValueOrDefault() * numPreferredSharesPerCommonSplitTrust;
                preferredShareBidPrice = preferredSharePrice.BidPrc.GetValueOrDefault() * numPreferredSharesPerCommonSplitTrust;
                preferredShareAskPrice = preferredSharePrice.AskPrc.GetValueOrDefault() * numPreferredSharesPerCommonSplitTrust;
            }

            //if estimated nav is not available and if instrinsic value is set, then use intrinsic value
            double? estimatedNav = FundForecastOperations.GetEstimatedNav(fundForecast);
            if (!estimatedNav.HasValue && fundForecast.IntrVal.HasValue)
                estimatedNav = fundForecast.IntrVal;

            if (estimatedNav.HasValue && securityPrice != null)
            {
                double leverageRatio = (1.0 + fundForecast.LevRatio.GetValueOrDefault());

                //last price
                double lastPrice = securityPrice.LastPrc.GetValueOrDefault();
                if (lastPrice > 0)
                {
                    lastPrice += preferredShareLastPrice;
                    fundForecast.LastPrc = securityPrice.LastPrc;
                    fundForecast.PDLastPrc = DataConversionUtils.CalculateReturn(lastPrice, estimatedNav);
                    if (leverageRatio != 0)
                        fundForecast.PDLastPrcUnLev = fundForecast.PDLastPrc / leverageRatio;
                    fundForecast.UnAdjPDLastPrc = fundForecast.PDLastPrc;
                    if (fundForecast.UnAdjEstNav.HasValue)
                        fundForecast.UnAdjPDLastPrc = DataConversionUtils.CalculateReturn(lastPrice, fundForecast.UnAdjEstNav);
                }

                //bid price
                double bidPrice = securityPrice.BidPrc.GetValueOrDefault();
                if (bidPrice > 0)
                {
                    bidPrice += preferredShareBidPrice;
                    fundForecast.PDBidPrc = DataConversionUtils.CalculateReturn(bidPrice, estimatedNav);
                    if (leverageRatio != 0)
                        fundForecast.PDBidPrcUnLev = fundForecast.PDBidPrc / leverageRatio;
                }

                //ask price
                double askPrice = securityPrice.AskPrc.GetValueOrDefault();
                if (askPrice > 0)
                {
                    askPrice += preferredShareAskPrice;
                    fundForecast.PDAskPrc = DataConversionUtils.CalculateReturn(askPrice, estimatedNav);
                    if (leverageRatio != 0)
                        fundForecast.PDAskPrcUnLev = fundForecast.PDAskPrc / leverageRatio;
                }

                //change from last discount to live discount
                if (fundForecast.PDLastPrc.HasValue && fundForecast.LastPD.HasValue)
                    fundForecast.PDChng =
                        fundForecast.PDLastPrc.GetValueOrDefault() - fundForecast.LastPD.GetValueOrDefault();
            }

            //adjustments for rights offering
            if (fundRightsOffer != null
                && !string.IsNullOrEmpty(fundRightsOffer.DisplayPostRightsOfferDiscount)
                && fundRightsOffer.DisplayPostRightsOfferDiscount.Equals("Y", StringComparison.CurrentCultureIgnoreCase))
            {
                double? estimatedNavPostRightsIssue = fundForecast.RONav;
                if (estimatedNavPostRightsIssue.HasValue)
                {
                    //bid price
                    double bidPrice = securityPrice.BidPrc.GetValueOrDefault();
                    if (bidPrice > 0)
                    {
                        bidPrice += preferredShareBidPrice;
                        fundForecast.ROPDBidPrc = DataConversionUtils.CalculateReturn(bidPrice, estimatedNavPostRightsIssue);
                    }

                    //ask price
                    double askPrice = securityPrice.AskPrc.GetValueOrDefault();
                    if (askPrice > 0)
                    {
                        askPrice += preferredShareAskPrice;
                        fundForecast.ROPDAskPrc = DataConversionUtils.CalculateReturn(askPrice, estimatedNavPostRightsIssue);
                    }
                }
            }

            CalculateDiscountChange(fundForecast, fundMaster);
        }

        /// <summary>
        /// Calculates Live Discounts to Published Nav for BDCs
        /// </summary>
        /// <param name="fundForecast"></param>
        /// <param name="securityPrice"></param>
        private void CalculatePublishedDiscountsForBDCs(
            FundForecast fundForecast, SecurityPrice securityPrice)
        {
            if (fundForecast.LastDvdAdjNav.HasValue && securityPrice != null)
            {
                double publishedNav = fundForecast.LastDvdAdjNav.GetValueOrDefault();
                double leverageRatio = (1.0 + fundForecast.LevRatio.GetValueOrDefault());

                //last price
                double lastPrice = securityPrice.LastPrc.GetValueOrDefault();
                if (lastPrice > 0)
                {
                    fundForecast.PubPDLastPrc
                        = DataConversionUtils.CalculateReturn(lastPrice, publishedNav);
                    if (leverageRatio != 0)
                        fundForecast.PubPDLastPrcUnLev = fundForecast.PubPDLastPrc / leverageRatio;
                }

                //bid price
                double bidPrice = securityPrice.BidPrc.GetValueOrDefault();
                if (bidPrice > 0)
                {
                    fundForecast.PubPDBidPrc
                        = DataConversionUtils.CalculateReturn(bidPrice, publishedNav);
                    if (leverageRatio != 0)
                        fundForecast.PubPDBidPrcUnLev = fundForecast.PubPDBidPrc / leverageRatio;
                }

                //ask price
                double askPrice = securityPrice.AskPrc.GetValueOrDefault();
                if (askPrice > 0)
                {
                    fundForecast.PubPDAskPrc
                        = DataConversionUtils.CalculateReturn(askPrice, publishedNav);
                    if (leverageRatio != 0)
                        fundForecast.PubPDAskPrcUnLev = fundForecast.PubPDAskPrc / leverageRatio;
                }
            }
        }

        /// <summary>
        /// Calculates Live Discounts for South Korean Pfds using Common Share Price
        /// Discount of Pfd Share Price to Common Share Price
        /// </summary>
        /// <param name="ticker"></param>
        /// <param name="fundForecast"></param>
        /// <param name="securityPriceDict"></param>
        /// <param name="pfdCommonShareTickerMap"></param>
        /// <param name="priceTickerMap"></param>
        private void CalculateDiscountsForSouthKoreaPfds(
            string ticker
            , FundForecast fundForecast
            , IDictionary<string, SecurityPrice> securityPriceDict
            , IDictionary<string, string> pfdCommonShareTickerMap
            , IDictionary<string, string> priceTickerMap)
        {
            try
            {
                //get security identifier of common share
                if (pfdCommonShareTickerMap.TryGetValue(ticker, out string commonShareTicker))
                {
                    //get security price of pfd share
                    SecurityPrice securityPricePfdShare = SecurityPriceLookupOperations.GetSecurityPrice(ticker, priceTickerMap, securityPriceDict);

                    //get security price of common share
                    SecurityPrice securityPriceCommonShare = SecurityPriceLookupOperations.GetSecurityPrice(commonShareTicker, priceTickerMap, securityPriceDict);

                    if (securityPricePfdShare != null && securityPriceCommonShare != null)
                    {
                        double leverageRatio = (1.0 + fundForecast.LevRatio.GetValueOrDefault());

                        //last price
                        if (securityPricePfdShare.LastPrc.GetValueOrDefault() > 0
                            && securityPriceCommonShare.LastPrc.GetValueOrDefault() > 0)
                        {
                            fundForecast.PDLastPrc
                                = DataConversionUtils.CalculateReturn(securityPricePfdShare.LastPrc.GetValueOrDefault(), securityPriceCommonShare.LastPrc.GetValueOrDefault());
                            fundForecast.LastPrc = securityPricePfdShare.LastPrc;
                            if (leverageRatio != 0)
                                fundForecast.PDLastPrcUnLev = fundForecast.PDLastPrc / leverageRatio;
                        }

                        //bid price
                        if (securityPricePfdShare.BidPrc.GetValueOrDefault() > 0
                            && securityPriceCommonShare.BidPrc.GetValueOrDefault() > 0)
                        {
                            fundForecast.PDBidPrc
                                = DataConversionUtils.CalculateReturn(securityPricePfdShare.BidPrc.GetValueOrDefault(), securityPriceCommonShare.BidPrc.GetValueOrDefault());
                            if (leverageRatio != 0)
                                fundForecast.PDBidPrcUnLev = fundForecast.PDBidPrc / leverageRatio;
                        }

                        //ask price
                        if (securityPricePfdShare.AskPrc.GetValueOrDefault() > 0
                            && securityPriceCommonShare.AskPrc.GetValueOrDefault() > 0)
                        {
                            fundForecast.PDAskPrc
                                = DataConversionUtils.CalculateReturn(securityPricePfdShare.AskPrc.GetValueOrDefault(), securityPriceCommonShare.AskPrc.GetValueOrDefault());
                            if (leverageRatio != 0)
                                fundForecast.PDAskPrcUnLev = fundForecast.PDAskPrc / leverageRatio;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error calculating (South Korean Pfds) Live Discounts for ticker: " + ticker);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ticker"></param>
        /// <param name="fundForecast"></param>
        /// <param name="fundRightsOffer"></param>
        /// <param name="securityPrice"></param>
        /// <param name="preferredSharePrice"></param>
        private void ProcessRightsOffers(string ticker
            , FundForecast fundForecast
            , FundRightsOffer fundRightsOffer
            , SecurityPrice securityPrice
            , SecurityPrice preferredSharePrice
            , FundRedemption fundRedemption)
        {
            double? estimatedNav = FundForecastOperations.GetEstimatedNav(fundForecast);
            if (estimatedNav.HasValue)
            {
                double sharesOutstanding = fundForecast.ShOut.GetValueOrDefault();

                //fund rights offer details
                double subscriptionRatio = fundRightsOffer.SubscriptionRatio.GetValueOrDefault();
                double overSubscriptionRatio = fundRightsOffer.OverSubscriptionRatio.GetValueOrDefault();
                double subscriptionDiscount = fundRightsOffer.SubscriptionPriceDiscount.GetValueOrDefault();
                string subscriptionDiscountField = fundRightsOffer.SubscriptionPriceDiscountField;

                //preferred share price
                double preferredShareLastPrice = 0;
                double numPreferredSharesPerCommonSplitTrust = 0;
                if (fundRedemption != null)
                    numPreferredSharesPerCommonSplitTrust = fundRedemption.NumPreferredSharesPerCommonSplitTrust.GetValueOrDefault();
                if (preferredSharePrice != null && preferredSharePrice.LastPrc.HasValue)
                    preferredShareLastPrice = preferredSharePrice.LastPrc.GetValueOrDefault()
                        * numPreferredSharesPerCommonSplitTrust;

                //security price
                double lastPrice = 0;
                if (securityPrice != null && securityPrice.LastPrc.HasValue)
                    lastPrice = securityPrice.LastPrc.GetValueOrDefault();
                lastPrice += preferredShareLastPrice;

                //calculations
                double totalFundNav = estimatedNav.GetValueOrDefault() * sharesOutstanding;
                double subscriptionShares = subscriptionRatio * sharesOutstanding;
                double overSubscriptionShares = overSubscriptionRatio * subscriptionShares;
                double totalNewSharesIssued = subscriptionShares + overSubscriptionShares;
                double subscriptionPrice = lastPrice;
                double discountedSubscriptionPrice = 0;
                if (subscriptionDiscountField.Equals("Price", StringComparison.CurrentCultureIgnoreCase))
                    discountedSubscriptionPrice = subscriptionPrice * subscriptionDiscount;
                else if (subscriptionDiscountField.Equals("Nav", StringComparison.CurrentCultureIgnoreCase))
                    discountedSubscriptionPrice = estimatedNav.GetValueOrDefault() * subscriptionDiscount;

                //if the rights offering price is known, use it
                if (fundRightsOffer.SubscriptionPrice.HasValue)
                    discountedSubscriptionPrice = fundRightsOffer.SubscriptionPrice.GetValueOrDefault();

                double totalFundNavPostRightsIssue = totalFundNav + (totalNewSharesIssued * discountedSubscriptionPrice);
                double totalSharesPostRightsIssue = sharesOutstanding + totalNewSharesIssued;
                double navPostRightsIssue = totalFundNavPostRightsIssue / totalSharesPostRightsIssue;
                double navDilution = (navPostRightsIssue / estimatedNav.GetValueOrDefault()) - 1.0;
                double pricePostRightsIssue = (1.0 + fundForecast.PDLastPrc.GetValueOrDefault()) * navPostRightsIssue;
                double priceChangePostRightsIssue = (pricePostRightsIssue / lastPrice) - 1.0;
                double estimatedDiscountPostRightsIssues = (lastPrice / navPostRightsIssue) - 1.0;

                if (!Double.IsInfinity(navPostRightsIssue) && !Double.IsNaN(navPostRightsIssue))
                    fundForecast.RONav = navPostRightsIssue;

                if (!Double.IsInfinity(pricePostRightsIssue) && !Double.IsNaN(pricePostRightsIssue))
                    fundForecast.ROPrc = pricePostRightsIssue;

                if (!Double.IsInfinity(priceChangePostRightsIssue) && !Double.IsNaN(priceChangePostRightsIssue))
                    fundForecast.ROPrcChng = priceChangePostRightsIssue;

                if (!Double.IsInfinity(navDilution) && !Double.IsNaN(navDilution))
                    fundForecast.RONavDilution = navDilution;

                if (!Double.IsInfinity(estimatedDiscountPostRightsIssues) && !Double.IsNaN(estimatedDiscountPostRightsIssues))
                    fundForecast.ROPDLastPrc = estimatedDiscountPostRightsIssues;

                fundForecast.RODscntDispFlag = (fundRightsOffer.DisplayPostRightsOfferDiscount.Equals("Y", StringComparison.CurrentCultureIgnoreCase)) ? 1 : 0;
            }
        }

        /// <summary>
        /// Calculates live Z and D scores
        /// </summary>
        /// <param name="ticker"></param>
        /// <param name="fundForecast"></param>
        /// <param name="fundRightsOffer"></param>
        /// <param name="fundHistStatsDict"></param>
        private void CalculateZDScores(string ticker
            , FundForecast fundForecast
            , FundRightsOffer fundRightsOffer
            , IDictionary<string, FundHistStats> fundHistStatsDict)
        {
            FundHistStats fundHistStats;
            if (fundHistStatsDict.TryGetValue(ticker, out fundHistStats))
            {
                double? lastPriceDiscount = fundForecast.PDLastPrc;

                //if there is a nav adjustment then ignore the nav adjustment and calculate the scores based on estimated nav
                if (fundForecast.UnAdjPDLastPrc.HasValue)
                    lastPriceDiscount = fundForecast.UnAdjPDLastPrc;

                //if use rights offer flag is set then use nav post rights issue
                if (fundRightsOffer != null
                        && !string.IsNullOrEmpty(fundRightsOffer.DisplayPostRightsOfferDiscount)
                        && fundRightsOffer.DisplayPostRightsOfferDiscount.Equals("Y", StringComparison.CurrentCultureIgnoreCase))
                    lastPriceDiscount = fundForecast.ROPDLastPrc;

                if (lastPriceDiscount.HasValue)
                {
                    //1W
                    if (fundHistStats.Mean1W.HasValue && fundHistStats.StdDev1W.HasValue)
                    {
                        fundForecast.DS1W = lastPriceDiscount - fundHistStats.Mean1W;
                        fundForecast.ZS1W = DataConversionUtils.CalculateZScore(fundForecast.DS1W, fundHistStats.StdDev1W);
                    }

                    //2W
                    if (fundHistStats.Mean2W.HasValue && fundHistStats.StdDev2W.HasValue)
                    {
                        fundForecast.DS2W = lastPriceDiscount - fundHistStats.Mean2W;
                        fundForecast.ZS2W = DataConversionUtils.CalculateZScore(fundForecast.DS2W, fundHistStats.StdDev2W);
                    }

                    //1M
                    if (fundHistStats.Mean1M.HasValue && fundHistStats.StdDev1M.HasValue)
                    {
                        fundForecast.DS1M = lastPriceDiscount - fundHistStats.Mean1M;
                        fundForecast.ZS1M = DataConversionUtils.CalculateZScore(fundForecast.DS1M, fundHistStats.StdDev1M);
                    }

                    //3M
                    if (fundHistStats.Mean3M.HasValue && fundHistStats.StdDev3M.HasValue)
                    {
                        fundForecast.DS3M = lastPriceDiscount - fundHistStats.Mean3M;
                        fundForecast.ZS3M = DataConversionUtils.CalculateZScore(fundForecast.DS3M, fundHistStats.StdDev3M);
                    }

                    //6M
                    if (fundHistStats.Mean6M.HasValue && fundHistStats.StdDev6M.HasValue)
                    {
                        fundForecast.DS6M = lastPriceDiscount - fundHistStats.Mean6M;
                        fundForecast.ZS6M = DataConversionUtils.CalculateZScore(fundForecast.DS6M, fundHistStats.StdDev6M);
                    }

                    //12M
                    if (fundHistStats.Mean12M.HasValue && fundHistStats.StdDev12M.HasValue)
                    {
                        fundForecast.DS12M = lastPriceDiscount - fundHistStats.Mean12M;
                        fundForecast.ZS12M = DataConversionUtils.CalculateZScore(fundForecast.DS12M, fundHistStats.StdDev12M);
                    }

                    //24M
                    if (fundHistStats.Mean24M.HasValue && fundHistStats.StdDev24M.HasValue)
                    {
                        fundForecast.DS24M = lastPriceDiscount - fundHistStats.Mean24M;
                        fundForecast.ZS24M = DataConversionUtils.CalculateZScore(fundForecast.DS24M, fundHistStats.StdDev24M);
                    }

                    //36M
                    if (fundHistStats.Mean36M.HasValue && fundHistStats.StdDev36M.HasValue)
                    {
                        fundForecast.DS36M = lastPriceDiscount - fundHistStats.Mean36M;
                        fundForecast.ZS36M = DataConversionUtils.CalculateZScore(fundForecast.DS36M, fundHistStats.StdDev36M);
                    }

                    //60M
                    if (fundHistStats.Mean60M.HasValue && fundHistStats.StdDev60M.HasValue)
                    {
                        fundForecast.DS60M = lastPriceDiscount - fundHistStats.Mean60M;
                        fundForecast.ZS60M = DataConversionUtils.CalculateZScore(fundForecast.DS60M, fundHistStats.StdDev60M);
                    }

                    //Life
                    if (fundHistStats.MeanLife.HasValue && fundHistStats.StdDevLife.HasValue)
                    {
                        fundForecast.DSLife = lastPriceDiscount - fundHistStats.MeanLife;
                        fundForecast.ZSLife = DataConversionUtils.CalculateZScore(fundForecast.DSLife, fundHistStats.StdDevLife);
                    }
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ticker"></param>
        /// <param name="fundForecast"></param>
        /// <param name="fundTenderOffer"></param>
        /// <param name="securityPrice"></param>
        private void ProcessTenderOffers(string ticker
            , FundForecast fundForecast
            , FundTenderOffer fundTenderOffer
            , SecurityPrice securityPrice)
        {
            double lastPrice = 0;
            double bidPrice = 0;
            double askPrice = 0;

            double? estimatedNav = fundForecast.EstNav;
            if (estimatedNav.HasValue && securityPrice != null)
            {
                lastPrice = securityPrice.LastPrc.GetValueOrDefault();
                bidPrice = securityPrice.BidPrc.GetValueOrDefault();
                askPrice = securityPrice.AskPrc.GetValueOrDefault();

                //fund tender offer details
                double tenderOffer = fundTenderOffer.SharesTendered.GetValueOrDefault();
                double discountPostTender = fundTenderOffer.DiscountPostTender.GetValueOrDefault();
                double tenderPriceDiscount = fundTenderOffer.TenderDiscount.GetValueOrDefault();
                double institutionalHoldings = fundTenderOffer.InstitutionalHoldings.GetValueOrDefault();
                double retailHoldings = fundTenderOffer.RetailHoldings.GetValueOrDefault();
                double institutionalHoldingsTendered = fundTenderOffer.InstitutionalHoldingsTendered.GetValueOrDefault();
                double retailHoldingsTendered = fundTenderOffer.RetailHoldingsTendered.GetValueOrDefault();

                double sharesTenderedTotal
                    = (institutionalHoldings * institutionalHoldingsTendered) + (retailHoldings * retailHoldingsTendered);
                double sharesTenderedActual = tenderOffer / sharesTenderedTotal;
                double sharesTenderedReturned = (1.0 - sharesTenderedActual);

                int daysDiff = DateUtils.DaysDiff(TodaysDate, fundTenderOffer.TenderEndDate);
                double expenseRatioAdjustment = 1.0 + (fundTenderOffer.ExpenseRatio.GetValueOrDefault() * (daysDiff / 365.0));

                double navPostTender
                    = (1.0 + (tenderOffer / (1.0 - tenderOffer) * (1.0 - tenderPriceDiscount)))
                    * estimatedNav.GetValueOrDefault()
                    * expenseRatioAdjustment;
                double priceOfSharesTendered
                    = tenderPriceDiscount * estimatedNav.GetValueOrDefault() * expenseRatioAdjustment;
                double priceOfSharesReturned = navPostTender * (1.0 + discountPostTender);
                double finalPrice
                    = (sharesTenderedActual * priceOfSharesTendered) + (sharesTenderedReturned * priceOfSharesReturned);
                double priceReturn = (finalPrice / lastPrice) - 1.0;

                //last price
                DateTime settleDate = DateUtils.AddDays(TodaysDate, 2);
                IEnumerable<CashItem> cfs =
                    new CashItem[] {
                        new CashItem(settleDate, -1.0 * lastPrice),
                        new CashItem(fundTenderOffer.TenderEndDate.GetValueOrDefault(), finalPrice) };

                double tenderIRRLastPrice = XIRR.RunScenario(cfs);

                //bid price
                cfs = new CashItem[] {
                    new CashItem(settleDate, -1.0 * bidPrice),
                    new CashItem(fundTenderOffer.TenderEndDate.GetValueOrDefault(), finalPrice) };

                double tenderIRRBidPrice = XIRR.RunScenario(cfs);

                //ask price
                cfs = new CashItem[] {
                    new CashItem(settleDate, -1.0 * askPrice),
                    new CashItem(fundTenderOffer.TenderEndDate.GetValueOrDefault(), finalPrice) };

                double tenderIRRAskPrice = XIRR.RunScenario(cfs);

                if (!Double.IsInfinity(sharesTenderedTotal) && !Double.IsNaN(sharesTenderedTotal))
                    fundForecast.TOSharesTot = sharesTenderedTotal;

                if (!Double.IsInfinity(sharesTenderedActual) && !Double.IsNaN(sharesTenderedActual))
                    fundForecast.TOSharesAct = sharesTenderedActual;

                if (!Double.IsInfinity(sharesTenderedReturned) && !Double.IsNaN(sharesTenderedReturned))
                    fundForecast.TOSharesRtnd = sharesTenderedReturned;

                if (!Double.IsInfinity(navPostTender) && !Double.IsNaN(navPostTender))
                    fundForecast.TONav = navPostTender;

                if (!Double.IsInfinity(priceOfSharesTendered) && !Double.IsNaN(priceOfSharesTendered))
                    fundForecast.TOPrc = priceOfSharesTendered;

                if (!Double.IsInfinity(priceOfSharesReturned) && !Double.IsNaN(priceOfSharesReturned))
                    fundForecast.TORtndPrc = priceOfSharesReturned;

                if (!Double.IsInfinity(finalPrice) && !Double.IsNaN(finalPrice))
                    fundForecast.TOFinalPrc = finalPrice;

                if (!Double.IsInfinity(priceReturn) && !Double.IsNaN(priceReturn))
                    fundForecast.TORtn = priceReturn;

                if (!Double.IsInfinity(tenderIRRLastPrice) && !Double.IsNaN(tenderIRRLastPrice))
                    fundForecast.TOIRRLastPrc = tenderIRRLastPrice;

                if (!Double.IsInfinity(tenderIRRBidPrice) && !Double.IsNaN(tenderIRRBidPrice))
                    fundForecast.TOIRRBidPrc = tenderIRRBidPrice;

                if (!Double.IsInfinity(tenderIRRAskPrice) && !Double.IsNaN(tenderIRRAskPrice))
                    fundForecast.TOIRRAskPrc = tenderIRRAskPrice;

                fundForecast.IRRType = "Tender";
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void CalculateCryptoFundNavs()
        {
            IDictionary<string, CryptoSecMst> cryptoSecurityDict = _cache.Get<IDictionary<string, CryptoSecMst>>(CacheKeys.CRYPTO_SECURITY_MST);
            IDictionary<string, SecurityPrice> securityPriceDict = _cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);
            IDictionary<string, string> priceTickerMap = _cache.Get<IDictionary<string, string>>(CacheKeys.PRICE_TICKER_MAP);
            IDictionary<string, FXRate> fxRateDict = _cache.Get<IDictionary<string, FXRate>>(CacheKeys.FX_RATES);
            IDictionary<string, FundForecast> fundForecastDict = _cache.Get<IDictionary<string, FundForecast>>(CacheKeys.FUND_FORECASTS);

            foreach (KeyValuePair<string, CryptoSecMst> kvp in cryptoSecurityDict)
            {
                CryptoSecMst security = kvp.Value;
                string ticker = security.Ticker;
                string currency = security.Curr;

                if (!string.IsNullOrEmpty(security.RefCryptoCoinId) &&
                    securityPriceDict.TryGetValue(security.RefCryptoCoinId, out SecurityPrice cryptoCoinPrice))
                {
                    double nav = cryptoCoinPrice.LastPrc.GetValueOrDefault() * security.NumCoinsPerUnit.GetValueOrDefault();
                    if (!(currency.Equals("USD", StringComparison.CurrentCultureIgnoreCase)))
                    {
                        if (fxRateDict.TryGetValue(currency, out FXRate fxRateFund))
                            nav *= (1.0 / fxRateFund.FXRateLatest.GetValueOrDefault());
                    }

                    if (fundForecastDict.TryGetValue(ticker, out FundForecast fundForecast))
                    {
                        security.CryptoNav = nav;
                        fundForecast.EstNav = nav;
                        fundForecast.CoinsPerUnit = security.NumCoinsPerUnit;

                        SecurityPrice securityPrice = SecurityPriceLookupOperations.GetSecurityPrice(ticker, priceTickerMap, securityPriceDict);
                        if (securityPrice != null)
                        {
                            //last price
                            double lastPrice = securityPrice.LastPrc.GetValueOrDefault();
                            if (lastPrice > 0)
                            {
                                fundForecast.LastPrc = securityPrice.LastPrc;
                                security.PDLastPrc = DataConversionUtils.CalculateReturn(lastPrice, nav);
                            }

                            //bid price
                            double bidPrice = securityPrice.BidPrc.GetValueOrDefault();
                            if (bidPrice > 0)
                            {
                                security.PDBidPrc = DataConversionUtils.CalculateReturn(bidPrice, nav);
                                fundForecast.PDBidPrc = security.PDBidPrc;
                            }

                            //ask price
                            double askPrice = securityPrice.AskPrc.GetValueOrDefault();
                            if (askPrice > 0)
                            {
                                security.PDAskPrc = DataConversionUtils.CalculateReturn(askPrice, nav);
                                fundForecast.PDAskPrc = security.PDAskPrc;
                            }
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Resets Rights Offer Details
        /// Sets calculation flag and resets fields (this is done if rights offer is expired/removed)
        /// </summary>
        public void ResetRightsOfferDetails()
        {
            IDictionary<string, FundForecast> fundForecastDict = _cache.Get<IDictionary<string, FundForecast>>(CacheKeys.FUND_FORECASTS);
            IDictionary<string, FundRightsOffer> fundRightsOffersDict = _cache.Get<IDictionary<string, FundRightsOffer>>(CacheKeys.FUND_RIGHTS_OFFERS);

            if (fundForecastDict != null && fundForecastDict.Count > 0)
            {
                foreach (KeyValuePair<string, FundForecast> kvp in fundForecastDict)
                {
                    FundForecast fundForecast = kvp.Value;

                    //reset fields
                    fundForecast.CalcRO = 0;
                    fundForecast.RONav = null;
                    fundForecast.RONavDilution = null;
                    fundForecast.ROPrc = null;
                    fundForecast.ROPrcChng = null;
                    fundForecast.ROPDLastPrc = null;
                    fundForecast.ROPDBidPrc = null;
                    fundForecast.ROPDAskPrc = null;
                    fundForecast.RODscntDispFlag = 0;

                    //set flag to calculate price, nav and discount post rights offer
                    if (fundRightsOffersDict.TryGetValue(fundForecast.Ticker, out FundRightsOffer fundRightsOffer))
                        fundForecast.CalcRO = 1;
                }
            }
        }

        /// <summary>
        /// Resets Tender Offer Details
        /// Sets calculation flag and resets fields (this is done if tender offer is expired/removed)
        /// </summary>
        public void ResetTenderOfferDetails()
        {
            IDictionary<string, FundForecast> fundForecastDict = _cache.Get<IDictionary<string, FundForecast>>(CacheKeys.FUND_FORECASTS);
            IDictionary<string, FundTenderOffer> fundTenderOffersDict = _cache.Get<IDictionary<string, FundTenderOffer>>(CacheKeys.FUND_TENDER_OFFERS);

            if (fundForecastDict != null && fundForecastDict.Count > 0)
            {
                foreach (KeyValuePair<string, FundForecast> kvp in fundForecastDict)
                {
                    FundForecast fundForecast = kvp.Value;

                    //reset fields
                    fundForecast.CalcTO = 0;
                    fundForecast.TOSharesTot = null;
                    fundForecast.TOSharesAct = null;
                    fundForecast.TOSharesRtnd = null;
                    fundForecast.TONav = null;
                    fundForecast.TOPrc = null;
                    fundForecast.TORtndPrc = null;
                    fundForecast.TOFinalPrc = null;
                    fundForecast.TORtn = null;
                    fundForecast.TOIRRLastPrc = null;
                    fundForecast.TOIRRBidPrc = null;
                    fundForecast.TOIRRAskPrc = null;

                    //set flag to calculate price, nav and discount post rights offer
                    if (fundTenderOffersDict.TryGetValue(fundForecast.Ticker, out FundTenderOffer fundTenderOffer))
                        fundForecast.CalcTO = 1;
                }
            }
        }

        /// <summary>
        /// Resets Proxy Formula Details
        /// Sets calculation flag and resets fields
        /// </summary>
        public void ResetFundProxyDetails()
        {
            IDictionary<string, FundForecast> fundForecastDict = _cache.Get<IDictionary<string, FundForecast>>(CacheKeys.FUND_FORECASTS);
            IDictionary<string, FundProxyFormula> fundProxyFormulaDict = _cache.Get<IDictionary<string, FundProxyFormula>>(CacheKeys.PROXY_FORMULAS);

            if (fundForecastDict != null && fundForecastDict.Count > 0)
            {
                foreach (FundForecast fundForecast in fundForecastDict.Values)
                {
                    //reset fields
                    fundForecast.CalcFundProxy = 0;
                    fundForecast.ProxyNav = null;
                    fundForecast.ProxyRtn = null;
                    fundForecast.AdjProxyRtn = null;
                    fundForecast.ProxyForm = null;

                    //set flag to calculate nav using proxy formula
                    if (fundProxyFormulaDict.TryGetValue(fundForecast.Ticker, out FundProxyFormula fundProxyFormula))
                        fundForecast.CalcFundProxy = 1;
                }
            }
        }

        /// <summary>
        /// Resets Alternate Proxy Formula Details
        /// Sets calculation flag and resets fields
        /// </summary>
        public void ResetFundAltProxyDetails()
        {
            IDictionary<string, FundForecast> fundForecastDict = _cache.Get<IDictionary<string, FundForecast>>(CacheKeys.FUND_FORECASTS);
            IDictionary<string, FundProxyFormula> fundProxyFormulaDict = _cache.Get<IDictionary<string, FundProxyFormula>>(CacheKeys.ALT_PROXY_FORMULAS);

            if (fundForecastDict != null && fundForecastDict.Count > 0)
            {
                foreach (FundForecast fundForecast in fundForecastDict.Values)
                {
                    //reset fields
                    fundForecast.CalcFundAltProxy = 0;
                    fundForecast.AltProxyNav = null;
                    fundForecast.AltProxyRtn = null;
                    fundForecast.AltAdjProxyRtn = null;
                    fundForecast.AltProxyForm = null;

                    //set flag to calculate nav using proxy formula
                    if (fundProxyFormulaDict.TryGetValue(fundForecast.Ticker, out FundProxyFormula fundProxyFormula))
                        fundForecast.CalcFundAltProxy = 1;
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ticker"></param>
        public void ResetFundProxyDetails(string ticker)
        {
            IDictionary<string, FundForecast> fundForecastDict = _cache.Get<IDictionary<string, FundForecast>>(CacheKeys.FUND_FORECASTS);
            if (fundForecastDict.TryGetValue(ticker, out FundForecast fundForecast))
                fundForecast.CalcFundProxy = 1;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ticker"></param>
        public void ResetFundAltProxyDetails(string ticker)
        {
            IDictionary<string, FundForecast> fundForecastDict = _cache.Get<IDictionary<string, FundForecast>>(CacheKeys.FUND_FORECASTS);
            if (fundForecastDict.TryGetValue(ticker, out FundForecast fundForecast))
                fundForecast.CalcFundAltProxy = 1;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ticker"></param>
        public void ResetFundPortProxyDetails(string ticker)
        {
            IDictionary<string, FundForecast> fundForecastDict = _cache.Get<IDictionary<string, FundForecast>>(CacheKeys.FUND_FORECASTS);
            if (fundForecastDict.TryGetValue(ticker, out FundForecast fundForecast))
                fundForecast.CalcPortProxy = 1;
        }

        /// <summary>
        /// Resets Port (Fixed Income) Proxy Formula Details
        /// Sets calculation flag and resets fields
        /// </summary>
        public void ResetFundPortProxyDetails()
        {
            IDictionary<string, FundForecast> fundForecastDict = _cache.Get<IDictionary<string, FundForecast>>(CacheKeys.FUND_FORECASTS);
            IDictionary<string, FundProxyFormula> fundPortProxyFormulaDict = _cache.Get<IDictionary<string, FundProxyFormula>>(CacheKeys.PORT_PROXY_FORMULAS);

            if (fundForecastDict != null && fundForecastDict.Count > 0)
            {
                foreach (KeyValuePair<string, FundForecast> kvp in fundForecastDict)
                {
                    FundForecast fundForecast = kvp.Value;

                    //reset fields
                    fundForecast.CalcPortProxy = 0;
                    fundForecast.PortFIProxyRtn = null;
                    fundForecast.PortFIProxyForm = null;

                    //set flag to calculate fixed income securities return in fund portfolio using proxy formula
                    if (fundPortProxyFormulaDict.TryGetValue(fundForecast.Ticker, out FundProxyFormula fundProxyFormula))
                        fundForecast.CalcPortProxy = 1;
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fundForecast"></param>
        /// <param name="fundMaster"></param>
        private void CalculateDiscountChange(FundForecast fundForecast, FundMaster fundMaster)
        {
            try
            {
                fundForecast.PDChngPBD = fundForecast.PDChng;
                if (fundMaster.PDPubNavDt.HasValue && fundForecast.LastNavDt.HasValue)
                {
                    if (fundForecast.LastNavDt.GetValueOrDefault().CompareTo(fundMaster.PDPubNavDt.GetValueOrDefault()) == 0)
                    {
                        fundForecast.PDChngPBD =
                        fundForecast.PDLastPrc.GetValueOrDefault() - fundMaster.PDDscntToLastPrc.GetValueOrDefault();
                    }
                }
            }
            catch (Exception e)
            {
                _logger.LogError("Error calculating discount change for ticker: " + fundMaster.Ticker, e);
            }
        }

        private double? CalculateRefIndexNav(string ticker, FundForecast fundForecast)
        {
            IDictionary<string, SecurityPrice> securityPriceDict = _cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);
            IDictionary<string, string> priceTickerMap = _cache.Get<IDictionary<string, string>>(CacheKeys.PRICE_TICKER_MAP);
            SecurityPrice securityPrice = SecurityPriceLookupOperations.GetSecurityPrice(ticker, priceTickerMap, securityPriceDict);
            if (securityPrice != null)
                return securityPrice.LastPrc.GetValueOrDefault();
            return null;
        }
    }
}