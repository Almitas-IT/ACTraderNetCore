
using aACTrader.DAO.Repository;
using aCommons;
using aCommons.Cef;
using aCommons.Utils;
using LazyCache;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;

namespace aACTrader.Operations.Impl
{
    public class FundAlertManagerNew
    {
        private readonly ILogger<FundAlertManagerNew> _logger;
        private readonly BaseDao _baseDao;
        private readonly CachingService _cache;

        public FundAlertManagerNew(ILogger<FundAlertManagerNew> logger
            , BaseDao baseDao
            , CachingService cache)
        {
            _logger = logger;
            _baseDao = baseDao;
            _cache = cache;
        }

        /// <summary>
        /// 
        /// </summary>
        public void ProcessWatchlistSecurityAlerts()
        {
            IList<FundAlertDetail> tradingAlertList = _cache.Get<IList<FundAlertDetail>>(CacheKeys.TRADING_TARGET_ALERTS);
            IDictionary<string, TradingTarget> tradingTargetDict = _cache.Get<IDictionary<string, TradingTarget>>(CacheKeys.TRADING_TARGETS);
            IDictionary<string, string> priceTickerMap = _cache.Get<IDictionary<string, string>>(CacheKeys.PRICE_TICKER_MAP);
            IDictionary<string, SecurityPrice> securityPriceDict = _cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);
            IDictionary<string, FundForecast> fundForecastDict = _cache.Get<IDictionary<string, FundForecast>>(CacheKeys.FUND_FORECASTS);

            foreach (FundAlertDetail data in tradingAlertList)
            {
                string ticker = data.Ticker;

                try
                {
                    //Reset Values
                    Reset(data);

                    if (tradingTargetDict.TryGetValue(ticker, out TradingTarget tradingTarget))
                    {
                        //Get Fund Forecast
                        fundForecastDict.TryGetValue(data.Ticker, out FundForecast fundForecast);

                        //Discount Target Checks
                        if (tradingTarget.DiscountTarget.HasValue && fundForecast != null)
                            CheckTradingDiscountTargets(data, tradingTarget, fundForecast);

                        //Price Target Checks
                        if (data.TargetPrice.HasValue)
                            CheckTradingPriceTarget(data, tradingTarget, priceTickerMap, securityPriceDict);

                        //IRR Target Checks
                        if (tradingTarget.IRRTarget.HasValue && fundForecast != null)
                            CheckIRRTargets(data, tradingTarget, fundForecast);

                        //Ref Entity Checks
                        if (!string.IsNullOrEmpty(tradingTarget.RefIndex))
                            CheckRefIndexTargets(data, tradingTarget, priceTickerMap, securityPriceDict);

                        if (string.IsNullOrEmpty(tradingTarget.JoinCondition)
                            || "OR".Equals(tradingTarget.JoinCondition, StringComparison.CurrentCultureIgnoreCase))
                        {
                            if (data.TargetPriceDiff.HasValue || data.TargetDiscountDiff.HasValue || data.TargetIRRDiff.HasValue)
                            {
                                if ("BUY".Equals(tradingTarget.TransactionType, StringComparison.CurrentCultureIgnoreCase))
                                    data.TargetMatchFlag = "B";
                                else
                                    data.TargetMatchFlag = "S";
                            }
                        }
                        else if ("AND".Equals(tradingTarget.JoinCondition, StringComparison.CurrentCultureIgnoreCase))
                        {
                            bool match = false;
                            if (data.TargetPrice.HasValue && data.TargetPriceDiff.HasValue)
                                match = true;

                            if (match && data.TargetDiscount.HasValue && data.TargetDiscountDiff.HasValue)
                                match = true;
                            else
                                match = false;

                            if (match && data.TargetIRR.HasValue && data.TargetIRRDiff.HasValue)
                                match = true;
                            else
                                match = false;

                            if (match)
                            {
                                if ("BUY".Equals(tradingTarget.TransactionType, StringComparison.CurrentCultureIgnoreCase))
                                    data.TargetMatchFlag = "B";
                                else
                                    data.TargetMatchFlag = "S";
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error processing Trading (Watchlist Securities) Targets for ticker: " + ticker);
                }
            }
        }

        private void Reset(FundAlertDetail fundAlertDetail)
        {
            fundAlertDetail.TargetPriceDiff = null;
            fundAlertDetail.TargetDiscountDiff = null;
            fundAlertDetail.TargetIRRDiff = null;
            fundAlertDetail.TargetMatchFlag = null;
            fundAlertDetail.PriceAdj = null;
        }

        public void SaveTradingTargets(IList<TradingTarget> tradingTargetList)
        {
            _logger.LogInformation("Saving Trading Target Updates...");
            _baseDao.SaveTradingTargets(tradingTargetList);

            _logger.LogInformation("Updating Trading Target Cache...");
            PopulateWatchlistSecurities();
            _logger.LogInformation("Updating Trading Target Cache... - DONE");
        }

        /// <summary>
        /// Populates Watchlist Securities
        /// Populates Trading Targets
        /// Compares live data to Target Targets 
        /// </summary>
        public void PopulateWatchlistSecurities()
        {
            // get watchlist securities
            //IDictionary<string, SecurityMaster> watchlistSecuritiesDict = BaseDao.GetWatchlistSecurities();
            //Cache.Remove(CacheKeys.WATCHLIST_SECURITIES);
            //Cache.Add(CacheKeys.WATCHLIST_SECURITIES, watchlistSecuritiesDict, DateTimeOffset.MaxValue);

            _logger.LogInformation("Populating Watchlist Securities - STARTED");

            // get trading targets
            IDictionary<string, TradingTarget> tradingTargetDict = _baseDao.GetTradingTargets();
            _cache.Remove(CacheKeys.TRADING_TARGETS);
            _cache.Add(CacheKeys.TRADING_TARGETS, tradingTargetDict, DateTimeOffset.MaxValue);

            IDictionary<string, SecurityMaster> watchlistSecuritiesDict = new Dictionary<string, SecurityMaster>(StringComparer.CurrentCultureIgnoreCase);

            // popuate watchlist (trading) target alerts
            IList<FundAlertDetail> fundAlertList = new List<FundAlertDetail>();
            foreach (KeyValuePair<string, TradingTarget> kvp in tradingTargetDict)
            {
                TradingTarget tradingTarget = kvp.Value;

                FundAlertDetail fundAlertDetail = new FundAlertDetail
                {
                    SecTicker = tradingTarget.Ticker,
                    Ticker = tradingTarget.Ticker,
                    TransactionType = tradingTarget.TransactionType,
                    TargetPrice = tradingTarget.PriceTarget,
                    TargetDiscount = tradingTarget.DiscountTarget,
                    TargetIRR = tradingTarget.IRRTarget,
                    RefIndex = tradingTarget.RefIndex,
                    PriceBeta = tradingTarget.PriceBeta,
                    PriceCap = tradingTarget.PriceCap,
                };

                fundAlertList.Add(fundAlertDetail);

                if (!watchlistSecuritiesDict.TryGetValue(tradingTarget.Ticker, out SecurityMaster securityMaster))
                {
                    securityMaster = new SecurityMaster();
                    securityMaster.Ticker = tradingTarget.Ticker;
                    securityMaster.FundCategory = tradingTarget.FundCategory;
                    watchlistSecuritiesDict.Add(tradingTarget.Ticker, securityMaster);
                }
            }

            _cache.Remove(CacheKeys.TRADING_TARGET_ALERTS);
            _cache.Add(CacheKeys.TRADING_TARGET_ALERTS, fundAlertList, DateTimeOffset.MaxValue);

            _cache.Remove(CacheKeys.WATCHLIST_SECURITIES);
            _cache.Add(CacheKeys.WATCHLIST_SECURITIES, watchlistSecuritiesDict, DateTimeOffset.MaxValue);

            _logger.LogInformation("Populating Watchlist Securities - DONE");
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="data"></param>
        /// <param name="tradingTarget"></param>
        /// <param name="priceTickerMap"></param>
        /// <param name="securityPriceDict"></param>
        public void CheckTradingPriceTarget(FundAlertDetail data
            , TradingTarget tradingTarget
            , IDictionary<string, string> priceTickerMap
            , IDictionary<string, SecurityPrice> securityPriceDict)
        {
            SecurityPrice securityPrice = SecurityPriceLookupOperations.GetSecurityPrice(data.Ticker, priceTickerMap, securityPriceDict);
            if (securityPrice != null && securityPrice.LastPrc.HasValue)
            {
                double lastPrice = securityPrice.LastPrc.GetValueOrDefault();
                if ("BUY".Equals(tradingTarget.TransactionType, StringComparison.CurrentCultureIgnoreCase))
                {
                    double buyPrice = data.TargetPrice.GetValueOrDefault();
                    if (lastPrice <= buyPrice)
                        data.TargetPriceDiff = buyPrice - lastPrice;
                }
                else if ("SELL".Equals(tradingTarget.TransactionType, StringComparison.CurrentCultureIgnoreCase))
                {
                    double sellPrice = data.TargetPrice.GetValueOrDefault();
                    if (lastPrice >= sellPrice)
                        data.TargetPriceDiff = sellPrice - lastPrice;
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="data"></param>
        /// <param name="tradingTarget"></param>
        /// <param name="fundForecast"></param>
        public void CheckTradingDiscountTargets(
            FundAlertDetail data
            , TradingTarget tradingTarget
            , FundForecast fundForecast)
        {
            if (!string.IsNullOrEmpty(tradingTarget.NavType))
            {
                string navType = tradingTarget.NavType.ToUpper();
                double? nav = null;
                if (navType.Equals("PUB NAV"))
                    nav = fundForecast.LastDvdAdjNav;
                else if (navType.Equals("EST NAV"))
                    nav = fundForecast.EstNav;
                else if (navType.Equals("USER OVR"))
                    nav = tradingTarget.NavOverride;

                if (nav.HasValue)
                {
                    double? derivedPrice = DataConversionUtils.CalculatePrice(tradingTarget.DiscountTarget, nav);
                    if (!tradingTarget.PriceTarget.HasValue)
                        data.TargetPrice = derivedPrice;
                }
            }

            if (fundForecast.PDLastPrc.HasValue)
            {
                double discountToLastPrice = fundForecast.PDLastPrc.GetValueOrDefault();
                if ("BUY".Equals(tradingTarget.TransactionType, StringComparison.CurrentCultureIgnoreCase))
                {
                    double buyTarget = tradingTarget.DiscountTarget.GetValueOrDefault();
                    if (discountToLastPrice <= buyTarget)
                        data.TargetDiscountDiff = buyTarget - discountToLastPrice;
                }
                else if ("SELL".Equals(tradingTarget.TransactionType, StringComparison.CurrentCultureIgnoreCase))
                {
                    double sellTarget = tradingTarget.DiscountTarget.GetValueOrDefault();
                    if (discountToLastPrice >= sellTarget)
                        data.TargetDiscountDiff = sellTarget - discountToLastPrice;
                }
            }
        }


        public void CheckRefIndexTargets(
            FundAlertDetail data
            , TradingTarget tradingTarget
            , IDictionary<string, string> priceTickerMap
            , IDictionary<string, SecurityPrice> securityPriceDict)
        {
            double priceAdj = 0, finalPriceAdj = 0;
            string refIndex = tradingTarget.RefIndex;
            SecurityPrice securityPrice = SecurityPriceLookupOperations.GetSecurityPrice(refIndex, priceTickerMap, securityPriceDict);
            if (securityPrice != null
                && securityPrice.PrcRtn.HasValue
                && securityPrice.PrcRtn.GetValueOrDefault() != 0)
            {
                data.RefIndexPriceChange = securityPrice.PrcRtn;

                //1 = Price Move Up (Ref Entity), -1 = Price Move Down (Ref Entity)
                int priceMoveType = (securityPrice.PrcRtn.GetValueOrDefault() > 0) ? 1 : -1;

                //Beta adjustment to be applied to Ref Entity Price Move
                //0 = Apply for both Up and Down Price Move, 1 = Price Up Move and -1 = Price Down Move
                int betaShiftInd = tradingTarget.PriceBetaShiftInd.GetValueOrDefault();
                int priceCapShiftInd = tradingTarget.PriceCapShiftInd.GetValueOrDefault();

                //if Beta = 0, then set it to 1, this will move the Price by % move of Ref Entity's Price
                double beta = (tradingTarget.PriceBeta.GetValueOrDefault() == 0) ? 1 : tradingTarget.PriceBeta.GetValueOrDefault();
                double priceCap = (tradingTarget.PriceCap.GetValueOrDefault() == 0) ? 1 : tradingTarget.PriceCap.GetValueOrDefault();

                //If Beta adjustment has to be applied to both Up and Down Price move of the Ref Entity
                if (betaShiftInd == 0)
                    priceAdj = securityPrice.PrcRtn.GetValueOrDefault() * beta;
                //Up Move
                else if (betaShiftInd > 0 && priceMoveType > 0)
                    priceAdj = securityPrice.PrcRtn.GetValueOrDefault() * beta;
                //Down Move
                else if (betaShiftInd < 0 && priceMoveType < 0)
                    priceAdj = securityPrice.PrcRtn.GetValueOrDefault() * beta;

                //Apply Price Cap
                //If Price Cap has to be applied to both Up and Down Price move of the Ref Entity
                //Up Move
                if (priceCapShiftInd >= 0 && priceMoveType > 0)
                    finalPriceAdj = Math.Min(priceCap, priceAdj);
                //Down Move
                else if (priceCapShiftInd <= 0 && priceMoveType < 0)
                    finalPriceAdj = Math.Max(-1.0 * priceCap, priceAdj);
            }

            data.PriceAdj = priceAdj;
            data.FinalPriceAdj = finalPriceAdj;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="data"></param>
        /// <param name="tradingTarget"></param>
        /// <param name="fundForecast"></param>
        private void CheckIRRTargets(
        FundAlertDetail data
        , TradingTarget tradingTarget
        , FundForecast fundForecast)
        {
            if (fundForecast.IRRLastPrc.HasValue)
            {
                double irrToLastPrice = fundForecast.IRRLastPrc.GetValueOrDefault();
                if ("BUY".Equals(tradingTarget.TransactionType, StringComparison.CurrentCultureIgnoreCase))
                {
                    double buyTarget = tradingTarget.IRRTarget.GetValueOrDefault();
                    if (irrToLastPrice >= buyTarget)
                        data.TargetIRRDiff = buyTarget - irrToLastPrice;
                }
                else if ("SELL".Equals(tradingTarget.TransactionType, StringComparison.CurrentCultureIgnoreCase))
                {
                    double sellTarget = tradingTarget.IRRTarget.GetValueOrDefault();
                    if (irrToLastPrice <= sellTarget)
                        data.TargetIRRDiff = sellTarget - irrToLastPrice;
                }
            }
        }
    }
}