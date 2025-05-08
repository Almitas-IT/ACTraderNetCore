using aACTrader.DAO.Repository;
using aACTrader.Services.Admin;
using aCommons;
using aCommons.Cef;
using aCommons.Utils;
using LazyCache;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;

namespace aACTrader.Operations.Impl
{
    public class DataValidationChecks
    {
        private readonly ILogger<DataValidationChecks> _logger;
        private readonly CachingService _cache;
        private readonly FundSupplementalDataDao _fundSupplementalDataDao;
        private readonly LogDataService _logDataService;

        private static readonly DateTime TodaysDate = DateTime.Now.Date;

        //live pricing fields
        int priceIndex = 0;
        double lastPrice = 0, bidPrice = 0, askPrice = 0;

        //delayed live pricing fields
        int delayedPriceIndex = 0;
        double delayedLastPrice = 0, delayedBidPrice = 0, delayedAskPrice = 0;

        public DataValidationChecks(ILogger<DataValidationChecks> logger
            , CachingService cache
            , FundSupplementalDataDao fundSupplementalDataDao
            , LogDataService logDataService)
        {
            this._logger = logger;
            this._cache = cache;
            this._fundSupplementalDataDao = fundSupplementalDataDao;
            this._logDataService = logDataService;
        }

        public void RunValidationChecks()
        {
            IDictionary<string, string> dataValidationDict = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);
            IDictionary<string, FundMaster> fundMasterDict = _cache.Get<IDictionary<string, FundMaster>>(CacheKeys.FUND_MASTER);
            IDictionary<string, FundForecast> fundForecastDict = _cache.Get<IDictionary<string, FundForecast>>(CacheKeys.FUND_FORECASTS);
            IDictionary<string, FundNav> fundNavDict = _cache.Get<IDictionary<string, FundNav>>(CacheKeys.FUND_NAVS);
            IDictionary<string, FundPortDate> fundPortDateDict = _cache.Get<IDictionary<string, FundPortDate>>(CacheKeys.FUND_PORT_DATES);
            IDictionary<string, FundNavUpdate> fundNavUpdateFreqDict = _cache.Get<IDictionary<string, FundNavUpdate>>(CacheKeys.FUND_NAV_UPDATE_FREQUENCY);

            _logger.LogInformation("Running Data Validation Checks... - STARTED");
            StringBuilder sb = new StringBuilder();
            foreach (KeyValuePair<string, FundForecast> kvp in fundForecastDict)
            {
                FundForecast fundForecast = kvp.Value;
                fundForecast.SecInfoFlag = null;

                try
                {
                    if (fundMasterDict.TryGetValue(fundForecast.Ticker, out FundMaster fundMaster))
                    {
                        if (!string.IsNullOrEmpty(fundMaster.AssetTyp)
                            && "CEF".Equals(fundMaster.AssetTyp, StringComparison.CurrentCultureIgnoreCase))
                        {

                            sb.Clear();

                            //check if security is modeled in AC Trader
                            if (string.IsNullOrEmpty(fundForecast.NavEstMthd)
                                || fundForecast.NavEstMthd.Equals("Published")
                                || fundForecast.NavEstMthd.Equals("NumisEstNav"))
                            {
                                sb.AppendLine("Security not modeled in AC Trader");
                            }

                            //check if security Nav is older than 3 days
                            if (fundNavDict.TryGetValue(fundForecast.Ticker, out FundNav fundNav))
                            {
                                DateTime? lastNavDate = FundForecastOperations.GetLastNavDate(fundNav);
                                if (lastNavDate.HasValue)
                                {
                                    if (fundNavUpdateFreqDict.TryGetValue(fundForecast.Ticker, out FundNavUpdate fundNavUpdate)
                                        && !string.IsNullOrEmpty(fundNavUpdate.NavUpdateFreq))
                                    {
                                        fundNavUpdate.NavComments = null;
                                        string fundNavUpdateFreq = fundNavUpdate.NavUpdateFreq;
                                        int fundNavUpdateLag = fundNavUpdate.NavUpdateLag.GetValueOrDefault();

                                        if (fundNavUpdateFreq.Equals("Daily"))
                                            CheckNavUpdate(sb, lastNavDate, 1, fundNav, fundNavUpdate);
                                        else if (fundNavUpdateFreq.Equals("Weekly"))
                                            CheckNavUpdate(sb, lastNavDate, 7, fundNav, fundNavUpdate);
                                        else if (fundNavUpdateFreq.Equals("Bi-Monthly"))
                                            CheckNavUpdate(sb, lastNavDate, 14, fundNav, fundNavUpdate);
                                        else if (fundNavUpdateFreq.Equals("Monthly"))
                                            CheckNavUpdate(sb, lastNavDate, 30, fundNav, fundNavUpdate);
                                        else if (fundNavUpdateFreq.Equals("Quarterly"))
                                            CheckNavUpdate(sb, lastNavDate, 90, fundNav, fundNavUpdate);
                                        else if (fundNavUpdateFreq.Equals("Semi-Annual"))
                                            CheckNavUpdate(sb, lastNavDate, 180, fundNav, fundNavUpdate);
                                    }

                                    //int daysDiff = DateUtils.DaysDiff(lastNavDate, TodaysDate);
                                    //if (daysDiff > 3)
                                    //{
                                    //    sb.AppendLine("Published Nav is " + daysDiff + " days old");
                                    //    sb.AppendLine("Published Nav Date (" + DateUtils.ConvertDate(lastNavDate, "yyyy-MM-dd") + "); Nav Source (" + fundNav.LastNavSource + ")");
                                    //}
                                }
                                else
                                {
                                    sb.AppendLine("Missing latest Published Nav");
                                }
                            }

                            // check for model scores
                            if (fundMaster.MdlScore <= 0.5)
                            {
                                sb.AppendLine("Security has low Model Score (" + Math.Round(fundMaster.MdlScore.GetValueOrDefault(), 2) + ")");
                            }

                            //check if port holdings date is older than 6 months
                            if (!string.IsNullOrEmpty(fundForecast.NavEstMthd)
                                && "Holdings".Equals(fundForecast.NavEstMthd))
                            {
                                if (fundPortDateDict.TryGetValue(fundForecast.Ticker, out FundPortDate fundPortDate)
                                    && fundPortDate.ALMPortfolioDate.HasValue)
                                {
                                    int monthsDiff = DateUtils.MonthsDiff(fundPortDate.ALMPortfolioDate.GetValueOrDefault(), TodaysDate);
                                    if (monthsDiff > 7)
                                    {
                                        sb.AppendLine("Stale Port Holdings - Data is " + monthsDiff + " months old");
                                        sb.AppendLine("ALM Port Date - (" + DateUtils.ConvertDate(fundPortDate.ALMPortfolioDate, "yyyy-MM-dd") + "); " +
                                            "BBG Port Date - (" + DateUtils.ConvertDate(fundPortDate.BBGPortfolioDate, "yyyy-MM-dd") + "); " +
                                            "User Port Date -  (" + DateUtils.ConvertDate(fundPortDate.UserPortfolioDate, "yyyy-MM-dd") + ")");
                                    }
                                }
                            }

                            // set flag
                            if (!string.IsNullOrEmpty(sb.ToString()))
                            {
                                dataValidationDict.Add(fundForecast.Ticker, sb.ToString());
                                fundForecast.SecInfoFlag = "Y";
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error running data validation checks for ticker: " + fundForecast.Ticker);
                }
            }

            _logger.LogInformation("Running Data Validation Checks... - DONE");
            _cache.Remove(CacheKeys.SECURITY_DATA_VALIDATION_CHECKS);
            _cache.Add(CacheKeys.SECURITY_DATA_VALIDATION_CHECKS, dataValidationDict, DateTimeOffset.MaxValue);
        }

        private void CheckNavUpdate(StringBuilder sb
            , DateTime? lastNavDate, int numDays, FundNav fundNav, FundNavUpdate fundNavUpdate)
        {
            DateTime previousBusinessDay = DateUtils.AddBusinessDays(TodaysDate, -(numDays + fundNavUpdate.NavUpdateLag.GetValueOrDefault()));
            if (DateTime.Compare(lastNavDate.GetValueOrDefault(), previousBusinessDay) < 0)
            {
                sb.AppendLine("Missing latest Published Nav");
                sb.AppendLine("Published Nav Date (" + DateUtils.ConvertDate(lastNavDate, "yyyy-MM-dd") + "); Nav Source (" + fundNav.LastNavSource + ")");
                sb.AppendLine("Nav Update Frequency: " + fundNavUpdate.NavUpdateFreq);
                sb.AppendLine("Nav Update Lag: " + fundNavUpdate.NavUpdateLag);

                fundNavUpdate.NavComments = "Missing latest Published Nav";
            }
        }

        /// <summary>
        /// Check if live pricing is working/updating
        /// </summary>
        public void CheckLivePricing()
        {
            IDictionary<string, SecurityPrice> securityPriceDict = _cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);
            IDictionary<string, string> priceTickerMap = _cache.Get<IDictionary<string, string>>(CacheKeys.PRICE_TICKER_MAP);

            string referenceTicker = "SPY US";
            SecurityPrice securityPrice = SecurityPriceLookupOperations.GetSecurityPrice(referenceTicker, priceTickerMap, securityPriceDict);
            if (securityPrice != null)
            {
                bool stalePriceFlag = false;

                double lastPriceTemp = lastPrice;
                double bidPriceTemp = bidPrice;
                double askPriceTemp = askPrice;

                if (priceIndex == 0)
                {
                    lastPrice = securityPrice.LastPrc.GetValueOrDefault();
                    bidPrice = securityPrice.BidPrc.GetValueOrDefault();
                    askPrice = securityPrice.AskPrc.GetValueOrDefault();
                    priceIndex++;
                }
                else
                {
                    stalePriceFlag = true;
                    if (securityPrice.LastPrc.GetValueOrDefault() != lastPrice)
                        stalePriceFlag = false;

                    if (stalePriceFlag && securityPrice.BidPrc.GetValueOrDefault() != bidPrice)
                        stalePriceFlag = false;

                    if (stalePriceFlag && securityPrice.AskPrc.GetValueOrDefault() != askPrice)
                        stalePriceFlag = false;

                    lastPrice = securityPrice.LastPrc.GetValueOrDefault();
                    bidPrice = securityPrice.BidPrc.GetValueOrDefault();
                    askPrice = securityPrice.AskPrc.GetValueOrDefault();
                }

                if (stalePriceFlag)
                {
                    string message = "SPY US Price is Stale. Last/Bid/Ask Price: "
                        + lastPriceTemp + "/" + bidPriceTemp + "/" + askPriceTemp + "/"
                        + lastPrice + "/" + bidPrice + "/" + askPrice;
                    _logDataService.SaveLog("DataValidationChecks", "CheckLivePricing", referenceTicker, message, "INFO");
                }
            }
        }

        /// <summary>
        /// Check if delayed live pricing is working/updating
        /// </summary>
        public void CheckLiveDelayedPricing()
        {
            IDictionary<string, SecurityPrice> securityPriceDict = _cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);
            IDictionary<string, string> priceTickerMap = _cache.Get<IDictionary<string, string>>(CacheKeys.PRICE_TICKER_MAP);

            string referenceTicker = "TSLA US";
            SecurityPrice securityPrice = SecurityPriceLookupOperations.GetSecurityPrice(referenceTicker, priceTickerMap, securityPriceDict);
            if (securityPrice != null)
            {
                bool staleDelayedPriceFlag = false;

                double lastPriceTemp = delayedLastPrice;
                double bidPriceTemp = delayedBidPrice;
                double askPriceTemp = delayedAskPrice;

                if (delayedPriceIndex == 0)
                {
                    delayedLastPrice = securityPrice.LastPrc.GetValueOrDefault();
                    delayedBidPrice = securityPrice.BidPrc.GetValueOrDefault();
                    delayedAskPrice = securityPrice.AskPrc.GetValueOrDefault();
                    delayedPriceIndex++;
                }
                else
                {
                    staleDelayedPriceFlag = true;
                    if (securityPrice.LastPrc.GetValueOrDefault() != delayedLastPrice)
                        staleDelayedPriceFlag = false;

                    if (staleDelayedPriceFlag && securityPrice.BidPrc.GetValueOrDefault() != delayedBidPrice)
                        staleDelayedPriceFlag = false;

                    if (staleDelayedPriceFlag && securityPrice.AskPrc.GetValueOrDefault() != delayedAskPrice)
                        staleDelayedPriceFlag = false;

                    delayedLastPrice = securityPrice.LastPrc.GetValueOrDefault();
                    delayedBidPrice = securityPrice.BidPrc.GetValueOrDefault();
                    delayedAskPrice = securityPrice.AskPrc.GetValueOrDefault();
                }

                if (staleDelayedPriceFlag)
                {
                    string message = "TSLA US Price is Stale. Last/Bid/Ask Price: "
                        + lastPriceTemp + "/" + bidPriceTemp + "/" + askPriceTemp + "/"
                        + delayedLastPrice + "/" + delayedBidPrice + "/" + delayedAskPrice;
                    _logDataService.SaveLog("DataValidationChecks", "CheckLiveDelayedPricing", referenceTicker, message, "INFO");
                }
            }
        }

        public void GetUserOverrideStaleDataChecks()
        {

        }
    }
}