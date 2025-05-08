using aACTrader.DAO.Repository;
using aACTrader.Model;
using aCommons;
using aCommons.Cef;
using aCommons.Utils;
using LazyCache;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;

namespace aACTrader.Operations.Impl
{
    public class FundAlertManager
    {
        private readonly ILogger<FundAlertManager> _logger;
        private readonly BaseDao _baseDao;
        private readonly CachingService _cache;
        private readonly DateTime _todaysDate = DateTime.Now;

        public FundAlertManager(ILogger<FundAlertManager> logger
            , BaseDao baseDao
            , CachingService cache)
        {
            _logger = logger;
            _baseDao = baseDao;
            _cache = cache;
        }

        /// <summary>
        /// Gets funds alerts (based on source data and live data)
        /// </summary>
        /// <param name="reqParams"></param>
        /// <returns></returns>
        public IList<FundAlert> GetFundAlerts(FundAlertParameters reqParams)
        {
            List<FundAlert> fundAlerts = new List<FundAlert>();
            List<FundAlert> fundAlertsSorted = new List<FundAlert>();

            if ("All".Equals(reqParams.Category, StringComparison.CurrentCultureIgnoreCase)
                || "Dividend Change".Equals(reqParams.Category, StringComparison.CurrentCultureIgnoreCase)
                || "Ex-Date".Equals(reqParams.Category, StringComparison.CurrentCultureIgnoreCase)
                || "Notification Date".Equals(reqParams.Category, StringComparison.CurrentCultureIgnoreCase))
            {
                IList<FundAlert> baseAlerts = _baseDao.GetFundAlerts(reqParams.Category, reqParams.StartDate, reqParams.EndDate);
                fundAlerts.AddRange(baseAlerts);
            }

            if ("All".Equals(reqParams.Category, StringComparison.CurrentCultureIgnoreCase)
                || "Ownership Limit".Equals(reqParams.Category, StringComparison.CurrentCultureIgnoreCase))
            {
                IList<FundAlert> ownershipAlerts = GetOwnershipAlerts();
                fundAlerts.AddRange(ownershipAlerts);
            }

            //if ("All".Equals(reqParams.Category, StringComparison.CurrentCultureIgnoreCase)
            //    || "Targets".Equals(reqParams.Category, StringComparison.CurrentCultureIgnoreCase))
            //{
            //    IList<FundAlert> targetAlerts = GetFundTargets();
            //    fundAlerts.AddRange(targetAlerts);

            //    IList<FundAlert> targetScoreAlerts = GetFundScoreAlerts();
            //    fundAlerts.AddRange(targetScoreAlerts);
            //}

            //if ("All".Equals(reqParams.Category, StringComparison.CurrentCultureIgnoreCase)
            //    || "Expected Alpha Differential".Equals(reqParams.Category, StringComparison.CurrentCultureIgnoreCase)
            //    || "Discount Differential".Equals(reqParams.Category, StringComparison.CurrentCultureIgnoreCase))
            //{
            //    IList<FundAlert> liveScoreAlerts = GetLiveScoreAlerts(reqParams);
            //    fundAlerts.AddRange(liveScoreAlerts);
            //}

            fundAlertsSorted = fundAlerts
                        .OrderBy(x => x.EffectiveDate)
                        .ThenBy(x => x.AlertCategory)
                        .ThenBy(x => x.Ticker)
                        .ToList<FundAlert>();

            return fundAlertsSorted;
        }

        /*
		 * Gets funds that are near or above the stock ownership thresholds set for each country  
		 */
        public IList<FundAlert> GetOwnershipAlerts()
        {
            IList<FundAlert> fundAlerts = new List<FundAlert>();

            try
            {
                string effectiveDateAsString = DateUtils.ConvertDate(DateTime.Now, "yyyy-MM-dd");

                IDictionary<string, Holding> almHoldingsDict = _cache.Get<IDictionary<string, Holding>>(CacheKeys.ALM_HOLDINGS);
                IList<FundAlertTarget> fundAlertTargetList = _cache.Get<IList<FundAlertTarget>>(CacheKeys.FUND_ALERT_TARGETS);
                IDictionary<string, FundAlertTarget> ownershipTargetDict = fundAlertTargetList.Where(f => f.AlertType.Equals("OwnershipLimit", StringComparison.CurrentCultureIgnoreCase)).ToDictionary(f => f.Ticker, f => f, StringComparer.CurrentCultureIgnoreCase);

                foreach (KeyValuePair<string, Holding> kvp in almHoldingsDict)
                {
                    string ticker = kvp.Key;
                    Holding holding = kvp.Value;

                    try
                    {
                        if (holding.PctOwnership.HasValue && !string.IsNullOrEmpty(holding.CountryCode))
                        {
                            if (ownershipTargetDict.ContainsKey(holding.CountryCode))
                            {
                                FundAlertTarget alertTarget = ownershipTargetDict[holding.CountryCode];
                                double ownershipLimit = alertTarget.BuyTarget.GetValueOrDefault();

                                if (holding.PctOwnership.GetValueOrDefault() > (ownershipLimit * 0.95))
                                {
                                    FundAlert fundAlert = new FundAlert
                                    {
                                        Ticker = ticker,
                                        EffectiveDateAsString = effectiveDateAsString,
                                        AlertCategory = "Ownership Limit",
                                        AlertType = alertTarget.Ticker + " Ownership Limit = " + Math.Round((ownershipLimit * 100), 2) + "%",
                                        AlertDetail = "Shares Outstanding: " + holding.SharesOutstandingAsString + " , "
                                    };
                                    fundAlert.AlertDetail += "Position: " + holding.PositionAsString + " , ";
                                    fundAlert.AlertDetail += "Ownership Pct: " + DataConversionUtils.FormatNumber(holding.PctOwnership.GetValueOrDefault() * 100) + "%";
                                    fundAlert.EffectiveDate = _todaysDate;

                                    fundAlerts.Add(fundAlert);
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error getting ownership limit for ticker: " + ticker);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error getting fund alerts for ownership limits");
            }
            return fundAlerts;
        }

        /*
		 * Gets funds that are below D-Score and Z-Score target value
		 * Gets funds that has lower expected alpha than the target category of funds
		 * Gets funds that has lower discount than the target category of funds
		 */
        public IList<FundAlert> GetLiveScoreAlerts(FundAlertParameters reqParams)
        {
            IList<FundAlert> fundAlerts = new List<FundAlert>();
            string ticker = string.Empty;

            try
            {
                string effectiveDateAsString = DateUtils.ConvertDate(DateTime.Now, "yyyy-MM-dd");

                IDictionary<string, FundMaster> fundMasterDict = _cache.Get<IDictionary<string, FundMaster>>(CacheKeys.FUND_MASTER);
                IDictionary<string, Holding> almHoldingsDict = _cache.Get<IDictionary<string, Holding>>(CacheKeys.ALM_HOLDINGS);
                IDictionary<string, FundForecast> fundForecastDict = _cache.Get<IDictionary<string, FundForecast>>(CacheKeys.FUND_FORECASTS);

                IDictionary<string, IDictionary<string, FundForecast>> maxValueByAssetClassDict = GetMaxValueByAssetClass(reqParams);
                IDictionary<string, FundForecast> maxExpectedAlphaByAssetClassDict = maxValueByAssetClassDict["Expected Alpha Differential"];

                foreach (KeyValuePair<string, Holding> kvp in almHoldingsDict)
                {
                    ticker = kvp.Key;
                    Holding holding = kvp.Value;

                    //if (ticker.Equals("OSL-U CT", StringComparison.CurrentCultureIgnoreCase))
                    //{
                    //	_logger.LogInformation("Ticker: " + ticker);
                    //}

                    try
                    {
                        FundForecast fundForecast;
                        if (fundForecastDict.TryGetValue(ticker, out fundForecast) && fundForecast.SecType.Equals("Closed End Fund", StringComparison.CurrentCultureIgnoreCase))
                        {
                            if ("All".Equals(reqParams.Category, StringComparison.CurrentCultureIgnoreCase)
                                || "Expected Alpha Differential".Equals(reqParams.Category, StringComparison.CurrentCultureIgnoreCase))
                            {
                                if (maxExpectedAlphaByAssetClassDict.Count > 0)
                                {
                                    FundMaster fundMaster;
                                    fundMasterDict.TryGetValue(ticker, out fundMaster);

                                    if (fundMaster != null)
                                    {
                                        bool includeFund = true;

                                        string category = string.Empty;
                                        if (reqParams.AssetClassLevel.Equals("Level 1", StringComparison.CurrentCultureIgnoreCase))
                                            category = fundMaster.AssetLvl1;
                                        else if (reqParams.AssetClassLevel.Equals("Level 2", StringComparison.CurrentCultureIgnoreCase))
                                            category = fundMaster.AssetLvl2;
                                        else if (reqParams.AssetClassLevel.Equals("Level 3", StringComparison.CurrentCultureIgnoreCase))
                                            category = fundMaster.AssetLvl3;

                                        if (reqParams.GroupByCurrency.Equals("Y", StringComparison.CurrentCultureIgnoreCase))
                                            category += "|" + fundMaster.Curr;

                                        if (reqParams.GroupByCountry.Equals("Y", StringComparison.CurrentCultureIgnoreCase))
                                            category += "|" + fundMaster.CntryCd;

                                        if (reqParams.GroupByState.Equals("Y", StringComparison.CurrentCultureIgnoreCase))
                                        {
                                            if (!string.IsNullOrEmpty(fundMaster.MuniState))
                                                category += "|" + fundMaster.MuniState;
                                            else
                                                includeFund = false;
                                        }

                                        if (reqParams.IncludeIRRFunds.Equals("N", StringComparison.CurrentCultureIgnoreCase))
                                        {
                                            if (fundMaster.IsIRRFund.Equals("Y", StringComparison.CurrentCultureIgnoreCase))
                                            {
                                                includeFund = false;
                                            }
                                        }

                                        if (includeFund)
                                        {
                                            FundForecast topFundForecast;
                                            if (maxExpectedAlphaByAssetClassDict.TryGetValue(category, out topFundForecast))
                                            {
                                                if (fundForecast.EAFinalAlpha.GetValueOrDefault() < topFundForecast.EAFinalAlpha.GetValueOrDefault())
                                                {
                                                    double difference = Math.Round(((fundForecast.EAFinalAlpha.GetValueOrDefault() - topFundForecast.EAFinalAlpha.GetValueOrDefault()) * 100), 2);

                                                    FundAlert fundAlert = new FundAlert
                                                    {
                                                        Ticker = ticker,
                                                        EffectiveDateAsString = effectiveDateAsString,
                                                        AlertCategory = "Expected Alpha Differential",
                                                        AlertType = category,
                                                        AlertDetail = "Expected Alpha: " + Math.Round((fundForecast.EAFinalAlpha.GetValueOrDefault() * 100), 2) + "%" + " , "
                                                    };
                                                    fundAlert.AlertDetail += "Top Fund Expected Alpha: " + Math.Round((topFundForecast.EAFinalAlpha.GetValueOrDefault() * 100), 2) + "%" + " , ";
                                                    fundAlert.AlertDetail += "Difference: " + difference + "%" + " , ";
                                                    fundAlert.AlertDetail += "Top Fund: " + topFundForecast.Ticker;

                                                    fundAlerts.Add(fundAlert);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error getting fund alerts for expected alpha differential for: " + ticker);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error getting fund alerts for expected alpha differential for: " + ticker);
            }

            return fundAlerts;
        }

        private IDictionary<string, IDictionary<string, FundForecast>> GetMaxValueByAssetClass(FundAlertParameters reqParams)
        {
            IDictionary<string, IDictionary<string, FundForecast>> result = new Dictionary<string, IDictionary<string, FundForecast>>(StringComparer.CurrentCultureIgnoreCase);
            IDictionary<string, FundForecast> maxExpectedAlphaByAssetClassDict = new Dictionary<string, FundForecast>(StringComparer.CurrentCultureIgnoreCase);
            IDictionary<string, FundForecast> maxDiscountByAssetClassDict = new Dictionary<string, FundForecast>(StringComparer.CurrentCultureIgnoreCase);

            result.Add("Expected Alpha Differential", maxExpectedAlphaByAssetClassDict);

            string assetClass = string.Empty;
            try
            {
                IDictionary<string, FundMaster> fundMasterDict = _cache.Get<IDictionary<string, FundMaster>>(CacheKeys.FUND_MASTER);
                IDictionary<string, FundForecast> fundForecastDict = _cache.Get<IDictionary<string, FundForecast>>(CacheKeys.FUND_FORECASTS);

                IList<FundForecast> fundForecasts = fundForecastDict.Values.ToList<FundForecast>();

                IDictionary<string, IList<string>> categories = new Dictionary<string, IList<string>>();

                foreach (FundMaster fundMaster in fundMasterDict.Values)
                {
                    if (!string.IsNullOrEmpty(fundMaster.Status) && "ACTV".Equals(fundMaster.Status, StringComparison.CurrentCultureIgnoreCase))
                    {
                        bool includeFund = true;

                        string category = string.Empty;
                        if (reqParams.AssetClassLevel.Equals("Level 1", StringComparison.CurrentCultureIgnoreCase))
                            category = fundMaster.AssetLvl1;
                        else if (reqParams.AssetClassLevel.Equals("Level 2", StringComparison.CurrentCultureIgnoreCase))
                            category = fundMaster.AssetLvl2;
                        else if (reqParams.AssetClassLevel.Equals("Level 3", StringComparison.CurrentCultureIgnoreCase))
                            category = fundMaster.AssetLvl3;

                        if (reqParams.GroupByCurrency.Equals("Y", StringComparison.CurrentCultureIgnoreCase))
                            category += "|" + fundMaster.Curr;

                        if (reqParams.GroupByCountry.Equals("Y", StringComparison.CurrentCultureIgnoreCase))
                            category += "|" + fundMaster.CntryCd;

                        if (reqParams.GroupByState.Equals("Y", StringComparison.CurrentCultureIgnoreCase))
                        {
                            if (!string.IsNullOrEmpty(fundMaster.MuniState))
                                category += "|" + fundMaster.MuniState;
                            else
                                includeFund = false;
                        }

                        if (reqParams.IncludeIRRFunds.Equals("N", StringComparison.CurrentCultureIgnoreCase))
                        {
                            if (fundMaster.IsIRRFund.Equals("Y", StringComparison.CurrentCultureIgnoreCase))
                            {
                                includeFund = false;
                            }
                        }

                        if (includeFund)
                        {
                            IList<string> tickers;
                            if (!categories.ContainsKey(category))
                            {
                                tickers = new List<string>();
                                categories.Add(category, tickers);
                            }
                            else
                            {
                                tickers = categories[category];
                            }

                            tickers.Add(fundMaster.Ticker);
                        }
                    }

                    foreach (KeyValuePair<string, IList<string>> kvp in categories)
                    {
                        string category = kvp.Key;
                        IList<string> tickers = kvp.Value;

                        if (tickers.Count > 0)
                        {
                            FundForecast fundForecast = fundForecasts
                                            .Where(f => f.SecType.Equals("Closed End Fund", StringComparison.CurrentCultureIgnoreCase))
                                            .Where(f => tickers.Any(t => t.Equals(f.Ticker)))
                                            .OrderByDescending(f => f.EAFinalAlpha.GetValueOrDefault())
                                            .FirstOrDefault();

                            maxExpectedAlphaByAssetClassDict.Add(category, fundForecast);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error getting max expected alpha and discount for fund class");
            }

            return result;
        }

        /*
		 * Gets funds with DScore and ZScore targets
		 */
        public IList<FundAlert> GetFundScoreAlerts()
        {
            IList<FundAlert> fundAlerts = new List<FundAlert>();

            try
            {
                string effectiveDateAsString = DateUtils.ConvertDate(DateTime.Now, "yyyy-MM-dd");

                IDictionary<string, Holding> almHoldingsDict = _cache.Get<IDictionary<string, Holding>>(CacheKeys.ALM_HOLDINGS);
                IDictionary<string, FundForecast> fundForecastDict = _cache.Get<IDictionary<string, FundForecast>>(CacheKeys.FUND_FORECASTS);
                IDictionary<string, FundHistStats> fundHistStatsDict = _cache.Get<IDictionary<string, FundHistStats>>(CacheKeys.FUND_STATS);
                IList<FundAlertTarget> fundAlertTargetList = _cache.Get<IList<FundAlertTarget>>(CacheKeys.FUND_ALERT_TARGETS);

                foreach (KeyValuePair<string, Holding> kvp in almHoldingsDict)
                {
                    string ticker = kvp.Key;
                    Holding holding = kvp.Value;

                    try
                    {
                        FundForecast fundForecast;
                        if (fundForecastDict.TryGetValue(ticker, out fundForecast) && fundForecast.SecType.Equals("Closed End Fund", StringComparison.CurrentCultureIgnoreCase))
                        {
                            FundHistStats fundHistStats;
                            fundHistStatsDict.TryGetValue(ticker, out fundHistStats);

                            foreach (FundAlertTarget fundTarget in fundAlertTargetList)
                            {
                                if (fundTarget.AlertType.Equals("DScore", StringComparison.CurrentCultureIgnoreCase))
                                {
                                    double liveScore = 0, meanScore = 0;
                                    double cheapScore = fundTarget.BuyTarget.GetValueOrDefault();
                                    double expensiveScore = fundTarget.SellTarget.GetValueOrDefault();

                                    if ("3M".Equals(fundTarget.Ticker, StringComparison.CurrentCultureIgnoreCase))
                                    {
                                        liveScore = fundForecast.DS3M.GetValueOrDefault();
                                        meanScore = fundHistStats.Mean3M.GetValueOrDefault();
                                    }
                                    else if ("6M".Equals(fundTarget.Ticker, StringComparison.CurrentCultureIgnoreCase))
                                    {
                                        liveScore = fundForecast.DS6M.GetValueOrDefault();
                                        meanScore = fundHistStats.Mean6M.GetValueOrDefault();
                                    }
                                    else if ("12M".Equals(fundTarget.Ticker, StringComparison.CurrentCultureIgnoreCase))
                                    {
                                        liveScore = fundForecast.DS12M.GetValueOrDefault();
                                        meanScore = fundHistStats.Mean12M.GetValueOrDefault();
                                    }
                                    else if ("24M".Equals(fundTarget.Ticker, StringComparison.CurrentCultureIgnoreCase))
                                    {
                                        liveScore = fundForecast.DS24M.GetValueOrDefault();
                                        meanScore = fundHistStats.Mean24M.GetValueOrDefault();
                                    }
                                    else if ("36M".Equals(fundTarget.Ticker, StringComparison.CurrentCultureIgnoreCase))
                                    {
                                        liveScore = fundForecast.DS36M.GetValueOrDefault();
                                        meanScore = fundHistStats.Mean36M.GetValueOrDefault();
                                    }
                                    else if ("60M".Equals(fundTarget.Ticker, StringComparison.CurrentCultureIgnoreCase))
                                    {
                                        liveScore = fundForecast.DS60M.GetValueOrDefault();
                                        meanScore = fundHistStats.Mean60M.GetValueOrDefault();
                                    }

                                    //cheap
                                    if (liveScore < cheapScore)
                                    {
                                        FundAlert fundAlert = new FundAlert
                                        {
                                            Ticker = ticker,
                                            EffectiveDateAsString = effectiveDateAsString,
                                            AlertCategory = "D-Score",
                                            AlertType = fundTarget.Ticker + " D-Score < " + Math.Round((cheapScore * 100), 2) + "%",
                                            AlertDetail = "Current Discount: " + Math.Round((fundForecast.PDLastPrc.GetValueOrDefault() * 100), 2) + "%" + " , "
                                        };

                                        if (fundHistStats != null && fundHistStats.Mean3M.HasValue)
                                        {
                                            fundAlert.AlertDetail += "Mean " + fundTarget.Ticker + " Discount: " + Math.Round((meanScore * 100), 2) + "%" + " , ";
                                        }

                                        fundAlert.AlertDetail += "D-Score: " + Math.Round((liveScore * 100), 2) + "%";

                                        fundAlerts.Add(fundAlert);
                                    }

                                    //expensive
                                    if (liveScore > expensiveScore)
                                    {
                                        FundAlert fundAlert = new FundAlert
                                        {
                                            Ticker = ticker,
                                            EffectiveDateAsString = effectiveDateAsString,
                                            AlertCategory = "D-Score",
                                            AlertType = fundTarget.Ticker + " D-Score > " + Math.Round((expensiveScore * 100), 2) + "%",
                                            AlertDetail = "Current Discount: " + Math.Round((fundForecast.PDLastPrc.GetValueOrDefault() * 100), 2) + "%" + " , "
                                        };

                                        if (fundHistStats != null && fundHistStats.Mean3M.HasValue)
                                        {
                                            fundAlert.AlertDetail += "Mean " + fundTarget.Ticker + " Discount: " + Math.Round((meanScore * 100), 2) + "%" + " , ";
                                        }

                                        fundAlert.AlertDetail += "D-Score: " + Math.Round((liveScore * 100), 2) + "%";

                                        fundAlerts.Add(fundAlert);
                                    }
                                }
                                else if (fundTarget.AlertType.Equals("ZScore", StringComparison.CurrentCultureIgnoreCase))
                                {
                                    double liveScore = 0, meanScore = 0, stdDev = 0;
                                    double cheapScore = fundTarget.BuyTarget.GetValueOrDefault();
                                    double expensiveScore = fundTarget.SellTarget.GetValueOrDefault();

                                    if ("3M".Equals(fundTarget.Ticker, StringComparison.CurrentCultureIgnoreCase))
                                    {
                                        liveScore = fundForecast.ZS3M.GetValueOrDefault();
                                        meanScore = fundHistStats.Mean3M.GetValueOrDefault();
                                        stdDev = fundHistStats.StdDev3M.GetValueOrDefault();
                                    }
                                    else if ("6M".Equals(fundTarget.Ticker, StringComparison.CurrentCultureIgnoreCase))
                                    {
                                        liveScore = fundForecast.ZS6M.GetValueOrDefault();
                                        meanScore = fundHistStats.Mean6M.GetValueOrDefault();
                                        stdDev = fundHistStats.StdDev6M.GetValueOrDefault();
                                    }
                                    else if ("12M".Equals(fundTarget.Ticker, StringComparison.CurrentCultureIgnoreCase))
                                    {
                                        liveScore = fundForecast.ZS12M.GetValueOrDefault();
                                        meanScore = fundHistStats.Mean12M.GetValueOrDefault();
                                        stdDev = fundHistStats.StdDev12M.GetValueOrDefault();
                                    }
                                    else if ("24M".Equals(fundTarget.Ticker, StringComparison.CurrentCultureIgnoreCase))
                                    {
                                        liveScore = fundForecast.ZS24M.GetValueOrDefault();
                                        meanScore = fundHistStats.Mean24M.GetValueOrDefault();
                                        stdDev = fundHistStats.StdDev24M.GetValueOrDefault();
                                    }
                                    else if ("36M".Equals(fundTarget.Ticker, StringComparison.CurrentCultureIgnoreCase))
                                    {
                                        liveScore = fundForecast.ZS36M.GetValueOrDefault();
                                        meanScore = fundHistStats.Mean36M.GetValueOrDefault();
                                        stdDev = fundHistStats.StdDev36M.GetValueOrDefault();
                                    }
                                    else if ("60M".Equals(fundTarget.Ticker, StringComparison.CurrentCultureIgnoreCase))
                                    {
                                        liveScore = fundForecast.ZS60M.GetValueOrDefault();
                                        meanScore = fundHistStats.Mean60M.GetValueOrDefault();
                                        stdDev = fundHistStats.StdDev60M.GetValueOrDefault();
                                    }

                                    //cheap
                                    if (liveScore < cheapScore)
                                    {
                                        FundAlert fundAlert = new FundAlert
                                        {
                                            Ticker = ticker,
                                            EffectiveDateAsString = effectiveDateAsString,
                                            AlertCategory = "Z-Score",
                                            AlertType = fundTarget.Ticker + " Z-Score < " + cheapScore,
                                            AlertDetail = "Current Discount: " + Math.Round((fundForecast.PDLastPrc.GetValueOrDefault() * 100), 2) + "%" + " , "
                                        };

                                        if (fundHistStats != null && fundHistStats.Mean3M.HasValue)
                                        {
                                            fundAlert.AlertDetail += "Mean " + fundTarget.Ticker + " Discount: " + Math.Round((meanScore * 100), 2) + "%" + " , ";
                                            fundAlert.AlertDetail += fundTarget.Ticker + " StdDev: " + Math.Round((stdDev * 100), 2) + "%" + " , ";
                                        }

                                        fundAlert.AlertDetail += "Z-Score: " + Math.Round(liveScore, 2);

                                        fundAlerts.Add(fundAlert);
                                    }

                                    //expensive
                                    if (liveScore > expensiveScore)
                                    {
                                        FundAlert fundAlert = new FundAlert
                                        {
                                            Ticker = ticker,
                                            EffectiveDateAsString = effectiveDateAsString,
                                            AlertCategory = "Z-Score",
                                            AlertType = fundTarget.Ticker + " Z-Score > " + expensiveScore,
                                            AlertDetail = "Current Discount: " + Math.Round((fundForecast.PDLastPrc.GetValueOrDefault() * 100), 2) + "%" + " , "
                                        };

                                        if (fundHistStats != null && fundHistStats.Mean3M.HasValue)
                                        {
                                            fundAlert.AlertDetail += "Mean " + fundTarget.Ticker + " Discount: " + Math.Round((meanScore * 100), 2) + "%" + " , ";
                                            fundAlert.AlertDetail += fundTarget.Ticker + " StdDev: " + +Math.Round((stdDev * 100), 2) + "%" + " , ";
                                        }

                                        fundAlert.AlertDetail += "Z-Score: " + Math.Round(liveScore, 2);

                                        fundAlerts.Add(fundAlert);
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error applying fund score targets for ticker: " + ticker);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error applying fund score targets");
            }

            return fundAlerts;
        }

        public IList<FundAlert> GetFundTargets()
        {
            IList<FundAlert> fundAlerts = new List<FundAlert>();

            try
            {
                string effectiveDateAsString = DateUtils.ConvertDate(DateTime.Now, "yyyy-MM-dd");

                IList<FundAlertTarget> fundAlertTargetList = _cache.Get<IList<FundAlertTarget>>(CacheKeys.FUND_ALERT_TARGETS);
                IDictionary<string, FundForecast> fundForecastDict = _cache.Get<IDictionary<string, FundForecast>>(CacheKeys.FUND_FORECASTS);

                string ticker = string.Empty;
                foreach (FundAlertTarget fundTarget in fundAlertTargetList)
                {
                    ticker = fundTarget.Ticker;

                    try
                    {
                        if (fundTarget.AlertType.Equals("DiscountTarget", StringComparison.CurrentCultureIgnoreCase))
                        {
                            FundForecast fundForecast;
                            if (fundForecastDict.TryGetValue(fundTarget.Ticker, out fundForecast))
                            {
                                if (fundForecast.PDLastPrc.HasValue)
                                {
                                    double discountToLastPrice = fundForecast.PDLastPrc.GetValueOrDefault();

                                    //buy target discount
                                    double buyTarget = -1.0 * fundTarget.BuyTarget.GetValueOrDefault();
                                    if (discountToLastPrice < buyTarget)
                                    {
                                        double difference = buyTarget - discountToLastPrice;
                                        FundAlert fundAlert = new FundAlert
                                        {
                                            Ticker = fundTarget.Ticker,
                                            EffectiveDateAsString = effectiveDateAsString,
                                            AlertCategory = "Discount Target",
                                            AlertType = "Buy Target: " + Math.Round((buyTarget * 100), 2) + "%",
                                            AlertDetail = "Current Discount: " + Math.Round((discountToLastPrice * 100), 2) + "%" + " , "
                                        };
                                        fundAlert.AlertDetail += "Buy Target Discount: " + Math.Round((buyTarget * 100), 2) + "%" + " , ";
                                        fundAlert.AlertDetail += "Difference: " + Math.Round((difference * 100), 2) + "%";

                                        fundAlerts.Add(fundAlert);
                                    }

                                    //sell target discount
                                    double sellTarget = -1.0 * fundTarget.SellTarget.GetValueOrDefault();
                                    if (discountToLastPrice > sellTarget)
                                    {
                                        double difference = sellTarget - discountToLastPrice;
                                        FundAlert fundAlert = new FundAlert
                                        {
                                            Ticker = fundTarget.Ticker,
                                            EffectiveDateAsString = effectiveDateAsString,
                                            AlertCategory = "Discount Target",
                                            AlertType = "Sell Target: " + Math.Round((sellTarget * 100), 2) + "%",
                                            AlertDetail = "Current Discount: " + Math.Round((discountToLastPrice * 100), 2) + "%" + " , "
                                        };
                                        fundAlert.AlertDetail += "Sell Target Discount: " + Math.Round((sellTarget * 100), 2) + "%" + " , ";
                                        fundAlert.AlertDetail += "Difference: " + Math.Round((difference * 100), 2) + "%";

                                        fundAlerts.Add(fundAlert);
                                    }
                                }
                            }
                        }
                        else if (fundTarget.AlertType.Equals("IRRTarget", StringComparison.CurrentCultureIgnoreCase))
                        {
                            FundForecast fundForecast;
                            if (fundForecastDict.TryGetValue(fundTarget.Ticker, out fundForecast))
                            {
                                if (fundForecast.IRRLastPrc.HasValue)
                                {
                                    double irrToLastPrice = fundForecast.IRRLastPrc.GetValueOrDefault();

                                    //buy irr
                                    double buyIRR = fundTarget.BuyTarget.GetValueOrDefault();
                                    if (irrToLastPrice > buyIRR)
                                    {
                                        double difference = irrToLastPrice - buyIRR;
                                        FundAlert fundAlert = new FundAlert
                                        {
                                            Ticker = fundTarget.Ticker,
                                            EffectiveDateAsString = effectiveDateAsString,
                                            AlertCategory = "IRR Target",
                                            AlertType = "Buy Target: " + Math.Round((buyIRR * 100), 2) + "%",
                                            AlertDetail = "Current IRR: " + Math.Round((irrToLastPrice * 100), 2) + "%" + " , "
                                        };
                                        fundAlert.AlertDetail += "Buy Target IRR: " + Math.Round((buyIRR * 100), 2) + "%" + " , ";
                                        fundAlert.AlertDetail += "Difference: " + Math.Round((difference * 100), 2) + "%";

                                        fundAlerts.Add(fundAlert);
                                    }

                                    //sell irr
                                    double sellIRR = fundTarget.SellTarget.GetValueOrDefault();
                                    if (irrToLastPrice < sellIRR)
                                    {
                                        double difference = sellIRR - irrToLastPrice;
                                        FundAlert fundAlert = new FundAlert
                                        {
                                            Ticker = fundTarget.Ticker,
                                            EffectiveDateAsString = effectiveDateAsString,
                                            AlertCategory = "IRR Target",
                                            AlertType = "Sell Target: " + Math.Round((sellIRR * 100), 2) + "%",
                                            AlertDetail = "Current IRR: " + Math.Round((irrToLastPrice * 100), 2) + "%" + " , "
                                        };
                                        fundAlert.AlertDetail += "Sell Target IRR: " + Math.Round((sellIRR * 100), 2) + "%" + " , ";
                                        fundAlert.AlertDetail += "Difference: " + Math.Round((difference * 100), 2) + "%";

                                        fundAlerts.Add(fundAlert);
                                    }
                                }
                            }
                        }
                        else if (fundTarget.AlertType.Equals("PriceTarget", StringComparison.CurrentCultureIgnoreCase))
                        {
                            FundForecast fundForecast;
                            if (fundForecastDict.TryGetValue(fundTarget.Ticker, out fundForecast))
                            {
                                if (fundForecast.LastPrc.HasValue)
                                {
                                    double lastPrice = fundForecast.LastPrc.GetValueOrDefault();

                                    //buy price
                                    double buyPrice = fundTarget.BuyTarget.GetValueOrDefault();
                                    if (lastPrice < buyPrice)
                                    {
                                        double difference = buyPrice - lastPrice;
                                        FundAlert fundAlert = new FundAlert
                                        {
                                            Ticker = fundTarget.Ticker,
                                            EffectiveDateAsString = effectiveDateAsString,
                                            AlertCategory = "Price Target",
                                            AlertType = "Buy Price: " + Math.Round(buyPrice, 2),
                                            AlertDetail = "Current Price: " + Math.Round(lastPrice, 2) + " , "
                                        };
                                        fundAlert.AlertDetail += "Buy Target Price: " + Math.Round(buyPrice, 2) + " , ";
                                        fundAlert.AlertDetail += "Difference: " + Math.Round(difference, 2);

                                        fundAlerts.Add(fundAlert);
                                    }

                                    //sell price
                                    double sellPrice = fundTarget.SellTarget.GetValueOrDefault();
                                    if (lastPrice > sellPrice)
                                    {
                                        double difference = sellPrice - lastPrice;
                                        FundAlert fundAlert = new FundAlert
                                        {
                                            Ticker = fundTarget.Ticker,
                                            EffectiveDateAsString = effectiveDateAsString,
                                            AlertCategory = "Price Target",
                                            AlertType = "Sell Price: " + Math.Round(sellPrice, 2),
                                            AlertDetail = "Current Price: " + Math.Round(lastPrice, 2) + " , "
                                        };
                                        fundAlert.AlertDetail += "Sell Target Price: " + Math.Round(sellPrice, 2) + " , ";
                                        fundAlert.AlertDetail += "Difference: " + Math.Round(difference, 2);

                                        fundAlerts.Add(fundAlert);
                                    }
                                }
                            }
                        }
                        else if (fundTarget.AlertType.Equals("ExpectedAlphaTarget", StringComparison.CurrentCultureIgnoreCase))
                        {
                            if (fundForecastDict.TryGetValue(fundTarget.Ticker, out FundForecast fundForecast))
                            {
                                if (fundForecast.EAFinalAlpha.HasValue)
                                {
                                    double expectedAlpha = fundForecast.EAFinalAlpha.GetValueOrDefault();

                                    //buy expected alpha
                                    double buyExpectedAlpha = fundTarget.BuyTarget.GetValueOrDefault();
                                    if (expectedAlpha > buyExpectedAlpha)
                                    {
                                        double difference = expectedAlpha - buyExpectedAlpha;
                                        FundAlert fundAlert = new FundAlert
                                        {
                                            Ticker = fundTarget.Ticker,
                                            EffectiveDateAsString = effectiveDateAsString,
                                            AlertCategory = "Expected Alpha Target",
                                            AlertType = "Buy Target: " + Math.Round((buyExpectedAlpha * 100), 2) + "%",
                                            AlertDetail = "Current Alpha: " + Math.Round((expectedAlpha * 100), 2) + "%" + " , "
                                        };
                                        fundAlert.AlertDetail += "Buy Target Alpha: " + Math.Round((buyExpectedAlpha * 100), 2) + "%" + " , ";
                                        fundAlert.AlertDetail += "Difference: " + Math.Round((difference * 100), 2) + "%";

                                        fundAlerts.Add(fundAlert);
                                    }

                                    //sell expected alpha
                                    double sellExpectedAlpha = fundTarget.SellTarget.GetValueOrDefault();
                                    if (expectedAlpha < sellExpectedAlpha)
                                    {
                                        double difference = sellExpectedAlpha - expectedAlpha;
                                        FundAlert fundAlert = new FundAlert
                                        {
                                            Ticker = fundTarget.Ticker,
                                            EffectiveDateAsString = effectiveDateAsString,
                                            AlertCategory = "Expected Alpha Target",
                                            AlertType = "Sell Target: " + Math.Round((sellExpectedAlpha * 100), 2) + "%",
                                            AlertDetail = "Current Alpha: " + Math.Round((expectedAlpha * 100), 2) + "%" + " , "
                                        };
                                        fundAlert.AlertDetail += "Sell Target Alpha: " + Math.Round((sellExpectedAlpha * 100), 2) + "%" + " , ";
                                        fundAlert.AlertDetail += "Difference: " + Math.Round((difference * 100), 2) + "%";

                                        fundAlerts.Add(fundAlert);
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error applying fund targets for ticker: " + ticker);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error applying fund targets");
            }

            return fundAlerts;
        }

        /// <summary>
        /// Gets discount, irr, price and expected alpha targets 
        /// </summary>
        public void PopulateFundAlertTargets()
        {
            IList<FundAlertTarget> fundAlertTargetList = _baseDao.GetFundAlertTargets();
            _cache.Remove(CacheKeys.FUND_ALERT_TARGETS);
            _cache.Add(CacheKeys.FUND_ALERT_TARGETS, fundAlertTargetList, DateTimeOffset.MaxValue);

            IDictionary<string, FundAlertTarget> fundAlertTargetDict = GetFundAlertTargets(fundAlertTargetList);
            _cache.Remove(CacheKeys.FUND_ALERT_TARGET_TYPES);
            _cache.Add(CacheKeys.FUND_ALERT_TARGET_TYPES, fundAlertTargetDict, DateTimeOffset.MaxValue);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fundAlertTargetList"></param>
        /// <returns></returns>
        private IDictionary<string, FundAlertTarget> GetFundAlertTargets(IList<FundAlertTarget> fundAlertTargetList)
        {
            IDictionary<string, FundAlertTarget> fundAlertTargetDict = new Dictionary<string, FundAlertTarget>();
            foreach (FundAlertTarget fundAlertTarget in fundAlertTargetList)
            {
                string ticker = fundAlertTarget.Ticker;
                string alertType = fundAlertTarget.AlertType;
                string key = ticker + "|" + alertType;

                fundAlertTargetDict.Add(key, fundAlertTarget);
            }

            return fundAlertTargetDict;
        }

        /// <summary>
        /// Save fund targets and
        /// Refresh cache 
        /// </summary>
        /// <param name="fundAlertTargets"></param>
        public void SaveFundAlertTargets(IList<FundAlertTarget> fundAlertTargets)
        {
            _baseDao.SaveFundAlertTargets(fundAlertTargets);
            PopulateFundAlertTargets();
        }
    }
}