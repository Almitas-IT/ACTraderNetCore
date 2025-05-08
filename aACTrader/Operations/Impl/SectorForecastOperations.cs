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
    public class SectorForecastOperations
    {
        private readonly ILogger<SectorForecastOperations> _logger;
        private readonly CachingService _cache;
        private const string PERIOD_1M = "1M";
        private const string PERIOD_3M = "3M";
        private const string PERIOD_6M = "6M";
        private const string PERIOD_1YR = "1Yr";
        private const string PERIOD_2YR = "2Yr";
        private const string PERIOD_3YR = "3Yr";
        private const string PERIOD_5YR = "5Yr";
        private const string PERIOD_10YR = "10Yr";

        private const string DELIMITER = "|";
        private const string NUMBER_FORMAT = "{0:#0.0}";

        public SectorForecastOperations(ILogger<SectorForecastOperations> logger, CachingService cache)
        {
            _logger = logger;
            _cache = cache;
        }

        public void Start()
        {
            _logger.LogInformation("Sector Forecast Operations... - STARTED");

            try
            {
                IDictionary<string, FundMaster> fundMasterDict = _cache.Get<IDictionary<string, FundMaster>>(CacheKeys.FUND_MASTER);
                IDictionary<string, FundForecast> fundForecastDict = _cache.Get<IDictionary<string, FundForecast>>(CacheKeys.FUND_FORECASTS);

                IDictionary<string, IList<FundForecast>> sectorForecastListDict = new Dictionary<string, IList<FundForecast>>(StringComparer.CurrentCultureIgnoreCase);
                IDictionary<string, SectorForecast> sectorForecastDict = new Dictionary<string, SectorForecast>(StringComparer.CurrentCultureIgnoreCase);

                foreach (KeyValuePair<string, FundMaster> kvp in fundMasterDict)
                {
                    string ticker = kvp.Key;
                    FundMaster fundMaster = kvp.Value;

                    if (!string.IsNullOrEmpty(fundMaster.Status)
                        && "ACTV".Equals(fundMaster.Status, StringComparison.CurrentCultureIgnoreCase))
                    {
                        if (fundForecastDict.TryGetValue(ticker, out FundForecast fundForecast))
                        {
                            string sector = "Other";
                            if (!string.IsNullOrEmpty(fundForecast.FundCat))
                                sector = fundForecast.FundCat;

                            //add sector
                            if (!sectorForecastDict.TryGetValue(sector, out SectorForecast sectorForecast))
                            {
                                sectorForecast = new SectorForecast();
                                sectorForecast.Sector = sector;
                                sectorForecastDict.Add(sector, sectorForecast);
                            }

                            //assign fund to sector
                            if (!sectorForecastListDict.TryGetValue(sector, out IList<FundForecast> fundForecastList))
                            {
                                fundForecastList = new List<FundForecast>();
                                sectorForecastListDict.Add(sector, fundForecastList);
                            }
                            fundForecastList.Add(fundForecast);
                        }
                    }
                }

                _cache.Add(CacheKeys.SECTOR_FORECASTS_MAP, sectorForecastListDict, DateTimeOffset.MaxValue);
                _cache.Add(CacheKeys.SECTOR_FORECASTS, sectorForecastDict, DateTimeOffset.MaxValue);

            }
            catch (Exception e)
            {
                _logger.LogError("Error in Sector Forecast Operations", e);
            }
            _logger.LogInformation("Sector Forecast Operations - DONE");
        }

        /// <summary>
        /// Calculate Sector Forecasts
        /// </summary>
        public void Calculate()
        {
            IDictionary<string, SectorForecast> sectorForecastDict = _cache.Get<IDictionary<string, SectorForecast>>(CacheKeys.SECTOR_FORECASTS);
            IDictionary<string, IList<FundForecast>> sectorForecastListDict = _cache.Get<IDictionary<string, IList<FundForecast>>>(CacheKeys.SECTOR_FORECASTS_MAP);
            IDictionary<string, PositionMaster> almPositionMasterDict = _cache.Get<IDictionary<string, PositionMaster>>(CacheKeys.POSITION_MASTER);

            try
            {
                foreach (KeyValuePair<string, IList<FundForecast>> kvp in sectorForecastListDict)
                {
                    string sector = kvp.Key;

                    try
                    {
                        IList<FundForecast> fundForecastList = kvp.Value;
                        if (fundForecastList != null && fundForecastList.Count > 0)
                        {
                            if (sectorForecastDict.TryGetValue(sector, out SectorForecast sectorForecast))
                                CalculateSectorForecasts(sectorForecast, fundForecastList, almPositionMasterDict);
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error calculating Sector Forecasts for sector: " + sector);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error calculating Sector Forecasts");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sectorForecast"></param>
        /// <param name="fundForecastList"></param>
        /// <param name="almPositionMasterDict"></param>
        private void CalculateSectorForecasts(
            SectorForecast sectorForecast
            , IList<FundForecast> fundForecastList
            , IDictionary<string, PositionMaster> almPositionMasterDict)
        {
            double almMarketCap = 0, fundMarketCap = 0;
            int countALMHoldings = 0;

            //Reset
            sectorForecast.OppMV = null;
            sectorForecast.OppMVPct = null;
            sectorForecast.OppShOwn = null;
            sectorForecast.TacMV = null;
            sectorForecast.TacMVPct = null;
            sectorForecast.TacShOwn = null;

            //if ("BDCs".Equals(sectorForecast.Sector, StringComparison.CurrentCultureIgnoreCase))
            //    _logger.LogInformation("Sector: " + sectorForecast.Sector);

            foreach (FundForecast fundForecast in fundForecastList)
            {
                if (fundForecast.MV.HasValue)
                    fundMarketCap += fundForecast.MV.GetValueOrDefault();

                if (almPositionMasterDict.TryGetValue(fundForecast.Ticker, out PositionMaster positionMaster))
                {
                    if (positionMaster.FundOpp.MV.HasValue)
                    {
                        sectorForecast.OppMV =
                            sectorForecast.OppMV.GetValueOrDefault() +
                            positionMaster.FundOpp.MV.GetValueOrDefault();
                        sectorForecast.OppMVPct =
                            sectorForecast.OppMVPct.GetValueOrDefault() +
                            positionMaster.FundOpp.MVPct.GetValueOrDefault();
                        sectorForecast.OppShOwn =
                            sectorForecast.OppShOwn.GetValueOrDefault() +
                            positionMaster.FundOpp.PosOwnPct.GetValueOrDefault();
                    }
                    if (positionMaster.FundTac.MV.HasValue)
                    {
                        sectorForecast.TacMV =
                            sectorForecast.TacMV.GetValueOrDefault() +
                            positionMaster.FundTac.MV.GetValueOrDefault();
                        sectorForecast.TacMVPct =
                            sectorForecast.TacMVPct.GetValueOrDefault() +
                            positionMaster.FundTac.MVPct.GetValueOrDefault();
                        sectorForecast.TacShOwn =
                            sectorForecast.TacShOwn.GetValueOrDefault() +
                            positionMaster.FundTac.PosOwnPct.GetValueOrDefault();
                    }
                }
            }

            //Median Discounts
            double? medianPDPublished = MedianExtensions.Median(fundForecastList.Where(f => f.LastPD.HasValue).Select(f => f.LastPD).ToList());
            double? medianPDLastPrice = MedianExtensions.Median(fundForecastList.Where(f => f.PDLastPrc.HasValue).Select(f => f.PDLastPrc).ToList());
            double? medianPDBidPrice = MedianExtensions.Median(fundForecastList.Where(f => f.PDBidPrc.HasValue).Select(f => f.PDBidPrc).ToList());
            double? medianPDAskPrice = MedianExtensions.Median(fundForecastList.Where(f => f.PDAskPrc.HasValue).Select(f => f.PDAskPrc).ToList());
            double? medianPDUnLevLastPrice = MedianExtensions.Median(fundForecastList.Where(f => f.PDLastPrcUnLev.HasValue).Select(f => f.PDLastPrcUnLev).ToList());
            double? medianPDUnLevBidPrice = MedianExtensions.Median(fundForecastList.Where(f => f.PDBidPrcUnLev.HasValue).Select(f => f.PDBidPrcUnLev).ToList());
            double? medianPDUnLeveAskPrice = MedianExtensions.Median(fundForecastList.Where(f => f.PDAskPrcUnLev.HasValue).Select(f => f.PDAskPrcUnLev).ToList());

            //Expected Alpha Return
            double? medianExpectedReturn = MedianExtensions.Median(fundForecastList.Where(f => f.EAFinalAlpha.HasValue).Select(f => f.EAFinalAlpha).ToList());
            double? medianExpectedAlpha = MedianExtensions.Median(fundForecastList.Where(f => f.EAAlpha.HasValue).Select(f => f.EAAlpha).ToList());

            //Z-Scores
            double? medianZScore1W = MedianExtensions.Median(fundForecastList.Where(f => f.ZS1W.HasValue).Select(f => f.ZS1W).ToList());
            double? medianZScore2W = MedianExtensions.Median(fundForecastList.Where(f => f.ZS2W.HasValue).Select(f => f.ZS2W).ToList());
            double? medianZScore1M = MedianExtensions.Median(fundForecastList.Where(f => f.ZS1M.HasValue).Select(f => f.ZS1M).ToList());
            double? medianZScore3M = MedianExtensions.Median(fundForecastList.Where(f => f.ZS3M.HasValue).Select(f => f.ZS3M).ToList());
            double? medianZScore6M = MedianExtensions.Median(fundForecastList.Where(f => f.ZS6M.HasValue).Select(f => f.ZS6M).ToList());
            double? medianZScore12M = MedianExtensions.Median(fundForecastList.Where(f => f.ZS12M.HasValue).Select(f => f.ZS12M).ToList());
            double? medianZScore24M = MedianExtensions.Median(fundForecastList.Where(f => f.ZS24M.HasValue).Select(f => f.ZS24M).ToList());
            double? medianZScore36M = MedianExtensions.Median(fundForecastList.Where(f => f.ZS36M.HasValue).Select(f => f.ZS36M).ToList());
            double? medianZScore60M = MedianExtensions.Median(fundForecastList.Where(f => f.ZS60M.HasValue).Select(f => f.ZS60M).ToList());

            //D-Scores
            double? medianDScore1W = MedianExtensions.Median(fundForecastList.Where(f => f.DS1W.HasValue).Select(f => f.DS1W).ToList());
            double? medianDScore2W = MedianExtensions.Median(fundForecastList.Where(f => f.DS2W.HasValue).Select(f => f.DS2W).ToList());
            double? medianDScore1M = MedianExtensions.Median(fundForecastList.Where(f => f.DS1M.HasValue).Select(f => f.DS1M).ToList());
            double? medianDScore3M = MedianExtensions.Median(fundForecastList.Where(f => f.DS3M.HasValue).Select(f => f.DS3M).ToList());
            double? medianDScore6M = MedianExtensions.Median(fundForecastList.Where(f => f.DS6M.HasValue).Select(f => f.DS6M).ToList());
            double? medianDScore12M = MedianExtensions.Median(fundForecastList.Where(f => f.DS12M.HasValue).Select(f => f.DS12M).ToList());
            double? medianDScore24M = MedianExtensions.Median(fundForecastList.Where(f => f.DS24M.HasValue).Select(f => f.DS24M).ToList());
            double? medianDScore36M = MedianExtensions.Median(fundForecastList.Where(f => f.DS36M.HasValue).Select(f => f.DS36M).ToList());
            double? medianDScore60M = MedianExtensions.Median(fundForecastList.Where(f => f.DS60M.HasValue).Select(f => f.DS60M).ToList());

            //Market Values
            sectorForecast.SecMV = fundMarketCap;
            if (countALMHoldings > 0)
            {
                sectorForecast.HldngMV = almMarketCap;
                if (fundMarketCap > 0)
                    sectorForecast.ShOwnPct = almMarketCap / fundMarketCap;
            }

            //Discounts
            sectorForecast.MedPubPD = medianPDPublished;
            sectorForecast.MedPDLastPrc = medianPDLastPrice;
            sectorForecast.MedPDBidPrc = medianPDBidPrice;
            sectorForecast.MedPDAskPrc = medianPDAskPrice;
            sectorForecast.MedPDLastPrcUnLev = medianPDUnLevLastPrice;
            sectorForecast.MedPDBidPrcUnLev = medianPDUnLevBidPrice;
            sectorForecast.MedPDAskPrcUnLev = medianPDUnLeveAskPrice;

            //Z-Scores
            sectorForecast.MedZS1W = medianZScore1W;
            sectorForecast.MedZS2W = medianZScore2W;
            sectorForecast.MedZS1M = medianZScore1M;
            sectorForecast.MedZS3M = medianZScore3M;
            sectorForecast.MedZS6M = medianZScore6M;
            sectorForecast.MedZS12M = medianZScore12M;
            sectorForecast.MedZS24M = medianZScore24M;
            sectorForecast.MedZS36M = medianZScore36M;
            sectorForecast.MedZS60M = medianZScore60M;

            //D-Scores
            sectorForecast.MedDS1W = medianDScore1W;
            sectorForecast.MedDS2W = medianDScore2W;
            sectorForecast.MedDS1M = medianDScore1M;
            sectorForecast.MedDS3M = medianDScore3M;
            sectorForecast.MedDS6M = medianDScore6M;
            sectorForecast.MedDS12M = medianDScore12M;
            sectorForecast.MedDS24M = medianDScore24M;
            sectorForecast.MedDS36M = medianDScore36M;
            sectorForecast.MedDS60M = medianDScore60M;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value1"></param>
        /// <param name="value2"></param>
        /// <returns></returns>
        private string CombineDecimalValues(double value1, double value2)
        {
            string result = string.Empty;

            try
            {
                string formattedValue1 = DataConversionUtils.FormatNumber(value1, NUMBER_FORMAT);
                string formattedValue2 = DataConversionUtils.FormatNumber(value2, NUMBER_FORMAT);

                result = string.Join(DELIMITER, formattedValue1, formattedValue2);
            }
            catch (Exception)
            {
            }

            return result;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value1"></param>
        /// <param name="value2"></param>
        /// <returns></returns>
        private string CombineValues(double? value1, double? value2)
        {
            string result = string.Empty;
            string formattedValue1 = string.Empty;
            string formattedValue2 = string.Empty;

            try
            {
                if (value1.HasValue)
                    formattedValue1 = DataConversionUtils.FormatNumber(value1.GetValueOrDefault() * 100.0, NUMBER_FORMAT) + "%";

                if (value2.HasValue)
                    formattedValue2 = DataConversionUtils.FormatNumber(value2.GetValueOrDefault() * 100.0, NUMBER_FORMAT) + "%";

                result = string.Join(DELIMITER, formattedValue1, formattedValue2);
            }
            catch (Exception)
            {
            }

            return result;
        }

        /// <summary>
        /// 
        /// </summary>
        public void CalculateFundSectorZDScores()
        {
            IDictionary<string, IDictionary<string, FundGroupHistStats>> fundSectorDiscountStatsDict = _cache.Get<IDictionary<string, IDictionary<string, FundGroupHistStats>>>(CacheKeys.FUND_SECTOR_DISCOUNT_STATS);
            IDictionary<string, FundForecast> fundForecastDict = _cache.Get<IDictionary<string, FundForecast>>(CacheKeys.FUND_FORECASTS);

            foreach (KeyValuePair<string, IDictionary<string, FundGroupHistStats>> kvp in fundSectorDiscountStatsDict)
            {
                string ticker = kvp.Key;
                IDictionary<string, FundGroupHistStats> statsDict = kvp.Value;

                try
                {
                    if (fundForecastDict.TryGetValue(ticker, out FundForecast fundForecast))
                    {
                        double? lastPriceDiscount = fundForecast.PDLastPrc;

                        //if there is a nav adjustment then ignore the nav adjustment and calculate the scores based on estimated nav
                        if (fundForecast.UnAdjPDLastPrc.HasValue)
                            lastPriceDiscount = fundForecast.UnAdjPDLastPrc;

                        if (lastPriceDiscount.HasValue)
                        {
                            //1M
                            if (statsDict.ContainsKey(PERIOD_1M))
                            {
                                FundGroupHistStats sectorStats = statsDict[PERIOD_1M];
                                fundForecast.DSSec1M = lastPriceDiscount - sectorStats.Mean;
                                fundForecast.ZSSec1M = DataConversionUtils.CalculateZScore(fundForecast.DSSec1M, sectorStats.StdDev);
                            }

                            //3M
                            if (statsDict.ContainsKey(PERIOD_3M))
                            {
                                FundGroupHistStats sectorStats = statsDict[PERIOD_3M];
                                fundForecast.DSSec3M = lastPriceDiscount - sectorStats.Mean;
                                fundForecast.ZSSec3M = DataConversionUtils.CalculateZScore(fundForecast.DSSec3M, sectorStats.StdDev);
                            }

                            //6M
                            if (statsDict.ContainsKey(PERIOD_6M))
                            {
                                FundGroupHistStats sectorStats = statsDict[PERIOD_6M];
                                fundForecast.DSSec6M = lastPriceDiscount - sectorStats.Mean;
                                fundForecast.ZSSec6M = DataConversionUtils.CalculateZScore(fundForecast.DSSec6M, sectorStats.StdDev);
                            }

                            //1Yr
                            if (statsDict.ContainsKey(PERIOD_1YR))
                            {
                                FundGroupHistStats sectorStats = statsDict[PERIOD_1YR];
                                fundForecast.DSSec12M = lastPriceDiscount - sectorStats.Mean;
                                fundForecast.ZSSec12M = DataConversionUtils.CalculateZScore(fundForecast.DSSec12M, sectorStats.StdDev);
                            }

                            //2Yr
                            if (statsDict.ContainsKey(PERIOD_2YR))
                            {
                                FundGroupHistStats sectorStats = statsDict[PERIOD_2YR];
                                fundForecast.DSSec24M = lastPriceDiscount - sectorStats.Mean;
                                fundForecast.ZSSec24M = DataConversionUtils.CalculateZScore(fundForecast.DSSec24M, sectorStats.StdDev);
                            }

                            //3Yr
                            if (statsDict.ContainsKey(PERIOD_3YR))
                            {
                                FundGroupHistStats sectorStats = statsDict[PERIOD_3YR];
                                fundForecast.DSSec36M = lastPriceDiscount - sectorStats.Mean;
                                fundForecast.ZSSec36M = DataConversionUtils.CalculateZScore(fundForecast.DSSec36M, sectorStats.StdDev);
                            }

                            //5Yr
                            if (statsDict.ContainsKey(PERIOD_5YR))
                            {
                                FundGroupHistStats sectorStats = statsDict[PERIOD_5YR];
                                fundForecast.DSSec60M = lastPriceDiscount - sectorStats.Mean;
                                fundForecast.ZSSec60M = DataConversionUtils.CalculateZScore(fundForecast.DSSec60M, sectorStats.StdDev);
                            }

                            //10Yr
                            if (statsDict.ContainsKey(PERIOD_10YR))
                            {
                                FundGroupHistStats sectorStats = statsDict[PERIOD_10YR];
                                fundForecast.DSSec120M = lastPriceDiscount - sectorStats.Mean;
                                fundForecast.ZSSec120M = DataConversionUtils.CalculateZScore(fundForecast.DSSec120M, sectorStats.StdDev);
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError("Error calculating Fund Z & D scores relative to Sector: " + ticker, ex);
                }
            }
        }
    }
}