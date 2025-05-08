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
    public class FundHistoryOperations
    {
        private readonly ILogger<FundHistoryOperations> _logger;
        private readonly BaseDao _baseDao;
        private readonly FundHistoryDao _fundHistoryDao;
        private readonly CachingService _cache;

        public FundHistoryOperations(ILogger<FundHistoryOperations> logger
            , BaseDao baseDao
            , FundHistoryDao fundHistoryDao
            , CachingService cache)
        {
            _logger = logger;
            _baseDao = baseDao;
            _fundHistoryDao = fundHistoryDao;
            _cache = cache;
            _logger.LogInformation("Initializing FundHistoryOperations...");
        }

        public IList<FundHistoryMaster> GetFundHistory(IList<ServiceDataContract> reqParams)
        {
            IDictionary<string, FundHistoryMaster> fundHistoryDict = new Dictionary<string, FundHistoryMaster>(StringComparer.CurrentCultureIgnoreCase);
            IList<FundHistoryMaster> result = null;

            try
            {
                foreach (ServiceDataContract input in reqParams)
                {
                    DateTime? startDate = DateUtils.ConvertToDate(input.StartDate, "yyyy-MM-dd");
                    DateTime? endDate = DateUtils.ConvertToDate(input.EndDate, "yyyy-MM-dd");

                    IEnumerable<FundNavHistory> fundHistory = null;
                    if ("FundHistory".Equals(input.RequestType, StringComparison.CurrentCultureIgnoreCase))
                    {
                        fundHistory = _baseDao.GetFundNavHistory(input.Ticker, startDate.GetValueOrDefault(), endDate.GetValueOrDefault());
                    }
                    else if ("SectorHistory".Equals(input.RequestType, StringComparison.CurrentCultureIgnoreCase))
                    {
                        fundHistory = _baseDao.GetSectorHistory(input.Country, input.SecurityType, input.CEFInstrumentType, input.Sector, input.FundCategory, startDate.GetValueOrDefault(), endDate.GetValueOrDefault());
                    }

                    foreach (FundNavHistory data in fundHistory)
                    {
                        string effectiveDateAsString = data.EffectiveDateAsString;

                        FundHistoryMaster master;
                        if (fundHistoryDict.TryGetValue(effectiveDateAsString, out master))
                        {
                        }
                        else
                        {
                            master = new FundHistoryMaster
                            {
                                EffectiveDate = data.EffectiveDate,
                                EffectiveDateAsString = effectiveDateAsString
                            };
                            fundHistoryDict.Add(effectiveDateAsString, master);
                        }

                        if (input.Index == 1)
                        {
                            master.Fund1History = data;
                        }
                        else if (input.Index == 2)
                        {
                            master.Fund2History = data;
                        }
                        else if (input.Index == 3)
                        {
                            master.FundCategory1History = data;
                        }
                        else if (input.Index == 4)
                        {
                            master.FundCategory2History = data;
                        }
                        else if (input.Index == 5)
                        {
                            master.FundAssetClassLevel1History = data;
                        }
                        else if (input.Index == 6)
                        {
                            master.FundAssetClassLevel2History = data;
                        }
                        else if (input.Index == 7)
                        {
                            master.FundAssetClassLevel3History = data;
                        }
                    }
                }

                result = fundHistoryDict.Values.OrderByDescending(f => f.EffectiveDate).ToList<FundHistoryMaster>();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error getting fund history");
            }
            return result;
        }

        public IList<FundHistoryMasterNew> GetFundHistoryNew(IList<ServiceDataContract> reqParams)
        {
            IDictionary<string, FundHistoryMasterNew> fundHistoryDict = new Dictionary<string, FundHistoryMasterNew>(StringComparer.CurrentCultureIgnoreCase);
            IList<FundHistoryMasterNew> result = null;

            try
            {
                foreach (ServiceDataContract input in reqParams)
                {
                    DateTime? startDate = DateUtils.ConvertToDate(input.StartDate, "yyyy-MM-dd");
                    DateTime? endDate = DateUtils.ConvertToDate(input.EndDate, "yyyy-MM-dd");

                    IEnumerable<FundNavHistoryNew> fundHistory = null;
                    if ("FundHistory".Equals(input.RequestType, StringComparison.CurrentCultureIgnoreCase))
                    {
                        fundHistory = _baseDao.GetFundHistoricalData(input.Ticker, startDate.GetValueOrDefault(), endDate.GetValueOrDefault(), "N");
                    }
                    else if ("SectorHistory".Equals(input.RequestType, StringComparison.CurrentCultureIgnoreCase))
                    {
                        fundHistory = _baseDao.GetSectorHistoricalData(input.Country, input.SecurityType, input.Sector, input.FundCategory, startDate.GetValueOrDefault(), endDate.GetValueOrDefault());
                    }

                    foreach (FundNavHistoryNew data in fundHistory)
                    {
                        string effectiveDateAsString = data.EffectiveDateAsString;

                        if (fundHistoryDict.TryGetValue(effectiveDateAsString, out FundHistoryMasterNew master))
                        {
                        }
                        else
                        {
                            master = new FundHistoryMasterNew
                            {
                                EffectiveDate = data.EffectiveDate,
                                EffectiveDateAsString = effectiveDateAsString
                            };
                            fundHistoryDict.Add(effectiveDateAsString, master);
                        }

                        if (input.Index == 1)
                            master.Fund1 = data;
                        else if (input.Index == 2)
                            master.Fund2 = data;
                        else if (input.Index == 3)
                            master.Fund3 = data;
                        else if (input.Index == 4)
                            master.Fund4 = data;
                        else if (input.Index == 5)
                            master.Fund5 = data;
                        else if (input.Index == 6)
                            master.Fund6 = data;
                        else if (input.Index == 7)
                            master.Fund7 = data;
                        else if (input.Index == 8)
                            master.Fund8 = data;
                        else if (input.Index == 9)
                            master.Fund9 = data;
                        else if (input.Index == 10)
                            master.Fund10 = data;
                    }
                }

                result = fundHistoryDict.Values.OrderByDescending(f => f.EffectiveDate).ToList<FundHistoryMasterNew>();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error getting fund history");
            }
            return result;
        }

        public IList<BDCFundHistory> GetBDCFundHistory(IList<ServiceDataContract> reqParams)
        {
            IDictionary<string, BDCFundHistory> fundHistoryDict = new Dictionary<string, BDCFundHistory>(StringComparer.CurrentCultureIgnoreCase);
            IList<BDCFundHistory> result = null;

            try
            {
                foreach (ServiceDataContract input in reqParams)
                {
                    DateTime? startDate = DateUtils.ConvertToDate(input.StartDate, "yyyy-MM-dd");
                    DateTime? endDate = DateUtils.ConvertToDate(input.EndDate, "yyyy-MM-dd");

                    IEnumerable<BDCData> fundHistory = null;
                    if ("FundHistory".Equals(input.RequestType, StringComparison.CurrentCultureIgnoreCase))
                    {
                        fundHistory = _baseDao.GetBDCHistoricalData(input.Ticker, startDate.GetValueOrDefault(), endDate.GetValueOrDefault(), "BDC");
                    }

                    foreach (BDCData data in fundHistory)
                    {
                        string effectiveDateAsString = data.DateAS;

                        if (fundHistoryDict.TryGetValue(effectiveDateAsString, out BDCFundHistory master))
                        {
                        }
                        else
                        {
                            master = new BDCFundHistory
                            {
                                EffectiveDateAsString = effectiveDateAsString
                            };
                            fundHistoryDict.Add(effectiveDateAsString, master);
                        }

                        if (input.Index == 1)
                            master.Fund1 = data;
                        else if (input.Index == 2)
                            master.Fund2 = data;
                        else if (input.Index == 3)
                            master.Fund3 = data;
                        else if (input.Index == 4)
                            master.Fund4 = data;
                        else if (input.Index == 5)
                            master.Fund5 = data;
                        else if (input.Index == 6)
                            master.Fund6 = data;
                        else if (input.Index == 7)
                            master.Fund7 = data;
                        else if (input.Index == 8)
                            master.Fund8 = data;
                        else if (input.Index == 9)
                            master.Fund9 = data;
                        else if (input.Index == 10)
                            master.Fund10 = data;
                    }
                }

                result = fundHistoryDict.Values.OrderByDescending(f => f.EffectiveDateAsString).ToList<BDCFundHistory>();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error getting fund history");
            }
            return result;
        }

        public FundHistoryMasterSummary GetFundHistorySummary(IList<ServiceDataContract> reqParams)
        {
            FundHistoryMasterSummary result = new FundHistoryMasterSummary();

            IDictionary<string, FundHistStats> fundHistStatsDict = _cache.Get<IDictionary<string, FundHistStats>>(CacheKeys.FUND_STATS);

            try
            {
                foreach (ServiceDataContract input in reqParams)
                {
                    FundGroupStatsSummary data = null;
                    if ("FundHistory".Equals(input.RequestType, StringComparison.CurrentCultureIgnoreCase))
                    {
                        FundHistStats fundHistStats;
                        if (fundHistStatsDict.TryGetValue(input.Ticker, out fundHistStats))
                        {
                            data = new FundGroupStatsSummary
                            {
                                Ticker = input.Ticker,
                                Stat1W = fundHistStats.Mean1W * 100.0,
                                Stat2W = fundHistStats.Mean2W * 100.0,
                                Stat1M = fundHistStats.Mean1M * 100.0,
                                Stat3M = fundHistStats.Mean3M * 100.0,
                                Stat6M = fundHistStats.Mean6M * 100.0,
                                Stat12M = fundHistStats.Mean12M * 100.0,
                                Stat24M = fundHistStats.Mean24M * 100.0,
                                Stat36M = fundHistStats.Mean36M * 100.0,
                                Stat60M = fundHistStats.Mean60M * 100.0,
                                Stat120M = fundHistStats.Mean120M * 100.0,
                            };
                        }
                    }
                    else if ("SectorHistory".Equals(input.RequestType, StringComparison.CurrentCultureIgnoreCase))
                    {
                        data = _baseDao.GetSectorHistoricalStats(input.Country, input.SecurityType, input.Sector, input.FundCategory, input.MeasureType);
                    }

                    if (input.Index == 1)
                        result.Fund1 = data;
                    else if (input.Index == 2)
                        result.Fund2 = data;
                    else if (input.Index == 3)
                        result.Fund3 = data;
                    else if (input.Index == 4)
                        result.Fund4 = data;
                    else if (input.Index == 5)
                        result.Fund5 = data;
                    else if (input.Index == 6)
                        result.Fund6 = data;
                    else if (input.Index == 7)
                        result.Fund7 = data;
                    else if (input.Index == 8)
                        result.Fund8 = data;
                    else if (input.Index == 9)
                        result.Fund9 = data;
                    else if (input.Index == 10)
                        result.Fund10 = data;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error getting fund history");
            }
            return result;
        }

        public IList<FundPerformanceSummary> GetFundPerformanceSummary(CachingService cache, string assetType, string assetClassLevel1, string country)
        {
            IDictionary<string, FundPerformanceSummary> fundPerformanceSummaryDict = new Dictionary<string, FundPerformanceSummary>();

            IDictionary<string, FundMaster> dict = cache.Get<IDictionary<string, FundMaster>>(CacheKeys.FUND_MASTER);
            foreach (KeyValuePair<string, FundMaster> kvp in dict)
            {
                string ticker = kvp.Key;
                FundMaster fundMaster = kvp.Value;

                if (CheckForFundGroup(fundMaster, assetType, assetClassLevel1, country))
                {
                    if (fundMaster.FundPerfRtn != null && fundMaster.FundPerfRisk != null)
                    {
                        //populate historical returns and risk
                        FundPerformanceReturn fundPerformanceReturn = fundMaster.FundPerfRtn;
                        FundPerformanceRisk fundPerformanceRisk = fundMaster.FundPerfRisk;

                        //6 months
                        string period = "6 Mnths";
                        string key = ticker + "|" + period;
                        FundPerformanceSummary fundPerformanceSummary = new FundPerformanceSummary
                        {
                            Ticker = ticker,
                            Period = period,
                            PeriodId = 1,
                            FundPriceRtn = fundPerformanceReturn.PriceRtn6Mnths,
                            FundNavRtn = fundPerformanceReturn.NavRtn6Mnths,
                            FundPriceVol = fundPerformanceRisk.PriceVol6Mnths,
                            FundNavVol = fundPerformanceRisk.NavVol6Mnths,
                            FundPriceSharpeRatio = fundPerformanceRisk.PriceSharpeRatio6Mnths,
                            FundNavSharpeRatio = fundPerformanceRisk.NavSharpeRatio6Mnths
                        };
                        fundPerformanceSummaryDict.Add(key, fundPerformanceSummary);

                        //12 months
                        period = "12 Mnths";
                        key = ticker + "|" + period;
                        fundPerformanceSummary = new FundPerformanceSummary
                        {
                            Ticker = ticker,
                            Period = period,
                            PeriodId = 2,
                            FundPriceRtn = fundPerformanceReturn.PriceRtn12Mnths,
                            FundNavRtn = fundPerformanceReturn.NavRtn12Mnths,
                            FundPriceVol = fundPerformanceRisk.PriceVol12Mnths,
                            FundNavVol = fundPerformanceRisk.NavVol12Mnths,
                            FundPriceSharpeRatio = fundPerformanceRisk.PriceSharpeRatio12Mnths,
                            FundNavSharpeRatio = fundPerformanceRisk.NavSharpeRatio12Mnths
                        };
                        fundPerformanceSummaryDict.Add(key, fundPerformanceSummary);

                        //24 months
                        period = "24 Mnths";
                        key = ticker + "|" + period;
                        fundPerformanceSummary = new FundPerformanceSummary
                        {
                            Ticker = ticker,
                            Period = period,
                            PeriodId = 3,
                            FundPriceRtn = fundPerformanceReturn.PriceRtn24Mnths,
                            FundNavRtn = fundPerformanceReturn.NavRtn24Mnths,
                            FundPriceVol = fundPerformanceRisk.PriceVol24Mnths,
                            FundNavVol = fundPerformanceRisk.NavVol24Mnths,
                            FundPriceSharpeRatio = fundPerformanceRisk.PriceSharpeRatio24Mnths,
                            FundNavSharpeRatio = fundPerformanceRisk.NavSharpeRatio24Mnths
                        };
                        fundPerformanceSummaryDict.Add(key, fundPerformanceSummary);

                        //36 months
                        period = "36 Mnths";
                        key = ticker + "|" + period;
                        fundPerformanceSummary = new FundPerformanceSummary
                        {
                            Ticker = ticker,
                            Period = period,
                            PeriodId = 4,
                            FundPriceRtn = fundPerformanceReturn.PriceRtn36Mnths,
                            FundNavRtn = fundPerformanceReturn.NavRtn36Mnths,
                            FundPriceVol = fundPerformanceRisk.PriceVol36Mnths,
                            FundNavVol = fundPerformanceRisk.NavVol36Mnths,
                            FundPriceSharpeRatio = fundPerformanceRisk.PriceSharpeRatio36Mnths,
                            FundNavSharpeRatio = fundPerformanceRisk.NavSharpeRatio36Mnths
                        };
                        fundPerformanceSummaryDict.Add(key, fundPerformanceSummary);

                        //60 months
                        period = "60 Mnths";
                        key = ticker + "|" + period;
                        fundPerformanceSummary = new FundPerformanceSummary
                        {
                            Ticker = ticker,
                            Period = period,
                            PeriodId = 5,
                            FundPriceRtn = fundPerformanceReturn.PriceRtn60Mnths,
                            FundNavRtn = fundPerformanceReturn.NavRtn60Mnths,
                            FundPriceVol = fundPerformanceRisk.PriceVol60Mnths,
                            FundNavVol = fundPerformanceRisk.NavVol60Mnths,
                            FundPriceSharpeRatio = fundPerformanceRisk.PriceSharpeRatio60Mnths,
                            FundNavSharpeRatio = fundPerformanceRisk.NavSharpeRatio60Mnths
                        };
                        fundPerformanceSummaryDict.Add(key, fundPerformanceSummary);

                        //120 months
                        period = "120 Mnths";
                        key = ticker + "|" + period;
                        fundPerformanceSummary = new FundPerformanceSummary
                        {
                            Ticker = ticker,
                            Period = period,
                            PeriodId = 6,
                            FundPriceRtn = fundPerformanceReturn.PriceRtn120Mnths,
                            FundNavRtn = fundPerformanceReturn.NavRtn120Mnths,
                            FundPriceVol = fundPerformanceRisk.PriceVol120Mnths,
                            FundNavVol = fundPerformanceRisk.NavVol120Mnths,
                            FundPriceSharpeRatio = fundPerformanceRisk.PriceSharpeRatio120Mnths,
                            FundNavSharpeRatio = fundPerformanceRisk.NavSharpeRatio120Mnths
                        };
                        fundPerformanceSummaryDict.Add(key, fundPerformanceSummary);

                        //life
                        period = "Life";
                        key = ticker + "|" + period;
                        fundPerformanceSummary = new FundPerformanceSummary
                        {
                            Ticker = ticker,
                            Period = period,
                            PeriodId = 7,
                            FundPriceRtn = fundPerformanceReturn.PriceRtnLife,
                            FundNavRtn = fundPerformanceReturn.NavRtnLife,
                            FundPriceVol = fundPerformanceRisk.PriceVolLife,
                            FundNavVol = fundPerformanceRisk.NavVolLife,
                            FundPriceSharpeRatio = fundPerformanceRisk.PriceSharpeRatioLife,
                            FundNavSharpeRatio = fundPerformanceRisk.NavSharpeRatioLife
                        };
                        fundPerformanceSummaryDict.Add(key, fundPerformanceSummary);

                        //populate fund performance ranks
                        IList<FundPerformanceRank> fundPerformanceRanksList = fundMaster.FundPerfRanks;
                        if (fundPerformanceRanksList != null && fundPerformanceRanksList.Count() > 0)
                        {
                            foreach (FundPerformanceRank data in fundPerformanceRanksList)
                            {
                                //6 months
                                string rPeriod = "6 Mnths";
                                string rKey = data.Ticker + "|" + rPeriod;
                                fundPerformanceSummary = null;
                                if (fundPerformanceSummaryDict.TryGetValue(rKey, out fundPerformanceSummary))
                                    PopulateFundPerformanceRanks(rPeriod, fundPerformanceSummary, data);

                                //12 months
                                rPeriod = "12 Mnths";
                                rKey = data.Ticker + "|" + rPeriod;
                                fundPerformanceSummary = null;
                                if (fundPerformanceSummaryDict.TryGetValue(rKey, out fundPerformanceSummary))
                                    PopulateFundPerformanceRanks(rPeriod, fundPerformanceSummary, data);

                                //24 months
                                rPeriod = "24 Mnths";
                                rKey = data.Ticker + "|" + rPeriod;
                                fundPerformanceSummary = null;
                                if (fundPerformanceSummaryDict.TryGetValue(rKey, out fundPerformanceSummary))
                                    PopulateFundPerformanceRanks(rPeriod, fundPerformanceSummary, data);

                                //36 months
                                rPeriod = "36 Mnths";
                                rKey = data.Ticker + "|" + rPeriod;
                                fundPerformanceSummary = null;
                                if (fundPerformanceSummaryDict.TryGetValue(rKey, out fundPerformanceSummary))
                                    PopulateFundPerformanceRanks(rPeriod, fundPerformanceSummary, data);

                                //60 months
                                rPeriod = "60 Mnths";
                                rKey = data.Ticker + "|" + rPeriod;
                                fundPerformanceSummary = null;
                                if (fundPerformanceSummaryDict.TryGetValue(rKey, out fundPerformanceSummary))
                                    PopulateFundPerformanceRanks(rPeriod, fundPerformanceSummary, data);

                                //120 months
                                rPeriod = "120 Mnths";
                                rKey = data.Ticker + "|" + rPeriod;
                                fundPerformanceSummary = null;
                                if (fundPerformanceSummaryDict.TryGetValue(rKey, out fundPerformanceSummary))
                                    PopulateFundPerformanceRanks(rPeriod, fundPerformanceSummary, data);

                                //Life
                                rPeriod = "Life";
                                rKey = data.Ticker + "|" + rPeriod;
                                fundPerformanceSummary = null;
                                if (fundPerformanceSummaryDict.TryGetValue(rKey, out fundPerformanceSummary))
                                    PopulateFundPerformanceRanks(rPeriod, fundPerformanceSummary, data);
                            }
                        }
                    }
                }
            }

            return fundPerformanceSummaryDict.Values.ToList<FundPerformanceSummary>();
        }

        private void PopulateFundPerformanceRanks(string period, FundPerformanceSummary fundPerformanceSummary, FundPerformanceRank fundPerformanceRank)
        {
            if (!string.IsNullOrEmpty(fundPerformanceRank.PeerGroupHierarchyRank) && fundPerformanceRank.PeerGroupHierarchyRank.Equals("1"))
            {
                fundPerformanceSummary.Category1.CategoryName = fundPerformanceRank.PeerGroup;
                fundPerformanceSummary.Category1.CategoryRank = 1;

                if (period.Equals("6 Mnths"))
                {
                    fundPerformanceSummary.Category1.PDRank = fundPerformanceRank.PD6MnthsRank;
                    fundPerformanceSummary.Category1.PriceRtnRank = fundPerformanceRank.PriceRtn6MnthsRank;
                    fundPerformanceSummary.Category1.NavRtnRank = fundPerformanceRank.NavRtn6MnthsRank;
                    fundPerformanceSummary.Category1.PriceVolRank = fundPerformanceRank.PriceVol6MnthsRank;
                    fundPerformanceSummary.Category1.NavVolRank = fundPerformanceRank.NavVol6MnthsRank;
                    fundPerformanceSummary.Category1.PriceSharpeRatioRank = fundPerformanceRank.PriceSharpeRatio6MnthsRank;
                    fundPerformanceSummary.Category1.NavSharpeRatioRank = fundPerformanceRank.NavSharpeRatio6MnthsRank;
                    fundPerformanceSummary.FundPD = fundPerformanceRank.PD6Mnths;
                }
                else if (period.Equals("12 Mnths"))
                {
                    fundPerformanceSummary.Category1.PDRank = fundPerformanceRank.PD12MnthsRank;
                    fundPerformanceSummary.Category1.PriceRtnRank = fundPerformanceRank.PriceRtn12MnthsRank;
                    fundPerformanceSummary.Category1.NavRtnRank = fundPerformanceRank.NavRtn12MnthsRank;
                    fundPerformanceSummary.Category1.PriceVolRank = fundPerformanceRank.PriceVol12MnthsRank;
                    fundPerformanceSummary.Category1.NavVolRank = fundPerformanceRank.NavVol12MnthsRank;
                    fundPerformanceSummary.Category1.PriceSharpeRatioRank = fundPerformanceRank.PriceSharpeRatio12MnthsRank;
                    fundPerformanceSummary.Category1.NavSharpeRatioRank = fundPerformanceRank.NavSharpeRatio12MnthsRank;
                    fundPerformanceSummary.FundPD = fundPerformanceRank.PD12Mnths;
                }
                else if (period.Equals("24 Mnths"))
                {
                    fundPerformanceSummary.Category1.PDRank = fundPerformanceRank.PD24MnthsRank;
                    fundPerformanceSummary.Category1.PriceRtnRank = fundPerformanceRank.PriceRtn24MnthsRank;
                    fundPerformanceSummary.Category1.NavRtnRank = fundPerformanceRank.NavRtn24MnthsRank;
                    fundPerformanceSummary.Category1.PriceVolRank = fundPerformanceRank.PriceVol24MnthsRank;
                    fundPerformanceSummary.Category1.NavVolRank = fundPerformanceRank.NavVol24MnthsRank;
                    fundPerformanceSummary.Category1.PriceSharpeRatioRank = fundPerformanceRank.PriceSharpeRatio24MnthsRank;
                    fundPerformanceSummary.Category1.NavSharpeRatioRank = fundPerformanceRank.NavSharpeRatio24MnthsRank;
                    fundPerformanceSummary.FundPD = fundPerformanceRank.PD24Mnths;
                }
                else if (period.Equals("36 Mnths"))
                {
                    fundPerformanceSummary.Category1.PDRank = fundPerformanceRank.PD36MnthsRank;
                    fundPerformanceSummary.Category1.PriceRtnRank = fundPerformanceRank.PriceRtn36MnthsRank;
                    fundPerformanceSummary.Category1.NavRtnRank = fundPerformanceRank.NavRtn36MnthsRank;
                    fundPerformanceSummary.Category1.PriceVolRank = fundPerformanceRank.PriceVol36MnthsRank;
                    fundPerformanceSummary.Category1.NavVolRank = fundPerformanceRank.NavVol36MnthsRank;
                    fundPerformanceSummary.Category1.PriceSharpeRatioRank = fundPerformanceRank.PriceSharpeRatio36MnthsRank;
                    fundPerformanceSummary.Category1.NavSharpeRatioRank = fundPerformanceRank.NavSharpeRatio36MnthsRank;
                    fundPerformanceSummary.FundPD = fundPerformanceRank.PD36Mnths;
                }
                else if (period.Equals("60 Mnths"))
                {
                    fundPerformanceSummary.Category1.PDRank = fundPerformanceRank.PD60MnthsRank;
                    fundPerformanceSummary.Category1.PriceRtnRank = fundPerformanceRank.PriceRtn60MnthsRank;
                    fundPerformanceSummary.Category1.NavRtnRank = fundPerformanceRank.NavRtn60MnthsRank;
                    fundPerformanceSummary.Category1.PriceVolRank = fundPerformanceRank.PriceVol60MnthsRank;
                    fundPerformanceSummary.Category1.NavVolRank = fundPerformanceRank.NavVol60MnthsRank;
                    fundPerformanceSummary.Category1.PriceSharpeRatioRank = fundPerformanceRank.PriceSharpeRatio60MnthsRank;
                    fundPerformanceSummary.Category1.NavSharpeRatioRank = fundPerformanceRank.NavSharpeRatio60MnthsRank;
                    fundPerformanceSummary.FundPD = fundPerformanceRank.PD60Mnths;
                }
                else if (period.Equals("120 Mnths"))
                {
                    fundPerformanceSummary.Category1.PDRank = fundPerformanceRank.PD120MnthsRank;
                    fundPerformanceSummary.Category1.PriceRtnRank = fundPerformanceRank.PriceRtn120MnthsRank;
                    fundPerformanceSummary.Category1.NavRtnRank = fundPerformanceRank.NavRtn120MnthsRank;
                    fundPerformanceSummary.Category1.PriceVolRank = fundPerformanceRank.PriceVol120MnthsRank;
                    fundPerformanceSummary.Category1.NavVolRank = fundPerformanceRank.NavVol120MnthsRank;
                    fundPerformanceSummary.Category1.PriceSharpeRatioRank = fundPerformanceRank.PriceSharpeRatio120MnthsRank;
                    fundPerformanceSummary.Category1.NavSharpeRatioRank = fundPerformanceRank.NavSharpeRatio120MnthsRank;
                    fundPerformanceSummary.FundPD = fundPerformanceRank.PD120Mnths;
                }
                else if (period.Equals("Life"))
                {
                    fundPerformanceSummary.Category1.PDRank = fundPerformanceRank.PDLifeRank;
                    fundPerformanceSummary.Category1.PriceRtnRank = fundPerformanceRank.PriceRtnLifeRank;
                    fundPerformanceSummary.Category1.NavRtnRank = fundPerformanceRank.NavRtnLifeRank;
                    fundPerformanceSummary.Category1.PriceVolRank = fundPerformanceRank.PriceVolLifeRank;
                    fundPerformanceSummary.Category1.NavVolRank = fundPerformanceRank.NavVolLifeRank;
                    fundPerformanceSummary.Category1.PriceSharpeRatioRank = fundPerformanceRank.PriceSharpeRatioLifeRank;
                    fundPerformanceSummary.Category1.NavSharpeRatioRank = fundPerformanceRank.NavSharpeRatioLifeRank;
                    fundPerformanceSummary.FundPD = fundPerformanceRank.PDLife;
                }
            }
            else if (!string.IsNullOrEmpty(fundPerformanceRank.PeerGroupHierarchyRank) && fundPerformanceRank.PeerGroupHierarchyRank.Equals("2"))
            {
                fundPerformanceSummary.Category2.CategoryName = fundPerformanceRank.PeerGroup;
                fundPerformanceSummary.Category2.CategoryRank = 2;

                if (period.Equals("6 Mnths"))
                {
                    fundPerformanceSummary.Category2.PDRank = fundPerformanceRank.PD6MnthsRank;
                    fundPerformanceSummary.Category2.PriceRtnRank = fundPerformanceRank.PriceRtn6MnthsRank;
                    fundPerformanceSummary.Category2.NavRtnRank = fundPerformanceRank.NavRtn6MnthsRank;
                    fundPerformanceSummary.Category2.PriceVolRank = fundPerformanceRank.PriceVol6MnthsRank;
                    fundPerformanceSummary.Category2.NavVolRank = fundPerformanceRank.NavVol6MnthsRank;
                    fundPerformanceSummary.Category2.PriceSharpeRatioRank = fundPerformanceRank.PriceSharpeRatio6MnthsRank;
                    fundPerformanceSummary.Category2.NavSharpeRatioRank = fundPerformanceRank.NavSharpeRatio6MnthsRank;
                    fundPerformanceSummary.FundPD = fundPerformanceRank.PD6Mnths;
                }
                else if (period.Equals("12 Mnths"))
                {
                    fundPerformanceSummary.Category2.PDRank = fundPerformanceRank.PD12MnthsRank;
                    fundPerformanceSummary.Category2.PriceRtnRank = fundPerformanceRank.PriceRtn12MnthsRank;
                    fundPerformanceSummary.Category2.NavRtnRank = fundPerformanceRank.NavRtn12MnthsRank;
                    fundPerformanceSummary.Category2.PriceVolRank = fundPerformanceRank.PriceVol12MnthsRank;
                    fundPerformanceSummary.Category2.NavVolRank = fundPerformanceRank.NavVol12MnthsRank;
                    fundPerformanceSummary.Category2.PriceSharpeRatioRank = fundPerformanceRank.PriceSharpeRatio12MnthsRank;
                    fundPerformanceSummary.Category2.NavSharpeRatioRank = fundPerformanceRank.NavSharpeRatio12MnthsRank;
                    fundPerformanceSummary.FundPD = fundPerformanceRank.PD12Mnths;
                }
                else if (period.Equals("24 Mnths"))
                {
                    fundPerformanceSummary.Category2.PDRank = fundPerformanceRank.PD24MnthsRank;
                    fundPerformanceSummary.Category2.PriceRtnRank = fundPerformanceRank.PriceRtn24MnthsRank;
                    fundPerformanceSummary.Category2.NavRtnRank = fundPerformanceRank.NavRtn24MnthsRank;
                    fundPerformanceSummary.Category2.PriceVolRank = fundPerformanceRank.PriceVol24MnthsRank;
                    fundPerformanceSummary.Category2.NavVolRank = fundPerformanceRank.NavVol24MnthsRank;
                    fundPerformanceSummary.Category2.PriceSharpeRatioRank = fundPerformanceRank.PriceSharpeRatio24MnthsRank;
                    fundPerformanceSummary.Category2.NavSharpeRatioRank = fundPerformanceRank.NavSharpeRatio24MnthsRank;
                    fundPerformanceSummary.FundPD = fundPerformanceRank.PD24Mnths;
                }
                else if (period.Equals("36 Mnths"))
                {
                    fundPerformanceSummary.Category2.PDRank = fundPerformanceRank.PD36MnthsRank;
                    fundPerformanceSummary.Category2.PriceRtnRank = fundPerformanceRank.PriceRtn36MnthsRank;
                    fundPerformanceSummary.Category2.NavRtnRank = fundPerformanceRank.NavRtn36MnthsRank;
                    fundPerformanceSummary.Category2.PriceVolRank = fundPerformanceRank.PriceVol36MnthsRank;
                    fundPerformanceSummary.Category2.NavVolRank = fundPerformanceRank.NavVol36MnthsRank;
                    fundPerformanceSummary.Category2.PriceSharpeRatioRank = fundPerformanceRank.PriceSharpeRatio36MnthsRank;
                    fundPerformanceSummary.Category2.NavSharpeRatioRank = fundPerformanceRank.NavSharpeRatio36MnthsRank;
                    fundPerformanceSummary.FundPD = fundPerformanceRank.PD36Mnths;
                }
                else if (period.Equals("60 Mnths"))
                {
                    fundPerformanceSummary.Category2.PDRank = fundPerformanceRank.PD60MnthsRank;
                    fundPerformanceSummary.Category2.PriceRtnRank = fundPerformanceRank.PriceRtn60MnthsRank;
                    fundPerformanceSummary.Category2.NavRtnRank = fundPerformanceRank.NavRtn60MnthsRank;
                    fundPerformanceSummary.Category2.PriceVolRank = fundPerformanceRank.PriceVol60MnthsRank;
                    fundPerformanceSummary.Category2.NavVolRank = fundPerformanceRank.NavVol60MnthsRank;
                    fundPerformanceSummary.Category2.PriceSharpeRatioRank = fundPerformanceRank.PriceSharpeRatio60MnthsRank;
                    fundPerformanceSummary.Category2.NavSharpeRatioRank = fundPerformanceRank.NavSharpeRatio60MnthsRank;
                    fundPerformanceSummary.FundPD = fundPerformanceRank.PD60Mnths;
                }
                else if (period.Equals("120 Mnths"))
                {
                    fundPerformanceSummary.Category2.PDRank = fundPerformanceRank.PD120MnthsRank;
                    fundPerformanceSummary.Category2.PriceRtnRank = fundPerformanceRank.PriceRtn120MnthsRank;
                    fundPerformanceSummary.Category2.NavRtnRank = fundPerformanceRank.NavRtn120MnthsRank;
                    fundPerformanceSummary.Category2.PriceVolRank = fundPerformanceRank.PriceVol120MnthsRank;
                    fundPerformanceSummary.Category2.NavVolRank = fundPerformanceRank.NavVol120MnthsRank;
                    fundPerformanceSummary.Category2.PriceSharpeRatioRank = fundPerformanceRank.PriceSharpeRatio120MnthsRank;
                    fundPerformanceSummary.Category2.NavSharpeRatioRank = fundPerformanceRank.NavSharpeRatio120MnthsRank;
                    fundPerformanceSummary.FundPD = fundPerformanceRank.PD120Mnths;
                }
                else if (period.Equals("Life"))
                {
                    fundPerformanceSummary.Category2.PDRank = fundPerformanceRank.PDLifeRank;
                    fundPerformanceSummary.Category2.PriceRtnRank = fundPerformanceRank.PriceRtnLifeRank;
                    fundPerformanceSummary.Category2.NavRtnRank = fundPerformanceRank.NavRtnLifeRank;
                    fundPerformanceSummary.Category2.PriceVolRank = fundPerformanceRank.PriceVolLifeRank;
                    fundPerformanceSummary.Category2.NavVolRank = fundPerformanceRank.NavVolLifeRank;
                    fundPerformanceSummary.Category2.PriceSharpeRatioRank = fundPerformanceRank.PriceSharpeRatioLifeRank;
                    fundPerformanceSummary.Category2.NavSharpeRatioRank = fundPerformanceRank.NavSharpeRatioLifeRank;
                    fundPerformanceSummary.FundPD = fundPerformanceRank.PDLife;
                }
            }
            else if (!string.IsNullOrEmpty(fundPerformanceRank.PeerGroupHierarchyRank) && fundPerformanceRank.PeerGroupHierarchyRank.Equals("3"))
            {
                fundPerformanceSummary.Category3.CategoryName = fundPerformanceRank.PeerGroup;
                fundPerformanceSummary.Category3.CategoryRank = 3;

                if (period.Equals("6 Mnths"))
                {
                    fundPerformanceSummary.Category3.PDRank = fundPerformanceRank.PD6MnthsRank;
                    fundPerformanceSummary.Category3.PriceRtnRank = fundPerformanceRank.PriceRtn6MnthsRank;
                    fundPerformanceSummary.Category3.NavRtnRank = fundPerformanceRank.NavRtn6MnthsRank;
                    fundPerformanceSummary.Category3.PriceVolRank = fundPerformanceRank.PriceVol6MnthsRank;
                    fundPerformanceSummary.Category3.NavVolRank = fundPerformanceRank.NavVol6MnthsRank;
                    fundPerformanceSummary.Category3.PriceSharpeRatioRank = fundPerformanceRank.PriceSharpeRatio6MnthsRank;
                    fundPerformanceSummary.Category3.NavSharpeRatioRank = fundPerformanceRank.NavSharpeRatio6MnthsRank;
                    fundPerformanceSummary.FundPD = fundPerformanceRank.PD6Mnths;
                }
                else if (period.Equals("12 Mnths"))
                {
                    fundPerformanceSummary.Category3.PDRank = fundPerformanceRank.PD12MnthsRank;
                    fundPerformanceSummary.Category3.PriceRtnRank = fundPerformanceRank.PriceRtn12MnthsRank;
                    fundPerformanceSummary.Category3.NavRtnRank = fundPerformanceRank.NavRtn12MnthsRank;
                    fundPerformanceSummary.Category3.PriceVolRank = fundPerformanceRank.PriceVol12MnthsRank;
                    fundPerformanceSummary.Category3.NavVolRank = fundPerformanceRank.NavVol12MnthsRank;
                    fundPerformanceSummary.Category3.PriceSharpeRatioRank = fundPerformanceRank.PriceSharpeRatio12MnthsRank;
                    fundPerformanceSummary.Category3.NavSharpeRatioRank = fundPerformanceRank.NavSharpeRatio12MnthsRank;
                    fundPerformanceSummary.FundPD = fundPerformanceRank.PD12Mnths;
                }
                else if (period.Equals("24 Mnths"))
                {
                    fundPerformanceSummary.Category3.PDRank = fundPerformanceRank.PD24MnthsRank;
                    fundPerformanceSummary.Category3.PriceRtnRank = fundPerformanceRank.PriceRtn24MnthsRank;
                    fundPerformanceSummary.Category3.NavRtnRank = fundPerformanceRank.NavRtn24MnthsRank;
                    fundPerformanceSummary.Category3.PriceVolRank = fundPerformanceRank.PriceVol24MnthsRank;
                    fundPerformanceSummary.Category3.NavVolRank = fundPerformanceRank.NavVol24MnthsRank;
                    fundPerformanceSummary.Category3.PriceSharpeRatioRank = fundPerformanceRank.PriceSharpeRatio24MnthsRank;
                    fundPerformanceSummary.Category3.NavSharpeRatioRank = fundPerformanceRank.NavSharpeRatio24MnthsRank;
                    fundPerformanceSummary.FundPD = fundPerformanceRank.PD24Mnths;
                }
                else if (period.Equals("36 Mnths"))
                {
                    fundPerformanceSummary.Category3.PDRank = fundPerformanceRank.PD36MnthsRank;
                    fundPerformanceSummary.Category3.PriceRtnRank = fundPerformanceRank.PriceRtn36MnthsRank;
                    fundPerformanceSummary.Category3.NavRtnRank = fundPerformanceRank.NavRtn36MnthsRank;
                    fundPerformanceSummary.Category3.PriceVolRank = fundPerformanceRank.PriceVol36MnthsRank;
                    fundPerformanceSummary.Category3.NavVolRank = fundPerformanceRank.NavVol36MnthsRank;
                    fundPerformanceSummary.Category3.PriceSharpeRatioRank = fundPerformanceRank.PriceSharpeRatio36MnthsRank;
                    fundPerformanceSummary.Category3.NavSharpeRatioRank = fundPerformanceRank.NavSharpeRatio36MnthsRank;
                    fundPerformanceSummary.FundPD = fundPerformanceRank.PD36Mnths;
                }
                else if (period.Equals("60 Mnths"))
                {
                    fundPerformanceSummary.Category3.PDRank = fundPerformanceRank.PD60MnthsRank;
                    fundPerformanceSummary.Category3.PriceRtnRank = fundPerformanceRank.PriceRtn60MnthsRank;
                    fundPerformanceSummary.Category3.NavRtnRank = fundPerformanceRank.NavRtn60MnthsRank;
                    fundPerformanceSummary.Category3.PriceVolRank = fundPerformanceRank.PriceVol60MnthsRank;
                    fundPerformanceSummary.Category3.NavVolRank = fundPerformanceRank.NavVol60MnthsRank;
                    fundPerformanceSummary.Category3.PriceSharpeRatioRank = fundPerformanceRank.PriceSharpeRatio60MnthsRank;
                    fundPerformanceSummary.Category3.NavSharpeRatioRank = fundPerformanceRank.NavSharpeRatio60MnthsRank;
                    fundPerformanceSummary.FundPD = fundPerformanceRank.PD60Mnths;
                }
                else if (period.Equals("120 Mnths"))
                {
                    fundPerformanceSummary.Category3.PDRank = fundPerformanceRank.PD120MnthsRank;
                    fundPerformanceSummary.Category3.PriceRtnRank = fundPerformanceRank.PriceRtn120MnthsRank;
                    fundPerformanceSummary.Category3.NavRtnRank = fundPerformanceRank.NavRtn120MnthsRank;
                    fundPerformanceSummary.Category3.PriceVolRank = fundPerformanceRank.PriceVol120MnthsRank;
                    fundPerformanceSummary.Category3.NavVolRank = fundPerformanceRank.NavVol120MnthsRank;
                    fundPerformanceSummary.Category3.PriceSharpeRatioRank = fundPerformanceRank.PriceSharpeRatio120MnthsRank;
                    fundPerformanceSummary.Category3.NavSharpeRatioRank = fundPerformanceRank.NavSharpeRatio120MnthsRank;
                    fundPerformanceSummary.FundPD = fundPerformanceRank.PD120Mnths;
                }
                else if (period.Equals("Life"))
                {
                    fundPerformanceSummary.Category3.PDRank = fundPerformanceRank.PDLifeRank;
                    fundPerformanceSummary.Category3.PriceRtnRank = fundPerformanceRank.PriceRtnLifeRank;
                    fundPerformanceSummary.Category3.NavRtnRank = fundPerformanceRank.NavRtnLifeRank;
                    fundPerformanceSummary.Category3.PriceVolRank = fundPerformanceRank.PriceVolLifeRank;
                    fundPerformanceSummary.Category3.NavVolRank = fundPerformanceRank.NavVolLifeRank;
                    fundPerformanceSummary.Category3.PriceSharpeRatioRank = fundPerformanceRank.PriceSharpeRatioLifeRank;
                    fundPerformanceSummary.Category3.NavSharpeRatioRank = fundPerformanceRank.NavSharpeRatioLifeRank;
                    fundPerformanceSummary.FundPD = fundPerformanceRank.PDLife;
                }
            }
            else if (!string.IsNullOrEmpty(fundPerformanceRank.PeerGroupHierarchyRank) && fundPerformanceRank.PeerGroupHierarchyRank.Equals("4"))
            {
                fundPerformanceSummary.Category4.CategoryName = fundPerformanceRank.PeerGroup;
                fundPerformanceSummary.Category4.CategoryRank = 4;

                if (period.Equals("6 Mnths"))
                {
                    fundPerformanceSummary.Category4.PDRank = fundPerformanceRank.PD6MnthsRank;
                    fundPerformanceSummary.Category4.PriceRtnRank = fundPerformanceRank.PriceRtn6MnthsRank;
                    fundPerformanceSummary.Category4.NavRtnRank = fundPerformanceRank.NavRtn6MnthsRank;
                    fundPerformanceSummary.Category4.PriceVolRank = fundPerformanceRank.PriceVol6MnthsRank;
                    fundPerformanceSummary.Category4.NavVolRank = fundPerformanceRank.NavVol6MnthsRank;
                    fundPerformanceSummary.Category4.PriceSharpeRatioRank = fundPerformanceRank.PriceSharpeRatio6MnthsRank;
                    fundPerformanceSummary.Category4.NavSharpeRatioRank = fundPerformanceRank.NavSharpeRatio6MnthsRank;
                    fundPerformanceSummary.FundPD = fundPerformanceRank.PD6Mnths;
                }
                else if (period.Equals("12 Mnths"))
                {
                    fundPerformanceSummary.Category4.PDRank = fundPerformanceRank.PD12MnthsRank;
                    fundPerformanceSummary.Category4.PriceRtnRank = fundPerformanceRank.PriceRtn12MnthsRank;
                    fundPerformanceSummary.Category4.NavRtnRank = fundPerformanceRank.NavRtn12MnthsRank;
                    fundPerformanceSummary.Category4.PriceVolRank = fundPerformanceRank.PriceVol12MnthsRank;
                    fundPerformanceSummary.Category4.NavVolRank = fundPerformanceRank.NavVol12MnthsRank;
                    fundPerformanceSummary.Category4.PriceSharpeRatioRank = fundPerformanceRank.PriceSharpeRatio12MnthsRank;
                    fundPerformanceSummary.Category4.NavSharpeRatioRank = fundPerformanceRank.NavSharpeRatio12MnthsRank;
                    fundPerformanceSummary.FundPD = fundPerformanceRank.PD12Mnths;
                }
                else if (period.Equals("24 Mnths"))
                {
                    fundPerformanceSummary.Category4.PDRank = fundPerformanceRank.PD24MnthsRank;
                    fundPerformanceSummary.Category4.PriceRtnRank = fundPerformanceRank.PriceRtn24MnthsRank;
                    fundPerformanceSummary.Category4.NavRtnRank = fundPerformanceRank.NavRtn24MnthsRank;
                    fundPerformanceSummary.Category4.PriceVolRank = fundPerformanceRank.PriceVol24MnthsRank;
                    fundPerformanceSummary.Category4.NavVolRank = fundPerformanceRank.NavVol24MnthsRank;
                    fundPerformanceSummary.Category4.PriceSharpeRatioRank = fundPerformanceRank.PriceSharpeRatio24MnthsRank;
                    fundPerformanceSummary.Category4.NavSharpeRatioRank = fundPerformanceRank.NavSharpeRatio24MnthsRank;
                    fundPerformanceSummary.FundPD = fundPerformanceRank.PD24Mnths;
                }
                else if (period.Equals("36 Mnths"))
                {
                    fundPerformanceSummary.Category4.PDRank = fundPerformanceRank.PD36MnthsRank;
                    fundPerformanceSummary.Category4.PriceRtnRank = fundPerformanceRank.PriceRtn36MnthsRank;
                    fundPerformanceSummary.Category4.NavRtnRank = fundPerformanceRank.NavRtn36MnthsRank;
                    fundPerformanceSummary.Category4.PriceVolRank = fundPerformanceRank.PriceVol36MnthsRank;
                    fundPerformanceSummary.Category4.NavVolRank = fundPerformanceRank.NavVol36MnthsRank;
                    fundPerformanceSummary.Category4.PriceSharpeRatioRank = fundPerformanceRank.PriceSharpeRatio36MnthsRank;
                    fundPerformanceSummary.Category4.NavSharpeRatioRank = fundPerformanceRank.NavSharpeRatio36MnthsRank;
                    fundPerformanceSummary.FundPD = fundPerformanceRank.PD36Mnths;
                }
                else if (period.Equals("60 Mnths"))
                {
                    fundPerformanceSummary.Category4.PDRank = fundPerformanceRank.PD60MnthsRank;
                    fundPerformanceSummary.Category4.PriceRtnRank = fundPerformanceRank.PriceRtn60MnthsRank;
                    fundPerformanceSummary.Category4.NavRtnRank = fundPerformanceRank.NavRtn60MnthsRank;
                    fundPerformanceSummary.Category4.PriceVolRank = fundPerformanceRank.PriceVol60MnthsRank;
                    fundPerformanceSummary.Category4.NavVolRank = fundPerformanceRank.NavVol60MnthsRank;
                    fundPerformanceSummary.Category4.PriceSharpeRatioRank = fundPerformanceRank.PriceSharpeRatio60MnthsRank;
                    fundPerformanceSummary.Category4.NavSharpeRatioRank = fundPerformanceRank.NavSharpeRatio60MnthsRank;
                    fundPerformanceSummary.FundPD = fundPerformanceRank.PD60Mnths;
                }
                else if (period.Equals("120 Mnths"))
                {
                    fundPerformanceSummary.Category4.PDRank = fundPerformanceRank.PD120MnthsRank;
                    fundPerformanceSummary.Category4.PriceRtnRank = fundPerformanceRank.PriceRtn120MnthsRank;
                    fundPerformanceSummary.Category4.NavRtnRank = fundPerformanceRank.NavRtn120MnthsRank;
                    fundPerformanceSummary.Category4.PriceVolRank = fundPerformanceRank.PriceVol120MnthsRank;
                    fundPerformanceSummary.Category4.NavVolRank = fundPerformanceRank.NavVol120MnthsRank;
                    fundPerformanceSummary.Category4.PriceSharpeRatioRank = fundPerformanceRank.PriceSharpeRatio120MnthsRank;
                    fundPerformanceSummary.Category4.NavSharpeRatioRank = fundPerformanceRank.NavSharpeRatio120MnthsRank;
                    fundPerformanceSummary.FundPD = fundPerformanceRank.PD120Mnths;
                }
                else if (period.Equals("Life"))
                {
                    fundPerformanceSummary.Category4.PDRank = fundPerformanceRank.PDLifeRank;
                    fundPerformanceSummary.Category4.PriceRtnRank = fundPerformanceRank.PriceRtnLifeRank;
                    fundPerformanceSummary.Category4.NavRtnRank = fundPerformanceRank.NavRtnLifeRank;
                    fundPerformanceSummary.Category4.PriceVolRank = fundPerformanceRank.PriceVolLifeRank;
                    fundPerformanceSummary.Category4.NavVolRank = fundPerformanceRank.NavVolLifeRank;
                    fundPerformanceSummary.Category4.PriceSharpeRatioRank = fundPerformanceRank.PriceSharpeRatioLifeRank;
                    fundPerformanceSummary.Category4.NavSharpeRatioRank = fundPerformanceRank.NavSharpeRatioLifeRank;
                    fundPerformanceSummary.FundPD = fundPerformanceRank.PDLife;
                }
            }
            else if (!string.IsNullOrEmpty(fundPerformanceRank.PeerGroupHierarchyRank) && fundPerformanceRank.PeerGroupHierarchyRank.Equals("5"))
            {
                fundPerformanceSummary.Category5.CategoryName = fundPerformanceRank.PeerGroup;
                fundPerformanceSummary.Category5.CategoryRank = 5;

                if (period.Equals("6 Mnths"))
                {
                    fundPerformanceSummary.Category5.PDRank = fundPerformanceRank.PD6MnthsRank;
                    fundPerformanceSummary.Category5.PriceRtnRank = fundPerformanceRank.PriceRtn6MnthsRank;
                    fundPerformanceSummary.Category5.NavRtnRank = fundPerformanceRank.NavRtn6MnthsRank;
                    fundPerformanceSummary.Category5.PriceVolRank = fundPerformanceRank.PriceVol6MnthsRank;
                    fundPerformanceSummary.Category5.NavVolRank = fundPerformanceRank.NavVol6MnthsRank;
                    fundPerformanceSummary.Category5.PriceSharpeRatioRank = fundPerformanceRank.PriceSharpeRatio6MnthsRank;
                    fundPerformanceSummary.Category5.NavSharpeRatioRank = fundPerformanceRank.NavSharpeRatio6MnthsRank;
                    fundPerformanceSummary.FundPD = fundPerformanceRank.PD6Mnths;
                }
                else if (period.Equals("12 Mnths"))
                {
                    fundPerformanceSummary.Category5.PDRank = fundPerformanceRank.PD12MnthsRank;
                    fundPerformanceSummary.Category5.PriceRtnRank = fundPerformanceRank.PriceRtn12MnthsRank;
                    fundPerformanceSummary.Category5.NavRtnRank = fundPerformanceRank.NavRtn12MnthsRank;
                    fundPerformanceSummary.Category5.PriceVolRank = fundPerformanceRank.PriceVol12MnthsRank;
                    fundPerformanceSummary.Category5.NavVolRank = fundPerformanceRank.NavVol12MnthsRank;
                    fundPerformanceSummary.Category5.PriceSharpeRatioRank = fundPerformanceRank.PriceSharpeRatio12MnthsRank;
                    fundPerformanceSummary.Category5.NavSharpeRatioRank = fundPerformanceRank.NavSharpeRatio12MnthsRank;
                    fundPerformanceSummary.FundPD = fundPerformanceRank.PD12Mnths;
                }
                else if (period.Equals("24 Mnths"))
                {
                    fundPerformanceSummary.Category5.PDRank = fundPerformanceRank.PD24MnthsRank;
                    fundPerformanceSummary.Category5.PriceRtnRank = fundPerformanceRank.PriceRtn24MnthsRank;
                    fundPerformanceSummary.Category5.NavRtnRank = fundPerformanceRank.NavRtn24MnthsRank;
                    fundPerformanceSummary.Category5.PriceVolRank = fundPerformanceRank.PriceVol24MnthsRank;
                    fundPerformanceSummary.Category5.NavVolRank = fundPerformanceRank.NavVol24MnthsRank;
                    fundPerformanceSummary.Category5.PriceSharpeRatioRank = fundPerformanceRank.PriceSharpeRatio24MnthsRank;
                    fundPerformanceSummary.Category5.NavSharpeRatioRank = fundPerformanceRank.NavSharpeRatio24MnthsRank;
                    fundPerformanceSummary.FundPD = fundPerformanceRank.PD24Mnths;
                }
                else if (period.Equals("36 Mnths"))
                {
                    fundPerformanceSummary.Category5.PDRank = fundPerformanceRank.PD36MnthsRank;
                    fundPerformanceSummary.Category5.PriceRtnRank = fundPerformanceRank.PriceRtn36MnthsRank;
                    fundPerformanceSummary.Category5.NavRtnRank = fundPerformanceRank.NavRtn36MnthsRank;
                    fundPerformanceSummary.Category5.PriceVolRank = fundPerformanceRank.PriceVol36MnthsRank;
                    fundPerformanceSummary.Category5.NavVolRank = fundPerformanceRank.NavVol36MnthsRank;
                    fundPerformanceSummary.Category5.PriceSharpeRatioRank = fundPerformanceRank.PriceSharpeRatio36MnthsRank;
                    fundPerformanceSummary.Category5.NavSharpeRatioRank = fundPerformanceRank.NavSharpeRatio36MnthsRank;
                    fundPerformanceSummary.FundPD = fundPerformanceRank.PD36Mnths;
                }
                else if (period.Equals("60 Mnths"))
                {
                    fundPerformanceSummary.Category5.PDRank = fundPerformanceRank.PD60MnthsRank;
                    fundPerformanceSummary.Category5.PriceRtnRank = fundPerformanceRank.PriceRtn60MnthsRank;
                    fundPerformanceSummary.Category5.NavRtnRank = fundPerformanceRank.NavRtn60MnthsRank;
                    fundPerformanceSummary.Category5.PriceVolRank = fundPerformanceRank.PriceVol60MnthsRank;
                    fundPerformanceSummary.Category5.NavVolRank = fundPerformanceRank.NavVol60MnthsRank;
                    fundPerformanceSummary.Category5.PriceSharpeRatioRank = fundPerformanceRank.PriceSharpeRatio60MnthsRank;
                    fundPerformanceSummary.Category5.NavSharpeRatioRank = fundPerformanceRank.NavSharpeRatio60MnthsRank;
                    fundPerformanceSummary.FundPD = fundPerformanceRank.PD60Mnths;
                }
                else if (period.Equals("120 Mnths"))
                {
                    fundPerformanceSummary.Category5.PDRank = fundPerformanceRank.PD120MnthsRank;
                    fundPerformanceSummary.Category5.PriceRtnRank = fundPerformanceRank.PriceRtn120MnthsRank;
                    fundPerformanceSummary.Category5.NavRtnRank = fundPerformanceRank.NavRtn120MnthsRank;
                    fundPerformanceSummary.Category5.PriceVolRank = fundPerformanceRank.PriceVol120MnthsRank;
                    fundPerformanceSummary.Category5.NavVolRank = fundPerformanceRank.NavVol120MnthsRank;
                    fundPerformanceSummary.Category5.PriceSharpeRatioRank = fundPerformanceRank.PriceSharpeRatio120MnthsRank;
                    fundPerformanceSummary.Category5.NavSharpeRatioRank = fundPerformanceRank.NavSharpeRatio120MnthsRank;
                    fundPerformanceSummary.FundPD = fundPerformanceRank.PD120Mnths;
                }
                else if (period.Equals("Life"))
                {
                    fundPerformanceSummary.Category5.PDRank = fundPerformanceRank.PDLifeRank;
                    fundPerformanceSummary.Category5.PriceRtnRank = fundPerformanceRank.PriceRtnLifeRank;
                    fundPerformanceSummary.Category5.NavRtnRank = fundPerformanceRank.NavRtnLifeRank;
                    fundPerformanceSummary.Category5.PriceVolRank = fundPerformanceRank.PriceVolLifeRank;
                    fundPerformanceSummary.Category5.NavVolRank = fundPerformanceRank.NavVolLifeRank;
                    fundPerformanceSummary.Category5.PriceSharpeRatioRank = fundPerformanceRank.PriceSharpeRatioLifeRank;
                    fundPerformanceSummary.Category5.NavSharpeRatioRank = fundPerformanceRank.NavSharpeRatioLifeRank;
                    fundPerformanceSummary.FundPD = fundPerformanceRank.PDLife;
                }
            }
            else if (!string.IsNullOrEmpty(fundPerformanceRank.PeerGroupHierarchyRank) && fundPerformanceRank.PeerGroupHierarchyRank.Equals("6"))
            {
                fundPerformanceSummary.Category6.CategoryName = fundPerformanceRank.PeerGroup;
                fundPerformanceSummary.Category6.CategoryRank = 6;

                if (period.Equals("6 Mnths"))
                {
                    fundPerformanceSummary.Category6.PDRank = fundPerformanceRank.PD6MnthsRank;
                    fundPerformanceSummary.Category6.PriceRtnRank = fundPerformanceRank.PriceRtn6MnthsRank;
                    fundPerformanceSummary.Category6.NavRtnRank = fundPerformanceRank.NavRtn6MnthsRank;
                    fundPerformanceSummary.Category6.PriceVolRank = fundPerformanceRank.PriceVol6MnthsRank;
                    fundPerformanceSummary.Category6.NavVolRank = fundPerformanceRank.NavVol6MnthsRank;
                    fundPerformanceSummary.Category6.PriceSharpeRatioRank = fundPerformanceRank.PriceSharpeRatio6MnthsRank;
                    fundPerformanceSummary.Category6.NavSharpeRatioRank = fundPerformanceRank.NavSharpeRatio6MnthsRank;
                    fundPerformanceSummary.FundPD = fundPerformanceRank.PD6Mnths;
                }
                else if (period.Equals("12 Mnths"))
                {
                    fundPerformanceSummary.Category6.PDRank = fundPerformanceRank.PD12MnthsRank;
                    fundPerformanceSummary.Category6.PriceRtnRank = fundPerformanceRank.PriceRtn12MnthsRank;
                    fundPerformanceSummary.Category6.NavRtnRank = fundPerformanceRank.NavRtn12MnthsRank;
                    fundPerformanceSummary.Category6.PriceVolRank = fundPerformanceRank.PriceVol12MnthsRank;
                    fundPerformanceSummary.Category6.NavVolRank = fundPerformanceRank.NavVol12MnthsRank;
                    fundPerformanceSummary.Category6.PriceSharpeRatioRank = fundPerformanceRank.PriceSharpeRatio12MnthsRank;
                    fundPerformanceSummary.Category6.NavSharpeRatioRank = fundPerformanceRank.NavSharpeRatio12MnthsRank;
                    fundPerformanceSummary.FundPD = fundPerformanceRank.PD12Mnths;
                }
                else if (period.Equals("24 Mnths"))
                {
                    fundPerformanceSummary.Category6.PDRank = fundPerformanceRank.PD24MnthsRank;
                    fundPerformanceSummary.Category6.PriceRtnRank = fundPerformanceRank.PriceRtn24MnthsRank;
                    fundPerformanceSummary.Category6.NavRtnRank = fundPerformanceRank.NavRtn24MnthsRank;
                    fundPerformanceSummary.Category6.PriceVolRank = fundPerformanceRank.PriceVol24MnthsRank;
                    fundPerformanceSummary.Category6.NavVolRank = fundPerformanceRank.NavVol24MnthsRank;
                    fundPerformanceSummary.Category6.PriceSharpeRatioRank = fundPerformanceRank.PriceSharpeRatio24MnthsRank;
                    fundPerformanceSummary.Category6.NavSharpeRatioRank = fundPerformanceRank.NavSharpeRatio24MnthsRank;
                    fundPerformanceSummary.FundPD = fundPerformanceRank.PD24Mnths;
                }
                else if (period.Equals("36 Mnths"))
                {
                    fundPerformanceSummary.Category6.PDRank = fundPerformanceRank.PD36MnthsRank;
                    fundPerformanceSummary.Category6.PriceRtnRank = fundPerformanceRank.PriceRtn36MnthsRank;
                    fundPerformanceSummary.Category6.NavRtnRank = fundPerformanceRank.NavRtn36MnthsRank;
                    fundPerformanceSummary.Category6.PriceVolRank = fundPerformanceRank.PriceVol36MnthsRank;
                    fundPerformanceSummary.Category6.NavVolRank = fundPerformanceRank.NavVol36MnthsRank;
                    fundPerformanceSummary.Category6.PriceSharpeRatioRank = fundPerformanceRank.PriceSharpeRatio36MnthsRank;
                    fundPerformanceSummary.Category6.NavSharpeRatioRank = fundPerformanceRank.NavSharpeRatio36MnthsRank;
                    fundPerformanceSummary.FundPD = fundPerformanceRank.PD36Mnths;
                }
                else if (period.Equals("60 Mnths"))
                {
                    fundPerformanceSummary.Category6.PDRank = fundPerformanceRank.PD60MnthsRank;
                    fundPerformanceSummary.Category6.PriceRtnRank = fundPerformanceRank.PriceRtn60MnthsRank;
                    fundPerformanceSummary.Category6.NavRtnRank = fundPerformanceRank.NavRtn60MnthsRank;
                    fundPerformanceSummary.Category6.PriceVolRank = fundPerformanceRank.PriceVol60MnthsRank;
                    fundPerformanceSummary.Category6.NavVolRank = fundPerformanceRank.NavVol60MnthsRank;
                    fundPerformanceSummary.Category6.PriceSharpeRatioRank = fundPerformanceRank.PriceSharpeRatio60MnthsRank;
                    fundPerformanceSummary.Category6.NavSharpeRatioRank = fundPerformanceRank.NavSharpeRatio60MnthsRank;
                    fundPerformanceSummary.FundPD = fundPerformanceRank.PD60Mnths;
                }
                else if (period.Equals("120 Mnths"))
                {
                    fundPerformanceSummary.Category6.PDRank = fundPerformanceRank.PD120MnthsRank;
                    fundPerformanceSummary.Category6.PriceRtnRank = fundPerformanceRank.PriceRtn120MnthsRank;
                    fundPerformanceSummary.Category6.NavRtnRank = fundPerformanceRank.NavRtn120MnthsRank;
                    fundPerformanceSummary.Category6.PriceVolRank = fundPerformanceRank.PriceVol120MnthsRank;
                    fundPerformanceSummary.Category6.NavVolRank = fundPerformanceRank.NavVol120MnthsRank;
                    fundPerformanceSummary.Category6.PriceSharpeRatioRank = fundPerformanceRank.PriceSharpeRatio120MnthsRank;
                    fundPerformanceSummary.Category6.NavSharpeRatioRank = fundPerformanceRank.NavSharpeRatio120MnthsRank;
                    fundPerformanceSummary.FundPD = fundPerformanceRank.PD120Mnths;
                }
                else if (period.Equals("Life"))
                {
                    fundPerformanceSummary.Category6.PDRank = fundPerformanceRank.PDLifeRank;
                    fundPerformanceSummary.Category6.PriceRtnRank = fundPerformanceRank.PriceRtnLifeRank;
                    fundPerformanceSummary.Category6.NavRtnRank = fundPerformanceRank.NavRtnLifeRank;
                    fundPerformanceSummary.Category6.PriceVolRank = fundPerformanceRank.PriceVolLifeRank;
                    fundPerformanceSummary.Category6.NavVolRank = fundPerformanceRank.NavVolLifeRank;
                    fundPerformanceSummary.Category6.PriceSharpeRatioRank = fundPerformanceRank.PriceSharpeRatioLifeRank;
                    fundPerformanceSummary.Category6.NavSharpeRatioRank = fundPerformanceRank.NavSharpeRatioLifeRank;
                    fundPerformanceSummary.FundPD = fundPerformanceRank.PDLife;
                }
            }
            else if (!string.IsNullOrEmpty(fundPerformanceRank.PeerGroupHierarchyRank) && fundPerformanceRank.PeerGroupHierarchyRank.Equals("7"))
            {
                fundPerformanceSummary.Category7.CategoryName = fundPerformanceRank.PeerGroup;
                fundPerformanceSummary.Category7.CategoryRank = 7;

                if (period.Equals("6 Mnths"))
                {
                    fundPerformanceSummary.Category7.PDRank = fundPerformanceRank.PD6MnthsRank;
                    fundPerformanceSummary.Category7.PriceRtnRank = fundPerformanceRank.PriceRtn6MnthsRank;
                    fundPerformanceSummary.Category7.NavRtnRank = fundPerformanceRank.NavRtn6MnthsRank;
                    fundPerformanceSummary.Category7.PriceVolRank = fundPerformanceRank.PriceVol6MnthsRank;
                    fundPerformanceSummary.Category7.NavVolRank = fundPerformanceRank.NavVol6MnthsRank;
                    fundPerformanceSummary.Category7.PriceSharpeRatioRank = fundPerformanceRank.PriceSharpeRatio6MnthsRank;
                    fundPerformanceSummary.Category7.NavSharpeRatioRank = fundPerformanceRank.NavSharpeRatio6MnthsRank;
                    fundPerformanceSummary.FundPD = fundPerformanceRank.PD6Mnths;
                }
                else if (period.Equals("12 Mnths"))
                {
                    fundPerformanceSummary.Category7.PDRank = fundPerformanceRank.PD12MnthsRank;
                    fundPerformanceSummary.Category7.PriceRtnRank = fundPerformanceRank.PriceRtn12MnthsRank;
                    fundPerformanceSummary.Category7.NavRtnRank = fundPerformanceRank.NavRtn12MnthsRank;
                    fundPerformanceSummary.Category7.PriceVolRank = fundPerformanceRank.PriceVol12MnthsRank;
                    fundPerformanceSummary.Category7.NavVolRank = fundPerformanceRank.NavVol12MnthsRank;
                    fundPerformanceSummary.Category7.PriceSharpeRatioRank = fundPerformanceRank.PriceSharpeRatio12MnthsRank;
                    fundPerformanceSummary.Category7.NavSharpeRatioRank = fundPerformanceRank.NavSharpeRatio12MnthsRank;
                    fundPerformanceSummary.FundPD = fundPerformanceRank.PD12Mnths;
                }
                else if (period.Equals("24 Mnths"))
                {
                    fundPerformanceSummary.Category7.PDRank = fundPerformanceRank.PD24MnthsRank;
                    fundPerformanceSummary.Category7.PriceRtnRank = fundPerformanceRank.PriceRtn24MnthsRank;
                    fundPerformanceSummary.Category7.NavRtnRank = fundPerformanceRank.NavRtn24MnthsRank;
                    fundPerformanceSummary.Category7.PriceVolRank = fundPerformanceRank.PriceVol24MnthsRank;
                    fundPerformanceSummary.Category7.NavVolRank = fundPerformanceRank.NavVol24MnthsRank;
                    fundPerformanceSummary.Category7.PriceSharpeRatioRank = fundPerformanceRank.PriceSharpeRatio24MnthsRank;
                    fundPerformanceSummary.Category7.NavSharpeRatioRank = fundPerformanceRank.NavSharpeRatio24MnthsRank;
                    fundPerformanceSummary.FundPD = fundPerformanceRank.PD24Mnths;
                }
                else if (period.Equals("36 Mnths"))
                {
                    fundPerformanceSummary.Category7.PDRank = fundPerformanceRank.PD36MnthsRank;
                    fundPerformanceSummary.Category7.PriceRtnRank = fundPerformanceRank.PriceRtn36MnthsRank;
                    fundPerformanceSummary.Category7.NavRtnRank = fundPerformanceRank.NavRtn36MnthsRank;
                    fundPerformanceSummary.Category7.PriceVolRank = fundPerformanceRank.PriceVol36MnthsRank;
                    fundPerformanceSummary.Category7.NavVolRank = fundPerformanceRank.NavVol36MnthsRank;
                    fundPerformanceSummary.Category7.PriceSharpeRatioRank = fundPerformanceRank.PriceSharpeRatio36MnthsRank;
                    fundPerformanceSummary.Category7.NavSharpeRatioRank = fundPerformanceRank.NavSharpeRatio36MnthsRank;
                    fundPerformanceSummary.FundPD = fundPerformanceRank.PD36Mnths;
                }
                else if (period.Equals("60 Mnths"))
                {
                    fundPerformanceSummary.Category7.PDRank = fundPerformanceRank.PD60MnthsRank;
                    fundPerformanceSummary.Category7.PriceRtnRank = fundPerformanceRank.PriceRtn60MnthsRank;
                    fundPerformanceSummary.Category7.NavRtnRank = fundPerformanceRank.NavRtn60MnthsRank;
                    fundPerformanceSummary.Category7.PriceVolRank = fundPerformanceRank.PriceVol60MnthsRank;
                    fundPerformanceSummary.Category7.NavVolRank = fundPerformanceRank.NavVol60MnthsRank;
                    fundPerformanceSummary.Category7.PriceSharpeRatioRank = fundPerformanceRank.PriceSharpeRatio60MnthsRank;
                    fundPerformanceSummary.Category7.NavSharpeRatioRank = fundPerformanceRank.NavSharpeRatio60MnthsRank;
                    fundPerformanceSummary.FundPD = fundPerformanceRank.PD60Mnths;
                }
                else if (period.Equals("120 Mnths"))
                {
                    fundPerformanceSummary.Category7.PDRank = fundPerformanceRank.PD120MnthsRank;
                    fundPerformanceSummary.Category7.PriceRtnRank = fundPerformanceRank.PriceRtn120MnthsRank;
                    fundPerformanceSummary.Category7.NavRtnRank = fundPerformanceRank.NavRtn120MnthsRank;
                    fundPerformanceSummary.Category7.PriceVolRank = fundPerformanceRank.PriceVol120MnthsRank;
                    fundPerformanceSummary.Category7.NavVolRank = fundPerformanceRank.NavVol120MnthsRank;
                    fundPerformanceSummary.Category7.PriceSharpeRatioRank = fundPerformanceRank.PriceSharpeRatio120MnthsRank;
                    fundPerformanceSummary.Category7.NavSharpeRatioRank = fundPerformanceRank.NavSharpeRatio120MnthsRank;
                    fundPerformanceSummary.FundPD = fundPerformanceRank.PD120Mnths;
                }
                else if (period.Equals("Life"))
                {
                    fundPerformanceSummary.Category7.PDRank = fundPerformanceRank.PDLifeRank;
                    fundPerformanceSummary.Category7.PriceRtnRank = fundPerformanceRank.PriceRtnLifeRank;
                    fundPerformanceSummary.Category7.NavRtnRank = fundPerformanceRank.NavRtnLifeRank;
                    fundPerformanceSummary.Category7.PriceVolRank = fundPerformanceRank.PriceVolLifeRank;
                    fundPerformanceSummary.Category7.NavVolRank = fundPerformanceRank.NavVolLifeRank;
                    fundPerformanceSummary.Category7.PriceSharpeRatioRank = fundPerformanceRank.PriceSharpeRatioLifeRank;
                    fundPerformanceSummary.Category7.NavSharpeRatioRank = fundPerformanceRank.NavSharpeRatioLifeRank;
                    fundPerformanceSummary.FundPD = fundPerformanceRank.PDLife;
                }
            }
        }

        public IList<FundCategoryPerformanceSummary> GetFundPerformanceCategories(CachingService cache, string assetType, string assetClassLevel1, string country)
        {
            IDictionary<string, FundCategoryPerformanceSummary> fundCategoryPerformanceSummaryDict = new Dictionary<string, FundCategoryPerformanceSummary>();

            IDictionary<string, FundMaster> dict = cache.Get<IDictionary<string, FundMaster>>(CacheKeys.FUND_MASTER);
            foreach (KeyValuePair<string, FundMaster> kvp in dict)
            {
                FundMaster fundMaster = kvp.Value;
                if (CheckForFundGroup(fundMaster, assetType, assetClassLevel1, country))
                {
                    if (fundMaster.FundPerfRanks != null)
                    {
                        IList<FundPerformanceRank> fundPerformanceRanksList = fundMaster.FundPerfRanks;
                        if (fundPerformanceRanksList != null && fundPerformanceRanksList.Count > 0)
                        {
                            foreach (FundPerformanceRank data in fundPerformanceRanksList)
                            {
                                string category = data.PeerGroup;
                                if (!fundCategoryPerformanceSummaryDict.TryGetValue(category, out FundCategoryPerformanceSummary fundCategoryPerformanceSummary))
                                {
                                    fundCategoryPerformanceSummary = new FundCategoryPerformanceSummary
                                    {
                                        CategoryName = category
                                    };

                                    if (!string.IsNullOrEmpty(data.PeerGroupHierarchyRank) && data.PeerGroupHierarchyRank.Equals("1"))
                                        fundCategoryPerformanceSummary.CategoryRank = 1;
                                    else if (!string.IsNullOrEmpty(data.PeerGroupHierarchyRank) && data.PeerGroupHierarchyRank.Equals("2"))
                                        fundCategoryPerformanceSummary.CategoryRank = 2;
                                    else if (!string.IsNullOrEmpty(data.PeerGroupHierarchyRank) && data.PeerGroupHierarchyRank.Equals("3"))
                                        fundCategoryPerformanceSummary.CategoryRank = 3;
                                    else if (!string.IsNullOrEmpty(data.PeerGroupHierarchyRank) && data.PeerGroupHierarchyRank.Equals("4"))
                                        fundCategoryPerformanceSummary.CategoryRank = 4;
                                    else if (!string.IsNullOrEmpty(data.PeerGroupHierarchyRank) && data.PeerGroupHierarchyRank.Equals("5"))
                                        fundCategoryPerformanceSummary.CategoryRank = 5;
                                    else if (!string.IsNullOrEmpty(data.PeerGroupHierarchyRank) && data.PeerGroupHierarchyRank.Equals("6"))
                                        fundCategoryPerformanceSummary.CategoryRank = 6;
                                    else if (!string.IsNullOrEmpty(data.PeerGroupHierarchyRank) && data.PeerGroupHierarchyRank.Equals("7"))
                                        fundCategoryPerformanceSummary.CategoryRank = 7;

                                    fundCategoryPerformanceSummaryDict.Add(category, fundCategoryPerformanceSummary);
                                }
                            }
                        }
                    }
                }
            }

            IList<FundCategoryPerformanceSummary> list = (fundCategoryPerformanceSummaryDict.Values.ToList<FundCategoryPerformanceSummary>())
                .OrderBy(s => s.CategoryRank).ToList<FundCategoryPerformanceSummary>();
            return list;
        }

        private bool CheckForFundGroup(FundMaster fundMaster, string assetType, string assetClassLevel1, string country)
        {
            bool result = false;

            if (!string.IsNullOrEmpty(fundMaster.SecTyp) && fundMaster.SecTyp.Contains(assetType))
            {
                if (!string.IsNullOrEmpty(fundMaster.Cntry) && fundMaster.Cntry.Equals(country, StringComparison.CurrentCultureIgnoreCase))
                {
                    if (assetClassLevel1.Equals("All", StringComparison.CurrentCultureIgnoreCase))
                    {
                        result = true;
                    }
                    else if (!string.IsNullOrEmpty(fundMaster.AssetLvl1) && fundMaster.AssetLvl1.Equals(assetClassLevel1, StringComparison.CurrentCultureIgnoreCase))
                    {
                        result = true;
                    }
                }
            }
            return result;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ticker"></param>
        /// <param name="startDate"></param>
        /// <param name="endDate"></param>
        /// <returns></returns>
        public IList<FundNavReportTO> GetFundNavEstDetails(string ticker, DateTime startDate, DateTime endDate)
        {
            IList<FundNavReportTO> list = _fundHistoryDao.GetFundNavReportDetails(ticker, startDate, endDate);

            if (list != null && list.Count > 0)
            {
                int rowId = 1;
                DateTime? prevNavDate = null;
                double? prevPubNav = null;
                double? prevPubAdjNav = null;
                double? prevEstNav = null;

                foreach (FundNavReportTO data in list)
                {
                    if (rowId == 1)
                    {
                        prevNavDate = data.PubNavDt;
                        prevPubNav = data.PubNav;
                        prevPubAdjNav = data.PubAdjNav;
                        prevEstNav = data.EstNav;
                    }
                    else
                    {
                        //if (!prevNavDate.Equals(data.PubNavDt.GetValueOrDefault()))
                        //{
                        data.PrevPubNav = prevPubNav;
                        data.PrevPubAdjNav = prevPubAdjNav;
                        data.PrevEstNav = prevEstNav;

                        try
                        {
                            if (data.PubAdjNav.HasValue && data.PrevPubAdjNav.HasValue)
                                data.PubNavChng = (data.PubAdjNav / data.PrevPubAdjNav) - 1.0;

                            if (data.PrevPubAdjNav.HasValue && data.PrevEstNav.HasValue)
                                data.EstNavChng = (data.PrevEstNav / data.PrevPubAdjNav) - 1.0;


                            if (data.PubNavChng.HasValue && data.EstNavChng.HasValue)
                            {
                                //data.EstNavErr = Math.Abs(data.PubNavChng.GetValueOrDefault() - data.EstNavChng.GetValueOrDefault());
                                data.EstNavErr = data.PubNavChng.GetValueOrDefault() - data.EstNavChng.GetValueOrDefault();
                                data.EstNavErrAbs = Math.Abs(data.PubNavChng.GetValueOrDefault() - data.EstNavChng.GetValueOrDefault());
                                if (data.PubNavChng.GetValueOrDefault() != 0)
                                    data.EstNavErrRel = Math.Abs((data.EstNavChng.GetValueOrDefault() - data.PubNavChng.GetValueOrDefault()) / data.PubNavChng.GetValueOrDefault());
                            }
                        }
                        catch (Exception)
                        {
                        }

                        prevNavDate = data.PubNavDt;
                        prevPubNav = data.PubNav;
                        prevPubAdjNav = data.PubAdjNav;
                        //prevEstNav = data.EstNav;
                        //}
                        //else
                        //{
                        //    previousNavDate = data.PubNavDt;
                        //    prevPubNav = data.PubNav;
                        //    prevEstNav = data.EstNav;
                        //}

                        prevEstNav = data.EstNav;
                    }
                    rowId++;
                }
            }

            return list.OrderByDescending(s => s.Date).ToList<FundNavReportTO>();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ticker"></param>
        /// <param name="startDate"></param>
        /// <param name="endDate"></param>
        /// <returns></returns>
        public IList<FundNavReportTO> GetLatestFundNavEstDetails(string country, Nullable<double> errorThreshold, DateTime asofDate)
        {
            IList<FundNavReportTO> list = _fundHistoryDao.GetLatestFundNavReportDetails(country, asofDate);
            IList<FundNavReportTO> results = new List<FundNavReportTO>();

            IDictionary<string, PositionMaster> positionDict = _cache.Get<IDictionary<string, PositionMaster>>(CacheKeys.POSITION_MASTER);

            if (list != null && list.Count > 0)
            {
                int rowId = 1;
                DateTime? prevNavDate = null;
                double? prevPubNav = null;
                double? prevPubAdjNav = null;
                double? prevEstNav = null;
                double? prevPortNav = null;
                double? prevETFNav = null;
                double? prevProxyNav = null;
                string prevTicker = null;

                foreach (FundNavReportTO data in list)
                {
                    if (string.IsNullOrEmpty(prevTicker) || !data.Ticker.Equals(prevTicker))
                    {
                        prevNavDate = data.PubNavDt;
                        prevPubNav = data.PubNav;
                        prevPubAdjNav = data.PubAdjNav;
                        prevPortNav = data.PortNav;
                        prevETFNav = data.ETFNav;
                        prevProxyNav = data.ProxyNav;
                        prevEstNav = data.EstNav;
                        prevTicker = data.Ticker;
                    }
                    else
                    {
                        if (data.Ticker.Equals(prevTicker) && !prevNavDate.Equals(data.PubNavDt.GetValueOrDefault()))
                        {
                            data.PrevPubNavDt = prevNavDate;
                            data.PrevPubNav = prevPubNav;
                            data.PrevPubAdjNav = prevPubAdjNav;
                            data.PrevEstNav = prevEstNav;
                            data.PrevPortNav = prevPortNav;
                            data.PrevProxyNav = prevProxyNav;
                            data.PrevETFNav = prevETFNav;

                            if (data.PubAdjNav.HasValue && data.PrevPubAdjNav.HasValue)
                                data.PubNavChng = (data.PubAdjNav / data.PrevPubAdjNav) - 1.0;

                            if (data.PrevPubAdjNav.HasValue && data.PrevEstNav.HasValue)
                                data.EstNavChng = (data.PrevEstNav / data.PrevPubAdjNav) - 1.0;

                            if (data.PubNavChng.HasValue && data.EstNavChng.HasValue)
                                //data.EstNavErr = Math.Abs(data.PubNavChng.GetValueOrDefault() - data.EstNavChng.GetValueOrDefault());
                                data.EstNavErr = data.PubNavChng.GetValueOrDefault() - data.EstNavChng.GetValueOrDefault();

                            prevNavDate = data.PubNavDt;
                            prevPubNav = data.PubNav;

                            PositionMaster positionMaster = null;
                            if (!positionDict.TryGetValue(data.Ticker, out positionMaster))
                            {
                                //search by CT ticker if composite ticker CN is provided
                                if (data.Ticker.EndsWith(" CN", StringComparison.CurrentCultureIgnoreCase))
                                {
                                    string newTicker = data.Ticker.Replace(" CN", " CT");
                                    positionDict.TryGetValue(newTicker, out positionMaster);
                                }
                            }

                            data.SortId = 1;
                            if (positionMaster != null)
                            {
                                data.TotalPos = positionMaster.FundAll.PosHeld;

                                if (Math.Abs(data.EstNavErr.GetValueOrDefault()) > 0.0035)
                                {
                                    data.ErrFlag = 1;
                                    data.SortId = 9;
                                }
                            }

                            if (errorThreshold == null)
                                results.Add(data);
                            else if (Math.Abs(data.EstNavErr.GetValueOrDefault()) > errorThreshold.GetValueOrDefault() / 100.0)
                                results.Add(data);
                        }
                        prevEstNav = data.EstNav;
                    }
                    rowId++;
                }
            }

            return results.OrderByDescending(s => s.SortId).ToList<FundNavReportTO>();
            //return results;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ticker"></param>
        /// <param name="effectiveDate"></param>
        /// <param name="lookbackDays"></param>
        /// <returns></returns>
        public FundHistoryScalar GetFundHistoricalScalarData(string ticker, DateTime effectiveDate, int lookbackDays)
        {
            FundHistoryScalar result = null;

            IDictionary<string, IDictionary<DateTime, FundHistoryScalar>> dict = _cache.Get<IDictionary<string, IDictionary<DateTime, FundHistoryScalar>>>(CacheKeys.FUND_FULL_HISTORY);
            if (dict != null && dict.TryGetValue(ticker, out IDictionary<DateTime, FundHistoryScalar> fundHistDict))
            {
                if (!fundHistDict.TryGetValue(effectiveDate, out result))
                {
                    if (lookbackDays != 0)
                    {
                        for (int i = 1; i <= lookbackDays; i++)
                        {
                            DateTime newEffectiveDate = DateUtils.AddDays(effectiveDate, -1 * i);
                            if (fundHistDict.TryGetValue(newEffectiveDate, out result))
                            {
                                break;
                            }
                        }
                    }
                }
                else
                {
                    fundHistDict.TryGetValue(effectiveDate, out result);
                }
            }
            else
            {
                result = _fundHistoryDao.GetFundHistoryScalar(ticker, effectiveDate, lookbackDays);
            }

            return result;
        }


    }
}