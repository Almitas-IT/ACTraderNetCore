using aACTrader.DAO.Repository;
using aCommons;
using aCommons.Cef;
using LazyCache;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;

namespace aACTrader.Operations.Impl
{
    public class FundRedemptionTriggerOperations
    {
        private readonly ILogger<FundRedemptionTriggerOperations> _logger;
        private readonly FundSupplementalDataDao _fundSupplementalDataDao;
        private readonly CachingService _cache;

        public FundRedemptionTriggerOperations(ILogger<FundRedemptionTriggerOperations> logger
            , FundSupplementalDataDao fundSupplementalDataDao
            , CachingService cache)
        {
            _logger = logger;
            _fundSupplementalDataDao = fundSupplementalDataDao;
            _cache = cache;
        }

        public void ProcessFundRedemptionTriggers()
        {
            IDictionary<string, FundRedemptionTrigger> fundRedemptionTriggerDict = _cache.Get<IDictionary<string, FundRedemptionTrigger>>(CacheKeys.FUND_REDEMPTION_TRIGGERS);

            IList<FundRedemptionTriggerDetail> detailList = new List<FundRedemptionTriggerDetail>();
            foreach (KeyValuePair<string, FundRedemptionTrigger> kvp in fundRedemptionTriggerDict)
            {
                FundRedemptionTrigger data = kvp.Value;

                if (data.TriggerType.Equals("Discount", StringComparison.CurrentCultureIgnoreCase))
                    ProcessDiscountTrigger(data, detailList);
                else if (data.TriggerType.Equals("NAVRtn", StringComparison.CurrentCultureIgnoreCase))
                    ProcessNavReturnTrigger(data, detailList);
                else if (data.TriggerType.Equals("PriceRtn", StringComparison.CurrentCultureIgnoreCase))
                    ProcessPriceReturnTrigger(data, detailList);
            }

            //Save
            _fundSupplementalDataDao.SaveFundRedemptionTriggerDetails(detailList);
        }

        private void ProcessDiscountTrigger(FundRedemptionTrigger fundRedemptionTrigger, IList<FundRedemptionTriggerDetail> detailList)
        {
            try
            {
                IList<FundDiscountHist> fundDiscountHistory = _fundSupplementalDataDao.GetFundDiscountHistory(
                    fundRedemptionTrigger.Ticker
                    , fundRedemptionTrigger.Source
                    , fundRedemptionTrigger.DataStartDate.GetValueOrDefault()
                    , fundRedemptionTrigger.DataEndDate.GetValueOrDefault());

                double averageDiscount = 0;
                int numObs = 0;
                foreach (FundDiscountHist data in fundDiscountHistory)
                {
                    if (data.PublishedDiscount.HasValue)
                    {
                        averageDiscount += data.PublishedDiscount.GetValueOrDefault();
                        numObs++;

                        //Add Detail Record
                        FundRedemptionTriggerDetail detailRecord = new FundRedemptionTriggerDetail();
                        detailRecord.Ticker = fundRedemptionTrigger.Ticker;
                        detailRecord.TriggerType = fundRedemptionTrigger.TriggerType;
                        detailRecord.ValueType = "D";
                        detailRecord.EffectiveDate = data.EffectiveDate;
                        detailRecord.FundNav = data.PublishedNav;
                        detailRecord.FundNavDate = data.PublishedNavDate;
                        detailRecord.FundDiscount = data.PublishedDiscount;
                        detailList.Add(detailRecord);
                    }
                }
                averageDiscount /= (double)numObs;

                //Summary
                FundRedemptionTriggerDetail summaryRecord = new FundRedemptionTriggerDetail();
                summaryRecord.Ticker = fundRedemptionTrigger.Ticker;
                summaryRecord.TriggerType = fundRedemptionTrigger.TriggerType;
                summaryRecord.ValueType = "S";
                summaryRecord.FundDiscount = averageDiscount;
                detailList.Add(summaryRecord);
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error calculating discount threshold for ticker: " + fundRedemptionTrigger.Ticker);
            }
        }

        private void ProcessNavReturnTrigger(FundRedemptionTrigger fundRedemptionTrigger, IList<FundRedemptionTriggerDetail> detailList)
        {
            try
            {
                //Fund
                IList<FundDataHist> fundDataHistory = _fundSupplementalDataDao.GetFundDataHistory(
                    fundRedemptionTrigger.Ticker
                    , "Fund"
                    , "Nav"
                    , fundRedemptionTrigger.Source
                    , fundRedemptionTrigger.DataStartDate.GetValueOrDefault()
                    , fundRedemptionTrigger.DataEndDate.GetValueOrDefault());

                foreach (FundDataHist data in fundDataHistory)
                {
                    //Add Detail Record
                    FundRedemptionTriggerDetail detailRecord = new FundRedemptionTriggerDetail();
                    detailRecord.Ticker = fundRedemptionTrigger.Ticker;
                    detailRecord.TriggerType = fundRedemptionTrigger.TriggerType;
                    detailRecord.ValueType = "D";
                    detailRecord.SecurityType = "F";
                    detailRecord.EffectiveDate = data.EffectiveDate;
                    detailRecord.FundNav = data.PublishedNav;
                    detailRecord.FundNavDate = data.PublishedNavDate;
                    detailRecord.FundDiscount = data.PublishedDiscount;
                    detailRecord.DvdExDate = data.DvdExDate;
                    detailRecord.DvdPayDate = data.DvdPayDate;
                    detailRecord.DvdAmount = data.DvdAmount;
                    detailRecord.DvdFrequency = data.DvdFreq;
                    detailRecord.DvdType = data.DvdType;

                    detailList.Add(detailRecord);
                }

                if (!string.IsNullOrEmpty(fundRedemptionTrigger.Benchmark))
                {
                    //Benchmark
                    IList<FundDataHist> benchmarkDataHistory = _fundSupplementalDataDao.GetFundDataHistory(
                        fundRedemptionTrigger.Benchmark
                        , "Benchmark"
                        , "Nav"
                        , fundRedemptionTrigger.Source
                        , fundRedemptionTrigger.DataStartDate.GetValueOrDefault()
                        , fundRedemptionTrigger.DataEndDate.GetValueOrDefault());


                    foreach (FundDataHist data in benchmarkDataHistory)
                    {
                        //Add Detail Record
                        FundRedemptionTriggerDetail detailRecord = new FundRedemptionTriggerDetail();
                        detailRecord.Ticker = fundRedemptionTrigger.Benchmark;
                        detailRecord.TriggerType = fundRedemptionTrigger.TriggerType;
                        detailRecord.ValueType = "D";
                        detailRecord.SecurityType = "B";
                        detailRecord.EffectiveDate = data.EffectiveDate;
                        detailRecord.DailyRtn = data.DailyRtn;

                        detailList.Add(detailRecord);
                    }
                }
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error calculating Nav Rtn for ticker: " + fundRedemptionTrigger.Ticker);
            }
        }

        private void ProcessPriceReturnTrigger(FundRedemptionTrigger fundRedemptionTrigger, IList<FundRedemptionTriggerDetail> detailList)
        {
            try
            {
                //Fund
                IList<FundDataHist> fundDataHistory = _fundSupplementalDataDao.GetFundDataHistory(
                    fundRedemptionTrigger.Ticker
                    , "Fund"
                    , "Price"
                    , fundRedemptionTrigger.Source
                    , fundRedemptionTrigger.DataStartDate.GetValueOrDefault()
                    , fundRedemptionTrigger.DataEndDate.GetValueOrDefault());

                foreach (FundDataHist data in fundDataHistory)
                {
                    //Add Detail Record
                    FundRedemptionTriggerDetail detailRecord = new FundRedemptionTriggerDetail();
                    detailRecord.Ticker = fundRedemptionTrigger.Ticker;
                    detailRecord.TriggerType = fundRedemptionTrigger.TriggerType;
                    detailRecord.ValueType = "D";
                    detailRecord.SecurityType = "F";
                    detailRecord.EffectiveDate = data.EffectiveDate;
                    detailRecord.Price = data.Price;
                    detailRecord.DvdExDate = data.DvdExDate;
                    detailRecord.DvdPayDate = data.DvdPayDate;
                    detailRecord.DvdAmount = data.DvdAmount;
                    detailRecord.DvdFrequency = data.DvdFreq;
                    detailRecord.DvdType = data.DvdType;

                    detailList.Add(detailRecord);
                }

                if (!string.IsNullOrEmpty(fundRedemptionTrigger.Benchmark))
                {
                    //Benchmark
                    IList<FundDataHist> benchmarkDataHistory = _fundSupplementalDataDao.GetFundDataHistory(
                        fundRedemptionTrigger.Benchmark
                        , "Benchmark"
                        , "Price"
                        , fundRedemptionTrigger.Source
                        , fundRedemptionTrigger.DataStartDate.GetValueOrDefault()
                        , fundRedemptionTrigger.DataEndDate.GetValueOrDefault());


                    foreach (FundDataHist data in benchmarkDataHistory)
                    {
                        //Add Detail Record
                        FundRedemptionTriggerDetail detailRecord = new FundRedemptionTriggerDetail();
                        detailRecord.Ticker = fundRedemptionTrigger.Benchmark;
                        detailRecord.TriggerType = fundRedemptionTrigger.TriggerType;
                        detailRecord.ValueType = "D";
                        detailRecord.SecurityType = "B";
                        detailRecord.EffectiveDate = data.EffectiveDate;
                        detailRecord.DailyRtn = data.DailyRtn;

                        detailList.Add(detailRecord);
                    }
                }
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error calculating Price Rtn for ticker: " + fundRedemptionTrigger.Ticker);
            }
        }
    }
}