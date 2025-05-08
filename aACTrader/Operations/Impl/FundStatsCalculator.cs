using aCommons;
using aCommons.Cef;
using LazyCache;
using MathNet.Numerics.Statistics;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;

namespace aACTrader.Operations.Impl
{
    public class FundStatsCalculator
    {

        private readonly ILogger<FundStatsCalculator> _logger;
        private readonly CachingService _cache;

        public FundStatsCalculator(ILogger<FundStatsCalculator> logger, CachingService cache)
        {
            _logger = logger;
            _cache = cache;
        }

        public void CalculateLiveDiscountStats()
        {
            IDictionary<string, FundMaster> fundMasterDict = _cache.Get<IDictionary<string, FundMaster>>(CacheKeys.FUND_MASTER);
            IDictionary<string, FundForecast> fundForecastDict = _cache.Get<IDictionary<string, FundForecast>>(CacheKeys.FUND_FORECASTS);
            IDictionary<string, IList<FundPDHistory>> fundDiscountDict = _cache.Get<IDictionary<string, IList<FundPDHistory>>>(CacheKeys.CEF_FUND_FULL_HISTORY);
            IDictionary<string, FundPDStats> fundPDStatsDict = _cache.Get<IDictionary<string, FundPDStats>>(CacheKeys.LIVE_FUND_PD_STATS);

            IList<double> period1M = new List<double>();
            IList<double> period3M = new List<double>();
            IList<double> period6M = new List<double>();
            IList<double> period1Y = new List<double>();
            IList<double> period2Y = new List<double>();
            IList<double> period3Y = new List<double>();
            IList<double> period5Y = new List<double>();
            IList<double> period10Y = new List<double>();
            IList<double> period25Y = new List<double>();

            if (fundDiscountDict != null)
            {
                foreach (KeyValuePair<string, FundMaster> kvp in fundMasterDict)
                {
                    string ticker = kvp.Key;

                    try
                    {
                        FundMaster fundMaster = kvp.Value;
                        if (fundMaster.CalcPDStats == 1)
                        {
                            if (!fundPDStatsDict.TryGetValue(ticker, out FundPDStats fundPDStats))
                            {
                                fundPDStats = new FundPDStats();
                                fundPDStats.Ticker = ticker;
                                fundPDStatsDict.Add(ticker, fundPDStats);
                            }

                            if (fundForecastDict.TryGetValue(ticker, out FundForecast fundForecast))
                            {
                                if (fundForecast != null && fundDiscountDict.TryGetValue(ticker, out IList<FundPDHistory> list))
                                {
                                    Process(ticker, fundForecast, list, fundPDStats, period1M, period3M, period6M,
                                        period1Y, period2Y, period3Y, period5Y, period10Y, period25Y);
                                }
                            }
                        }

                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error calculating PD stats for ticker: " + ticker);
                    }
                }
            }
            _logger.LogInformation("Saved PD Stats Funds - DONE");
        }

        private void Process(string ticker
            , FundForecast fundForecast, IList<FundPDHistory> fundHistoryList
            , FundPDStats fundPDStats
            , IList<double> period1M, IList<double> period3M, IList<double> period6M
            , IList<double> period1Y, IList<double> period2Y, IList<double> period3Y, IList<double> period5Y, IList<double> period10Y, IList<double> period25Y
            )
        {
            try
            {
                period1M.Clear();
                period3M.Clear();
                period6M.Clear();
                period1Y.Clear();
                period2Y.Clear();
                period3Y.Clear();
                period5Y.Clear();
                period10Y.Clear();
                period25Y.Clear();

                fundPDStats.LastNavDt = fundForecast.LastNavDt;
                fundPDStats.PubNav = fundForecast.LastDvdAdjNav;
                fundPDStats.PubPD = fundForecast.LastPD;
                fundPDStats.EstNav = fundForecast.EstNav;
                fundPDStats.EstPD = fundForecast.PDLastPrc;

                double estPD = fundForecast.PDLastPrc.GetValueOrDefault();
                foreach (FundPDHistory fundHistory in fundHistoryList)
                {
                    if (fundHistory.Ind1M == 1)
                        period1M.Add(fundHistory.PD.GetValueOrDefault());
                    if (fundHistory.Ind3M == 1)
                        period3M.Add(fundHistory.PD.GetValueOrDefault());
                    if (fundHistory.Ind6M == 1)
                        period6M.Add(fundHistory.PD.GetValueOrDefault());
                    if (fundHistory.Ind1Y == 1)
                        period1Y.Add(fundHistory.PD.GetValueOrDefault());
                    if (fundHistory.Ind2Y == 1)
                        period2Y.Add(fundHistory.PD.GetValueOrDefault());
                    if (fundHistory.Ind3Y == 1)
                        period3Y.Add(fundHistory.PD.GetValueOrDefault());
                    if (fundHistory.Ind5Y == 1)
                        period5Y.Add(fundHistory.PD.GetValueOrDefault());
                    if (fundHistory.Ind10Y == 1)
                        period10Y.Add(fundHistory.PD.GetValueOrDefault());
                    if (fundHistory.Ind25Y == 1)
                        period25Y.Add(fundHistory.PD.GetValueOrDefault());
                }

                //1M
                double?[] result = CalculateStats(ticker, period1M, estPD, "1M");
                fundPDStats.Avg1M = result[0]; fundPDStats.PRnk1M = result[1];

                //3M
                result = CalculateStats(ticker, period3M, estPD, "3M");
                fundPDStats.Avg3M = result[0]; fundPDStats.PRnk3M = result[1];

                //6M
                result = CalculateStats(ticker, period6M, estPD, "6M");
                fundPDStats.Avg6M = result[0]; fundPDStats.PRnk6M = result[1];

                //1Y
                result = CalculateStats(ticker, period1Y, estPD, "1Y");
                fundPDStats.Avg1Y = result[0]; fundPDStats.PRnk1Y = result[1];

                //2Y
                result = CalculateStats(ticker, period2Y, estPD, "2Y");
                fundPDStats.Avg2Y = result[0]; fundPDStats.PRnk2Y = result[1];

                //3Y
                result = CalculateStats(ticker, period3Y, estPD, "3Y");
                fundPDStats.Avg3Y = result[0]; fundPDStats.PRnk3Y = result[1];

                //5Y
                result = CalculateStats(ticker, period5Y, estPD, "5Y");
                fundPDStats.Avg5Y = result[0]; fundPDStats.PRnk5Y = result[1];

                //10Y
                result = CalculateStats(ticker, period10Y, estPD, "10Y");
                fundPDStats.Avg10Y = result[0]; fundPDStats.PRnk10Y = result[1];

                //25Y
                result = CalculateStats(ticker, period25Y, estPD, "25Y");
                fundPDStats.Avg25Y = result[0]; fundPDStats.PRnk25Y = result[1];
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error calculating PD stats for ticker: " + ticker);
            }
            finally
            {
                try
                {
                    period1M.Clear();
                    period3M.Clear();
                    period6M.Clear();
                    period1Y.Clear();
                    period2Y.Clear();
                    period3Y.Clear();
                    period5Y.Clear();
                    period10Y.Clear();
                    period25Y.Clear();
                }
                catch (Exception)
                {
                }
            }
        }

        private double?[] CalculateStats(string ticker, IList<double> list, double estPD, string period)
        {
            try
            {
                double[] pd = list.ToArray<double>();
                double avgPD = Statistics.Mean(pd);
                double percentile = Statistics.QuantileRank(pd, estPD);
                double?[] result = { avgPD, percentile };
                return result;
            }
            catch (Exception)
            {
                //_logger.LogError(ex, "Error calculating stats for ticker/period: " + ticker + "/" + period);
                return new double?[] { null, null };
            }
        }
    }
}
