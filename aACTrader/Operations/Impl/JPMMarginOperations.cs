using aCommons;
using aCommons.Cef;
using aCommons.Utils;
using LazyCache;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;

namespace aACTrader.Operations.Impl
{
    public class JPMMarginOperations
    {
        private readonly ILogger<JPMMarginOperations> _logger;
        private readonly CachingService _cache;
        private readonly IConfiguration _configuration;

        private IList<Range<double>> EquityOwnershipRange;
        private IList<Range<double>> LiquidityRateRange;
        private IRangeComparer<Range<double>, double> DoubleRangeComparer;

        public JPMMarginOperations(ILogger<JPMMarginOperations> logger
           , CachingService cache
           , IConfiguration configuration)
        {
            _logger = logger;
            _cache = cache;
            _configuration = configuration;
            Initialize();
        }

        public void Initialize()
        {
            EquityOwnershipRange = new Range<double>[]
            {
                new Range<double>(0, 5.0),
                new Range<double>(5.01, 7.5),
                new Range<double>(7.51, 10.0),
                new Range<double>(10.01, double.MaxValue),
            };

            LiquidityRateRange = new Range<double>[]
            {
                new Range<double>(0, 0.25),
                new Range<double>(0.251, 0.50),
                new Range<double>(0.51, 1.00),
                new Range<double>(1.01, 1.50),
                new Range<double>(1.51, 2.00),
                new Range<double>(2.01, 3.00),
                new Range<double>(3.01, 4.00),
                new Range<double>(4.01, 5.00),
                new Range<double>(5.01, 10.00),
                new Range<double>(10.01, double.MaxValue),
            };

            DoubleRangeComparer = new RangeComparer<double>();
        }

        public IList<SecurityMarginDetailTO> GenerateMarginReport(string fundName)
        {
            int rowId = 1;
            IList<SecurityMarginDetailTO> list = new List<SecurityMarginDetailTO>();
            IDictionary<string, PositionMaster> positionDict = _cache.Get<IDictionary<string, PositionMaster>>(CacheKeys.POSITION_MASTER);
            IDictionary<string, JPMSecurityMarginDetail> securityMarginDict = _cache.Get<IDictionary<string, JPMSecurityMarginDetail>>(CacheKeys.LATEST_JPM_SECURITY_MARGIN_DETAILS);
            foreach (KeyValuePair<string, PositionMaster> kvp in positionDict)
            {
                PositionMaster position = kvp.Value;
                FundSummaryDetail fundSummaryDetail = null;

                if ("OPP".Equals(fundName, StringComparison.CurrentCultureIgnoreCase))
                {
                    if (position.FundOpp != null && position.FundOpp.BkrJPM != null && position.FundOpp.BkrJPM.PosHeld.HasValue)
                        fundSummaryDetail = position.FundOpp;
                }
                else if ("TAC".Equals(fundName, StringComparison.CurrentCultureIgnoreCase))
                {
                    if (position.FundTac != null && position.FundTac.BkrJPM != null && position.FundTac.BkrJPM.PosHeld.HasValue)
                        fundSummaryDetail = position.FundTac;
                }

                if (fundSummaryDetail != null)
                {
                    //if (position.FundOpp.BkrJPM != null && position.FundOpp.BkrJPM.PosHeld.HasValue)
                    //{
                    SecurityMarginDetailTO totalRow = CreateTotalRow(fundName, position, securityMarginDict, rowId++);
                    SecurityMarginDetailTO baselineRow = CalculateBaselineRate(totalRow, securityMarginDict, rowId++);
                    SecurityMarginDetailTO addonLiquidityRow = CalculateLiquidityAddOn(totalRow, securityMarginDict, rowId++);
                    SecurityMarginDetailTO addonEquityOwnershipRow = CalculateEquityOwnershipAddOn(totalRow, securityMarginDict, rowId++);
                    SecurityMarginDetailTO maxRequirementRow = CalculateAdjustmentForMaxRequirement(totalRow, securityMarginDict, rowId++);
                    SecurityMarginDetailTO industryConcentrationEqRow = CalculateEquityIndustryConcentrationAddOn(totalRow, securityMarginDict, rowId++);
                    SecurityMarginDetailTO industryConcentrationFIRow = CalculateFIIndustryConcentrationAddOn(totalRow, securityMarginDict, rowId++);
                    list.Add(totalRow);
                    list.Add(baselineRow);
                    list.Add(addonLiquidityRow);
                    list.Add(addonEquityOwnershipRow);
                    list.Add(maxRequirementRow);
                    list.Add(industryConcentrationEqRow);
                    list.Add(industryConcentrationFIRow);
                    //}
                }
            }

            return list.OrderBy(xl => xl.RowId).ToList<SecurityMarginDetailTO>();
        }

        private SecurityMarginDetailTO CreateTotalRow(string fundName
            , PositionMaster position
            , IDictionary<string, JPMSecurityMarginDetail> securityMarginDict
            , int rowId)
        {
            string marginRule = "Total";
            SecurityMarginDetailTO data = new SecurityMarginDetailTO();
            data.FundName = fundName;
            data.ALMTicker = position.SecTicker;
            data.Currency = position.Curr;

            data.Position = position.FundOpp.BkrJPM.PosHeld;
            data.Price = position.Price;
            data.MV = position.FundOpp.BkrJPM.ClsMV * 1000000;
            data.Avg20DayVol = position.Avg20DayVol;
            data.TotalMV += Math.Abs(data.MV.GetValueOrDefault());
            data.ShOut = position.ShOut;

            data.MarginRule = marginRule;
            data.RowId = rowId;

            string id = data.FundName + "|" + data.ALMTicker + "|" + "Risk Based";
            if (securityMarginDict.TryGetValue(id, out JPMSecurityMarginDetail securityMarginDetail))
            {
                data.JPMTicker = securityMarginDetail.Ticker;
                data.Sedol = securityMarginDetail.Sedol;
                data.Cusip = securityMarginDetail.Cusip;
                data.SecType = securityMarginDetail.SecType;
                data.SecDesc = securityMarginDetail.SecDesc;
                data.Industry = securityMarginDetail.Industry;
                data.Country = securityMarginDetail.Country;
            }

            id = data.FundName + "|" + data.ALMTicker + "|" + marginRule;
            if (securityMarginDict.TryGetValue(id, out securityMarginDetail))
            {
                data.MarginAmt = securityMarginDetail.TotalMarginRequirement;
                //data.TotalMarginAmt = securityMarginDetail.TotalMarginRequirement;
                //if (data.TotalMarginAmt.HasValue)
                //    data.TotalMarginRt = data.MV / data.TotalMarginAmt;
            }

            return data;
        }

        private SecurityMarginDetailTO CalculateBaselineRate(SecurityMarginDetailTO totalRow, IDictionary<string, JPMSecurityMarginDetail> securityMarginDict, int rowId)
        {
            try
            {
                string marginRule = "Baseline - Equity";
                SecurityMarginDetailTO data = new SecurityMarginDetailTO();
                data.Id = totalRow.ALMTicker;
                data.FundName = totalRow.FundName;
                data.ALMTicker = totalRow.ALMTicker;
                data.MarginRule = marginRule;
                data.RowId = rowId;

                string id = data.FundName + "|" + data.ALMTicker + "|" + "Risk Based";
                if (securityMarginDict.TryGetValue(id, out JPMSecurityMarginDetail securityMarginDetail))
                {
                    string measure = securityMarginDetail.Measure1;
                    if (!string.IsNullOrEmpty(measure))
                    {
                        string[] measures = measure.Split("/");
                        if (measures != null && measures.Length > 1)
                        {
                            //calculated
                            double rate = Math.Abs(Convert.ToDouble(measures[0]));
                            data.AppliedReqRt = rate / 100.0;
                            data.MarginAmtCalc = data.AppliedReqRt * totalRow.MV;
                            totalRow.MarginAmtCalc += data.MarginAmtCalc.GetValueOrDefault();
                            totalRow.BaselineAmt = data.MarginAmtCalc;

                            //reported
                            data.Measure1 = rate;
                            data.Measure2 = DataConversionUtils.ConvertToDouble(securityMarginDetail.Measure2);
                            data.Measure3 = DataConversionUtils.ConvertToDouble(securityMarginDetail.Measure3);

                            data.AppliedReqRt = DataConversionUtils.ConvertToDouble(securityMarginDetail.AppliedReq);

                            data.AppliedReqDesc = securityMarginDetail.AppliedReqDesc;
                            data.Measure1Desc = securityMarginDetail.Measure1Desc;
                            data.Measure2Desc = securityMarginDetail.Measure2Desc;
                            data.Measure3Desc = securityMarginDetail.Measure3Desc;
                        }
                    }
                }

                id = data.FundName + "|" + data.ALMTicker + "|" + marginRule;
                if (securityMarginDict.TryGetValue(id, out securityMarginDetail))
                {
                    data.MarginAmt = securityMarginDetail.MarginRequirement;
                }

                return data;
            }
            catch (Exception ex)
            {
                _logger.LogError("Error calculating Baseline Rate: " + totalRow.ALMTicker, ex);
            }

            return null;
        }

        private SecurityMarginDetailTO CalculateLiquidityAddOn(SecurityMarginDetailTO totalRow
            , IDictionary<string, JPMSecurityMarginDetail> securityMarginDict, int rowId)
        {
            try
            {
                string marginRule = "Add-On - Liquidity";
                SecurityMarginDetailTO data = new SecurityMarginDetailTO();
                data.Id = totalRow.ALMTicker;
                data.FundName = totalRow.FundName;
                data.ALMTicker = totalRow.ALMTicker;
                data.MarginRule = marginRule;
                data.RowId = rowId;

                //calculated
                if (totalRow.Avg20DayVol.HasValue)
                {
                    data.Measure1Calc = totalRow.Position / totalRow.Avg20DayVol;
                    int range = BinarySearch(LiquidityRateRange, data.Measure1Calc.GetValueOrDefault(), DoubleRangeComparer);
                    //_logger.LogInformation("Range: " + range);
                    double addOn = 0;
                    string addReq = null;
                    if (range == 0)
                        addOn = 0;
                    else if (range == 1)
                    {
                        addOn = 0.25;
                        data.MarginAmtCalc = totalRow.BaselineAmt * addOn;
                    }
                    else if (range == 2)
                    {
                        addOn = 0.50;
                        data.MarginAmtCalc = totalRow.BaselineAmt * addOn;
                    }
                    else if (range == 3)
                    {
                        addOn = 1.00;
                        data.MarginAmtCalc = totalRow.BaselineAmt * addOn;
                    }
                    else if (range == 4)
                    {
                        addOn = 1.50;
                        data.MarginAmtCalc = totalRow.BaselineAmt * addOn;
                    }
                    else if (range == 5)
                    {
                        addOn = 2.00;
                        data.MarginAmtCalc = totalRow.BaselineAmt * addOn;
                    }
                    else if (range == 6)
                    {
                        addOn = 2.50;
                        data.MarginAmtCalc = totalRow.BaselineAmt * addOn;
                    }
                    else if (range == 7)
                    {
                        addOn = 3.00;
                        data.MarginAmtCalc = totalRow.BaselineAmt * addOn;
                    }
                    else if (range == 8)
                    {
                        addOn = 5.00;
                        data.MarginAmtCalc = totalRow.BaselineAmt * addOn;
                    }
                    else if (range == 3)
                    {
                        addReq = "Max Stress";
                        data.MarginAmtCalc = totalRow.MV;
                    }

                    totalRow.MarginAmtCalc += data.MarginAmtCalc.GetValueOrDefault();
                    data.AppliedReqRt = addOn;
                    data.AppliedReqDesc = addReq;
                }

                //reported
                string id = data.FundName + "|" + data.ALMTicker + "|" + marginRule;
                if (securityMarginDict.TryGetValue(id, out JPMSecurityMarginDetail securityMarginDetail))
                {
                    data.MarginAmt = securityMarginDetail.MarginRequirement;

                    data.Measure1 = DataConversionUtils.ConvertToDouble(securityMarginDetail.Measure1);
                    data.Measure2 = DataConversionUtils.ConvertToDouble(securityMarginDetail.Measure2);
                    data.Measure3 = DataConversionUtils.ConvertToDouble(securityMarginDetail.Measure3);

                    data.AppliedReqRt = DataConversionUtils.ConvertToDouble(securityMarginDetail.AppliedReq);

                    data.AppliedReqDesc = securityMarginDetail.AppliedReqDesc;
                    data.Measure1Desc = securityMarginDetail.Measure1Desc;
                    data.Measure2Desc = securityMarginDetail.Measure2Desc;
                    data.Measure3Desc = securityMarginDetail.Measure3Desc;
                }

                return data;
            }
            catch (Exception ex)
            {
                _logger.LogError("Error calculating Liquidity Add-On: " + totalRow.ALMTicker, ex);
            }

            return null;
        }

        private SecurityMarginDetailTO CalculateEquityOwnershipAddOn(SecurityMarginDetailTO totalRow
            , IDictionary<string, JPMSecurityMarginDetail> securityMarginDict, int rowId)
        {
            try
            {
                string marginRule = "Add-On - Equity Ownership";
                SecurityMarginDetailTO data = new SecurityMarginDetailTO();
                data.Id = totalRow.ALMTicker;
                data.FundName = totalRow.FundName;
                data.ALMTicker = totalRow.ALMTicker;
                data.MarginRule = marginRule;
                data.RowId = rowId;

                //calculated
                if (totalRow.ShOut.HasValue)
                {
                    data.Measure1Calc = (totalRow.Position / totalRow.ShOut) * 100;
                    int range = BinarySearch(EquityOwnershipRange, data.Measure1Calc.GetValueOrDefault(), DoubleRangeComparer);
                    _logger.LogInformation("Range: " + range);
                    double addOn = 0;
                    string addReq = null;
                    if (range == 0)
                        addOn = 0;
                    else if (range == 1)
                    {
                        addOn = 0.50;
                        data.MarginAmtCalc = totalRow.BaselineAmt * addOn;
                    }
                    else if (range == 2)
                    {
                        addOn = 1.50;
                        data.MarginAmtCalc = totalRow.BaselineAmt * addOn;
                    }
                    else if (range == 3)
                    {
                        addReq = "Max Stress";
                        data.MarginAmtCalc = totalRow.MV;
                    }

                    totalRow.MarginAmtCalc += data.MarginAmtCalc.GetValueOrDefault();
                    data.AppliedReqRt = addOn;
                    data.AppliedReqDesc = addReq;
                }

                //reported
                string id = data.FundName + "|" + data.ALMTicker + "|" + marginRule;
                if (securityMarginDict.TryGetValue(id, out JPMSecurityMarginDetail securityMarginDetail))
                {
                    data.MarginAmt = securityMarginDetail.MarginRequirement;

                    data.Measure1 = DataConversionUtils.ConvertToDouble(securityMarginDetail.Measure1);
                    data.Measure2 = DataConversionUtils.ConvertToDouble(securityMarginDetail.Measure2);
                    data.Measure3 = DataConversionUtils.ConvertToDouble(securityMarginDetail.Measure3);

                    data.AppliedReqRt = DataConversionUtils.ConvertToDouble(securityMarginDetail.AppliedReq);

                    data.AppliedReqDesc = securityMarginDetail.AppliedReqDesc;
                    data.Measure1Desc = securityMarginDetail.Measure1Desc;
                    data.Measure2Desc = securityMarginDetail.Measure2Desc;
                    data.Measure3Desc = securityMarginDetail.Measure3Desc;
                }

                return data;
            }
            catch (Exception ex)
            {
                _logger.LogError("Error calculating Equity Ownership Add-On: " + totalRow.ALMTicker, ex);
            }

            return null;
        }

        private SecurityMarginDetailTO CalculateAdjustmentForMaxRequirement(SecurityMarginDetailTO totalRow
            , IDictionary<string, JPMSecurityMarginDetail> securityMarginDict, int rowId)
        {
            try
            {
                string marginRule = "Adjustment for Max Rqmt (Eq)";
                SecurityMarginDetailTO data = new SecurityMarginDetailTO();
                data.Id = totalRow.ALMTicker;
                data.FundName = totalRow.FundName;
                data.ALMTicker = totalRow.ALMTicker;
                data.MarginRule = marginRule;
                data.RowId = rowId;

                //reported
                string id = data.FundName + "|" + data.ALMTicker + "|" + marginRule;
                if (securityMarginDict.TryGetValue(id, out JPMSecurityMarginDetail securityMarginDetail))
                {
                    data.MarginAmt = securityMarginDetail.MarginRequirement;

                    data.Measure1 = DataConversionUtils.ConvertToDouble(securityMarginDetail.Measure1);
                    data.Measure2 = DataConversionUtils.ConvertToDouble(securityMarginDetail.Measure2);
                    data.Measure3 = DataConversionUtils.ConvertToDouble(securityMarginDetail.Measure3);

                    data.AppliedReqRt = DataConversionUtils.ConvertToDouble(securityMarginDetail.AppliedReq);

                    data.AppliedReqDesc = securityMarginDetail.AppliedReqDesc;
                    data.Measure1Desc = securityMarginDetail.Measure1Desc;
                    data.Measure2Desc = securityMarginDetail.Measure2Desc;
                    data.Measure3Desc = securityMarginDetail.Measure3Desc;
                }

                return data;
            }
            catch (Exception ex)
            {
                _logger.LogError("Error calculating Liquidity Add-On: " + totalRow.ALMTicker, ex);
            }

            return null;
        }

        private SecurityMarginDetailTO CalculateEquityIndustryConcentrationAddOn(SecurityMarginDetailTO totalRow
            , IDictionary<string, JPMSecurityMarginDetail> securityMarginDict, int rowId)
        {
            try
            {
                string marginRule = "Add-On - Net Industry Concentration (Eq)";
                SecurityMarginDetailTO data = new SecurityMarginDetailTO();
                data.Id = totalRow.ALMTicker;
                data.FundName = totalRow.FundName;
                data.ALMTicker = totalRow.ALMTicker;
                data.MarginRule = marginRule;
                data.RowId = rowId;

                //reported
                string id = data.FundName + "|" + data.ALMTicker + "|" + marginRule;
                if (securityMarginDict.TryGetValue(id, out JPMSecurityMarginDetail securityMarginDetail))
                {
                    data.MarginAmt = securityMarginDetail.MarginRequirement;

                    data.Measure1 = DataConversionUtils.ConvertToDouble(securityMarginDetail.Measure1);
                    data.Measure2 = DataConversionUtils.ConvertToDouble(securityMarginDetail.Measure2);
                    data.Measure3 = DataConversionUtils.ConvertToDouble(securityMarginDetail.Measure3);

                    data.AppliedReqRt = DataConversionUtils.ConvertToDouble(securityMarginDetail.AppliedReq);

                    data.AppliedReqDesc = securityMarginDetail.AppliedReqDesc;
                    data.Measure1Desc = securityMarginDetail.Measure1Desc;
                    data.Measure2Desc = securityMarginDetail.Measure2Desc;
                    data.Measure3Desc = securityMarginDetail.Measure3Desc;
                }

                return data;
            }
            catch (Exception ex)
            {
                _logger.LogError("Error calculating Equity Industry Concentration: " + totalRow.ALMTicker, ex);
            }

            return null;
        }

        private SecurityMarginDetailTO CalculateFIIndustryConcentrationAddOn(SecurityMarginDetailTO totalRow
            , IDictionary<string, JPMSecurityMarginDetail> securityMarginDict, int rowId)
        {
            try
            {
                string marginRule = "Add-On - Net Industry Concentration (FI)";
                SecurityMarginDetailTO data = new SecurityMarginDetailTO();
                data.Id = totalRow.ALMTicker;
                data.FundName = totalRow.FundName;
                data.ALMTicker = totalRow.ALMTicker;
                data.MarginRule = marginRule;
                data.RowId = rowId;

                //reported
                string id = data.FundName + "|" + data.ALMTicker + "|" + marginRule;
                if (securityMarginDict.TryGetValue(id, out JPMSecurityMarginDetail securityMarginDetail))
                {
                    data.MarginAmt = securityMarginDetail.MarginRequirement;

                    data.Measure1 = DataConversionUtils.ConvertToDouble(securityMarginDetail.Measure1);
                    data.Measure2 = DataConversionUtils.ConvertToDouble(securityMarginDetail.Measure2);
                    data.Measure3 = DataConversionUtils.ConvertToDouble(securityMarginDetail.Measure3);

                    data.AppliedReqRt = DataConversionUtils.ConvertToDouble(securityMarginDetail.AppliedReq);

                    data.AppliedReqDesc = securityMarginDetail.AppliedReqDesc;
                    data.Measure1Desc = securityMarginDetail.Measure1Desc;
                    data.Measure2Desc = securityMarginDetail.Measure2Desc;
                    data.Measure3Desc = securityMarginDetail.Measure3Desc;
                }

                return data;
            }
            catch (Exception ex)
            {
                _logger.LogError("Error calculating FI Industry Concentration: " + totalRow.ALMTicker, ex);
            }

            return null;
        }

        /// <summary>
        /// See contract for Array.BinarySearch
        /// </summary>
        public int BinarySearch<TRange, TValue>(IList<TRange> ranges,
                                                       TValue value,
                                                       IRangeComparer<TRange, TValue> comparer)
        {
            int min = 0;
            int max = ranges.Count - 1;

            while (min <= max)
            {
                int mid = (min + max) / 2;
                int comparison = comparer.Compare(ranges[mid], value);
                if (comparison == 0)
                {
                    return mid;
                }
                if (comparison < 0)
                {
                    min = mid + 1;
                }
                else if (comparison > 0)
                {
                    max = mid - 1;
                }
            }
            return ~min;
        }
    }
}
