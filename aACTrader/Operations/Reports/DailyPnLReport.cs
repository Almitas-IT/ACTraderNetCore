using aACTrader.DAO.Repository;
using aACTrader.Operations.Impl;
using aCommons;
using aCommons.DTO;
using aCommons.Utils;
using LazyCache;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;

namespace aACTrader.Operations.Reports
{
    /// <summary>
    /// This report calculates the intra-day PnL
    /// </summary>
    public class DailyPnLReport
    {
        private readonly ILogger<DailyPnLReport> _logger;
        private readonly CachingService _cache;
        private readonly HoldingsDao _holdingsDao;
        private readonly IConfiguration _configuration;
        private int _updateHoldingsCount;

        public DailyPnLReport(ILogger<DailyPnLReport> logger
            , CachingService cache
            , HoldingsDao holdingsDao
            , IConfiguration configuration)
        {
            _logger = logger;
            _cache = cache;
            _holdingsDao = holdingsDao;
            _configuration = configuration;
            _updateHoldingsCount = 0;
        }

        public void UpdateHoldings()
        {
            if (_updateHoldingsCount == 0)
            {
                _logger.LogInformation("Update Holdings Daily PnL - STARTED");
                IList<AlmHoldingPerfTO> holdings = _holdingsDao.GetAlmPositionsByDate(DateTime.Today);
                _cache.Remove(CacheKeys.DAILY_PNL_HOLDINGS);
                _cache.Add(CacheKeys.DAILY_PNL_HOLDINGS, holdings, DateTimeOffset.MaxValue);
                _logger.LogInformation("Update Holdings Daily PnL - DONE");
            }
            _updateHoldingsCount++;
            if (_updateHoldingsCount > 20)
                _updateHoldingsCount = 0;
        }

        public void CalculateDailyPnL()
        {
            try
            {
                //_logger.LogInformation("Calculating Daily PnL - STARTED");
                DailyPnLTO dailyPnLTO = new DailyPnLTO();

                //calculate daily pnl
                UpdateHoldings();
                IDictionary<string, AlmHoldingPerfTO> dict = GetDailyPnL(DateTime.Today, "Sector");
                if (dict != null && dict.Count > 0)
                {
                    IList<AlmHoldingPerfTO> list = dict.Values.ToList<AlmHoldingPerfTO>();
                    dailyPnLTO.Positions = list;

                    //pnl summary
                    PopulateGroup(dailyPnLTO, dict["TOTAL"]);
                    PopulateGroup(dailyPnLTO, dict["OPP"]);
                    PopulateGroup(dailyPnLTO, dict["TAC"]);
                    PopulateGroup(dailyPnLTO, dict["USD"]);
                    PopulateGroup(dailyPnLTO, dict["GBP"]);
                    PopulateGroup(dailyPnLTO, dict["CAD"]);
                    PopulateGroup(dailyPnLTO, dict["AUD"]);
                    PopulateGroup(dailyPnLTO, dict["EUR"]);

                    //top long positions
                    IList<AlmHoldingPerfTO> oppLongPositions = list.Where(p => p.FundOppMVFlag == 1).OrderByDescending(p => p.FundOppMV).Take(10).ToList<AlmHoldingPerfTO>();
                    IList<AlmHoldingPerfTO> tacLongPositions = list.Where(p => p.FundTacMVFlag == 1).OrderByDescending(p => p.FundTacMV).Take(10).ToList<AlmHoldingPerfTO>();
                    for (int i = 0; i < oppLongPositions.Count; i++)
                    {
                        PositionRankTO positionRankTO = new PositionRankTO();
                        positionRankTO.Rank = i + 1;
                        positionRankTO.OppTicker = oppLongPositions[i].Ticker;
                        positionRankTO.OppMV = oppLongPositions[i].FundOppMV;
                        positionRankTO.OppWt = oppLongPositions[i].FundOppClsWt;
                        positionRankTO.TacTicker = tacLongPositions[i].Ticker;
                        positionRankTO.TacMV = tacLongPositions[i].FundTacMV;
                        positionRankTO.TacWt = tacLongPositions[i].FundTacClsWt;
                        dailyPnLTO.LongPositions.Add(positionRankTO);
                    }

                    //top short positions
                    IList<AlmHoldingPerfTO> oppShortPositions = list.Where(p => p.FundOppMVFlag == 1).OrderBy(p => p.FundOppMV).Take(10).ToList<AlmHoldingPerfTO>();
                    IList<AlmHoldingPerfTO> tacShortPositions = list.Where(p => p.FundTacMVFlag == 1).OrderBy(p => p.FundTacMV).Take(10).ToList<AlmHoldingPerfTO>();
                    for (int i = 0; i < oppShortPositions.Count; i++)
                    {
                        PositionRankTO positionRankTO = new PositionRankTO();
                        positionRankTO.Rank = i + 1;
                        positionRankTO.OppTicker = oppShortPositions[i].Ticker;
                        positionRankTO.OppMV = oppShortPositions[i].FundOppMV;
                        positionRankTO.OppWt = oppShortPositions[i].FundOppClsWt;
                        positionRankTO.TacTicker = tacShortPositions[i].Ticker;
                        positionRankTO.TacMV = tacShortPositions[i].FundTacMV;
                        positionRankTO.TacWt = tacShortPositions[i].FundTacClsWt;
                        dailyPnLTO.ShortPositions.Add(positionRankTO);
                    }

                    //top up & down price positions
                    IList<AlmHoldingPerfTO> priceUpMoves = list.Where(p => p.PrcFlag == 1).OrderByDescending(p => p.PrcRtnLcl).Take(10).ToList<AlmHoldingPerfTO>();
                    IList<AlmHoldingPerfTO> priceDownMoves = list.Where(p => p.PrcFlag == 1).OrderBy(p => p.PrcRtnLcl).Take(10).ToList<AlmHoldingPerfTO>();
                    for (int i = 0; i < priceUpMoves.Count; i++)
                    {
                        PositionPerfTO positionPerfTO = new PositionPerfTO();
                        positionPerfTO.Rank = i + 1;
                        positionPerfTO.UpTicker = priceUpMoves[i].Ticker;
                        positionPerfTO.UpMove = priceUpMoves[i].PrcRtnLcl;
                        positionPerfTO.UpMoveRptd = priceUpMoves[i].PrcRtnLclRptd;
                        positionPerfTO.UpMoveUSD = priceUpMoves[i].PrcRtn;
                        positionPerfTO.DownTicker = priceDownMoves[i].Ticker;
                        positionPerfTO.DownMove = priceDownMoves[i].PrcRtnLcl;
                        positionPerfTO.DownMoveRptd = priceDownMoves[i].PrcRtnLclRptd;
                        positionPerfTO.DownMoveUSD = priceDownMoves[i].PrcRtn;
                        dailyPnLTO.PricePerf.Add(positionPerfTO);
                    }

                    //top up & down MV changes
                    IList<AlmHoldingPerfTO> mvChangeUpMoves = list.Where(p => p.MVChngFlag == 1).OrderByDescending(p => p.MVChng).Take(10).ToList<AlmHoldingPerfTO>();
                    IList<AlmHoldingPerfTO> mvChangeDownMoves = list.Where(p => p.MVChngFlag == 1).OrderBy(p => p.MVChng).Take(10).ToList<AlmHoldingPerfTO>();
                    for (int i = 0; i < mvChangeUpMoves.Count; i++)
                    {
                        PositionPerfTO positionPerfTO = new PositionPerfTO();
                        positionPerfTO.Rank = i + 1;
                        positionPerfTO.UpTicker = mvChangeUpMoves[i].Ticker;
                        positionPerfTO.UpPrevMV = mvChangeUpMoves[i].ClsMV;
                        positionPerfTO.UpMV = mvChangeUpMoves[i].MV;
                        positionPerfTO.UpMove = mvChangeUpMoves[i].MVChng;
                        positionPerfTO.DownTicker = mvChangeDownMoves[i].Ticker;
                        positionPerfTO.DownPrevMV = mvChangeDownMoves[i].ClsMV;
                        positionPerfTO.DownMV = mvChangeDownMoves[i].MV;
                        positionPerfTO.DownMove = mvChangeDownMoves[i].MVChng;
                        dailyPnLTO.MVPerf.Add(positionPerfTO);
                    }

                    _cache.Remove(CacheKeys.DAILY_PNL_SUMMARY);
                    _cache.Add(CacheKeys.DAILY_PNL_SUMMARY, dailyPnLTO, DateTimeOffset.MaxValue);

                    IDictionary<string, BatchMonitorTO> bmDict = _cache.Get<Dictionary<string, BatchMonitorTO>>(CacheKeys.BATCH_MONITOR);
                    bmDict["Daily PnL Update"].LastUpdate = DateTime.Now;

                    // _logger.LogInformation("Calculating Daily PnL - END");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error calculating daily PnL.");
            }
        }

        public IDictionary<string, AlmHoldingPerfTO> GetDailyPnL(DateTime asOfDate, string groupBy)
        {
            IDictionary<string, SecurityMaster> positionSecurityMasterDict = _cache.Get<IDictionary<string, SecurityMaster>>(CacheKeys.POSITION_SECURITY_MASTER_DETAILS);
            IDictionary<string, SecurityPrice> securityPriceDict = _cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);
            IDictionary<string, string> priceTickerMap = _cache.Get<IDictionary<string, string>>(CacheKeys.PRICE_TICKER_MAP);
            IDictionary<string, FXRate> fxRateDict = _cache.Get<IDictionary<string, FXRate>>(CacheKeys.FX_RATES);
            IList<AlmHoldingPerfTO> holdings = _cache.Get<IList<AlmHoldingPerfTO>>(CacheKeys.DAILY_PNL_HOLDINGS);

            IDictionary<string, AlmHoldingPerfTO> dict = new Dictionary<string, AlmHoldingPerfTO>();
            bool groupData = true;
            string groupByField = "Symbol";
            string groupValue = string.Empty;

            if (!string.IsNullOrEmpty(groupBy))
                groupByField = groupBy;

            foreach (AlmHoldingPerfTO data in holdings)
            {
                switch (groupByField)
                {
                    case "Symbol":
                        groupValue = data.Symbol;
                        groupData = false;
                        break;
                    case "Sector":
                        groupValue = data.Sector;
                        break;
                    case "Country":
                        groupValue = data.Cntry;
                        break;
                    case "Currency":
                        groupValue = data.Curr;
                        break;
                    case "AssetType":
                        groupValue = data.AssetType;
                        break;
                    case "SecurityType":
                        groupValue = data.SecType;
                        break;
                    default:
                        groupValue = data.Symbol;
                        break;
                }

                if (string.IsNullOrEmpty(groupValue))
                    groupValue = "Other";

                //broker provided data
                //closing price - (average across brokers and funds)
                //sometimes broker provided prices are different for opportunity and tactical funds and sometimes the price is quoted in different currencies
                //using broker provided data as not all securities are priced using Neovest and BBG and price multiplier to apply for futures is not clear at this point
                data.SortId = 10;
                data.PrcLcl = data.ClsPrcLcl;
                data.Prc = data.ClsPrc;
                data.FX = data.ClsFX;
                data.MV = data.ClsMV;
                data.FundOppMV = data.FundOppClsMV;
                data.FundTacMV = data.FundTacClsMV;
                data.FundOppMVFlag = 0;
                data.FundTacMVFlag = 0;
                data.PrcFlag = 0;
                data.MVChngFlag = 0;
                data.ErrorFlag = 0;
                data.Error = string.Empty;
                data.GrpName = string.Empty;
                data.IsGroup = 0;
                data.ClsPrcLclAlmAct = null;
                data.ClsPrcLclAlm = null;
                data.ClsPrcLclDiff = null;
                data.PrcLclAct = null;
                data.PrcChng = null;
                data.PrcRtn = null;
                data.PrcRtnLcl = null;
                data.PrcRtnLclRptd = null;
                data.FXRtn = null;
                data.MVChng = null;
                data.TrdDt = null;
                data.PrcSrc = string.Empty;
                data.PrcCurr = string.Empty;
                data.PrcCurr1 = string.Empty;
                data.PrcMult = null;
                data.Error = string.Empty;
                data.TrdPos = null;
                data.FundOppPrcChng = null;
                data.FundOppPrcRtn = null;
                data.FundOppMVChng = null;
                data.FundTacPrcChng = null;
                data.FundTacPrcRtn = null;
                data.FundTacMVChng = null;

                //Live FX
                FXRate fxRate = null;
                if (!string.IsNullOrEmpty(data.Curr))
                    fxRateDict.TryGetValue(data.Curr, out fxRate);
                if (fxRate != null)
                {
                    data.FX = fxRate.FXRateLatest;
                    data.FXRtn = fxRate.FXReturn;
                }
                else
                {
                    data.Error = "Missing Live Fx Rate";
                }

                //Live Price
                double priceMultiplier;
                string ticker = data.Ticker;
                string smTicker = data.Ticker;
                SecurityMaster securityMaster;
                positionSecurityMasterDict.TryGetValue(ticker, out securityMaster);
                if (securityMaster == null)
                    positionSecurityMasterDict.TryGetValue(smTicker, out securityMaster);

                SecurityPrice securityPrice = SecurityPriceLookupOperations.GetSecurityPrice(ticker, priceTickerMap, securityPriceDict);
                if (securityPrice == null)
                    securityPrice = SecurityPriceLookupOperations.GetSecurityPrice(smTicker, priceTickerMap, securityPriceDict);
                if (securityPrice != null && (securityPrice.Src.Equals("NV") || securityPrice.Src.Equals("BBG")))
                {
                    priceMultiplier = 1.0;
                    data.PrcCurr = securityPrice.Curr;
                    data.PrcCurr1 = securityPrice.Curr;
                    data.ClsPrcLclAlmAct = securityPrice.ClsPrc;
                    data.PrcLclAct = securityPrice.LastPrc;
                    data.PrcRtnLcl = securityPrice.PrcRtn;
                    data.PrcRtnLclRptd = securityPrice.PrcChng;
                    data.PrcSrc = securityPrice.Src;
                    data.TrdDt = securityPrice.TrdDt;

                    //ignore securities where there is a data issue (data provided by the broker is not correct)
                    if (data.DataIssue == 0)
                    {
                        if (!string.IsNullOrEmpty(data.PrcCurr))
                        {
                            //security currency is GBP and price currency is GBp/GBX
                            if ("GBP".Equals(data.Curr) && ("GBp".Equals(data.PrcCurr) || "GBX".Equals(data.PrcCurr)))
                            {
                                priceMultiplier = 0.01;
                                data.PrcCurr1 = data.Curr;
                            }
                            //security currency is GBp and price currency is GBP
                            else if ("GBp".Equals(data.Curr) && "GBP".Equals(data.PrcCurr))
                            {
                                priceMultiplier = 100.0;
                                data.PrcCurr1 = data.Curr;
                            }
                            //security currency is USD and price currency is GBp/GBX
                            else if ("USD".Equals(data.Curr) && ("GBp".Equals(data.PrcCurr) || "GBX".Equals(data.PrcCurr)))
                            {
                                priceMultiplier = 0.01;
                                if (fxRateDict.TryGetValue("GBP", out FXRate fxRateFund))
                                    priceMultiplier *= fxRateFund.FXRateLatest.GetValueOrDefault();
                                data.PrcCurr1 = data.Curr;
                            }
                            else if ("USD".Equals(data.Curr) && !"USD".Equals(data.PrcCurr))
                            {
                                if (fxRateDict.TryGetValue(data.PrcCurr, out FXRate fxRateFund))
                                    priceMultiplier = fxRateFund.FXRateLatest.GetValueOrDefault();
                                data.PrcCurr1 = data.Curr;
                            }
                            else if (!"USD".Equals(data.Curr) && "USD".Equals(data.PrcCurr))
                            {
                                if (fxRateDict.TryGetValue(data.Curr, out FXRate fxRateFund))
                                    priceMultiplier = 1 / fxRateFund.FXRateLatest.GetValueOrDefault();
                                data.PrcCurr1 = data.Curr;
                            }

                            if (!data.PrcCurr1.Equals(data.Curr))
                                data.Error += "; Security Currency <> Price Currency";
                        }

                        data.PrcMult = priceMultiplier;
                        data.ClsPrcLclAlm = data.ClsPrcLclAlmAct * priceMultiplier;
                        if (data.ClsPrcLclAlmAct.HasValue)
                            data.ClsPrcLclDiff = CommonUtils.SubtractNullableDoubles(data.ClsPrcLclAlm, data.ClsPrcLcl);
                        data.PrcLcl = data.PrcLclAct * priceMultiplier;

                        //calculate price change (difference between live price and closing price provided by broker)
                        //this is average closing price across all funds
                        data.Prc = data.PrcLcl;
                        if (data.FX.HasValue)
                            data.Prc *= data.FX.GetValueOrDefault();
                        data.PrcChng = CommonUtils.SubtractNullableDoubles(data.Prc, data.ClsPrc);
                        data.PrcRtn = CommonUtils.Rtn(data.Prc, data.ClsPrc);

                        //OPP
                        //calculate price change for OPP fund (sometimes price provided by broker for each fund is different)
                        data.FundOppPrcChng = CommonUtils.SubtractNullableDoubles(data.Prc, data.FundOppClsPrc);
                        data.FundOppPrcRtn = CommonUtils.Rtn(data.Prc, data.FundOppClsPrc);
                        data.FundOppMV *= (1 + data.PrcRtn.GetValueOrDefault());
                        data.FundOppMVChng = CommonUtils.SubtractNullableDoubles(data.FundOppMV, data.FundOppClsMV);

                        //TAC
                        //calculate price change for TAC fund (sometimes price provided by broker for each fund is different)
                        data.FundTacPrcChng = CommonUtils.SubtractNullableDoubles(data.Prc, data.FundTacClsPrc);
                        data.FundTacPrcRtn = CommonUtils.Rtn(data.Prc, data.FundTacClsPrc);
                        data.FundTacMV *= (1 + data.PrcRtn.GetValueOrDefault());
                        data.FundTacMVChng = CommonUtils.SubtractNullableDoubles(data.FundTacMV, data.FundTacClsMV);

                        data.MV = CommonUtils.AddNullableDoubles(data.FundOppMV, data.FundTacMV);
                        data.MVChng = CommonUtils.SubtractNullableDoubles(data.MV, data.ClsMV);

                        if (Math.Abs(data.PrcRtn.GetValueOrDefault()) > 0.05)
                            data.ErrorFlag = 1;
                    }
                }
                else
                {
                    data.Error += "; Missing Live Price";
                    data.ErrorFlag = 2;
                }

                //flags for displaying on pnl dashboard
                if (data.FundOppMV.HasValue)
                    data.FundOppMVFlag = 1;
                if (data.FundTacMV.HasValue)
                    data.FundTacMVFlag = 1;
                if (data.PrcRtnLcl.HasValue)
                    data.PrcFlag = 1;
                if (data.MVChng.HasValue)
                    data.MVChngFlag = 1;

                //AddHolding(dict, data, groupValue, 10);
                AddHoldingToGroup(dict, data, groupValue, groupData, 20);
                AddHoldingToGroup(dict, data, "TOTAL", true, 1);
                AddHoldingToGroup(dict, data, "OPP", true, 2);
                AddHoldingToGroup(dict, data, "TAC", true, 3);
                GroupByCurrency(dict, data, "USD", true, 4);
                GroupByCurrency(dict, data, "GBP", true, 5);
                GroupByCurrency(dict, data, "CAD", true, 6);
                GroupByCurrency(dict, data, "AUD", true, 7);
                GroupByCurrency(dict, data, "EUR", true, 8);
            }
            return dict;
        }

        private void AddHoldingToGroup(IDictionary<string, AlmHoldingPerfTO> dict, AlmHoldingPerfTO data, string groupName, bool groupData, int sortId)
        {
            //if group by is not selected then do not group data
            if (!groupData)
            {
                if (!dict.TryGetValue(groupName, out AlmHoldingPerfTO record))
                {
                    record = data;
                    record.GrpName = groupName;
                    record.IsGroup = 0;
                    record.SortId = sortId;
                    dict.Add(data.Symbol, record);
                }
            }
            else
            {
                if (!dict.TryGetValue(groupName, out AlmHoldingPerfTO record))
                {
                    record = new AlmHoldingPerfTO();
                    record.AsOfDt = data.AsOfDt;
                    record.Symbol = groupName;
                    record.GrpName = groupName;
                    record.IsGroup = 1;
                    record.SortId = sortId;
                    record.MVChngFlag = 0;
                    record.PrcFlag = 0;
                    record.FundOppMVFlag = 0;
                    record.FundTacMVFlag = 0;

                    dict.Add(record.Symbol, record);
                }

                // add position
                if (!dict.TryGetValue(data.Symbol, out AlmHoldingPerfTO position))
                {
                    position = new AlmHoldingPerfTO();
                    position = data;
                    position.GrpName = groupName;
                    position.IsGroup = 0;
                    position.SortId = sortId;
                    dict.Add(position.Symbol, position);
                }

                if (groupName.Equals("OPP"))
                {
                    record.ClsMV = CommonUtils.AddNullableDoubles(record.ClsMV, data.FundOppClsMV);
                    record.MV = CommonUtils.AddNullableDoubles(record.MV, data.FundOppMV);
                    record.MVChng = CommonUtils.AddNullableDoubles(record.MVChng, data.FundOppMVChng);
                    record.ClsWt = CommonUtils.AddNullableDoubles(record.ClsWt, data.FundOppClsWt);

                    record.FundOppClsMV = CommonUtils.AddNullableDoubles(record.FundOppClsMV, data.FundOppClsMV);
                    record.FundOppMV = CommonUtils.AddNullableDoubles(record.FundOppMV, data.FundOppMV);
                    record.FundOppMVChng = CommonUtils.AddNullableDoubles(record.FundOppMVChng, data.FundOppMVChng);
                    record.FundOppClsWt = CommonUtils.AddNullableDoubles(record.FundOppClsWt, data.FundOppClsWt);
                }
                else if (groupName.Equals("TAC"))
                {
                    record.ClsMV = CommonUtils.AddNullableDoubles(record.ClsMV, data.FundTacClsMV);
                    record.MV = CommonUtils.AddNullableDoubles(record.MV, data.FundTacMV);
                    record.MVChng = CommonUtils.AddNullableDoubles(record.MVChng, data.FundTacMVChng);
                    record.ClsWt = CommonUtils.AddNullableDoubles(record.ClsWt, data.FundTacClsWt);

                    record.FundTacClsMV = CommonUtils.AddNullableDoubles(record.FundTacClsMV, data.FundTacClsMV);
                    record.FundTacMV = CommonUtils.AddNullableDoubles(record.FundTacMV, data.FundTacMV);
                    record.FundTacMVChng = CommonUtils.AddNullableDoubles(record.FundTacMVChng, data.FundTacMVChng);
                    record.FundTacClsWt = CommonUtils.AddNullableDoubles(record.FundTacClsWt, data.FundTacClsWt);
                }
                else
                {
                    record.ClsMV = CommonUtils.AddNullableDoubles(record.ClsMV, data.ClsMV);
                    record.MV = CommonUtils.AddNullableDoubles(record.MV, data.MV);
                    record.MVChng = CommonUtils.AddNullableDoubles(record.MVChng, data.MVChng);
                    record.ClsWt = CommonUtils.AddNullableDoubles(record.ClsWt, data.ClsWt);

                    record.FundOppClsMV = CommonUtils.AddNullableDoubles(record.FundOppClsMV, data.FundOppClsMV);
                    record.FundOppMV = CommonUtils.AddNullableDoubles(record.FundOppMV, data.FundOppMV);
                    record.FundOppMVChng = CommonUtils.AddNullableDoubles(record.FundOppMVChng, data.FundOppMVChng);
                    record.FundOppClsWt = CommonUtils.AddNullableDoubles(record.FundOppClsWt, data.FundOppClsWt);

                    record.FundTacClsMV = CommonUtils.AddNullableDoubles(record.FundTacClsMV, data.FundTacClsMV);
                    record.FundTacMV = CommonUtils.AddNullableDoubles(record.FundTacMV, data.FundTacMV);
                    record.FundTacMVChng = CommonUtils.AddNullableDoubles(record.FundTacMVChng, data.FundTacMVChng);
                    record.FundTacClsWt = CommonUtils.AddNullableDoubles(record.FundTacClsWt, data.FundTacClsWt);
                }
            }
        }

        private void GroupByCurrency(IDictionary<string, AlmHoldingPerfTO> dict, AlmHoldingPerfTO data, string groupName, bool groupData, int sortId)
        {
            if (!dict.TryGetValue(groupName, out AlmHoldingPerfTO record))
            {
                record = new AlmHoldingPerfTO();
                record.AsOfDt = data.AsOfDt;
                record.Symbol = groupName;
                record.GrpName = groupName;
                record.IsGroup = 1;
                record.SortId = sortId;
                record.MVChngFlag = 0;
                record.PrcFlag = 0;
                record.FundOppMVFlag = 0;
                record.FundTacMVFlag = 0;

                dict.Add(record.Symbol, record);
            }

            if (record != null && groupName.Equals(data.Curr))
            {
                record.ClsMV = CommonUtils.AddNullableDoubles(record.ClsMV, data.ClsMV);
                record.MV = CommonUtils.AddNullableDoubles(record.MV, data.MV);
                record.MVChng = CommonUtils.AddNullableDoubles(record.MVChng, data.MVChng);
                record.ClsWt = CommonUtils.AddNullableDoubles(record.ClsWt, data.ClsWt);

                record.FundOppClsMV = CommonUtils.AddNullableDoubles(record.FundOppClsMV, data.FundOppClsMV);
                record.FundOppMV = CommonUtils.AddNullableDoubles(record.FundOppMV, data.FundOppMV);
                record.FundOppMVChng = CommonUtils.AddNullableDoubles(record.FundOppMVChng, data.FundOppMVChng);
                record.FundOppClsWt = CommonUtils.AddNullableDoubles(record.FundOppClsWt, data.FundOppClsWt);

                record.FundTacClsMV = CommonUtils.AddNullableDoubles(record.FundTacClsMV, data.FundTacClsMV);
                record.FundTacMV = CommonUtils.AddNullableDoubles(record.FundTacMV, data.FundTacMV);
                record.FundTacMVChng = CommonUtils.AddNullableDoubles(record.FundTacMVChng, data.FundTacMVChng);
                record.FundTacClsWt = CommonUtils.AddNullableDoubles(record.FundTacClsWt, data.FundTacClsWt);
            }
        }

        private void PopulateGroup(DailyPnLTO dailyPnLTO, AlmHoldingPerfTO data)
        {
            SectorSummaryTO summary = new SectorSummaryTO();
            summary.Group = data.GrpName;
            summary.PrevMV = data.ClsMV;
            summary.MV = data.MV;
            summary.MVChng = data.MVChng;
            summary.MVChngPct = CommonUtils.Pct(summary.MVChng, summary.PrevMV);
            dailyPnLTO.SectorSummary.Add(summary);
        }
    }
}
