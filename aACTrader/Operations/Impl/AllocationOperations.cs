using aACTrader.DAO.Repository;
using aACTrader.Model;
using aCommons;
using aCommons.Cef;
using aCommons.DTO;
using aCommons.Trading;
using aCommons.Utils;
using LazyCache;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace aACTrader.Operations.Impl
{
    public class AllocationOperations
    {
        private readonly ILogger<AllocationOperations> _logger;
        private readonly CachingService _cache;
        private readonly TradingDao _tradingDao;
        private readonly IConfiguration _configuration;

        public AllocationOperations(ILogger<AllocationOperations> logger
            , CachingService cache
            , TradingDao tradingDao
            , IConfiguration configuration)
        {
            _logger = logger;
            _cache = cache;
            _tradingDao = tradingDao;
            _configuration = configuration;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IList<TradeSummaryTO> GetDailyTradeSummary(OrderParameters parameters)
        {
            IDictionary<string, TradeSummaryTO> dict = new Dictionary<string, TradeSummaryTO>();

            IDictionary<string, FundMaster> fundMasterDict = _cache.Get<IDictionary<string, FundMaster>>(CacheKeys.FUND_MASTER);
            IDictionary<string, double> fundMVDict = _cache.Get<IDictionary<string, double>>(CacheKeys.ALM_FUND_MARKET_VALUES);
            IDictionary<string, PositionMaster> positionDict = _cache.Get<IDictionary<string, PositionMaster>>(CacheKeys.POSITION_MASTER);
            IDictionary<string, FXRate> fxRateDict = _cache.Get<IDictionary<string, FXRate>>(CacheKeys.FX_RATES);
            IDictionary<string, string> securityDict = _cache.Get<IDictionary<string, string>>(CacheKeys.POSITION_IDENTIFIER_MAP);
            IDictionary<string, SecurityMasterExt> securityMasterExtDict = _cache.Get<IDictionary<string, SecurityMasterExt>>(CacheKeys.SECURITY_EXT_DETAILS);
            IDictionary<string, SecurityMarginDetail> jpmSecurityMarginDict = _cache.Get<IDictionary<string, SecurityMarginDetail>>(CacheKeys.JPM_SECURITY_MARGIN_RATES);
            IDictionary<string, SecurityMargin> fidelitySecurityMarginDict = _cache.Get<IDictionary<string, SecurityMargin>>(CacheKeys.FIDELITY_SECURITY_MARGIN_RATES);

            int sortId = 20;
            DateTime currentTime = DateTime.Now;
            bool includeOrder = true;

            IList<OrderSummary> tradeList;
            if (_configuration["ConnectionStrings:ENV"].Equals("PROD"))
                tradeList = _tradingDao.GetCompletedOrders(DateTime.Today);
            else
                tradeList = _tradingDao.GetCompletedOrders(DateTime.Today);
            //tradeList = _tradingDao.GetCompletedOrders(DateTime.Now.AddDays(-4));

            double totalFundMV = fundMVDict["All_MM"];
            double oppFundMV = fundMVDict["OPP_MM"];
            double tacFundMV = fundMVDict["TAC_MM"];

            //calculate fund weights
            double oppFundWt = oppFundMV / totalFundMV;
            double tacFundWt = tacFundMV / totalFundMV;

            //add custom groups
            AddCustomTradeGroups(dict);

            foreach (OrderSummary orderSummary in tradeList)
            {
                includeOrder = true;

                //Trader Filter
                if (!parameters.Trader.Equals("All"))
                    if (!parameters.Trader.Equals(orderSummary.Trader, StringComparison.CurrentCultureIgnoreCase))
                        includeOrder = false;

                //ALMTrader Filter
                if (!parameters.ALMTrader.Equals("All"))
                    if (!parameters.ALMTrader.Equals(orderSummary.ALMTrader, StringComparison.CurrentCultureIgnoreCase))
                        includeOrder = false;

                //Symbol Filter
                if (!string.IsNullOrEmpty(parameters.Symbol))
                    if (orderSummary.BBGSym.IndexOf(parameters.Symbol, StringComparison.OrdinalIgnoreCase) < 0)
                        includeOrder = false;

                //Time Filter
                if (!string.IsNullOrEmpty(parameters.TimeFilter)
                    && !parameters.TimeFilter.Equals("All") && !string.IsNullOrEmpty(orderSummary.OrdStatusUpdTm))
                {
                    DateTime orderUpdateTime = DateTime.ParseExact(orderSummary.OrdStatusUpdTm, "HH:mm:ss", CultureInfo.InvariantCulture);
                    TimeSpan ts = currentTime - orderUpdateTime;
                    //_logger.LogInformation("No. of Minutes (Difference) = " + ts.TotalMinutes);

                    double totalMins = ts.TotalMinutes;
                    switch (parameters.TimeFilter)
                    {
                        case "mins1":
                            if (totalMins > 1) includeOrder = false; break;
                        case "mins5":
                            if (totalMins > 5) includeOrder = false; break;
                        case "mins10":
                            if (totalMins > 10) includeOrder = false; break;
                        case "mins30":
                            if (totalMins > 30) includeOrder = false; break;
                        case "hrs1":
                            if (totalMins > 60) includeOrder = false; break;
                        case "hrs3":
                            if (totalMins > 180) includeOrder = false; break;
                        case "hrs6":
                            if (totalMins > 360) includeOrder = false; break;
                    }
                }

                if (includeOrder)
                {
                    string symbol = !string.IsNullOrEmpty(orderSummary.ALMSym) ? orderSummary.ALMSym : orderSummary.Sym;
                    string tradingSymbol = orderSummary.Sym;

                    //Trade
                    TradeSummaryTO tradeData = CreateTradeSummaryObject(orderSummary, symbol, jpmSecurityMarginDict, fidelitySecurityMarginDict);

                    //Position
                    PositionMaster positionMaster = GetPositionDetails(symbol, tradeData.Sedol, tradeData.ISIN, positionDict, securityDict);
                    if (positionMaster != null)
                    {
                        tradeData.ShOut = positionMaster.ShOut;

                        //Opp Fund
                        if (positionMaster.FundOpp.PosHeld.HasValue)
                        {
                            tradeData.FundOpp.SecWt = positionMaster.FundOpp.ClsMVPct;
                            tradeData.FundOpp.LiveSecWt = positionMaster.FundOpp.ClsMVPct;
                            tradeData.FundOpp.BegMV = positionMaster.FundOpp.ClsMV.GetValueOrDefault() * 1000000.0;
                            tradeData.FundOpp.SecOwnWt = positionMaster.FundOpp.PosOwnPct;

                            if (positionMaster.FundOpp != null)
                                tradeData.FundOpp.All.TotalPos = positionMaster.FundOpp.PosHeld;
                            if (positionMaster.FundOpp.BkrFido != null)
                                tradeData.FundOpp.Fidelity.TotalPos = positionMaster.FundOpp.BkrFido.PosHeld;
                            if (positionMaster.FundOpp.BkrJPM != null)
                                tradeData.FundOpp.JPM.TotalPos = positionMaster.FundOpp.BkrJPM.PosHeld;
                            if (positionMaster.FundOpp.BkrIB != null)
                                tradeData.FundOpp.IB.TotalPos = positionMaster.FundOpp.BkrIB.PosHeld;
                            if (positionMaster.FundOpp.BkrJeff != null)
                                tradeData.FundOpp.Jefferies.TotalPos = positionMaster.FundOpp.BkrJeff.PosHeld;
                            if (positionMaster.FundOpp.BkrEDF != null)
                                tradeData.FundOpp.EDF.TotalPos = positionMaster.FundOpp.BkrEDF.PosHeld;
                            if (positionMaster.FundOpp.BkrTD != null)
                                tradeData.FundOpp.TD.TotalPos = positionMaster.FundOpp.BkrTD.PosHeld;
                        }

                        //Tac Fund
                        if (positionMaster.FundTac.PosHeld.HasValue)
                        {
                            tradeData.FundTac.SecWt = positionMaster.FundTac.ClsMVPct;
                            tradeData.FundTac.LiveSecWt = positionMaster.FundTac.ClsMVPct;
                            tradeData.FundTac.BegMV = positionMaster.FundTac.ClsMV.GetValueOrDefault() * 1000000.0;
                            tradeData.FundTac.SecOwnWt = positionMaster.FundTac.PosOwnPct;

                            if (positionMaster.FundTac != null)
                                tradeData.FundTac.All.TotalPos = positionMaster.FundTac.PosHeld;
                            if (positionMaster.FundTac.BkrFido != null)
                                tradeData.FundTac.Fidelity.TotalPos = positionMaster.FundTac.BkrFido.PosHeld;
                            if (positionMaster.FundTac.BkrJPM != null)
                                tradeData.FundTac.JPM.TotalPos = positionMaster.FundTac.BkrJPM.PosHeld;
                            if (positionMaster.FundTac.BkrIB != null)
                                tradeData.FundTac.IB.TotalPos = positionMaster.FundTac.BkrIB.PosHeld;
                            if (positionMaster.FundTac.BkrJeff != null)
                                tradeData.FundTac.Jefferies.TotalPos = positionMaster.FundTac.BkrJeff.PosHeld;
                            if (positionMaster.FundTac.BkrEDF != null)
                                tradeData.FundTac.EDF.TotalPos = positionMaster.FundTac.BkrEDF.PosHeld;
                            if (positionMaster.FundTac.BkrTD != null)
                                tradeData.FundTac.TD.TotalPos = positionMaster.FundTac.BkrTD.PosHeld;
                        }

                        tradeData.SecOwnWt = CommonUtils.AddNullableDoubles(tradeData.FundOpp.SecOwnWt, tradeData.FundTac.SecOwnWt);
                    }
                    //else
                    //{
                    //    _logger.LogInformation("New Position: " + symbol);
                    //}

                    //
                    SecurityMasterExt securityMasterExt = CommonOperationsUtil.GetSecurityMasterExt(symbol, tradingSymbol, tradeData.Sedol, tradeData.ISIN, securityMasterExtDict, securityDict);
                    if (securityMasterExt != null)
                    {
                        tradeData.Beta = securityMasterExt.RiskBeta;
                        tradeData.Mult = securityMasterExt.Multiplier;
                        tradeData.Sector = securityMasterExt.FundGroup;
                        tradeData.Sec1940Act = securityMasterExt.Security1940Act;
                    }

                    //Duration
                    if (fundMasterDict.TryGetValue(symbol, out FundMaster fundMaster))
                    {
                        tradeData.Dur = fundMaster.Dur;
                        tradeData.DurSrc = fundMaster.DurSrc;
                    }
                    else if (securityMasterExt != null && securityMasterExt.Duration.HasValue)
                    {
                        tradeData.Dur = securityMasterExt.Duration;
                        tradeData.DurSrc = "User Ovr";
                    }

                    //Calculate
                    CalculateTradeSummary(dict, tradeData, fxRateDict, oppFundMV, tacFundMV, oppFundWt, tacFundWt);

                    tradeData.SortId = sortId;
                    sortId++;

                    string id = tradeData.Sym + "|" + tradeData.Qty + "|" + tradeData.Prc + "|" + tradeData.ExBkr;
                    tradeData.Id = id;

                    if (!dict.TryGetValue(id, out TradeSummaryTO tradeDataTemp))
                        dict.Add(id, tradeData);
                    else
                        _logger.LogInformation("Id already exists: " + id);
                }
            }

            IList<TradeSummaryTO> list = dict.Values.ToList<TradeSummaryTO>();
            return list.OrderBy(x => x.SortId).ThenBy(x => x.Sym).ToList<TradeSummaryTO>();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="dict"></param>
        private void AddCustomTradeGroups(IDictionary<string, TradeSummaryTO> dict)
        {
            //All Trades
            TradeSummaryTO allTrades = new TradeSummaryTO();
            allTrades.Sym = "Total";
            allTrades.SortId = 1;
            allTrades.IsGrpRow = 1;
            dict.Add(allTrades.Sym, allTrades);

            //Neovest Trades
            TradeSummaryTO nvTrades = new TradeSummaryTO();
            nvTrades.Sym = "NV";
            nvTrades.SortId = 2;
            nvTrades.IsGrpRow = 1;
            dict.Add(nvTrades.Sym, nvTrades);

            //Non-Neovest Trades
            TradeSummaryTO nonNVTrades = new TradeSummaryTO();
            nonNVTrades.Sym = "Non NV";
            nonNVTrades.SortId = 3;
            nonNVTrades.IsGrpRow = 1;
            dict.Add(nonNVTrades.Sym, nonNVTrades);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="orderSummary"></param>
        /// <param name="symbol"></param>
        /// <returns></returns>
        private TradeSummaryTO CreateTradeSummaryObject(OrderSummary orderSummary, string symbol
            , IDictionary<string, SecurityMarginDetail> jpmSecurityMarginDict
            , IDictionary<string, SecurityMargin> fidelitySecurityMarginDict)
        {
            TradeSummaryTO data = new TradeSummaryTO();
            data.Sym = orderSummary.Sym;
            data.ALMSym = orderSummary.ALMSym;
            data.BBGSym = orderSummary.BBGSym;
            data.Side = orderSummary.OrdSide;
            data.Dest = orderSummary.OrdDest;
            data.Strategy = orderSummary.OrdBkrStrat;
            data.Trader = orderSummary.Trader;
            data.Curr = orderSummary.Curr;
            data.Src = orderSummary.TrdSrc;
            data.ISIN = orderSummary.ISIN;
            data.Sedol = orderSummary.Sedol;
            data.OrdTime = orderSummary.OrdTm;
            data.OrdUpdTime = orderSummary.OrdStatusUpdTm;
            data.OrdDate = orderSummary.OrdDt;
            data.Qty = orderSummary.TrdCumQty.GetValueOrDefault();
            data.Prc = orderSummary.AvgTrdPr.GetValueOrDefault();
            data.SecWtInd = 0;

            //Executing Broker
            if (!string.IsNullOrEmpty(data.Dest) && !"Non NV".Equals(data.Src))
            {
                if (data.Dest.StartsWith("JPM", StringComparison.CurrentCultureIgnoreCase) || data.Dest.StartsWith("JP:", StringComparison.CurrentCultureIgnoreCase))
                    data.ExBkr = "JPM";
                else if (data.Dest.StartsWith("JEF", StringComparison.CurrentCultureIgnoreCase))
                    data.ExBkr = "JEFFERIES";
                else if (data.Dest.StartsWith("SCOTIA", StringComparison.CurrentCultureIgnoreCase))
                    data.ExBkr = "SCOTIA";
                else if (data.Dest.StartsWith("JONE", StringComparison.CurrentCultureIgnoreCase))
                    data.ExBkr = "JONE";
                else if (data.Dest.StartsWith("KBWI", StringComparison.CurrentCultureIgnoreCase))
                    data.ExBkr = "KBW";
                else if (data.Dest.StartsWith("PIPR", StringComparison.CurrentCultureIgnoreCase))
                    data.ExBkr = "PIPR";
                else if (data.Dest.StartsWith("JMP", StringComparison.CurrentCultureIgnoreCase))
                    data.ExBkr = "JMP";
                else if (data.Dest.StartsWith("CLST", StringComparison.CurrentCultureIgnoreCase))
                    data.ExBkr = "CLST";
                else if (data.Dest.StartsWith("PEEH", StringComparison.CurrentCultureIgnoreCase))
                    data.ExBkr = "PEELHUNT";
                else if (data.Dest.StartsWith("PEEL", StringComparison.CurrentCultureIgnoreCase))
                    data.ExBkr = "PEELHUNT";
            }
            else
            {
                data.ExBkr = data.Dest;
            }

            //Traded Qty
            if ("Sell".Equals(data.Side) || "Sell Short".Equals(data.Side) || "SSx".Equals(data.Side))
                data.Qty *= -1;

            //OPP
            if (jpmSecurityMarginDict.TryGetValue("OPP|" + data.ALMSym, out SecurityMarginDetail securityMarginDetail))
                data.FundOpp.MarginRtJPM = securityMarginDetail.MarginRate.GetValueOrDefault();
            //TAC
            if (jpmSecurityMarginDict.TryGetValue("TAC|" + data.ALMSym, out securityMarginDetail))
                data.FundTac.MarginRtJPM = securityMarginDetail.MarginRate.GetValueOrDefault();

            //OPP
            if (fidelitySecurityMarginDict.TryGetValue("OPP|" + data.ALMSym, out SecurityMargin securityMargin))
                data.FundOpp.MarginRtFido = securityMargin.MarginPct.GetValueOrDefault();
            //TAC
            if (fidelitySecurityMarginDict.TryGetValue("TAC|" + data.ALMSym, out securityMargin))
                data.FundTac.MarginRtFido = securityMargin.MarginPct.GetValueOrDefault();

            return data;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="dict"></param>
        /// <param name="tradeData"></param>
        /// <param name="fxRateDict"></param>
        /// <param name="oppFundMV"></param>
        /// <param name="tacFundMV"></param>
        /// <param name="oppFundWt"></param>
        /// <param name="tacFundWt"></param>
        private void CalculateTradeSummary(IDictionary<string, TradeSummaryTO> dict
            , TradeSummaryTO tradeData
            , IDictionary<string, FXRate> fxRateDict
            , double oppFundMV, double tacFundMV, double oppFundWt, double tacFundWt)
        {
            ////Allocate Trade (Opportunity Fund & Tactical Fund) PRO-RATA
            //tradeData.FundOpp.AllocPos = (int)(tradeData.Qty * oppFundWt);
            //tradeData.FundTac.AllocPos = tradeData.Qty - tradeData.FundOpp.AllocPos;

            string symbol = !string.IsNullOrEmpty(tradeData.ALMSym) ? tradeData.ALMSym : tradeData.Sym;

            AllocateTradeSummary(tradeData, oppFundMV, tacFundMV, oppFundWt, tacFundWt);

            //FX Rate
            double latestFxRate = 1.0;
            if (!string.IsNullOrEmpty(tradeData.Curr))
            {
                string currency = tradeData.Curr;
                if (!(currency.Equals("USD", StringComparison.CurrentCultureIgnoreCase)))
                {
                    if ("GBp".Equals(currency) || "GBX".Equals(currency))
                    {
                        latestFxRate /= 100.0;
                        currency = "GBP";
                    }

                    if (fxRateDict.TryGetValue(currency, out FXRate fxRateFund))
                        latestFxRate *= fxRateFund.FXRateLatest.GetValueOrDefault();
                }
            }
            tradeData.Fx = latestFxRate;

            //Allocation
            //MV (Local)
            tradeData.FundOpp.AllocMVLcl = tradeData.FundOpp.AllocPos * tradeData.Prc;
            tradeData.FundTac.AllocMVLcl = tradeData.FundTac.AllocPos * tradeData.Prc;
            if (tradeData.Mult.HasValue)
            {
                tradeData.FundOpp.AllocMVLcl *= tradeData.Mult;
                tradeData.FundTac.AllocMVLcl *= tradeData.Mult;
            }

            //MV (USD)
            tradeData.FundOpp.AllocMV = tradeData.FundOpp.AllocMVLcl * latestFxRate;
            tradeData.FundTac.AllocMV = tradeData.FundTac.AllocMVLcl * latestFxRate;

            //Beta
            if (tradeData.Beta.HasValue)
            {
                tradeData.FundOpp.AllocBetaContr = tradeData.FundOpp.AllocMV * tradeData.Beta;
                tradeData.FundTac.AllocBetaContr = tradeData.FundTac.AllocMV * tradeData.Beta;
            }

            //Duration
            if (tradeData.Dur.HasValue)
            {
                if (tradeData.Beta.HasValue)
                {
                    tradeData.FundOpp.AllocDurContr = tradeData.FundOpp.AllocBetaContr * tradeData.Dur;
                    tradeData.FundTac.AllocDurContr = tradeData.FundTac.AllocBetaContr * tradeData.Dur;
                }
                else
                {
                    tradeData.FundOpp.AllocDurContr = tradeData.FundOpp.AllocMV * tradeData.Dur;
                    tradeData.FundTac.AllocDurContr = tradeData.FundTac.AllocMV * tradeData.Dur;
                }
            }

            //Total
            tradeData.MVLocal = tradeData.FundOpp.AllocMVLcl.GetValueOrDefault() + tradeData.FundTac.AllocMVLcl.GetValueOrDefault();
            tradeData.MV = tradeData.FundOpp.AllocMV.GetValueOrDefault() + tradeData.FundTac.AllocMV.GetValueOrDefault();
            tradeData.BetaContr = tradeData.FundOpp.AllocBetaContr.GetValueOrDefault() + tradeData.FundTac.AllocBetaContr.GetValueOrDefault();
            tradeData.DurContr = tradeData.FundOpp.AllocDurContr.GetValueOrDefault() + tradeData.FundTac.AllocDurContr.GetValueOrDefault();

            //New Weights
            double? liveOppMV = CommonUtils.AddNullableDoubles(tradeData.FundOpp.BegMV, tradeData.FundOpp.AllocPos);
            if (liveOppMV.HasValue)
                tradeData.FundOpp.LiveSecWt = liveOppMV / oppFundMV;

            double? liveTacMV = CommonUtils.AddNullableDoubles(tradeData.FundTac.BegMV, tradeData.FundTac.AllocPos);
            if (liveTacMV.HasValue)
                tradeData.FundTac.LiveSecWt = liveTacMV / tacFundMV;

            //Security Wt
            if (tradeData.FundOpp.LiveSecWt.HasValue || tradeData.FundTac.LiveSecWt.HasValue)
            {
                double oppSecurityWt = tradeData.FundOpp.LiveSecWt.GetValueOrDefault();
                double tacSecurityWt = tradeData.FundTac.LiveSecWt.GetValueOrDefault();
                if (Math.Abs(oppSecurityWt) - Math.Abs(tacSecurityWt) > 0.01)
                    tradeData.SecWtInd = 1;
            }

            //Security Ownership
            if (tradeData.ShOut.HasValue && tradeData.ShOut.GetValueOrDefault() > 0)
            {
                tradeData.FundOpp.LiveSecOwnWt = (tradeData.FundOpp.AllocPos.GetValueOrDefault() + tradeData.FundOpp.All.TotalPos.GetValueOrDefault()) / tradeData.ShOut.GetValueOrDefault();
                tradeData.FundTac.LiveSecOwnWt = (tradeData.FundTac.AllocPos.GetValueOrDefault() + tradeData.FundTac.All.TotalPos.GetValueOrDefault()) / tradeData.ShOut.GetValueOrDefault();

                tradeData.LiveSecOwnWt = CommonUtils.AddNullableDoubles(tradeData.FundOpp.LiveSecOwnWt, tradeData.FundTac.LiveSecOwnWt);
            }

            //if (!string.IsNullOrEmpty(tradeData.Sec1940Act) && "Y".Equals(tradeData.Sec1940Act, StringComparison.CurrentCultureIgnoreCase))
            //{
            //    if (Math.Abs(tradeData.FundOpp.LiveSecOwnWt.GetValueOrDefault()) >= 0.03)
            //        tradeData.PosFlag = 1;

            //    if (Math.Abs(tradeData.FundTac.LiveSecOwnWt.GetValueOrDefault()) >= 0.03)
            //        tradeData.PosFlag = 1;
            //}

            if (symbol.EndsWith(" CT") || symbol.EndsWith(" CN") || symbol.EndsWith(" LN"))
            {
                if (Math.Abs(tradeData.LiveSecOwnWt.GetValueOrDefault()) >= 0.1)
                    tradeData.PosFlag = 1;
            }
            else
            {
                if (Math.Abs(tradeData.FundOpp.LiveSecOwnWt.GetValueOrDefault()) >= 0.03)
                    tradeData.PosFlag = 1;

                if (Math.Abs(tradeData.FundTac.LiveSecOwnWt.GetValueOrDefault()) >= 0.03)
                    tradeData.PosFlag = 1;
            }

            //NV
            PopulateTradeSummaryGroup(dict["NV"], tradeData, "NV");
            PopulateTradeSummaryGroup(dict["Non NV"], tradeData, "Non NV");
            PopulateTradeSummaryGroup(dict["Total"], tradeData, "Total");
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="summaryGroup"></param>
        /// <param name="tradeData"></param>
        private void PopulateTradeSummaryGroup(TradeSummaryTO summaryGroup, TradeSummaryTO tradeData, string source)
        {
            if (summaryGroup != null)
            {
                if ("Total".Equals(source) || tradeData.Src.Equals(source))
                {
                    summaryGroup.MVLocal += tradeData.MVLocal;
                    summaryGroup.MV += tradeData.MV;
                    summaryGroup.BetaContr += tradeData.BetaContr;
                    summaryGroup.DurContr += tradeData.DurContr;

                    summaryGroup.FundOpp.AllocBetaContr = CommonUtils.AddNullableDoubles(summaryGroup.FundOpp.AllocBetaContr, tradeData.FundOpp.AllocBetaContr);
                    summaryGroup.FundTac.AllocMVLcl = CommonUtils.AddNullableDoubles(summaryGroup.FundOpp.AllocMVLcl, tradeData.FundOpp.AllocMVLcl);
                    summaryGroup.FundOpp.AllocMV = CommonUtils.AddNullableDoubles(summaryGroup.FundOpp.AllocMV, tradeData.FundOpp.AllocMV);

                    summaryGroup.FundTac.AllocBetaContr = CommonUtils.AddNullableDoubles(summaryGroup.FundTac.AllocBetaContr, tradeData.FundTac.AllocBetaContr);
                    summaryGroup.FundTac.AllocMVLcl = CommonUtils.AddNullableDoubles(summaryGroup.FundTac.AllocMVLcl, tradeData.FundTac.AllocMVLcl);
                    summaryGroup.FundTac.AllocMV = CommonUtils.AddNullableDoubles(summaryGroup.FundTac.AllocMV, tradeData.FundTac.AllocMV);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tradeData"></param>
        /// <param name="oppFundMV"></param>
        /// <param name="tacFundMV"></param>
        /// <param name="oppFundWt"></param>
        /// <param name="tacFundWt"></param>
        private void AllocateTradeSummary(TradeSummaryTO tradeData
            , double oppFundMV, double tacFundMV, double oppFundWt, double tacFundWt)
        {
            double oppClsSecWt = tradeData.FundOpp.SecWt.GetValueOrDefault();
            double tacClsSecWt = tradeData.FundTac.SecWt.GetValueOrDefault();
            double oppLiveSecWt = tradeData.FundOpp.LiveSecWt.HasValue ? tradeData.FundOpp.LiveSecWt.GetValueOrDefault() : oppClsSecWt;
            double tacLiveSecWt = tradeData.FundTac.LiveSecWt.HasValue ? tradeData.FundTac.LiveSecWt.GetValueOrDefault() : tacClsSecWt;

            //Order Qty
            int tradeQty = tradeData.Qty;

            //Order Side
            int orderSideId = 0;
            string orderSide = tradeData.Side;
            if (orderSide.Equals("B") || orderSide.Equals("BC") || orderSide.Equals("Buy") || orderSide.Equals("Buy Cover"))
                orderSideId = 1;
            else if (orderSide.Equals("S") || orderSide.Equals("SS") || orderSide.Equals("Sell") || orderSide.Equals("Sell Short") || orderSide.Equals("SSx"))
                orderSideId = 2;

            //NEW POSITION - then allocate PRO-RATA
            if (oppLiveSecWt == 0 && tacLiveSecWt == 0)
            {
                //allocate PRO-RATA
                tradeData.FundOpp.AllocPos = (int)(tradeQty * oppFundWt);
                tradeData.FundTac.AllocPos = tradeQty - tradeData.FundOpp.AllocPos;
                tradeData.AllocRule = "New Position";
            }
            //else if (Math.Abs(oppLiveSecWt - tacLiveSecWt) * 10000 <= 5)
            //{
            //    //allocate PRO-RATA
            //    tradeData.FundOpp.AllocPos = (int)(tradeQty * oppFundWt);
            //    tradeData.FundTac.AllocPos = tradeQty - tradeData.FundOpp.AllocPos;
            //    tradeData.AllocRule = "Pro-Rata";
            //}
            //BUY or BUY_COVER
            else if (orderSideId == 1)
            {
                if (oppLiveSecWt > tacLiveSecWt)
                {
                    int sharesToAdd = (int)((oppLiveSecWt - tacLiveSecWt) * tacFundMV);
                    if (tradeQty > sharesToAdd)
                    {
                        //allocate shares to balance the funds and then allocate PRO-RATA on remaining balance
                        tradeData.FundTac.AllocPos = sharesToAdd;
                        tradeQty -= sharesToAdd;

                        //allocate PRO-RATA
                        tradeData.FundOpp.AllocPos = (int)(tradeQty * oppFundWt);
                        //add to pre-allocated shares
                        tradeData.FundTac.AllocPos += (tradeQty - tradeData.FundOpp.AllocPos);
                    }
                    else
                    {
                        //allocate all to balance the security weights in the fund
                        tradeData.FundTac.AllocPos = tradeQty;
                    }
                }
                else if (oppLiveSecWt < tacLiveSecWt)
                {
                    int sharesToAdd = (int)((tacLiveSecWt - oppLiveSecWt) * oppFundMV);
                    if (tradeQty > sharesToAdd)
                    {
                        //allocate shares to balance the funds and then allocate PRO-RATA on remaining balance
                        tradeData.FundOpp.AllocPos = sharesToAdd;
                        tradeQty -= sharesToAdd;

                        //allocate PRO-RATA
                        tradeData.FundTac.AllocPos = (int)(tradeQty * tacFundWt);
                        //add to pre-allocated shares
                        tradeData.FundOpp.AllocPos += (tradeQty - tradeData.FundTac.AllocPos);
                    }
                    else
                    {
                        tradeData.FundOpp.AllocPos = tradeQty;
                    }
                }
                tradeData.AllocRule = "Buy Wtd";
            }
            //SELL or SELL_SHORT
            else if (orderSideId == 2)
            {
                if (oppLiveSecWt > tacLiveSecWt)
                {
                    int sharesToAdd = -1 * (int)((oppLiveSecWt - tacLiveSecWt) * oppFundMV);
                    if (tradeQty > sharesToAdd)
                    {
                        tradeData.FundOpp.AllocPos = tradeQty;
                    }
                    else
                    {
                        tradeData.FundOpp.AllocPos = sharesToAdd;
                        tradeQty -= sharesToAdd;

                        //allocate PRO-RATA
                        tradeData.FundTac.AllocPos = (int)(tradeQty * tacFundWt);
                        //add to pre-allocated shares
                        tradeData.FundOpp.AllocPos += (tradeQty - tradeData.FundTac.AllocPos);
                    }
                }
                else if (oppLiveSecWt < tacLiveSecWt)
                {
                    int sharesToAdd = -1 * (int)((tacLiveSecWt - oppLiveSecWt) * tacFundMV);
                    if (tradeQty > sharesToAdd)
                    {
                        tradeData.FundTac.AllocPos = tradeQty;
                    }
                    else
                    {
                        tradeData.FundTac.AllocPos = sharesToAdd;
                        tradeQty -= sharesToAdd;

                        //allocate PRO-RATA
                        tradeData.FundOpp.AllocPos = (int)(tradeQty * oppFundWt);
                        //add to pre-allocated shares
                        tradeData.FundTac.AllocPos += (tradeQty - tradeData.FundOpp.AllocPos);
                    }
                }
                tradeData.AllocRule = "Sell Wtd";
            }
        }


        ///////////////////////////////////////////////////////////////////////////////////////////////////////////
        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IList<TradeReportTO> GetDailyTrades()
        {
            IList<TradeReportTO> list = new List<TradeReportTO>();
            IDictionary<string, double> fundMVDict = _cache.Get<IDictionary<string, double>>(CacheKeys.ALM_FUND_MARKET_VALUES);
            IDictionary<string, PositionMaster> positionDict = _cache.Get<IDictionary<string, PositionMaster>>(CacheKeys.POSITION_MASTER);
            IDictionary<string, FXRate> fxRateDict = _cache.Get<IDictionary<string, FXRate>>(CacheKeys.FX_RATES);
            IDictionary<string, string> securityDict = _cache.Get<IDictionary<string, string>>(CacheKeys.POSITION_IDENTIFIER_MAP);
            IDictionary<string, SecurityMasterExt> securityMasterExtDict = _cache.Get<IDictionary<string, SecurityMasterExt>>(CacheKeys.SECURITY_EXT_DETAILS);

            double totalFundMV = fundMVDict["All_MM"];
            double oppFundMV = fundMVDict["OPP_MM"];
            double tacFundMV = fundMVDict["TAC_MM"];

            //calculate fund weights
            double oppFundWt = oppFundMV / totalFundMV;

            IList<OrderSummary> tradeList;
            if (_configuration["ConnectionStrings:ENV"].Equals("PROD"))
                tradeList = _tradingDao.GetExecutedOrders(DateTime.Today);
            else
                //tradeList = _tradingDao.GetExecutedOrders(DateTime.Today);
                tradeList = _tradingDao.GetExecutedOrders(DateTime.Now.AddDays(-2));

            foreach (OrderSummary orderSummary in tradeList)
            {
                string symbol = !string.IsNullOrEmpty(orderSummary.ALMSym) ? orderSummary.ALMSym : orderSummary.Sym;
                string tradingSymbol = orderSummary.Sym;

                //Trade
                TradeReportTO tradeData = CreateTradeObject(orderSummary, symbol);

                //Position
                PositionMaster positionMaster = GetPositionDetails(symbol, tradeData.Sedol, tradeData.ISIN, positionDict, securityDict);
                if (positionMaster != null)
                {
                    //Opp Fund
                    if (positionMaster.FundOpp.PosHeld.HasValue)
                    {
                        tradeData.FundOpp.SecWt = positionMaster.FundOpp.ClsMVPct;
                        tradeData.FundOpp.LiveSecWt = positionMaster.FundOpp.ClsMVPct;

                        if (positionMaster.FundOpp != null)
                            tradeData.FundOpp.All.TotalPos = positionMaster.FundOpp.PosHeld;
                        if (positionMaster.FundOpp.BkrFido != null)
                            tradeData.FundOpp.Fidelity.TotalPos = positionMaster.FundOpp.BkrFido.PosHeld;
                        if (positionMaster.FundOpp.BkrJPM != null)
                            tradeData.FundOpp.JPM.TotalPos = positionMaster.FundOpp.BkrJPM.PosHeld;
                        if (positionMaster.FundOpp.BkrIB != null)
                            tradeData.FundOpp.IB.TotalPos = positionMaster.FundOpp.BkrIB.PosHeld;
                        if (positionMaster.FundOpp.BkrJeff != null)
                            tradeData.FundOpp.Jefferies.TotalPos = positionMaster.FundOpp.BkrJeff.PosHeld;
                        if (positionMaster.FundOpp.BkrEDF != null)
                            tradeData.FundOpp.EDF.TotalPos = positionMaster.FundOpp.BkrEDF.PosHeld;
                        if (positionMaster.FundOpp.BkrTD != null)
                            tradeData.FundOpp.TD.TotalPos = positionMaster.FundOpp.BkrTD.PosHeld;
                    }

                    //Tac Fund
                    if (positionMaster.FundTac.PosHeld.HasValue)
                    {
                        tradeData.FundTac.SecWt = positionMaster.FundTac.ClsMVPct;
                        tradeData.FundTac.LiveSecWt = positionMaster.FundTac.ClsMVPct;

                        if (positionMaster.FundTac != null)
                            tradeData.FundTac.All.TotalPos = positionMaster.FundTac.PosHeld;
                        if (positionMaster.FundTac.BkrFido != null)
                            tradeData.FundTac.Fidelity.TotalPos = positionMaster.FundTac.BkrFido.PosHeld;
                        if (positionMaster.FundTac.BkrJPM != null)
                            tradeData.FundTac.JPM.TotalPos = positionMaster.FundTac.BkrJPM.PosHeld;
                        if (positionMaster.FundTac.BkrIB != null)
                            tradeData.FundTac.IB.TotalPos = positionMaster.FundTac.BkrIB.PosHeld;
                        if (positionMaster.FundTac.BkrJeff != null)
                            tradeData.FundTac.Jefferies.TotalPos = positionMaster.FundTac.BkrJeff.PosHeld;
                        if (positionMaster.FundTac.BkrEDF != null)
                            tradeData.FundTac.EDF.TotalPos = positionMaster.FundTac.BkrEDF.PosHeld;
                        if (positionMaster.FundTac.BkrTD != null)
                            tradeData.FundTac.TD.TotalPos = positionMaster.FundTac.BkrTD.PosHeld;
                    }

                    //Security Wt
                    if (tradeData.FundOpp.SecWt.HasValue || tradeData.FundTac.SecWt.HasValue)
                    {
                        double oppSecurityWt = tradeData.FundOpp.SecWt.GetValueOrDefault();
                        double tacSecurityWt = tradeData.FundTac.SecWt.GetValueOrDefault();
                        if (Math.Abs(oppSecurityWt) - Math.Abs(tacSecurityWt) > 0.01)
                            tradeData.SecWtInd = 1;
                    }
                }
                else
                {
                    _logger.LogInformation("New Position: " + symbol);
                }

                SecurityMasterExt securityMasterExt = CommonOperationsUtil.GetSecurityMasterExt(symbol, tradingSymbol, tradeData.Sedol, tradeData.ISIN, securityMasterExtDict, securityDict);
                if (securityMasterExt != null)
                {
                    tradeData.Beta = securityMasterExt.RiskBeta;
                    tradeData.Mult = securityMasterExt.Multiplier;
                }

                //Calculate
                Calculate(tradeData, fxRateDict, oppFundMV, tacFundMV, oppFundWt);

                list.Add(tradeData);
            }

            return list;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="orderSummary"></param>
        /// <param name="symbol"></param>
        /// <returns></returns>
        private TradeReportTO CreateTradeObject(OrderSummary orderSummary, string symbol)
        {
            TradeReportTO data = new TradeReportTO();
            data.MainOrdId = orderSummary.MainOrdId;
            data.Ticker = symbol;
            data.Sedol = orderSummary.Sedol;
            data.ISIN = orderSummary.ISIN;
            data.OrdSide = orderSummary.OrdSide;
            data.OrdTime = orderSummary.OrdTm;
            data.OrdUpdTime = orderSummary.OrdStatusUpdTm;
            data.OrdDest = orderSummary.OrdDest;
            data.OrdBkrStrtg = orderSummary.OrdBkrStrat;
            data.Curr = orderSummary.Curr;
            data.IsSwap = orderSummary.CashOrSwap;
            data.Src = orderSummary.TrdSrc;
            data.Trader = orderSummary.Trader;
            data.ALMTrader = orderSummary.ALMTrader;
            //
            data.TrdQty = orderSummary.TrdCumQty;
            data.TrdPrc = orderSummary.AvgTrdPr;
            //
            data.SecWtInd = 0;

            //Executing Broker
            if (!string.IsNullOrEmpty(data.OrdDest) && !"Manual".Equals(data.Src))
            {
                if (data.OrdDest.StartsWith("JEF", StringComparison.CurrentCultureIgnoreCase))
                    data.ExBkr = "Jeff";
                else if (data.OrdDest.StartsWith("SCO", StringComparison.CurrentCultureIgnoreCase))
                    data.ExBkr = "Scotia";
                else if (data.OrdDest.StartsWith("JON", StringComparison.CurrentCultureIgnoreCase))
                    data.ExBkr = "Jone";
                else
                    data.ExBkr = "Jpm";
            }
            else
            {
                data.ExBkr = data.OrdDest;
            }

            //Traded Qty
            if ("Sell".Equals(data.OrdSide) || "Sell Short".Equals(data.OrdSide) || "SSx".Equals(data.OrdSide))
                data.TrdQty *= -1;

            return data;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tradeData"></param>
        /// <param name="fxRateDict"></param>
        /// <param name="oppFundMV"></param>
        /// <param name="tacFundMV"></param>
        /// <param name="oppFundWt"></param>
        private void Calculate(TradeReportTO tradeData, IDictionary<string, FXRate> fxRateDict
            , double oppFundMV, double tacFundMV, double oppFundWt)
        {
            //Allocate Trade (Opportunity Fund & Tactical Fund) PRO-RATA
            tradeData.FundOpp.AllocPos = (int)(tradeData.TrdQty.GetValueOrDefault() * oppFundWt);
            tradeData.FundTac.AllocPos = tradeData.TrdQty - tradeData.FundOpp.AllocPos;

            //AllocateTrade(tradeData, oppFundMV, tacFundMV, oppFundWt);

            //FX Rate
            double latestFxRate = 1.0;
            if (!string.IsNullOrEmpty(tradeData.Curr))
            {
                string currency = tradeData.Curr;
                if (!(currency.Equals("USD", StringComparison.CurrentCultureIgnoreCase)))
                {
                    if ("GBp".Equals(currency) || "GBX".Equals(currency))
                    {
                        latestFxRate /= 100.0;
                        currency = "GBP";
                    }

                    if (fxRateDict.TryGetValue(currency, out FXRate fxRateFund))
                        latestFxRate *= fxRateFund.FXRateLatest.GetValueOrDefault();
                }
            }
            tradeData.Fx = latestFxRate;

            //Allocation
            //MV (Local)
            tradeData.FundOpp.AllocMVLcl = tradeData.FundOpp.AllocPos * tradeData.TrdPrc;
            tradeData.FundTac.AllocMVLcl = tradeData.FundTac.AllocPos * tradeData.TrdPrc;
            if (tradeData.Mult.HasValue)
            {
                tradeData.FundOpp.AllocMVLcl *= tradeData.Mult;
                tradeData.FundTac.AllocMVLcl *= tradeData.Mult;
            }

            //MV (USD)
            tradeData.FundOpp.AllocMV = tradeData.FundOpp.AllocMVLcl * latestFxRate;
            tradeData.FundTac.AllocMV = tradeData.FundTac.AllocMVLcl * latestFxRate;

            //Beta
            if (tradeData.Beta.HasValue)
            {
                tradeData.FundOpp.AllocBetaContr = tradeData.FundOpp.AllocMV * tradeData.Beta;
                tradeData.FundTac.AllocBetaContr = tradeData.FundTac.AllocMV * tradeData.Beta;
            }

            //Total
            tradeData.MVLocal = tradeData.FundOpp.AllocMVLcl + tradeData.FundTac.AllocMVLcl;
            tradeData.MV = tradeData.FundOpp.AllocMV + tradeData.FundTac.AllocMV;
            tradeData.BetaContr = tradeData.FundOpp.AllocBetaContr + tradeData.FundTac.AllocBetaContr;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ticker"></param>
        /// <param name="sedol"></param>
        /// <param name="isin"></param>
        /// <param name="positionMasterDict"></param>
        /// <param name="positionTickerMap"></param>
        /// <returns></returns>
        public PositionMaster GetPositionDetails(string ticker, string sedol, string isin,
            IDictionary<string, PositionMaster> positionMasterDict, IDictionary<string, string> positionTickerMap)
        {
            PositionMaster positionMaster = null;
            if (!positionMasterDict.TryGetValue(ticker, out positionMaster))
            {
                //search by CT ticker if composite ticker CN is provided
                if (ticker.EndsWith(" CN", StringComparison.CurrentCultureIgnoreCase))
                {
                    string newTicker = ticker.Replace(" CN", " CT");
                    positionMasterDict.TryGetValue(newTicker, out positionMaster);
                }
            }

            //search by SEDOL
            if (positionMaster == null)
            {
                if (!string.IsNullOrEmpty(sedol)
                    && positionTickerMap.TryGetValue("Sedol|" + sedol, out string positionTicker))
                {
                    positionMasterDict.TryGetValue(positionTicker, out positionMaster);
                }
            }

            return positionMaster;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tradeData"></param>
        /// <param name="oppFundMV"></param>
        /// <param name="tacFundMV"></param>
        /// <param name="oppFundWt"></param>
        private void AllocateTrade(TradeReportTO tradeData
            , double oppFundMV, double tacFundMV, double oppFundWt)
        {
            double oppClsSecWt = tradeData.FundOpp.SecWt.GetValueOrDefault();
            double tacClsSecWt = tradeData.FundTac.SecWt.GetValueOrDefault();
            double oppLiveSecWt = tradeData.FundOpp.LiveSecWt.HasValue ? tradeData.FundOpp.LiveSecWt.GetValueOrDefault() : oppClsSecWt;
            double tacLiveSecWt = tradeData.FundTac.LiveSecWt.HasValue ? tradeData.FundTac.LiveSecWt.GetValueOrDefault() : tacClsSecWt;

            //Order Qty
            double tradeQty = tradeData.TrdQty.GetValueOrDefault();

            //Order Side
            int orderSideId = 0;
            string orderSide = tradeData.OrdSide;
            if (orderSide.Equals("B") || orderSide.Equals("BC") || orderSide.Equals("Buy") || orderSide.Equals("Buy Cover"))
                orderSideId = 1;
            else if (orderSide.Equals("S") || orderSide.Equals("SS") || orderSide.Equals("Sell") || orderSide.Equals("Sell Short") || orderSide.Equals("SSx"))
                orderSideId = 2;

            //BUY or BUY_COVER
            if (orderSideId == 1)
            {
                if (oppLiveSecWt > tacLiveSecWt)
                {
                    int sharesToAdd = (int)((oppLiveSecWt - tacLiveSecWt) * tacFundMV);
                    if ((int)tradeQty > sharesToAdd)
                    {
                        tradeData.FundTac.AllocPos = sharesToAdd;
                        tradeData.FundOpp.AllocPos = tradeData.TrdQty - sharesToAdd;
                    }
                    else
                    {
                        tradeData.FundTac.AllocPos = sharesToAdd;
                    }
                }
                else if (oppLiveSecWt < tacLiveSecWt)
                {
                    int sharesToAdd = (int)((oppLiveSecWt - tacLiveSecWt) * oppFundMV);
                    if ((int)tradeQty > sharesToAdd)
                    {
                        tradeData.FundOpp.AllocPos = sharesToAdd;
                        tradeData.FundTac.AllocPos = tradeData.TrdQty - sharesToAdd;
                    }
                    else
                    {
                        tradeData.FundOpp.AllocPos = sharesToAdd;
                    }
                }
            }

            //Security Wt
            if (tradeData.FundOpp.SecWt.HasValue || tradeData.FundTac.SecWt.HasValue)
            {
                double oppSecurityWt = tradeData.FundOpp.SecWt.GetValueOrDefault();
                double tacSecurityWt = tradeData.FundTac.SecWt.GetValueOrDefault();
                if (Math.Abs(oppSecurityWt) - Math.Abs(tacSecurityWt) > 0.01)
                    tradeData.SecWtInd = 1;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void PopulateLivePositions()
        {
            //_logger.LogInformation("PopulatePositions - STARTED");
            IDictionary<string, PositionReportTO> positionReportDict = new Dictionary<string, PositionReportTO>();
            GetLivePositions(positionReportDict);
            IList<PositionReportTO> list = (new List<PositionReportTO>(positionReportDict.Values))
                                                .OrderBy(x => x.SortId)
                                                .ThenBy(x => x.Ticker)
                                                .ToList<PositionReportTO>();

            //_cache.Remove(CacheKeys.LIVE_POSITIONS);
            _cache.Add(CacheKeys.LIVE_POSITIONS, list, DateTimeOffset.MaxValue);
            //IList<PositionReportTO> list1 = _cache.Get<IList<PositionReportTO>>(CacheKeys.LIVE_POSITIONS);
            //_logger.LogInformation("Live Positions Count: " + list1.Count);
            //_logger.LogInformation("PopulatePositions - DONE");
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="positionReportDict"></param>
        public void GetLivePositions(IDictionary<string, PositionReportTO> positionReportDict)
        {
            IDictionary<string, PositionMaster> positionMasterDict = _cache.Get<IDictionary<string, PositionMaster>>(CacheKeys.POSITION_MASTER);
            IDictionary<string, SecurityMaster> positionSecurityMasterDict = _cache.Get<IDictionary<string, SecurityMaster>>(CacheKeys.POSITION_SECURITY_MASTER_DETAILS);
            IDictionary<string, SecurityMasterExt> securityMasterExtDict = _cache.Get<IDictionary<string, SecurityMasterExt>>(CacheKeys.SECURITY_EXT_DETAILS);
            IDictionary<string, SecurityPrice> securityPriceDict = _cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);
            IDictionary<string, string> priceTickerMap = _cache.Get<IDictionary<string, string>>(CacheKeys.PRICE_TICKER_MAP);
            IDictionary<string, FXRate> fxRateDict = _cache.Get<IDictionary<string, FXRate>>(CacheKeys.FX_RATES);
            IDictionary<string, double> almHoldingsMarketValuesDict = _cache.Get<IDictionary<string, double>>(CacheKeys.ALM_FUND_MARKET_VALUES);
            IDictionary<string, FundForecast> fundForecastDict = _cache.Get<IDictionary<string, FundForecast>>(CacheKeys.FUND_FORECASTS);

            PopulatePositions(positionReportDict
                , positionMasterDict
                , positionSecurityMasterDict
                , securityMasterExtDict
                , securityPriceDict
                , priceTickerMap
                , fxRateDict
                , almHoldingsMarketValuesDict
                , fundForecastDict);

            PopulateTrades(positionReportDict
                , positionMasterDict
                , positionSecurityMasterDict
                , securityMasterExtDict
                , securityPriceDict
                , priceTickerMap
                , fxRateDict
                , almHoldingsMarketValuesDict
                , fundForecastDict);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public void PopulatePositions(IDictionary<string, PositionReportTO> positionReportDict
            , IDictionary<string, PositionMaster> positionMasterDict
            , IDictionary<string, SecurityMaster> postionSecurityMasterDict
            , IDictionary<string, SecurityMasterExt> securityMasterExtDict
            , IDictionary<string, SecurityPrice> securityPriceDict
            , IDictionary<string, string> priceTickerMap
            , IDictionary<string, FXRate> fxRateDict
            , IDictionary<string, double> almHoldingsMarketValuesDict
            , IDictionary<string, FundForecast> fundForecastDict
            )
        {
            try
            {
                double oppFundMV = almHoldingsMarketValuesDict["OPP_MM"];
                double tacFundMV = almHoldingsMarketValuesDict["TAC_MM"];

                //add custom position groups
                AddCustomGroups(positionReportDict, oppFundMV, tacFundMV);

                //get all position group
                PositionReportTO allPositionsGroup;
                positionReportDict.TryGetValue("All", out allPositionsGroup);

                foreach (KeyValuePair<string, PositionMaster> kvp in positionMasterDict)
                {
                    PositionMaster positionMaster = kvp.Value;
                    string holdingsTicker = positionMaster.Ticker;

                    if (!string.IsNullOrEmpty(holdingsTicker)
                        && !holdingsTicker.StartsWith("Swap")
                        && positionMaster.FundAll.PosHeld.HasValue)
                    {
                        string securityMasterTicker = !string.IsNullOrEmpty(positionMaster.SecTicker) ? positionMaster.SecTicker : positionMaster.Ticker;

                        //add position
                        PositionReportTO position = PopulatePosition(holdingsTicker
                            , securityMasterTicker
                            , positionMaster.Curr
                            , positionMaster
                            , postionSecurityMasterDict
                            , securityMasterExtDict
                            , securityPriceDict
                            , priceTickerMap
                            , fxRateDict
                            , fundForecastDict
                            , oppFundMV
                            , tacFundMV);

                        AddPosition(positionReportDict, position, allPositionsGroup);
                    }
                    //else
                    //{
                    //    _logger.LogInformation("Skipping Position:" + holdingsTicker);
                    //}
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error generating Live Position Report");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="positionReportDict"></param>
        /// <param name="positionMasterDict"></param>
        /// <param name="postionSecurityMasterDict"></param>
        /// <param name="securityMasterExtDict"></param>
        /// <param name="securityPriceDict"></param>
        /// <param name="priceTickerMap"></param>
        /// <param name="fxRateDict"></param>
        /// <param name="almHoldingsMarketValuesDict"></param>
        /// <param name="fundForecastDict"></param>
        private void PopulateTrades(IDictionary<string, PositionReportTO> positionReportDict
            , IDictionary<string, PositionMaster> positionMasterDict
            , IDictionary<string, SecurityMaster> postionSecurityMasterDict
            , IDictionary<string, SecurityMasterExt> securityMasterExtDict
            , IDictionary<string, SecurityPrice> securityPriceDict
            , IDictionary<string, string> priceTickerMap
            , IDictionary<string, FXRate> fxRateDict
            , IDictionary<string, double> almHoldingsMarketValuesDict
            , IDictionary<string, FundForecast> fundForecastDict
            )
        {
            IDictionary<string, TradePosition> tradeExecutionDict = _cache.Get<IDictionary<string, TradePosition>>(CacheKeys.TRADE_EXECUTIONS);
            IDictionary<string, IDictionary<string, PositionDetailTO>> tradeDetailsDict = _cache.Get<IDictionary<string, IDictionary<string, PositionDetailTO>>>(CacheKeys.LIVE_TRADE_DETAILS);

            //calculate fund weights
            double totalFundMV = almHoldingsMarketValuesDict["All_MM"];
            double oppFundMV = almHoldingsMarketValuesDict["OPP_MM"];
            double tacFundMV = almHoldingsMarketValuesDict["TAC_MM"];
            double oppFundWt = oppFundMV / totalFundMV;

            foreach (KeyValuePair<string, TradePosition> kvp in tradeExecutionDict)
            {
                string ticker = kvp.Key;
                TradePosition tradePosition = kvp.Value;
                string currency = tradePosition.Curr;

                //if (ticker.Equals("BMEZ US"))
                //    _logger.LogInformation("Ticker: " + ticker);

                if (tradePosition.IsExecuted.Equals("Y"))
                {
                    double latestFxRate = 1.0;
                    if (!string.IsNullOrEmpty(currency))
                    {
                        if (!(currency.Equals("USD", StringComparison.CurrentCultureIgnoreCase)))
                        {
                            if ("GBp".Equals(currency) || "GBX".Equals(currency))
                            {
                                latestFxRate /= 100.0;
                                currency = "GBP";
                            }

                            if (fxRateDict.TryGetValue(currency, out FXRate fxRate))
                                latestFxRate *= fxRate.FXRateLatest.GetValueOrDefault();
                        }
                    }

                    PositionReportTO position;
                    positionReportDict.TryGetValue(ticker, out position);
                    if (position == null)
                        positionReportDict.TryGetValue(tradePosition.PosSym, out position);

                    //all postitions group
                    PositionReportTO allPositionsGroup;
                    positionReportDict.TryGetValue("All", out allPositionsGroup);

                    if (position == null)
                    {
                        //_logger.LogInformation("Adding NEW position: " + ticker);

                        //add position
                        position = PopulatePosition(tradePosition.Sym
                            , tradePosition.PosSym
                            , tradePosition.Curr
                            , null
                            , postionSecurityMasterDict
                            , securityMasterExtDict
                            , securityPriceDict
                            , priceTickerMap
                            , fxRateDict
                            , fundForecastDict
                            , oppFundMV
                            , tacFundMV);

                        AddPosition(positionReportDict, position, allPositionsGroup);
                    }

                    if (position != null)
                    {
                        //OPP
                        position.FundOpp.AllocPos = (int)(tradePosition.Pos.GetValueOrDefault() * oppFundWt);
                        position.FundOpp.AllocMV = position.FundOpp.AllocPos * tradePosition.AvgPrc * latestFxRate;
                        if (position.PrcMult.HasValue)
                            position.FundOpp.AllocMV *= position.PrcMult;
                        position.FundOpp.LivePos += position.FundOpp.AllocPos;
                        position.FundOpp.LiveMV += position.FundOpp.AllocMV.GetValueOrDefault();
                        if (position.Beta.HasValue)
                        {
                            position.FundOpp.AllocBetaContr = position.FundOpp.AllocMV * position.Beta;
                            position.FundOpp.AllocBeta = position.FundOpp.AllocBetaContr / oppFundMV;
                            position.FundOpp.LiveBetaContr += position.FundOpp.AllocBetaContr.GetValueOrDefault();
                            position.FundOpp.LiveBeta += position.FundOpp.AllocBeta.GetValueOrDefault();
                        }

                        //TAC
                        position.FundTac.AllocPos = tradePosition.Pos - position.FundOpp.AllocPos;
                        position.FundTac.AllocMV = position.FundTac.AllocPos * tradePosition.AvgPrc * latestFxRate;
                        if (position.PrcMult.HasValue)
                            position.FundTac.AllocMV *= position.PrcMult;
                        position.FundTac.LivePos += position.FundTac.AllocPos;
                        position.FundTac.LiveMV += position.FundTac.AllocMV.GetValueOrDefault();
                        if (position.Beta.HasValue)
                        {
                            position.FundTac.AllocBetaContr = position.FundTac.AllocMV * position.Beta;
                            position.FundTac.AllocBeta = position.FundTac.AllocBetaContr / tacFundMV;
                            position.FundTac.LiveBetaContr += position.FundTac.AllocBetaContr.GetValueOrDefault();
                            position.FundTac.LiveBeta += position.FundTac.AllocBeta.GetValueOrDefault();
                        }

                        //ALL
                        position.FundAll.AllocPos = CommonUtils.AddNullableInts(position.FundOpp.AllocPos, position.FundTac.AllocPos);
                        position.FundAll.LivePos = CommonUtils.AddNullableDoubles(position.FundOpp.LivePos, position.FundTac.LivePos);
                        if (position.ShOut.HasValue && position.ShOut.GetValueOrDefault() > 0)
                            position.FundAll.LiveSecOwnWt = position.FundAll.LivePos / position.ShOut.GetValueOrDefault();

                        if (positionReportDict.TryGetValue(position.GrpName, out PositionReportTO positionGroup))
                        {
                            positionGroup.FundOpp.AllocMV = CommonUtils.AddNullableDoubles(positionGroup.FundOpp.AllocMV, position.FundOpp.AllocMV);
                            positionGroup.FundOpp.AllocBetaContr = CommonUtils.AddNullableDoubles(positionGroup.FundOpp.AllocBetaContr, position.FundOpp.AllocBetaContr);
                            positionGroup.FundOpp.LiveMV += position.FundOpp.AllocMV.GetValueOrDefault();
                            positionGroup.FundOpp.LiveBetaContr += position.FundOpp.AllocBetaContr.GetValueOrDefault();
                            positionGroup.FundOpp.LiveBeta += position.FundOpp.AllocBeta.GetValueOrDefault();

                            positionGroup.FundTac.AllocMV = CommonUtils.AddNullableDoubles(positionGroup.FundTac.AllocMV, position.FundTac.AllocMV);
                            positionGroup.FundTac.AllocBetaContr = CommonUtils.AddNullableDoubles(positionGroup.FundTac.AllocBetaContr, position.FundTac.AllocBetaContr);
                            positionGroup.FundTac.LiveMV += position.FundTac.AllocMV.GetValueOrDefault();
                            positionGroup.FundTac.LiveBetaContr += position.FundTac.AllocBetaContr.GetValueOrDefault();
                            positionGroup.FundTac.LiveBeta += position.FundTac.AllocBeta.GetValueOrDefault();
                        }

                        allPositionsGroup.FundOpp.AllocMV = CommonUtils.AddNullableDoubles(allPositionsGroup.FundOpp.AllocMV, position.FundOpp.AllocMV);
                        allPositionsGroup.FundOpp.AllocBetaContr = CommonUtils.AddNullableDoubles(allPositionsGroup.FundOpp.AllocBetaContr, position.FundOpp.AllocBetaContr);
                        allPositionsGroup.FundOpp.LiveMV += position.FundOpp.AllocMV.GetValueOrDefault();
                        allPositionsGroup.FundOpp.LiveBetaContr += position.FundOpp.AllocBetaContr.GetValueOrDefault();
                        allPositionsGroup.FundOpp.LiveBeta += position.FundOpp.AllocBeta.GetValueOrDefault();

                        allPositionsGroup.FundTac.AllocMV = CommonUtils.AddNullableDoubles(allPositionsGroup.FundTac.AllocMV, position.FundTac.AllocMV);
                        allPositionsGroup.FundTac.AllocBetaContr = CommonUtils.AddNullableDoubles(allPositionsGroup.FundTac.AllocBetaContr, position.FundTac.AllocBetaContr);
                        allPositionsGroup.FundTac.LiveMV += position.FundTac.AllocMV.GetValueOrDefault();
                        allPositionsGroup.FundTac.LiveBetaContr += position.FundTac.AllocBetaContr.GetValueOrDefault();
                        allPositionsGroup.FundTac.LiveBeta += position.FundTac.AllocBeta.GetValueOrDefault();

                        //add trade details
                        if (tradeDetailsDict.TryGetValue(ticker, out IDictionary<string, PositionDetailTO> positionDetailDict))
                        {
                            IList<PositionDetailTO> tradeList = new List<PositionDetailTO>(positionDetailDict.Values);
                            position.TrdDetails = tradeList;
                        }
                    }
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="position"></param>
        /// <param name="positionGroup"></param>
        private void AddTradeToPositionGroup(PositionReportTO position, PositionReportTO positionGroup)
        {
            positionGroup.FundOpp.AllocMV = CommonUtils.AddNullableDoubles(positionGroup.FundOpp.AllocMV, position.FundOpp.AllocMV);
            positionGroup.FundOpp.AllocBetaContr = CommonUtils.AddNullableDoubles(positionGroup.FundOpp.AllocBetaContr, position.FundOpp.AllocBetaContr);
            positionGroup.FundOpp.LiveMV += position.FundOpp.AllocMV.GetValueOrDefault();
            positionGroup.FundOpp.LiveBetaContr += position.FundOpp.AllocBetaContr.GetValueOrDefault();

            positionGroup.FundTac.AllocMV = CommonUtils.AddNullableDoubles(positionGroup.FundTac.AllocMV, position.FundTac.AllocMV);
            positionGroup.FundTac.AllocBetaContr = CommonUtils.AddNullableDoubles(positionGroup.FundTac.AllocBetaContr, position.FundTac.AllocBetaContr);
            positionGroup.FundTac.LiveMV += position.FundTac.AllocMV.GetValueOrDefault();
            positionGroup.FundTac.LiveBetaContr += position.FundTac.AllocBetaContr.GetValueOrDefault();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ticker"></param>
        /// <param name="smTicker"></param>
        /// <param name="currency"></param>
        /// <param name="positionMaster"></param>
        /// <param name="positionSecurityMasterDict"></param>
        /// <param name="securityMasterExtDict"></param>
        /// <param name="securityPriceDict"></param>
        /// <param name="priceTickerMap"></param>
        /// <param name="fxRateDict"></param>
        /// <param name="fundForecastDict"></param>
        /// <param name="oppFundMV"></param>
        /// <param name="tacFundMV"></param>
        /// <returns></returns>
        private PositionReportTO PopulatePosition(string ticker
            , string smTicker
            , string currency
            , PositionMaster positionMaster
            , IDictionary<string, SecurityMaster> positionSecurityMasterDict
            , IDictionary<string, SecurityMasterExt> securityMasterExtDict
            , IDictionary<string, SecurityPrice> securityPriceDict
            , IDictionary<string, string> priceTickerMap
            , IDictionary<string, FXRate> fxRateDict
            , IDictionary<string, FundForecast> fundForecastDict
            , double oppFundMV
            , double tacFundMV)
        {
            string groupName = "Other";
            int groupSortOrder = 10000;

            SecurityMasterExt securityMasterExt;
            securityMasterExtDict.TryGetValue(ticker, out securityMasterExt);
            if (securityMasterExt == null)
                securityMasterExtDict.TryGetValue(smTicker, out securityMasterExt);
            if (securityMasterExt != null)
            {
                groupName = securityMasterExt.FundGroup;
                groupSortOrder = securityMasterExt.SortOrder.GetValueOrDefault();
            }

            SecurityMaster securityMaster;
            positionSecurityMasterDict.TryGetValue(ticker, out securityMaster);
            if (securityMaster == null)
                positionSecurityMasterDict.TryGetValue(smTicker, out securityMaster);

            SecurityPrice securityPrice = SecurityPriceLookupOperations.GetSecurityPrice(ticker, priceTickerMap, securityPriceDict);
            if (securityPrice == null)
                securityPrice = SecurityPriceLookupOperations.GetSecurityPrice(smTicker, priceTickerMap, securityPriceDict);

            FXRate fxRate = null;
            if (!string.IsNullOrEmpty(currency))
                fxRateDict.TryGetValue(currency, out fxRate);

            FundForecast fundForecast;
            fundForecastDict.TryGetValue(ticker, out fundForecast);
            if (fundForecast == null)
                fundForecastDict.TryGetValue(smTicker, out fundForecast);

            //Create Position
            PositionReportTO position = new PositionReportTO();
            position.Ticker = ticker;
            position.SecTicker = smTicker;
            if (securityMaster != null)
                position.SecName = securityMaster.SecurityDescription;
            position.Curr = currency;
            position.GrpName = groupName;
            position.SortId = groupSortOrder;
            position.IsGrpRow = 0;
            if (positionMaster != null)
                position.ShOut = positionMaster.ShOut;
            if (position.ShOut != null && fundForecast != null)
                position.ShOut = fundForecast.ShOut;

            if (securityMasterExt != null)
            {
                position.Beta = securityMasterExt.RiskBeta;
                position.PrcMult = securityMasterExt.Multiplier;
                position.Sec13F = securityMasterExt.Security13F;
                position.Sec1940Act = securityMasterExt.Security1940Act;
                position.Lvl1 = securityMasterExt.Level1;
                position.Lvl2 = securityMasterExt.Level2;
                position.Lvl3 = securityMasterExt.Level3;
            }

            if (positionMaster != null)
            {
                //All
                position.FundAll.OrigPos = positionMaster.FundAll.PosHeld;
                position.FundAll.LivePos = positionMaster.FundAll.PosHeld;
                position.FundAll.SwapPos = positionMaster.FundAll.SwapPos;
                position.FundAll.CashPos = CommonUtils.SubtractNullableDoubles(position.FundAll.OrigPos, position.FundAll.SwapPos);
                position.FundAll.BegMV = positionMaster.FundAll.ClsMV.GetValueOrDefault() * 1000000.0;
                position.FundAll.SecWt = positionMaster.FundAll.ClsMVPct;
                position.FundAll.SecOwnWt = positionMaster.FundAll.PosOwnPct;
                if (position.ShOut.HasValue && position.ShOut.GetValueOrDefault() > 0)
                    position.FundAll.LiveSecOwnWt = position.FundAll.LivePos / position.ShOut.GetValueOrDefault();

                //Opp
                position.FundOpp.OrigPos = positionMaster.FundOpp.PosHeld;
                position.FundOpp.LivePos = positionMaster.FundOpp.PosHeld;
                position.FundOpp.SwapPos = positionMaster.FundOpp.SwapPos;
                position.FundOpp.CashPos = CommonUtils.SubtractNullableDoubles(position.FundOpp.OrigPos, position.FundOpp.SwapPos);
                position.FundOpp.BegMV = positionMaster.FundOpp.ClsMV.GetValueOrDefault() * 1000000.0;
                position.FundOpp.SecWt = positionMaster.FundOpp.ClsMVPct;
                position.FundOpp.SecOwnWt = positionMaster.FundOpp.PosOwnPct;

                //Tac
                position.FundTac.OrigPos = positionMaster.FundTac.PosHeld;
                position.FundTac.LivePos = positionMaster.FundTac.PosHeld;
                position.FundTac.SwapPos = positionMaster.FundTac.SwapPos;
                position.FundTac.CashPos = CommonUtils.SubtractNullableDoubles(position.FundTac.OrigPos, position.FundTac.SwapPos);
                position.FundTac.BegMV = positionMaster.FundTac.ClsMV.GetValueOrDefault() * 1000000.0;
                position.FundTac.SecWt = positionMaster.FundTac.ClsMVPct;
                position.FundTac.SecOwnWt = positionMaster.FundTac.PosOwnPct;

                //All
                position.SecOwnWt = CommonUtils.AddNullableDoubles(position.FundOpp.SecOwnWt, position.FundTac.SecOwnWt);

                if (!string.IsNullOrEmpty(position.Sec1940Act) && "Y".Equals(position.Sec1940Act, StringComparison.CurrentCultureIgnoreCase))
                {
                    if (Math.Abs(position.FundOpp.SecOwnWt.GetValueOrDefault()) >= 0.03)
                        position.PosFlag = 1;

                    if (Math.Abs(position.FundTac.SecOwnWt.GetValueOrDefault()) >= 0.03)
                        position.PosFlag = 1;
                }

                if (ticker.EndsWith(" CT") || ticker.EndsWith(" CN"))
                {
                    if (Math.Abs(position.SecOwnWt.GetValueOrDefault()) >= 0.1)
                        position.PosFlag = 1;
                }
            }

            //Live Prices
            if (securityPrice != null)
            {
                position.ClsPrc = securityPrice.ClsPrc;
                position.LivePrc = securityPrice.LastPrc;
                position.PrcChng = securityPrice.PrcRtn;
                position.PrcSrc = securityPrice.Src;

                double securityRtn = (1.0 + securityPrice.PrcRtn.GetValueOrDefault());
                double fxRtn = 1.0;
                if (fxRate != null)
                {
                    fxRtn = (1.0 + fxRate.FXReturn.GetValueOrDefault());
                    position.LiveFx = fxRate.FXRateLatest;
                }

                position.FundOpp.LiveMV = position.FundOpp.BegMV * securityRtn * fxRtn;
                position.FundTac.LiveMV = position.FundTac.BegMV * securityRtn * fxRtn;
                if (position.Beta.HasValue)
                {
                    position.FundOpp.LiveBetaContr = position.FundOpp.LiveMV * position.Beta.GetValueOrDefault();
                    position.FundOpp.LiveBeta = position.FundOpp.LiveBetaContr / oppFundMV;

                    position.FundTac.LiveBetaContr = position.FundTac.LiveMV * position.Beta.GetValueOrDefault();
                    position.FundTac.LiveBeta = position.FundTac.LiveBetaContr / tacFundMV;
                }
                position.FundAll.LiveMV = position.FundOpp.LiveMV + position.FundTac.LiveMV;
            }

            if (fundForecast != null)
            {
                position.NavDt = fundForecast.LastNavDt;
                position.PubNav = fundForecast.LastDvdAdjNav;
                position.EstNav = fundForecast.EstNav;
                position.LastPD = fundForecast.PDLastPrc;
                position.BidPD = fundForecast.PDBidPrc;
                position.AskPD = fundForecast.PDAskPrc;
            }

            return position;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="positionReportDict"></param>
        /// <param name="position"></param>
        /// <param name="allPositionGroup"></param>
        private void AddPosition(IDictionary<string, PositionReportTO> positionReportDict
            , PositionReportTO position
            , PositionReportTO allPositionGroup)
        {
            string groupName = position.GrpName;
            int groupSortOrder = position.SortId;
            if (positionReportDict.TryGetValue(groupName, out PositionReportTO positionGroup))
            {
                //add position
                position.SortId = positionGroup.SortId + 1;
                AddPositionToGroup(position, positionGroup);
                AddPositionToGroup(position, allPositionGroup);
                positionReportDict.Add(position.Ticker, position);
            }
            else
            {
                //add position group
                positionGroup = new PositionReportTO();
                positionGroup.Ticker = groupName;
                positionGroup.GrpName = groupName;
                positionGroup.SortId = groupSortOrder;
                positionGroup.IsGrpRow = 1;
                positionReportDict.Add(positionGroup.Ticker, positionGroup);

                //add position
                position.SortId = positionGroup.SortId + 1;
                AddPositionToGroup(position, positionGroup);
                AddPositionToGroup(position, allPositionGroup);
                positionReportDict.Add(position.Ticker, position);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="position"></param>
        /// <param name="positionGroup"></param>
        private void AddPositionToGroup(PositionReportTO position, PositionReportTO positionGroup)
        {
            //All
            positionGroup.FundAll.BegMV += position.FundAll.BegMV;
            positionGroup.FundAll.LiveMV += position.FundAll.LiveMV;
            positionGroup.FundAll.LiveBetaContr += position.FundAll.LiveBetaContr;
            positionGroup.FundAll.LiveBeta += position.FundAll.LiveBeta;

            //Opp
            positionGroup.FundOpp.BegMV += position.FundOpp.BegMV;
            positionGroup.FundOpp.LiveMV += position.FundOpp.LiveMV;
            positionGroup.FundOpp.LiveBetaContr += position.FundOpp.LiveBetaContr;
            positionGroup.FundOpp.LiveBeta += position.FundOpp.LiveBeta;

            //Tac
            positionGroup.FundTac.BegMV += position.FundTac.BegMV;
            positionGroup.FundTac.LiveMV += position.FundTac.LiveMV;
            positionGroup.FundTac.LiveBetaContr += position.FundTac.LiveBetaContr;
            positionGroup.FundTac.LiveBeta += position.FundTac.LiveBeta;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="positionReportDict"></param>
        /// <param name="oppFundMV"></param>
        /// <param name="tacFundMV"></param>
        private void AddCustomGroups(IDictionary<string, PositionReportTO> positionReportDict
            , double oppFundMV, double tacFundMV)
        {
            //PH Loader Fund Navs
            PositionReportTO phPositionGroup = new PositionReportTO();
            phPositionGroup.Ticker = "Fund Nav (PH Loader)";
            phPositionGroup.GrpName = "Fund Nav (PH Loader)";
            phPositionGroup.IsGrpRow = 1;
            phPositionGroup.SortId = 1;
            phPositionGroup.FundOpp.BegMV = oppFundMV;
            phPositionGroup.FundTac.BegMV = tacFundMV;
            positionReportDict.Add(phPositionGroup.GrpName, phPositionGroup);

            //All Positions
            PositionReportTO allPositionGroup = new PositionReportTO();
            allPositionGroup.Ticker = "All";
            allPositionGroup.GrpName = "All";
            allPositionGroup.IsGrpRow = 1;
            allPositionGroup.SortId = 2;
            positionReportDict.Add(allPositionGroup.GrpName, allPositionGroup);
        }
    }
}