using aACTrader.DAO.Repository;
using aACTrader.Model;
using aACTrader.Operations.Impl;
using aCommons;
using aCommons.Cef;
using aCommons.DTO;
using aCommons.Trading;
using aCommons.Utils;
using aCommons.Web;
using LazyCache;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace aACTrader.Operations.Reports
{
    public class TradeSummaryReport
    {
        private readonly ILogger<TradeSummaryReport> _logger;
        private readonly CachingService _cache;
        private readonly TradingDao _tradingDao;
        private readonly IConfiguration _configuration;
        private readonly CommonOperations _commonOperations;

        public TradeSummaryReport(ILogger<TradeSummaryReport> logger
            , CachingService cache
            , TradingDao tradingDao
            , IConfiguration configuration
            , CommonOperations commonOperations)
        {
            _logger = logger;
            _cache = cache;
            _tradingDao = tradingDao;
            _configuration = configuration;
            _commonOperations = commonOperations;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IList<TradeSummaryTO> GetDailyTradeSummary(OrderParameters parameters)
        {
            IDictionary<string, TradeSummaryTO> dict = new Dictionary<string, TradeSummaryTO>();

            IDictionary<string, FundMaster> fundMasterDict = _cache.Get<IDictionary<string, FundMaster>>(CacheKeys.FUND_MASTER);
            IDictionary<string, FundPortDate> fundPortDateDict = _cache.Get<IDictionary<string, FundPortDate>>(CacheKeys.FUND_PORT_DATES);
            IDictionary<string, double> fundMVDict = _cache.Get<IDictionary<string, double>>(CacheKeys.ALM_FUND_MARKET_VALUES);
            IDictionary<string, PositionMaster> positionDict = _cache.Get<IDictionary<string, PositionMaster>>(CacheKeys.POSITION_MASTER);
            IDictionary<string, FXRate> fxRateDict = _cache.Get<IDictionary<string, FXRate>>(CacheKeys.FX_RATES);
            IDictionary<string, FXRate> fxRatePDDict = _cache.Get<IDictionary<string, FXRate>>(CacheKeys.FX_RATES_PD);
            IDictionary<string, string> securityDict = _cache.Get<IDictionary<string, string>>(CacheKeys.POSITION_IDENTIFIER_MAP);
            IDictionary<string, SecurityMasterExt> securityMasterExtDict = _cache.Get<IDictionary<string, SecurityMasterExt>>(CacheKeys.SECURITY_EXT_DETAILS);
            IDictionary<string, SecurityMarginDetail> jpmSecurityMarginDict = _cache.Get<IDictionary<string, SecurityMarginDetail>>(CacheKeys.JPM_SECURITY_MARGIN_RATES);
            IDictionary<string, SecurityMargin> fidelitySecurityMarginDict = _cache.Get<IDictionary<string, SecurityMargin>>(CacheKeys.FIDELITY_SECURITY_MARGIN_RATES);
            IDictionary<string, FundCurrExpTO> fundCurrencyExpDict = _cache.Get<IDictionary<string, FundCurrExpTO>>(CacheKeys.FUND_CURRENCY_EXPOSURES);
            IDictionary<string, SecurityRiskFactor> securityRiskFactorDict = _cache.Get<IDictionary<string, SecurityRiskFactor>>(CacheKeys.SECURITY_RISK_FACTORS_WITH_DATES);

            int sortId = 20;
            DateTime currentTime = DateTime.Now;
            bool includeOrder = true;

            IList<OrderSummary> tradeList;
            if (_configuration["ConnectionStrings:ENV"].Equals("PROD"))
                tradeList = _tradingDao.GetCompletedOrders(DateTime.Today);
            else
                //tradeList = _tradingDao.GetCompletedOrders(DateTime.Today);
                tradeList = _tradingDao.GetCompletedOrders(DateTime.Now.AddDays(-3));

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
                    TradeSummaryTO tradeData = CreateTradeSummaryObject(orderSummary);

                    //Position
                    PositionMaster positionMaster = CommonOperationsUtil.GetPositionDetails(symbol, tradeData.Sedol, tradeData.ISIN, positionDict, securityDict);
                    if (positionMaster != null)
                    {
                        tradeData.ShOut = positionMaster.ShOut;
                        tradeData.AltSym = positionMaster.Ticker;

                        //Opp Fund
                        if (positionMaster.FundOpp.PosHeld.HasValue)
                        {
                            tradeData.FundOpp.SecWt = positionMaster.FundOpp.ClsMVPct;
                            tradeData.FundOpp.LiveSecWt = positionMaster.FundOpp.ClsMVPct;
                            tradeData.FundOpp.BegMV = positionMaster.FundOpp.ClsMV.GetValueOrDefault() * 1000000.0;
                            tradeData.FundOpp.SecOwnWt = positionMaster.FundOpp.PosOwnPct;

                            //if (positionMaster.FundOpp != null)
                            //    tradeData.FundOpp.All.TotalPos = positionMaster.FundOpp.PosHeld;

                            ////Broker Positions
                            //if (positionMaster.FundOpp.BkrFido != null)
                            //    tradeData.FundOpp.Fidelity.TotalPos = positionMaster.FundOpp.BkrFido.PosHeld;
                            //if (positionMaster.FundOpp.BkrJPM != null)
                            //    tradeData.FundOpp.JPM.TotalPos = positionMaster.FundOpp.BkrJPM.PosHeld;
                            //if (positionMaster.FundOpp.BkrIB != null)
                            //    tradeData.FundOpp.IB.TotalPos = positionMaster.FundOpp.BkrIB.PosHeld;
                            //if (positionMaster.FundOpp.BkrJeff != null)
                            //    tradeData.FundOpp.Jefferies.TotalPos = positionMaster.FundOpp.BkrJeff.PosHeld;
                            //if (positionMaster.FundOpp.BkrEDF != null)
                            //    tradeData.FundOpp.EDF.TotalPos = positionMaster.FundOpp.BkrEDF.PosHeld;
                            //if (positionMaster.FundOpp.BkrTD != null)
                            //    tradeData.FundOpp.TD.TotalPos = positionMaster.FundOpp.BkrTD.PosHeld;
                            //if (positionMaster.FundOpp.BkrScotia != null)
                            //    tradeData.FundOpp.Scotia.TotalPos = positionMaster.FundOpp.BkrScotia.PosHeld;
                            //if (positionMaster.FundOpp.BkrBMO != null)
                            //    tradeData.FundOpp.BMO.TotalPos = positionMaster.FundOpp.BkrBMO.PosHeld;
                            //if (positionMaster.FundOpp.BkrUBS != null)
                            //    tradeData.FundOpp.UBS.TotalPos = positionMaster.FundOpp.BkrUBS.PosHeld;
                            //if (positionMaster.FundOpp.BkrMS != null)
                            //    tradeData.FundOpp.MS.TotalPos = positionMaster.FundOpp.BkrMS.PosHeld;
                            //if (positionMaster.FundOpp.BkrBAML != null)
                            //    tradeData.FundOpp.BAML.TotalPos = positionMaster.FundOpp.BkrBAML.PosHeld;
                            //if (positionMaster.FundOpp.BkrSTON != null)
                            //    tradeData.FundOpp.STON.TotalPos = positionMaster.FundOpp.BkrSTON.PosHeld;
                        }

                        //Tac Fund
                        if (positionMaster.FundTac.PosHeld.HasValue)
                        {
                            tradeData.FundTac.SecWt = positionMaster.FundTac.ClsMVPct;
                            tradeData.FundTac.LiveSecWt = positionMaster.FundTac.ClsMVPct;
                            tradeData.FundTac.BegMV = positionMaster.FundTac.ClsMV.GetValueOrDefault() * 1000000.0;
                            tradeData.FundTac.SecOwnWt = positionMaster.FundTac.PosOwnPct;

                            //if (positionMaster.FundTac != null)
                            //    tradeData.FundTac.All.TotalPos = positionMaster.FundTac.PosHeld;

                            ////Broker Positions
                            //if (positionMaster.FundTac.BkrFido != null)
                            //    tradeData.FundTac.Fidelity.TotalPos = positionMaster.FundTac.BkrFido.PosHeld;
                            //if (positionMaster.FundTac.BkrJPM != null)
                            //    tradeData.FundTac.JPM.TotalPos = positionMaster.FundTac.BkrJPM.PosHeld;
                            //if (positionMaster.FundTac.BkrIB != null)
                            //    tradeData.FundTac.IB.TotalPos = positionMaster.FundTac.BkrIB.PosHeld;
                            //if (positionMaster.FundTac.BkrJeff != null)
                            //    tradeData.FundTac.Jefferies.TotalPos = positionMaster.FundTac.BkrJeff.PosHeld;
                            //if (positionMaster.FundTac.BkrEDF != null)
                            //    tradeData.FundTac.EDF.TotalPos = positionMaster.FundTac.BkrEDF.PosHeld;
                            //if (positionMaster.FundTac.BkrTD != null)
                            //    tradeData.FundTac.TD.TotalPos = positionMaster.FundTac.BkrTD.PosHeld;
                            //if (positionMaster.FundTac.BkrScotia != null)
                            //    tradeData.FundTac.Scotia.TotalPos = positionMaster.FundTac.BkrScotia.PosHeld;
                            //if (positionMaster.FundTac.BkrBMO != null)
                            //    tradeData.FundTac.BMO.TotalPos = positionMaster.FundTac.BkrBMO.PosHeld;
                            //if (positionMaster.FundTac.BkrUBS != null)
                            //    tradeData.FundTac.UBS.TotalPos = positionMaster.FundTac.BkrUBS.PosHeld;
                            //if (positionMaster.FundTac.BkrMS != null)
                            //    tradeData.FundTac.MS.TotalPos = positionMaster.FundTac.BkrMS.PosHeld;
                            //if (positionMaster.FundTac.BkrBAML != null)
                            //    tradeData.FundTac.BAML.TotalPos = positionMaster.FundTac.BkrBAML.PosHeld;
                            //if (positionMaster.FundTac.BkrSTON != null)
                            //    tradeData.FundTac.STON.TotalPos = positionMaster.FundTac.BkrSTON.PosHeld;
                        }

                        tradeData.SecOwnWt = CommonUtils.AddNullableDoubles(tradeData.FundOpp.SecOwnWt, tradeData.FundTac.SecOwnWt);
                    }
                    //else
                    //{
                    //    _logger.LogInformation("New Position: " + symbol);
                    //}

                    //Risk Beta, Duration...
                    SecurityMasterExt securityMasterExt = CommonOperationsUtil.GetSecurityMasterExt(symbol, tradingSymbol, tradeData.Sedol, tradeData.ISIN, securityMasterExtDict, securityDict);
                    if (securityMasterExt != null)
                    {
                        tradeData.Beta = securityMasterExt.RiskBeta;
                        tradeData.CBeta = securityMasterExt.RiskBeta;
                        tradeData.Mult = securityMasterExt.Multiplier;
                        tradeData.Sector = securityMasterExt.FundGroup;
                        tradeData.Sec1940Act = securityMasterExt.Security1940Act;
                    }

                    //Corporate Action Beta
                    SecurityRiskFactor securityRiskFactor = CommonOperationsUtil.GetSecurityRiskFactor(symbol, tradingSymbol, tradeData.Sedol, tradeData.ISIN, securityRiskFactorDict, securityDict);
                    if (securityRiskFactor != null)
                    {
                        tradeData.CBeta = securityRiskFactor.RiskBeta;
                    }

                    //Duration
                    if (fundPortDateDict.TryGetValue(symbol, out FundPortDate fundPortDate))
                    {
                        tradeData.Dur = fundPortDate.Dur;
                        tradeData.DurSrc = fundPortDate.DurSrc;
                    }
                    else if (fundPortDateDict.TryGetValue(tradingSymbol, out fundPortDate))
                    {
                        tradeData.Dur = fundPortDate.Dur;
                        tradeData.DurSrc = fundPortDate.DurSrc;
                    }
                    else if (securityMasterExt != null && securityMasterExt.Duration.HasValue)
                    {
                        tradeData.Dur = securityMasterExt.Duration;
                        tradeData.DurSrc = "User Ovr";
                    }

                    //Calculate
                    CalculateTradeSummary(dict, tradeData, fxRateDict, fxRatePDDict, oppFundMV, tacFundMV, oppFundWt, tacFundWt);

                    //Calculate Currency Exposure
                    //CalculateCurrencyExposures(fundCurrencyExpDict, tradeData);

                    //NV
                    PopulateTradeSummaryGroup(dict["NV"], tradeData, "NV");
                    PopulateTradeSummaryGroup(dict["EMSX"], tradeData, "EMSX");
                    PopulateTradeSummaryGroup(dict["Non NV"], tradeData, "Non NV");
                    PopulateTradeSummaryGroup(dict["Total"], tradeData, "Total");

                    tradeData.SortId = sortId;
                    sortId++;

                    string id = tradeData.Sym + "|" + tradeData.Qty + "|" + tradeData.Prc.GetValueOrDefault() + "|" + tradeData.ExBkr;
                    tradeData.Id = id;

                    if (!dict.TryGetValue(id, out TradeSummaryTO tradeDataTemp))
                        dict.Add(id, tradeData);
                    //else
                    //    _logger.LogInformation("Id already exists: " + id);
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
            allTrades.ALMSym = "Total";
            allTrades.SortId = 1;
            allTrades.IsGrpRow = 1;
            dict.Add(allTrades.Sym, allTrades);

            //Neovest Trades
            TradeSummaryTO nvTrades = new TradeSummaryTO();
            nvTrades.Sym = "NV";
            nvTrades.ALMSym = "NV";
            nvTrades.SortId = 2;
            nvTrades.IsGrpRow = 1;
            dict.Add(nvTrades.Sym, nvTrades);

            //EMSX Trades
            TradeSummaryTO emsxTrades = new TradeSummaryTO();
            emsxTrades.Sym = "EMSX";
            emsxTrades.ALMSym = "EMSX";
            emsxTrades.SortId = 3;
            emsxTrades.IsGrpRow = 1;
            dict.Add(emsxTrades.Sym, emsxTrades);

            //Non-Neovest Trades
            TradeSummaryTO nonNVTrades = new TradeSummaryTO();
            nonNVTrades.Sym = "Non NV";
            nonNVTrades.ALMSym = "Non NV";
            nonNVTrades.SortId = 4;
            nonNVTrades.IsGrpRow = 1;
            dict.Add(nonNVTrades.Sym, nonNVTrades);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="orderSummary"></param>
        /// <returns></returns>
        private TradeSummaryTO CreateTradeSummaryObject(OrderSummary orderSummary)
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
            data.IsSwap = orderSummary.IsSwap;

            //Executing Broker
            data.ExBkr = data.Dest;
            //if (!string.IsNullOrEmpty(data.Dest) && !"Non NV".Equals(data.Src))
            //{
            //    if (data.Dest.StartsWith("JPM", StringComparison.CurrentCultureIgnoreCase) || data.Dest.StartsWith("JP:", StringComparison.CurrentCultureIgnoreCase))
            //        data.ExBkr = "JPM";
            //    else if (data.Dest.StartsWith("JEF", StringComparison.CurrentCultureIgnoreCase))
            //        data.ExBkr = "JEFFERIES";
            //    else if (data.Dest.StartsWith("SCOTIA", StringComparison.CurrentCultureIgnoreCase))
            //        data.ExBkr = "SCOTIA";
            //    else if (data.Dest.StartsWith("JONE", StringComparison.CurrentCultureIgnoreCase))
            //        data.ExBkr = "JONE";
            //    else if (data.Dest.StartsWith("KBWI", StringComparison.CurrentCultureIgnoreCase))
            //        data.ExBkr = "KBW";
            //    else if (data.Dest.StartsWith("PIPR", StringComparison.CurrentCultureIgnoreCase))
            //        data.ExBkr = "PIPR";
            //    else if (data.Dest.StartsWith("JMP", StringComparison.CurrentCultureIgnoreCase))
            //        data.ExBkr = "JMP";
            //    else if (data.Dest.StartsWith("CLST", StringComparison.CurrentCultureIgnoreCase))
            //        data.ExBkr = "CLST";
            //    else if (data.Dest.StartsWith("PEEH", StringComparison.CurrentCultureIgnoreCase))
            //        data.ExBkr = "PEELHUNT";
            //    else if (data.Dest.StartsWith("PEEL", StringComparison.CurrentCultureIgnoreCase))
            //        data.ExBkr = "PEELHUNT";
            //    else
            //        data.ExBkr = data.Dest;
            //}
            //else
            //{
            //    data.ExBkr = data.Dest;
            //}

            //Traded Qty
            if ("Sell".Equals(data.Side, StringComparison.CurrentCultureIgnoreCase) || "Sell Short".Equals(data.Side) || "SSx".Equals(data.Side) || "SHRT".Equals(data.Side))
                data.Qty *= -1;

            ////OPP
            //if (jpmSecurityMarginDict.TryGetValue("OPP|" + data.ALMSym, out SecurityMarginDetail securityMarginDetail))
            //    data.FundOpp.MarginRtJPM = securityMarginDetail.MarginRate.GetValueOrDefault();
            ////TAC
            //if (jpmSecurityMarginDict.TryGetValue("TAC|" + data.ALMSym, out securityMarginDetail))
            //    data.FundTac.MarginRtJPM = securityMarginDetail.MarginRate.GetValueOrDefault();

            ////OPP
            //if (fidelitySecurityMarginDict.TryGetValue("OPP|" + data.ALMSym, out SecurityMargin securityMargin))
            //    data.FundOpp.MarginRtFido = securityMargin.MarginPct.GetValueOrDefault();
            ////TAC
            //if (fidelitySecurityMarginDict.TryGetValue("TAC|" + data.ALMSym, out securityMargin))
            //    data.FundTac.MarginRtFido = securityMargin.MarginPct.GetValueOrDefault();

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
            , IDictionary<string, FXRate> fxRatePDDict
            , double oppFundMV, double tacFundMV, double oppFundWt, double tacFundWt)
        {
            //Allocate Trade (Opportunity Fund & Tactical Fund) PRO-RATA
            string symbol = !string.IsNullOrEmpty(tradeData.ALMSym) ? tradeData.ALMSym : tradeData.Sym;

            AllocateTradeSummary(tradeData, oppFundWt);

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

                    if (latestFxRate == 1.0)
                    {
                        _logger.LogInformation("Missing FX value in Live FX rates feed for: " + currency + "/" + symbol);
                        if (fxRatePDDict.TryGetValue(currency, out fxRateFund))
                            latestFxRate *= fxRateFund.FXRatePD.GetValueOrDefault();
                        _logger.LogInformation("Applying previous day FX value for: " + currency + "/" + symbol);
                    }
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

            //Corporate Action Beta
            if (tradeData.CBeta.HasValue)
            {
                tradeData.FundOpp.AllocCBetaContr = tradeData.FundOpp.AllocMV * tradeData.CBeta;
                tradeData.FundTac.AllocCBetaContr = tradeData.FundTac.AllocMV * tradeData.CBeta;
            }

            //Duration
            if (tradeData.Dur.HasValue)
            {
                tradeData.FundOpp.AllocDurContr = tradeData.FundOpp.AllocMV * tradeData.Dur;
                tradeData.FundTac.AllocDurContr = tradeData.FundTac.AllocMV * tradeData.Dur;
            }

            //Total
            tradeData.MVLocal = tradeData.FundOpp.AllocMVLcl.GetValueOrDefault() + tradeData.FundTac.AllocMVLcl.GetValueOrDefault();
            tradeData.MV = tradeData.FundOpp.AllocMV.GetValueOrDefault() + tradeData.FundTac.AllocMV.GetValueOrDefault();
            tradeData.BetaContr = tradeData.FundOpp.AllocBetaContr.GetValueOrDefault() + tradeData.FundTac.AllocBetaContr.GetValueOrDefault();
            tradeData.CBetaContr = tradeData.FundOpp.AllocCBetaContr.GetValueOrDefault() + tradeData.FundTac.AllocCBetaContr.GetValueOrDefault();
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
                    summaryGroup.MVLocal = CommonUtils.AddNullableDoubles(summaryGroup.MVLocal, tradeData.MVLocal);
                    summaryGroup.MV = CommonUtils.AddNullableDoubles(summaryGroup.MV, tradeData.MV);
                    summaryGroup.BetaContr = CommonUtils.AddNullableDoubles(summaryGroup.BetaContr, tradeData.BetaContr);
                    summaryGroup.CBetaContr = CommonUtils.AddNullableDoubles(summaryGroup.CBetaContr, tradeData.CBetaContr);
                    summaryGroup.DurContr = CommonUtils.AddNullableDoubles(summaryGroup.DurContr, tradeData.DurContr);
                    //summaryGroup.SpotCAD = CommonUtils.AddNullableDoubles(summaryGroup.SpotCAD, tradeData.SpotCAD);
                    //summaryGroup.SpotGBP = CommonUtils.AddNullableDoubles(summaryGroup.SpotGBP, tradeData.SpotGBP);
                    //summaryGroup.SpotEUR = CommonUtils.AddNullableDoubles(summaryGroup.SpotEUR, tradeData.SpotEUR);
                    //summaryGroup.SpotAUD = CommonUtils.AddNullableDoubles(summaryGroup.SpotAUD, tradeData.SpotAUD);
                    //summaryGroup.FwdCAD = CommonUtils.AddNullableDoubles(summaryGroup.FwdCAD, tradeData.FwdCAD);
                    //summaryGroup.FwdGBP = CommonUtils.AddNullableDoubles(summaryGroup.FwdGBP, tradeData.FwdGBP);
                    //summaryGroup.FwdEUR = CommonUtils.AddNullableDoubles(summaryGroup.FwdEUR, tradeData.FwdEUR);
                    //summaryGroup.FwdAUD = CommonUtils.AddNullableDoubles(summaryGroup.FwdAUD, tradeData.FwdAUD);

                    summaryGroup.FundOpp.AllocBetaContr = CommonUtils.AddNullableDoubles(summaryGroup.FundOpp.AllocBetaContr, tradeData.FundOpp.AllocBetaContr);
                    summaryGroup.FundOpp.AllocCBetaContr = CommonUtils.AddNullableDoubles(summaryGroup.FundOpp.AllocCBetaContr, tradeData.FundOpp.AllocCBetaContr);
                    summaryGroup.FundTac.AllocMVLcl = CommonUtils.AddNullableDoubles(summaryGroup.FundOpp.AllocMVLcl, tradeData.FundOpp.AllocMVLcl);
                    summaryGroup.FundOpp.AllocMV = CommonUtils.AddNullableDoubles(summaryGroup.FundOpp.AllocMV, tradeData.FundOpp.AllocMV);

                    summaryGroup.FundTac.AllocBetaContr = CommonUtils.AddNullableDoubles(summaryGroup.FundTac.AllocBetaContr, tradeData.FundTac.AllocBetaContr);
                    summaryGroup.FundTac.AllocCBetaContr = CommonUtils.AddNullableDoubles(summaryGroup.FundTac.AllocCBetaContr, tradeData.FundTac.AllocCBetaContr);
                    summaryGroup.FundTac.AllocMVLcl = CommonUtils.AddNullableDoubles(summaryGroup.FundTac.AllocMVLcl, tradeData.FundTac.AllocMVLcl);
                    summaryGroup.FundTac.AllocMV = CommonUtils.AddNullableDoubles(summaryGroup.FundTac.AllocMV, tradeData.FundTac.AllocMV);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tradeData"></param>
        /// <param name="oppFundWt"></param>
        private void AllocateTradeSummary(TradeSummaryTO tradeData, double oppFundWt)
        {
            //allocate PRO-RATA
            int tradeQty = tradeData.Qty;
            tradeData.FundOpp.AllocPos = (int)(tradeQty * oppFundWt);
            tradeData.FundTac.AllocPos = tradeQty - tradeData.FundOpp.AllocPos;
            tradeData.AllocRule = "Pro-Rata";
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tradeData"></param>
        /// <param name="oppFundMV"></param>
        /// <param name="tacFundMV"></param>
        /// <param name="oppFundWt"></param>
        /// <param name="tacFundWt"></param>
        private void AllocateTradeSummaryNew(TradeSummaryTO tradeData
            , double oppFundMV, double tacFundMV, double oppFundWt, double tacFundWt)
        {
            //double oppClsSecWt = tradeData.FundOpp.SecWt.GetValueOrDefault();
            //double tacClsSecWt = tradeData.FundTac.SecWt.GetValueOrDefault();
            //double oppLiveSecWt = oppClsSecWt;
            //double tacLiveSecWt = tacClsSecWt;
            double oppLiveSecWt = tradeData.FundOpp.LiveSecWt.HasValue ? tradeData.FundOpp.LiveSecWt.GetValueOrDefault() : tradeData.FundOpp.SecWt.GetValueOrDefault();
            double tacLiveSecWt = tradeData.FundTac.LiveSecWt.HasValue ? tradeData.FundTac.LiveSecWt.GetValueOrDefault() : tradeData.FundTac.SecWt.GetValueOrDefault();

            //Order Qty
            int tradeQty = tradeData.Qty;

            //Order Side
            int orderSideId = 0;
            string orderSide = tradeData.Side;
            if (orderSide.Equals("B") || orderSide.Equals("BC") || orderSide.Equals("Buy", StringComparison.CurrentCultureIgnoreCase) || orderSide.Equals("Buy Cover"))
                orderSideId = 1;
            else if (orderSide.Equals("S") || orderSide.Equals("SS") || orderSide.Equals("Sell", StringComparison.CurrentCultureIgnoreCase)
                || orderSide.Equals("Sell Short") || orderSide.Equals("SSx") || orderSide.Equals("SHRT"))
                orderSideId = 2;
            tradeData.OrderSideId = orderSideId;

            //Security Price ($)
            double securityPrice = tradeData.Prc.GetValueOrDefault() * tradeData.Fx.GetValueOrDefault();

            //NEW POSITION - then allocate PRO-RATA
            if (oppLiveSecWt == 0 && tacLiveSecWt == 0)
            {
                //allocate PRO-RATA
                tradeData.FundOpp.AllocPos = (int)(tradeQty * oppFundWt);
                tradeData.FundTac.AllocPos = tradeQty - tradeData.FundOpp.AllocPos;
                tradeData.AllocRule = "New Position";
            }
            //BUY or BUY_COVER
            else if (orderSideId == 1)
            {
                if (oppLiveSecWt > tacLiveSecWt)
                {
                    int sharesToAdd = (int)(((oppLiveSecWt - tacLiveSecWt) * tacFundMV) / securityPrice);
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
                    int sharesToAdd = (int)(((tacLiveSecWt - oppLiveSecWt) * oppFundMV) / securityPrice);
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
                    int sharesToAdd = (int)(((oppLiveSecWt - tacLiveSecWt) * oppFundMV) / securityPrice);
                    if (tradeQty > sharesToAdd)
                    {
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
                else if (oppLiveSecWt < tacLiveSecWt)
                {
                    int sharesToAdd = (int)(((tacLiveSecWt - oppLiveSecWt) * tacFundMV) / securityPrice);
                    if (tradeQty > sharesToAdd)
                    {
                        tradeData.FundTac.AllocPos = sharesToAdd;
                        tradeQty -= sharesToAdd;

                        //allocate PRO-RATA
                        tradeData.FundOpp.AllocPos = (int)(tradeQty * oppFundWt);
                        //add to pre-allocated shares
                        tradeData.FundTac.AllocPos += (tradeQty - tradeData.FundOpp.AllocPos);
                    }
                    else
                    {
                        tradeData.FundTac.AllocPos = tradeQty;
                    }
                }
                tradeData.AllocRule = "Sell Wtd";
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fundCurrencyExpDict"></param>
        /// <param name="tradeData"></param>
        private void CalculateCurrencyExposures(IDictionary<string, FundCurrExpTO> fundCurrencyExpDict, TradeSummaryTO tradeData)
        {
            try
            {
                string symbol = !string.IsNullOrEmpty(tradeData.ALMSym) ? tradeData.ALMSym : tradeData.Sym;

                if (string.IsNullOrEmpty(tradeData.IsSwap))
                {
                    //Calculate Spot Exposures
                    if ("CAD".Equals(tradeData.Curr, StringComparison.CurrentCultureIgnoreCase))
                        tradeData.SpotCAD = tradeData.MV * -1.0;
                    else if ("GBX".Equals(tradeData.Curr, StringComparison.CurrentCultureIgnoreCase) || "GBP".Equals(tradeData.Curr, StringComparison.CurrentCultureIgnoreCase))
                        tradeData.SpotGBP = tradeData.MV * -1.0;
                    else if ("EUR".Equals(tradeData.Curr, StringComparison.CurrentCultureIgnoreCase))
                        tradeData.SpotEUR = tradeData.MV * -1.0;
                    else if ("AUD".Equals(tradeData.Curr, StringComparison.CurrentCultureIgnoreCase))
                        tradeData.SpotAUD = tradeData.MV * -1.0;


                    //Calculate Forward Exposures
                    if (fundCurrencyExpDict.TryGetValue(symbol, out FundCurrExpTO currExp))
                    {
                        tradeData.ExpCAD = currExp.CADExp;
                        tradeData.ExpGBP = currExp.GBPExp;
                        tradeData.ExpEUR = currExp.EURExp;
                        tradeData.ExpAUD = currExp.AUDExp;

                        if (currExp.CADExp.HasValue)
                            tradeData.FwdCAD = tradeData.MV * currExp.CADExp.GetValueOrDefault();
                        if (currExp.GBPExp.HasValue)
                            tradeData.FwdGBP = tradeData.MV * currExp.GBPExp.GetValueOrDefault();
                        if (currExp.EURExp.HasValue)
                            tradeData.FwdEUR = tradeData.MV * currExp.EURExp.GetValueOrDefault();
                        if (currExp.AUDExp.HasValue)
                            tradeData.FwdAUD = tradeData.MV * currExp.AUDExp.GetValueOrDefault();
                    }
                }
                else
                {
                    _logger.LogInformation("IsSwap Trade:" + tradeData.IsSwap + "/" + tradeData.Sym + "/" + tradeData.Qty);

                    //Calculate Forward Exposures
                    if (fundCurrencyExpDict.TryGetValue(symbol, out FundCurrExpTO currExp))
                    {
                        if ("CAD".Equals(tradeData.Curr) && currExp.USDExp.HasValue)
                            tradeData.FwdCAD = tradeData.MV * currExp.USDExp.GetValueOrDefault();
                        else if (("GBX".Equals(tradeData.Curr) || "GBP".Equals(tradeData.Curr)) && currExp.USDExp.HasValue)
                            tradeData.FwdGBP = tradeData.MV * currExp.USDExp.GetValueOrDefault();
                        else if ("EUR".Equals(tradeData.Curr) && currExp.USDExp.HasValue)
                            tradeData.FwdEUR = tradeData.MV * currExp.USDExp.GetValueOrDefault();
                        else if ("AUD".Equals(tradeData.Curr) && currExp.USDExp.HasValue)
                            tradeData.FwdAUD = tradeData.MV * currExp.USDExp.GetValueOrDefault();
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("Error calculating currency exposures for ticker: " + tradeData.Sym, ex);
            }
        }

        public void SaveTradeSummary()
        {
            try
            {
                OrderParameters parameters = new OrderParameters();
                parameters.Trader = "All";
                parameters.ALMTrader = "All";
                parameters.TimeFilter = "All";
                IList<TradeSummaryTO> list = GetDailyTradeSummary(parameters);
                _tradingDao.SaveTradeSummary(list);
            }
            catch (Exception ex)
            {
                _logger.LogError("Error saving Trade Summary", ex);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="list"></param>
        /// <returns></returns>
        public IList<TradeSummaryTO> AllocateTrades(IList<TradeSummaryTO> list)
        {
            IDictionary<string, double> fundMVDict = _cache.Get<IDictionary<string, double>>(CacheKeys.ALM_FUND_MARKET_VALUES);
            IDictionary<string, PositionMaster> positionDict = _commonOperations.GetPositions();
            IDictionary<string, FXRate> fxRateDict = _cache.Get<IDictionary<string, FXRate>>(CacheKeys.FX_RATES);
            IDictionary<string, string> securityDict = _cache.Get<IDictionary<string, string>>(CacheKeys.POSITION_IDENTIFIER_MAP);
            IDictionary<string, SecurityMasterExt> securityMasterExtDict = _cache.Get<IDictionary<string, SecurityMasterExt>>(CacheKeys.SECURITY_EXT_DETAILS);

            double totalFundMV = fundMVDict["All_MM"];
            double oppFundMV = fundMVDict["OPP_MM"];
            double tacFundMV = fundMVDict["TAC_MM"];

            //calculate fund weights
            double oppFundWt = oppFundMV / totalFundMV;
            double tacFundWt = tacFundMV / totalFundMV;

            foreach (TradeSummaryTO tradeData in list)
            {
                try
                {
                    string symbol = !string.IsNullOrEmpty(tradeData.ALMSym) ? tradeData.ALMSym : tradeData.BBGSym;
                    string tradingSymbol = tradeData.Sym;

                    //Position
                    PositionMaster positionMaster = CommonOperationsUtil.GetPositionDetails(symbol, tradeData.Sedol, tradeData.ISIN, positionDict, securityDict);
                    if (positionMaster != null)
                    {
                        tradeData.ShOut = positionMaster.ShOut;

                        //OPP Fund
                        if (positionMaster.FundOpp != null && positionMaster.FundOpp.PosHeld.HasValue)
                        {
                            tradeData.FundOpp.SecWt = positionMaster.FundOpp.ClsMVPct;
                            tradeData.FundOpp.BegMV = positionMaster.FundOpp.ClsMV.GetValueOrDefault() * 1000000.0;
                            tradeData.FundOpp.LivePos = positionMaster.FundOpp.LivePos.HasValue ? positionMaster.FundOpp.LivePos : positionMaster.FundOpp.PosHeld;
                            tradeData.FundOpp.LiveMV = (positionMaster.FundOpp.LiveMV.HasValue ? positionMaster.FundOpp.LiveMV : positionMaster.FundOpp.ClsMV).GetValueOrDefault() * 1000000.0;
                            tradeData.FundOpp.LiveSecWt = positionMaster.FundOpp.LiveMVPct.HasValue ? positionMaster.FundOpp.LiveMVPct : positionMaster.FundOpp.ClsMVPct;
                        }

                        //TAC Fund
                        if (positionMaster.FundTac != null && positionMaster.FundTac.PosHeld.HasValue)
                        {
                            tradeData.FundTac.SecWt = positionMaster.FundTac.ClsMVPct;
                            tradeData.FundTac.BegMV = positionMaster.FundTac.ClsMV.GetValueOrDefault() * 1000000.0;
                            tradeData.FundTac.LivePos = positionMaster.FundTac.LivePos.HasValue ? positionMaster.FundTac.LivePos : positionMaster.FundTac.PosHeld;
                            tradeData.FundTac.LiveMV = (positionMaster.FundTac.LiveMV.HasValue ? positionMaster.FundTac.LiveMV : positionMaster.FundTac.ClsMV).GetValueOrDefault() * 1000000.0;
                            tradeData.FundTac.LiveSecWt = positionMaster.FundTac.LiveMVPct.HasValue ? positionMaster.FundTac.LiveMVPct : positionMaster.FundTac.ClsMVPct;
                        }
                    }
                    else
                    {
                        _logger.LogInformation("New Position: " + symbol);
                    }

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

                    //Populate Securitty
                    SecurityMasterExt securityMasterExt = CommonOperationsUtil.GetSecurityMasterExt(symbol, tradingSymbol, tradeData.Sedol, tradeData.ISIN, securityMasterExtDict, securityDict);
                    if (securityMasterExt != null)
                        tradeData.Mult = securityMasterExt.Multiplier;

                    //Calculate Allocations
                    AllocateTradeSummaryNew(tradeData, oppFundMV, tacFundMV, oppFundWt, tacFundWt);

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

                    //New Weights
                    double orderSideMultiplier = 1.0;
                    if (tradeData.OrderSideId == 2)
                        orderSideMultiplier = -1.0;

                    double? liveOppMV = CommonUtils.AddNullableDoubles(tradeData.FundOpp.LiveMV, orderSideMultiplier * tradeData.FundOpp.AllocMV);
                    if (liveOppMV.HasValue)
                        tradeData.FundOpp.LiveSecWt = liveOppMV / oppFundMV;

                    double? liveTacMV = CommonUtils.AddNullableDoubles(tradeData.FundTac.LiveMV, orderSideMultiplier * tradeData.FundTac.AllocMV);
                    if (liveTacMV.HasValue)
                        tradeData.FundTac.LiveSecWt = liveTacMV / tacFundMV;

                    //Live Position
                    if (positionMaster != null)
                    {
                        positionMaster.FundOpp.LivePos = CommonUtils.AddNullableDoubles(tradeData.FundOpp.LivePos, orderSideMultiplier * tradeData.FundOpp.AllocPos);
                        positionMaster.FundTac.LivePos = CommonUtils.AddNullableDoubles(tradeData.FundTac.LivePos, orderSideMultiplier * tradeData.FundTac.AllocPos);
                        positionMaster.FundOpp.LiveMV = liveOppMV / 1000000.0;
                        positionMaster.FundTac.LiveMV = liveTacMV / 1000000.0;
                        positionMaster.FundOpp.LiveMVPct = tradeData.FundOpp.LiveSecWt;
                        positionMaster.FundTac.LiveMVPct = tradeData.FundTac.LiveSecWt;

                        tradeData.FundOpp.LivePos = positionMaster.FundOpp.LivePos;
                        tradeData.FundTac.LivePos = positionMaster.FundTac.LivePos;
                        tradeData.FundOpp.LiveMV = positionMaster.FundOpp.LiveMV.GetValueOrDefault() * 1000000.0;
                        tradeData.FundTac.LiveMV = positionMaster.FundTac.LiveMV.GetValueOrDefault() * 1000000.0;
                    }

                    //Security Wt
                    if (tradeData.FundOpp.LiveSecWt.HasValue || tradeData.FundTac.LiveSecWt.HasValue)
                    {
                        double oppSecurityWt = tradeData.FundOpp.LiveSecWt.GetValueOrDefault();
                        double tacSecurityWt = tradeData.FundTac.LiveSecWt.GetValueOrDefault();
                        if (Math.Abs(oppSecurityWt) - Math.Abs(tacSecurityWt) > 0.01)
                            tradeData.SecWtInd = 1;
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError("Error allocating trade: " + tradeData.ALMSym + "/" + tradeData.BBGSym, ex);
                }
            }
            return list;
        }
    }
}