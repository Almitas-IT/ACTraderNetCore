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
    public class TradeSummaryReportNew
    {
        private readonly ILogger<TradeSummaryReportNew> _logger;
        private readonly CachingService _cache;
        private readonly TradingDao _tradingDao;
        private readonly IConfiguration _configuration;
        private readonly CommonOperations _commonOperations;
        private Random _random;

        public TradeSummaryReportNew(ILogger<TradeSummaryReportNew> logger
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
            _random = new Random();
        }

        public void PopulateTradeSummary()
        {
            //_logger.LogInformation("PopulateTradeSummary - STARTED");
            IList<TradeSummaryReportTO> list = GetTradeSummary();
            _cache.Add(CacheKeys.TRADE_SUMMARY, list, DateTimeOffset.MaxValue);
            //_logger.LogInformation("PopulateTradeSummary - DONE");
        }

        public IList<TradeSummaryReportTO> GetTradeSummary()
        {
            IDictionary<string, TradeSummaryReportTO> dict = new Dictionary<string, TradeSummaryReportTO>();

            IDictionary<string, FundMaster> fundMasterDict = _cache.Get<IDictionary<string, FundMaster>>(CacheKeys.FUND_MASTER);
            IDictionary<string, FundPortDate> fundPortDateDict = _cache.Get<IDictionary<string, FundPortDate>>(CacheKeys.FUND_PORT_DATES);
            IDictionary<string, double> fundMVDict = _cache.Get<IDictionary<string, double>>(CacheKeys.ALM_FUND_MARKET_VALUES);
            IDictionary<string, PositionMaster> positionDict = _cache.Get<IDictionary<string, PositionMaster>>(CacheKeys.POSITION_MASTER);
            IDictionary<string, FXRate> fxRateDict = _cache.Get<IDictionary<string, FXRate>>(CacheKeys.FX_RATES);
            IDictionary<string, FXRate> fxRatePDDict = _cache.Get<IDictionary<string, FXRate>>(CacheKeys.FX_RATES_PD);
            IDictionary<string, string> securityDict = _cache.Get<IDictionary<string, string>>(CacheKeys.POSITION_IDENTIFIER_MAP);
            IDictionary<string, SecurityMasterExt> securityMasterExtDict = _cache.Get<IDictionary<string, SecurityMasterExt>>(CacheKeys.SECURITY_EXT_DETAILS);
            IDictionary<string, SecurityMarginDetail> jpmSecurityMarginDict = _cache.Get<IDictionary<string, SecurityMarginDetail>>(CacheKeys.JPM_SECURITY_MARGIN_RATES);
            IDictionary<string, FundCurrExpTO> fundCurrencyExpDict = _cache.Get<IDictionary<string, FundCurrExpTO>>(CacheKeys.FUND_CURRENCY_EXPOSURES);
            IDictionary<string, SecurityRiskFactor> securityRiskFactorDict = _cache.Get<IDictionary<string, SecurityRiskFactor>>(CacheKeys.SECURITY_RISK_FACTORS_WITH_DATES);

            int sortId = 20;
            DateTime currentTime = DateTime.Now;

            IList<OrderSummary> tradeList;
            if (_configuration["ConnectionStrings:ENV"].Equals("PROD"))
                tradeList = _tradingDao.GetCompletedOrders(DateTime.Today);
            else
                //tradeList = _tradingDao.GetCompletedOrders(DateTime.Today);
                tradeList = _tradingDao.GetCompletedOrders(DateTime.Now.AddDays(-4));

            //add custom groups
            AddCustomTradeGroups(dict);

            foreach (OrderSummary orderSummary in tradeList)
            {
                string symbol = !string.IsNullOrEmpty(orderSummary.ALMSym) ? orderSummary.ALMSym : orderSummary.Sym;
                string tradingSymbol = orderSummary.Sym;

                //Trade
                TradeSummaryReportTO tradeData = CreateTradeSummaryObject(orderSummary);

                //Position
                PositionMaster positionMaster = CommonOperationsUtil.GetPositionDetails(symbol, tradeData.Sedol, tradeData.ISIN, positionDict, securityDict);
                if (positionMaster != null)
                {
                    tradeData.ShOut = positionMaster.ShOut;
                    tradeData.AltSym = positionMaster.Ticker;

                }
                //else
                //{
                //    _logger.LogInformation("New Position: " + symbol);
                //}

                //Risk Beta, Duration...
                SecurityMasterExt securityMasterExt = CommonOperationsUtil.GetSecurityMasterExt(symbol, tradingSymbol, tradeData.Sedol, tradeData.ISIN, securityMasterExtDict, securityDict);
                if (securityMasterExt != null)
                {
                    //tradeData.Beta = securityMasterExt.RiskBeta * _random.NextDouble();
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
                CalculateTradeSummary(tradeData, fxRateDict, fxRatePDDict);

                //NV
                PopulateTradeSummaryGroup(dict["NV"], tradeData, "NV");
                PopulateTradeSummaryGroup(dict["EMSX"], tradeData, "EMSX");
                PopulateTradeSummaryGroup(dict["Non NV"], tradeData, "Non NV");
                PopulateTradeSummaryGroup(dict["Total"], tradeData, "Total");

                tradeData.SortId = sortId;
                sortId++;

                string id = tradeData.Sym + "|" + tradeData.Qty + "|" + tradeData.Prc.GetValueOrDefault() + "|" + tradeData.ExBkr;
                tradeData.Id = id;

                if (!dict.TryGetValue(id, out TradeSummaryReportTO tradeDataTemp))
                    dict.Add(id, tradeData);
                //else
                //    _logger.LogInformation("Id already exists: " + id);
            }

            IList<TradeSummaryReportTO> list = dict.Values.ToList<TradeSummaryReportTO>();
            return list.OrderBy(x => x.SortId).ThenBy(x => x.Sym).ToList<TradeSummaryReportTO>();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="dict"></param>
        private void AddCustomTradeGroups(IDictionary<string, TradeSummaryReportTO> dict)
        {
            //All Trades
            TradeSummaryReportTO allTrades = new TradeSummaryReportTO();
            allTrades.Id = "Total";
            allTrades.Sym = "Total";
            allTrades.ALMSym = "Total";
            allTrades.SortId = 1;
            allTrades.IsGrpRow = 1;
            dict.Add(allTrades.Id, allTrades);

            //Neovest Trades
            TradeSummaryReportTO nvTrades = new TradeSummaryReportTO();
            nvTrades.Id = "NV";
            nvTrades.Sym = "NV";
            nvTrades.ALMSym = "NV";
            nvTrades.SortId = 2;
            nvTrades.IsGrpRow = 1;
            dict.Add(nvTrades.Id, nvTrades);

            //EMSX Trades
            TradeSummaryReportTO emsxTrades = new TradeSummaryReportTO();
            emsxTrades.Id = "EMSX";
            emsxTrades.Sym = "EMSX";
            emsxTrades.ALMSym = "EMSX";
            emsxTrades.SortId = 3;
            emsxTrades.IsGrpRow = 1;
            dict.Add(emsxTrades.Id, emsxTrades);

            //Non-Neovest Trades
            TradeSummaryReportTO nonNVTrades = new TradeSummaryReportTO();
            nonNVTrades.Id = "Non NV";
            nonNVTrades.Sym = "Non NV";
            nonNVTrades.ALMSym = "Non NV";
            nonNVTrades.SortId = 4;
            nonNVTrades.IsGrpRow = 1;
            dict.Add(nonNVTrades.Id, nonNVTrades);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="orderSummary"></param>
        /// <returns></returns>
        private TradeSummaryReportTO CreateTradeSummaryObject(OrderSummary orderSummary)
        {
            TradeSummaryReportTO data = new TradeSummaryReportTO();
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

            return data;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tradeData"></param>
        /// <param name="fxRateDict"></param>
        /// <param name="fxRatePDDict"></param>
        private void CalculateTradeSummary(TradeSummaryReportTO tradeData
            , IDictionary<string, FXRate> fxRateDict
            , IDictionary<string, FXRate> fxRatePDDict)
        {
            string symbol = !string.IsNullOrEmpty(tradeData.ALMSym) ? tradeData.ALMSym : tradeData.Sym;

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
                        _logger.LogInformation("Missing FX value in live FX rates feed for: " + currency + "/" + symbol);
                        if (fxRatePDDict.TryGetValue(currency, out fxRateFund))
                            latestFxRate *= fxRateFund.FXRatePD.GetValueOrDefault();
                        _logger.LogInformation("Applying previous day FX value for: " + currency + "/" + symbol);
                    }
                }
            }
            tradeData.Fx = latestFxRate;
            tradeData.MVLocal = tradeData.Qty * tradeData.Prc;
            if (tradeData.Mult.HasValue)
                tradeData.MVLocal *= tradeData.Mult;
            tradeData.MV = tradeData.MVLocal * latestFxRate;
            if (tradeData.Beta.HasValue)
                tradeData.BetaContr = tradeData.MV * tradeData.Beta;
            if (tradeData.CBeta.HasValue)
                tradeData.CBetaContr = tradeData.MV * tradeData.CBeta;
            if (tradeData.Dur.HasValue)
                tradeData.DurContr = tradeData.MV * tradeData.Dur;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="summaryGroup"></param>
        /// <param name="tradeData"></param>
        /// <param name="source"></param>
        private void PopulateTradeSummaryGroup(TradeSummaryReportTO summaryGroup, TradeSummaryReportTO tradeData, string source)
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
                }
            }
        }
    }
}