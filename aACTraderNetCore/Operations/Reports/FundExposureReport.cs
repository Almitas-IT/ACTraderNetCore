using aACTrader.DAO.Repository;
using aCommons;
using aCommons.Fund;
using LazyCache;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;

namespace aACTrader.Operations.Reports
{
    public class FundExposureReport
    {
        private readonly ILogger<FundExposureReport> _logger;
        private readonly CachingService _cache;
        private readonly FundDao _fundDao;
        private readonly HoldingsDao _holdingsDao;
        private readonly IConfiguration _configuration;

        private const string GRP_FUND_MV = "Total Fund Market Value";
        private const string GRP_LONG_CASH = "Long Cash Ex Crypto, Futures & Currency";
        private const string GRP_SHORT_CASH = "Short Cash Ex Crypto, Futures & Currency";
        private const string GRP_LONG_SWAP = "Long Swap Notional Ex Crypto";
        private const string GRP_SHORT_SWAP = "Short Swap Notional Ex Crypto";
        private const string GRP_LONG_FUTURES = "Long Futures Notional Ex Crypto";
        private const string GRP_SHORT_FUTURES = "Short Futures Notional Ex Crypto";
        private const string GRP_LONG_CRYPTO = "Long Crypto Incl Cash & Futures";
        private const string GRP_SHORT_CRYPTO = "Short Crypto Incl Cash & Futures";
        private const string GRP_CURRENCY = "Currency Fx & Currency Futures";
        private const string GRP_LONG_FUTURES_ALL = "Long Futures";
        private const string GRP_SHORT_FUTURES_ALL = "Short Futures";
        private const string GRP_GROSS_FUTURES = "Gross Futures";
        private const string GRP_NET_FUTURES = "Net Futures";
        private const string GRP_NET_EXP = "Net Exposure";
        private const string GRP_NET_EXP_PCT = "Net Exposure (%)";
        private const string GRP_GROSS_EXP = "Gross Exposure";
        private const string GRP_GROSS_EXP_PCT = "Gross Exposure (%)";
        private const string GRP_GROSS_EX_CRYPTO_EXP = "Gross Exposure Ex-Crypto";
        private const string GRP_GROSS_EX_CRYPTO_EXP_PCT = "Gross Exposure Ex-Crypto (%)";
        private const string GRP_GROSS_LONG_EXP = "Gross Long";
        private const string GRP_GROSS_SHORT_EXP = "Gross Short";
        private const string GRP_EMPTY = " ";
        private const string GRP_EMPTY_1 = "  ";
        private const string GRP_EMPTY_2 = "   ";
        private const string GRP_EMPTY_3 = "    ";

        private Dictionary<string, int> sectorExposureDict = new Dictionary<string, int>(StringComparer.CurrentCultureIgnoreCase)
        {
            {"Total", 10},
            {"Fixed Income", 20},
            {"Fixed Income/USA", 30},
            {"Fixed Income/USA/Preferred Stocks", 40},
            {"Fixed Income/USA/CEF", 50},
            {"Fixed Income/USA/Debt and Others", 60},
            {"Fixed Income/USA/Business Development Companies", 70},
            {"Fixed Income/USA/Mortgage Reits", 80},
            {"Fixed Income/USA/Holding Companies and Other Equities", 90},
            {"Fixed Income/USA/TBA Mortgages", 100},
            {"Fixed Income/USA/US Fixed Income Futures", 110},
            {"Fixed Income/International", 150},
            {"Fixed Income/International/CEF", 160},
            {"Fixed Income/International/Preferred Stocks", 170},
            {"Equity", 300},
            {"Equity/USA", 310},
            {"Equity/USA/CEF", 320},
            {"Equity/USA/Business Development Companies", 330},
            {"Equity/USA/Holding Companies and Other Equities", 340},
            {"Equity/USA/Preferred Stocks", 350},
            {"Equity/USA/Property Reit", 360},
            {"Equity/USA/US Equity Futures", 370},
            {"Equity/USA/Equity Options", 380},
            {"Equity/USA/Equity Futures", 390},
            {"Equity/International", 500},
            {"Equity/International/CEF", 510},
            {"Equity/International/Business Development Companies", 520},
            {"Equity/International/Holding Companies and Other Equities", 530},
            {"Equity/International/Preferred Stocks", 540},
            {"Equity/International/Property Reit", 550},
            {"Equity/International/US Equity Futures", 560},
            {"Equity/International/Equity Options", 570},
            {"Equity/International/Equity Futures", 580},
            {"Crypto Currency", 700},
            {"Crypto Currency/Crypto Currency", 710},
            {"Crypto Currency/Crypto Currency/Closed-End Fund", 720},
            {"Crypto Currency/Crypto Currency/Grantor Trust", 730},
            {"Crypto Currency/Crypto Currency/Crypto Currency Hedges", 740},
            {"Cash", 1000},
            {"Cash/Cash", 1100},
            {"Cash/Cash/Cash", 1200},
            {"Other", 2000},
        };

        // This grouping is as per Quarterly Presentation reports for clients
        private Dictionary<string, int> customSectorExposureGroupingDict = new Dictionary<string, int>(StringComparer.CurrentCultureIgnoreCase)
        {
            {"Total", 10},
            {"Fixed Income", 20},
            {"Fixed Income/USA", 30},
            {"Fixed Income/USA/Preferred Stocks", 40},
            {"Fixed Income/USA/CEF", 50},
            {"Fixed Income/USA/CEF/US", 60},
            {"Fixed Income/USA/CEF/London", 70},
            {"Fixed Income/USA/Debt and Others", 80},
            {"Fixed Income/USA/Business Development Companies", 90},
            {"Fixed Income/USA/Mortgage Reits", 100},
            {"Fixed Income/USA/Holding Companies and Other Equities", 110},
            {"Fixed Income/USA/US Fixed Income Futures", 120},
            {"Fixed Income/International", 200},
            {"Fixed Income/International/CEF", 210},
            {"Fixed Income/International/CEF/London", 220},
            {"Fixed Income/International/CEF/US", 230},
            {"Fixed Income/International/CEF/Canada", 240},
            {"Fixed Income/International/CEF/Australia", 250},
            {"Fixed Income/International/CEF/Amsterdam", 260},
            {"Fixed Income/International/Preferred Stocks", 270},
            {"Equity", 400},
            {"Equity/USA", 410},
            {"Equity/USA/CEF", 420},
            {"Equity/USA/CEF/US", 430},
            {"Equity/USA/CEF/London", 440},
            {"Equity/USA/CEF/Amsterdam", 450},
            {"Equity/USA/CEF/Australia", 460},
            {"Equity/USA/Preferred Stocks", 470},
            {"Equity/USA/US Equity Futures", 480},
            {"Equity/International", 600},
            {"Equity/International/CEF", 610},
            {"Equity/International/CEF/London", 620},
            {"Equity/International/CEF/US", 630},
            {"Equity/International/CEF/Canada", 640},
            {"Equity/International/CEF/Amsterdam", 650},
            {"Equity/International/CEF/Switzerland", 660},
            {"Equity/International/CEF/Australia", 670},
            {"Equity/International/CEF/Italy", 680},
            {"Equity/International/CEF/Hond Kong", 690},
            {"Equity/International/Holding Companies and Other Equities", 700},
            {"Equity/International/Property Reit", 710},
            {"Equity/International/Equity Futures", 720},
            {"Crypto Currency", 900},
            {"Crypto Currency/Crypto Currency", 910},
            {"Crypto Currency/Crypto Currency/Closed-End Fund", 920},
            {"Crypto Currency/Crypto Currency/Grantor Trust", 930},
            {"Crypto Currency/Crypto Currency/Crypto Currency Hedges", 940},
            {"Cash", 2000},
            {"Cash/Cash", 2100},
            {"Cash/Cash/Cash", 2200},
            {"Other", 2300},
        };

        /// <summary>
        /// 
        /// </summary>
        /// <param name="logger"></param>
        /// <param name="cache"></param>
        /// <param name="fundDao"></param>
        /// <param name="configuration"></param>
        public FundExposureReport(ILogger<FundExposureReport> logger
            , CachingService cache
            , FundDao fundDao
            , HoldingsDao holdingsDao
            , IConfiguration configuration)
        {
            _logger = logger;
            _cache = cache;
            _fundDao = fundDao;
            _holdingsDao = holdingsDao;
            _configuration = configuration;
        }

        public IList<FundExposureSummaryTO> GetFundSectorExposureReport(string asofDate)
        {
            IList<SecurityPerformance> positions = _fundDao.GetDailyPnLDetailsWithClassifications(asofDate);
            IDictionary<string, FundExposureSummaryTO> fundExposureDict = new Dictionary<string, FundExposureSummaryTO>();

            FundExposureSummaryTO totalSummary = null;

            int sortId = 1;
            foreach (SecurityPerformance data in positions)
            {
                if (data.EndMV.HasValue)
                {
                    string level1 = data.Level1;
                    string level2 = data.Level2;
                    string level3 = data.Level3;

                    //All Positions
                    string grpName = "Total";
                    if (sectorExposureDict.ContainsKey(grpName))
                        sortId = sectorExposureDict[grpName];
                    totalSummary = GetGroup(fundExposureDict, grpName, sortId, grpName);
                    AddPositionExposure(totalSummary, data);

                    //Level 1
                    grpName = level1;
                    sortId = sectorExposureDict["Other"];
                    if (sectorExposureDict.ContainsKey(grpName))
                        sortId = sectorExposureDict[grpName];
                    string displayName = "-" + level1;
                    FundExposureSummaryTO summary = GetGroup(fundExposureDict, grpName, sortId, displayName);
                    AddPositionExposure(summary, data);
                    AddPosition(summary, data);

                    //Level 2
                    if (!string.IsNullOrEmpty(level2))
                    {
                        grpName = level1 + "/" + level2;
                        sortId = sectorExposureDict["Other"];
                        if (sectorExposureDict.ContainsKey(grpName))
                            sortId = sectorExposureDict[grpName];
                        displayName = "--" + level2;
                        summary = GetGroup(fundExposureDict, grpName, sortId, displayName);
                        AddPositionExposure(summary, data);
                        AddPosition(summary, data);
                    }

                    //Level 3
                    if (!string.IsNullOrEmpty(level3))
                    {
                        grpName = level1 + "/" + level2 + "/" + level3;
                        sortId = sectorExposureDict["Other"];
                        if (sectorExposureDict.ContainsKey(grpName))
                            sortId = sectorExposureDict[grpName];
                        displayName = "----" + level3;
                        summary = GetGroup(fundExposureDict, grpName, sortId, displayName);
                        AddPositionExposure(summary, data);
                        AddPosition(summary, data);

                        if ("CEF".Equals(level3, StringComparison.CurrentCultureIgnoreCase)
                            && !string.IsNullOrEmpty(data.ExchCode))
                        {
                            grpName = level1 + "/" + level2 + "/" + level3 + "/" + data.ExchCode;
                            //sortId = sectorExposureDict["Other"];
                            //if (sectorExposureDict.ContainsKey(grpName))
                            //sortId = sectorExposureDict[grpName];
                            sortId++;
                            displayName = "--------" + data.ExchCode;
                            summary = GetGroup(fundExposureDict, grpName, sortId, displayName);
                            AddPositionExposure(summary, data);
                            AddPosition(summary, data);
                        }
                    }
                }
            }

            IList<FundExposureSummaryTO> list = fundExposureDict.Values.ToList<FundExposureSummaryTO>();
            foreach (FundExposureSummaryTO data in list)
            {
                if (totalSummary != null && totalSummary.OppValue > 0)
                {
                    data.OppWt = data.OppValue / totalSummary.OppValue;
                    data.OppCashWt = data.OppCashValue / totalSummary.OppValue;
                    data.OppSwapWt = data.OppSwapValue / totalSummary.OppValue;
                }

                if (totalSummary != null && totalSummary.TacValue > 0)
                {
                    data.TacWt = data.TacValue / totalSummary.TacValue;
                    data.TacCashWt = data.TacCashValue / totalSummary.TacValue;
                    data.TacSwapWt = data.TacSwapValue / totalSummary.TacValue;
                }
            }

            return list.OrderBy(x => x.SortId).ToList<FundExposureSummaryTO>();
        }

        public IList<FundExposureSummaryTO> GetFundSectorExposureReportNew(string asofDate)
        {
            IList<SecurityPerformance> positions = _fundDao.GetDailyPnLDetailsWithClassifications(asofDate);
            IDictionary<string, FundExposureSummaryTO> fundExposureDict = new Dictionary<string, FundExposureSummaryTO>();

            FundExposureSummaryTO totalSummary = null;

            int sortId = 1;
            foreach (SecurityPerformance data in positions)
            {
                if (data.EndMV.HasValue)
                {
                    string level1 = data.CustomLevel1;
                    string level2 = data.CustomLevel2;
                    string level3 = data.CustomLevel3;
                    string level4 = data.CustomLevel4;

                    //All Positions
                    string grpName = "Total";
                    if (customSectorExposureGroupingDict.ContainsKey(grpName))
                        sortId = customSectorExposureGroupingDict[grpName];
                    totalSummary = GetGroup(fundExposureDict, grpName, sortId, grpName);
                    AddPositionExposure(totalSummary, data);

                    //Level 1
                    grpName = level1;
                    sortId = customSectorExposureGroupingDict["Other"];
                    if (customSectorExposureGroupingDict.ContainsKey(grpName))
                        sortId = customSectorExposureGroupingDict[grpName];
                    string displayName = "-" + level1;
                    FundExposureSummaryTO summary = GetGroup(fundExposureDict, grpName, sortId, displayName);
                    AddPositionExposure(summary, data);
                    AddPosition(summary, data);

                    //Level 2
                    if (!string.IsNullOrEmpty(level2))
                    {
                        grpName = level1 + "/" + level2;
                        sortId = customSectorExposureGroupingDict["Other"];
                        if (customSectorExposureGroupingDict.ContainsKey(grpName))
                            sortId = customSectorExposureGroupingDict[grpName];
                        displayName = "--" + level2;
                        summary = GetGroup(fundExposureDict, grpName, sortId, displayName);
                        AddPositionExposure(summary, data);
                        AddPosition(summary, data);
                    }

                    //Level 3
                    if (!string.IsNullOrEmpty(level3))
                    {
                        grpName = level1 + "/" + level2 + "/" + level3;
                        sortId = customSectorExposureGroupingDict["Other"];
                        if (customSectorExposureGroupingDict.ContainsKey(grpName))
                            sortId = customSectorExposureGroupingDict[grpName];
                        displayName = "----" + level3;
                        summary = GetGroup(fundExposureDict, grpName, sortId, displayName);
                        AddPositionExposure(summary, data);
                        AddPosition(summary, data);
                    }

                    //Level 4
                    if (!string.IsNullOrEmpty(level4))
                    {
                        grpName = level1 + "/" + level2 + "/" + level3 + "/" + level4;
                        sortId = customSectorExposureGroupingDict["Other"];
                        if (customSectorExposureGroupingDict.ContainsKey(grpName))
                            sortId = customSectorExposureGroupingDict[grpName];
                        displayName = "----" + level4;
                        summary = GetGroup(fundExposureDict, grpName, sortId, displayName);
                        AddPositionExposure(summary, data);
                        AddPosition(summary, data);
                    }
                }
            }

            IList<FundExposureSummaryTO> list = fundExposureDict.Values.ToList<FundExposureSummaryTO>();
            foreach (FundExposureSummaryTO data in list)
            {
                if (totalSummary != null && totalSummary.OppValue > 0)
                {
                    data.OppWt = data.OppValue / totalSummary.OppValue;
                    data.OppCashWt = data.OppCashValue / totalSummary.OppValue;
                    data.OppSwapWt = data.OppSwapValue / totalSummary.OppValue;
                }

                if (totalSummary != null && totalSummary.TacValue > 0)
                {
                    data.TacWt = data.TacValue / totalSummary.TacValue;
                    data.TacCashWt = data.TacCashValue / totalSummary.TacValue;
                    data.TacSwapWt = data.TacSwapValue / totalSummary.TacValue;
                }
            }

            return list.OrderBy(x => x.SortId).ToList<FundExposureSummaryTO>();
        }

        private FundExposureSummaryTO GetGroup(IDictionary<string, FundExposureSummaryTO> fundExposureDict, string grpName, int sortId, string displayName)
        {
            if (!fundExposureDict.TryGetValue(grpName, out FundExposureSummaryTO summary))
            {
                summary = new FundExposureSummaryTO(grpName, sortId++);
                summary.DispName = displayName;
                fundExposureDict.Add(grpName, summary);
            }
            return summary;
        }

        public IList<FundExposureSummaryTO> GetFundExposures(string asofDate)
        {
            IList<SecurityPerformance> positions = _fundDao.GetDailyPnLDetails(asofDate);
            IDictionary<string, FundExposureSummaryTO> fundExposureDict = new Dictionary<string, FundExposureSummaryTO>();

            CreateGroups(fundExposureDict);

            foreach (SecurityPerformance data in positions)
            {
                //if (data.EndMV.GetValueOrDefault() > 0) data.IsLong = 1; else data.IsLong = 0;
                if (data.Position.GetValueOrDefault() > 0) data.IsLong = 1; else data.IsLong = 0;

                data.IsCrypto = 0;
                data.IsSwap = 0;
                data.IsCurrency = 0;
                data.IsFuture = 0;

                if (!string.IsNullOrEmpty(data.BroadSector))
                {
                    if ("Crypto Currency".Equals(data.BroadSector, StringComparison.CurrentCultureIgnoreCase))
                        data.IsCrypto = 1;
                }

                if (!string.IsNullOrEmpty(data.Ticker))
                {
                    if (data.Ticker.StartsWith("Swap", StringComparison.CurrentCultureIgnoreCase))
                        data.IsSwap = 1;
                }

                //if (data.IsCrypto == 0 && data.IsSwap == 0 && !string.IsNullOrEmpty(data.SecurityType))
                //{
                //    if (data.SecurityType.Contains("Future", StringComparison.CurrentCultureIgnoreCase))
                //        data.IsFuture = 1;
                //}

                if (!string.IsNullOrEmpty(data.SecurityType))
                {
                    if (data.SecurityType.Contains("Future", StringComparison.CurrentCultureIgnoreCase))
                        data.IsFuture = 1;
                }

                if (data.IsSwap == 0)
                {
                    if (!string.IsNullOrEmpty(data.BroadSector))
                    {
                        if ("Cash".Equals(data.BroadSector, StringComparison.CurrentCultureIgnoreCase))
                            data.IsCurrency = 1;
                    }
                    else if ("USD".Equals(data.Ticker, StringComparison.CurrentCultureIgnoreCase) || "Fx forwards".Equals(data.Ticker, StringComparison.CurrentCultureIgnoreCase))
                    {
                        data.IsCurrency = 1;
                    }
                }

                //Total Fund Market Value
                FundExposureSummaryTO summary = fundExposureDict[GRP_FUND_MV];
                AddPositionExposure(summary, data);
                AddPosition(summary, data);

                //Long Cash Ex Crypto, Futures & Currency
                if (data.IsLong == 1 && data.IsCrypto == 0 && data.IsSwap == 0 && data.IsFuture == 0 && data.IsCurrency == 0)
                {
                    summary = fundExposureDict[GRP_LONG_CASH];
                    AddPositionExposure(summary, data);
                    AddPosition(summary, data);
                }

                //Short Cash Ex Crypto, Futures & Currency
                if (data.IsLong == 0 && data.IsCrypto == 0 && data.IsSwap == 0 && data.IsFuture == 0 && data.IsCurrency == 0)
                {
                    summary = fundExposureDict[GRP_SHORT_CASH];
                    AddPositionExposure(summary, data);
                    AddPosition(summary, data);
                }

                //Long Swap Notional Ex Crypto
                if (data.IsLong == 1 && data.IsCrypto == 0 && data.IsSwap == 1)
                {
                    summary = fundExposureDict[GRP_LONG_SWAP];
                    AddPositionExposure(summary, data);
                    AddPosition(summary, data);
                }

                //Short Swap Notional Ex Crypto
                if (data.IsLong == 0 && data.IsCrypto == 0 && data.IsSwap == 1)
                {
                    summary = fundExposureDict[GRP_SHORT_SWAP];
                    AddPositionExposure(summary, data);
                    AddPosition(summary, data);
                }

                //Long Futures Notional Ex Crypto
                if (data.IsLong == 1 && data.IsCrypto == 0 && data.IsSwap == 0 && data.IsFuture == 1)
                {
                    summary = fundExposureDict[GRP_LONG_FUTURES];
                    AddPositionExposure(summary, data);
                    AddPosition(summary, data);
                }

                //Short Futures Notional Ex Crypto
                if (data.IsLong == 0 && data.IsCrypto == 0 && data.IsSwap == 0 && data.IsFuture == 1)
                {
                    summary = fundExposureDict[GRP_SHORT_FUTURES];
                    AddPositionExposure(summary, data);
                    AddPosition(summary, data);
                }

                //Long Crypto Incl Cash & Futures
                if (data.IsLong == 1 && data.IsCrypto == 1)
                {
                    summary = fundExposureDict[GRP_LONG_CRYPTO];
                    AddPositionExposure(summary, data);
                    AddPosition(summary, data);
                }

                //Short Crypto Incl Cash & Futures
                if (data.IsLong == 0 && data.IsCrypto == 1)
                {
                    summary = fundExposureDict[GRP_SHORT_CRYPTO];
                    AddPositionExposure(summary, data);
                    AddPosition(summary, data);
                }

                //Currency Fx & Currency Futures
                if (data.IsCurrency == 1)
                {
                    summary = fundExposureDict[GRP_CURRENCY];
                    AddPositionExposure(summary, data);
                    AddPosition(summary, data);
                }

                //Long Futures (All)
                if (data.IsLong == 1 && data.IsSwap == 0 && data.IsFuture == 1)
                {
                    summary = fundExposureDict[GRP_LONG_FUTURES_ALL];
                    AddPositionExposure(summary, data);
                    AddPosition(summary, data);
                }

                //Short Futures (All)
                if (data.IsLong == 0 && data.IsSwap == 0 && data.IsFuture == 1)
                {
                    summary = fundExposureDict[GRP_SHORT_FUTURES_ALL];
                    AddPositionExposure(summary, data);
                    AddPosition(summary, data);
                }

                //Net Position
                if (data.IsCurrency == 0)
                {
                    summary = fundExposureDict[GRP_NET_EXP];
                    AddPositionExposure(summary, data);
                }

                //Gross Long
                if (data.IsLong == 1 && data.IsCurrency == 0)
                {
                    summary = fundExposureDict[GRP_GROSS_LONG_EXP];
                    AddPositionExposure(summary, data);
                }

                //Gross Short
                if (data.IsLong == 0 && data.IsCurrency == 0)
                {
                    summary = fundExposureDict[GRP_GROSS_SHORT_EXP];
                    AddPositionExposure(summary, data);
                }
            }

            AddGrossNetExposures(fundExposureDict);

            IList<FundExposureSummaryTO> list = fundExposureDict.Values.ToList<FundExposureSummaryTO>();
            return list.OrderBy(x => x.SortId).ToList<FundExposureSummaryTO>();
        }

        private void AddPositionExposure(FundExposureSummaryTO summary, SecurityPerformance pos)
        {
            double endMV = pos.EndMV.GetValueOrDefault();
            summary.TotValue += endMV;
            if (pos.FundType.Equals("Opp", StringComparison.CurrentCultureIgnoreCase))
            {
                summary.OppValue += endMV;
                if (!string.IsNullOrEmpty(pos.Swap) && "Y".Equals(pos.Swap))
                    summary.OppSwapValue += endMV;
                if (!string.IsNullOrEmpty(pos.Swap) && "N".Equals(pos.Swap))
                    summary.OppCashValue += endMV;
            }
            if (pos.FundType.Equals("Tac", StringComparison.CurrentCultureIgnoreCase))
            {
                summary.TacValue += endMV;
                if (!string.IsNullOrEmpty(pos.Swap) && "Y".Equals(pos.Swap))
                    summary.TacSwapValue += endMV;
                if (!string.IsNullOrEmpty(pos.Swap) && "N".Equals(pos.Swap))
                    summary.TacCashValue += endMV;
            }
            summary.DtPeriod = pos.DatePeriod;
        }

        private void AddPosition(FundExposureSummaryTO summary, SecurityPerformance pos)
        {
            PositionDetailTO posTO = new PositionDetailTO();
            posTO.Ticker = pos.Ticker;
            posTO.YKey = pos.YellowKey;
            posTO.Fund = pos.FundType;
            posTO.Curr = pos.Currency;
            posTO.SecType = pos.SecurityType;
            posTO.Sector = pos.Sector;
            posTO.BrdSector = pos.BroadSector;
            posTO.Pos = pos.Position;
            posTO.BegMV = pos.BegMV;
            posTO.EndMV = pos.EndMV;
            posTO.ClsPrc = pos.ClosePrice;
            posTO.LastPrc = pos.LastPrice;
            posTO.ClsFx = pos.CloseFXRate;
            posTO.LastFx = pos.LastFXRate;
            posTO.PnL = pos.PnLUSD;
            posTO.TrdPnL = pos.TradingPnL;
            posTO.TrdType = pos.TradeType;
            posTO.IsLong = pos.IsLong;
            posTO.IsCrypto = pos.IsCrypto;
            posTO.IsSwap = pos.IsSwap;
            posTO.IsFuture = pos.IsFuture;
            posTO.IsCurrency = pos.IsCurrency;

            summary.Positions.Add(posTO);
        }

        private void AddGrossNetExposures(IDictionary<string, FundExposureSummaryTO> fundExposureDict)
        {
            try
            {
                FundExposureSummaryTO fundMV = fundExposureDict[GRP_FUND_MV];
                FundExposureSummaryTO grossLongExp = fundExposureDict[GRP_GROSS_LONG_EXP];
                FundExposureSummaryTO grossShortExp = fundExposureDict[GRP_GROSS_SHORT_EXP];
                FundExposureSummaryTO cryptoLongExp = fundExposureDict[GRP_LONG_CRYPTO];
                FundExposureSummaryTO cryptoShortExp = fundExposureDict[GRP_SHORT_CRYPTO];
                FundExposureSummaryTO futuresLongExp = fundExposureDict[GRP_LONG_FUTURES_ALL];
                FundExposureSummaryTO futuresShortExp = fundExposureDict[GRP_SHORT_FUTURES_ALL];

                //Gross Futures
                FundExposureSummaryTO grossFuturesExp = fundExposureDict[GRP_GROSS_FUTURES];
                grossFuturesExp.TotValue = futuresLongExp.TotValue + (-1.0 * futuresShortExp.TotValue);
                grossFuturesExp.OppValue = futuresLongExp.OppValue + (-1.0 * futuresShortExp.OppValue);
                grossFuturesExp.TacValue = futuresLongExp.TacValue + (-1.0 * futuresShortExp.TacValue);

                //Net Futures
                FundExposureSummaryTO netFuturesExp = fundExposureDict[GRP_NET_FUTURES];
                netFuturesExp.TotValue = futuresLongExp.TotValue + futuresShortExp.TotValue;
                netFuturesExp.OppValue = futuresLongExp.OppValue + futuresShortExp.OppValue;
                netFuturesExp.TacValue = futuresLongExp.TacValue + futuresShortExp.TacValue;

                //Gross Exposure
                FundExposureSummaryTO grossExp = fundExposureDict[GRP_GROSS_EXP];
                grossExp.TotValue = grossLongExp.TotValue + (-1.0 * grossShortExp.TotValue);
                grossExp.OppValue = grossLongExp.OppValue + (-1.0 * grossShortExp.OppValue);
                grossExp.TacValue = grossLongExp.TacValue + (-1.0 * grossShortExp.TacValue);

                //Gross Exposure Excluding Crypto
                FundExposureSummaryTO grossExCryptoExp = fundExposureDict[GRP_GROSS_EX_CRYPTO_EXP];
                grossExCryptoExp.TotValue = grossExp.TotValue - (cryptoLongExp.TotValue - cryptoShortExp.TotValue);
                grossExCryptoExp.OppValue = grossExp.OppValue - (cryptoLongExp.OppValue - cryptoShortExp.OppValue);
                grossExCryptoExp.TacValue = grossExp.TacValue - (cryptoLongExp.TacValue - cryptoShortExp.TacValue);

                //Net Position Pct
                FundExposureSummaryTO netExp = fundExposureDict[GRP_NET_EXP];
                FundExposureSummaryTO netExpPct = fundExposureDict[GRP_NET_EXP_PCT];
                netExpPct.TotValue = (netExp.TotValue / fundMV.TotValue) * 100.0;
                netExpPct.OppValue = (netExp.OppValue / fundMV.OppValue) * 100.0;
                netExpPct.TacValue = (netExp.TacValue / fundMV.TacValue) * 100.0;

                //Gross Exposure Pct
                FundExposureSummaryTO grossExpPct = fundExposureDict[GRP_GROSS_EXP_PCT];
                grossExpPct.TotValue = (grossExp.TotValue / fundMV.TotValue) * 100.0;
                grossExpPct.OppValue = (grossExp.OppValue / fundMV.OppValue) * 100.0;
                grossExpPct.TacValue = (grossExp.TacValue / fundMV.TacValue) * 100.0;

                //Gross Exposure Ex Crypto Pct
                FundExposureSummaryTO grossExCryptoExpPct = fundExposureDict[GRP_GROSS_EX_CRYPTO_EXP_PCT];
                grossExCryptoExpPct.TotValue = (grossExCryptoExp.TotValue / fundMV.TotValue) * 100.0;
                grossExCryptoExpPct.OppValue = (grossExCryptoExp.OppValue / fundMV.OppValue) * 100.0;
                grossExCryptoExpPct.TacValue = (grossExCryptoExp.TacValue / fundMV.TacValue) * 100.0;
            }
            catch (Exception ex)
            {
                _logger.LogError("Error calculating Gross/Net total exposures", ex);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fundExposureDict"></param>
        private void CreateGroups(IDictionary<string, FundExposureSummaryTO> fundExposureDict)
        {
            fundExposureDict.Add(GRP_FUND_MV, new FundExposureSummaryTO(GRP_FUND_MV, 1));
            fundExposureDict.Add(GRP_LONG_CASH, new FundExposureSummaryTO(GRP_LONG_CASH, 3));
            fundExposureDict.Add(GRP_SHORT_CASH, new FundExposureSummaryTO(GRP_SHORT_CASH, 4));
            fundExposureDict.Add(GRP_LONG_SWAP, new FundExposureSummaryTO(GRP_LONG_SWAP, 5));
            fundExposureDict.Add(GRP_SHORT_SWAP, new FundExposureSummaryTO(GRP_SHORT_SWAP, 6));
            fundExposureDict.Add(GRP_LONG_FUTURES, new FundExposureSummaryTO(GRP_LONG_FUTURES, 7));
            fundExposureDict.Add(GRP_SHORT_FUTURES, new FundExposureSummaryTO(GRP_SHORT_FUTURES, 8));
            fundExposureDict.Add(GRP_LONG_CRYPTO, new FundExposureSummaryTO(GRP_LONG_CRYPTO, 9));
            fundExposureDict.Add(GRP_SHORT_CRYPTO, new FundExposureSummaryTO(GRP_SHORT_CRYPTO, 10));
            fundExposureDict.Add(GRP_CURRENCY, new FundExposureSummaryTO(GRP_CURRENCY, 11));
            fundExposureDict.Add(GRP_LONG_FUTURES_ALL, new FundExposureSummaryTO(GRP_LONG_FUTURES_ALL, 12));
            fundExposureDict.Add(GRP_SHORT_FUTURES_ALL, new FundExposureSummaryTO(GRP_SHORT_FUTURES_ALL, 13));
            fundExposureDict.Add(GRP_EMPTY, new FundExposureSummaryTO(GRP_EMPTY, 14));

            fundExposureDict.Add(GRP_GROSS_FUTURES, new FundExposureSummaryTO(GRP_GROSS_FUTURES, 20));
            fundExposureDict.Add(GRP_NET_FUTURES, new FundExposureSummaryTO(GRP_NET_FUTURES, 21));
            fundExposureDict.Add(GRP_EMPTY_1, new FundExposureSummaryTO(GRP_EMPTY_1, 22));

            fundExposureDict.Add(GRP_GROSS_LONG_EXP, new FundExposureSummaryTO(GRP_GROSS_LONG_EXP, 31));
            fundExposureDict.Add(GRP_GROSS_SHORT_EXP, new FundExposureSummaryTO(GRP_GROSS_SHORT_EXP, 32));

            fundExposureDict.Add(GRP_EMPTY_2, new FundExposureSummaryTO(GRP_EMPTY_2, 40));
            fundExposureDict.Add(GRP_NET_EXP, new FundExposureSummaryTO(GRP_NET_EXP, 41));
            fundExposureDict.Add(GRP_GROSS_EXP, new FundExposureSummaryTO(GRP_GROSS_EXP, 42));
            fundExposureDict.Add(GRP_GROSS_EX_CRYPTO_EXP, new FundExposureSummaryTO(GRP_GROSS_EX_CRYPTO_EXP, 43));

            fundExposureDict.Add(GRP_EMPTY_3, new FundExposureSummaryTO(GRP_EMPTY_3, 50));
            fundExposureDict.Add(GRP_NET_EXP_PCT, new FundExposureSummaryTO(GRP_NET_EXP_PCT, 51));
            fundExposureDict.Add(GRP_GROSS_EXP_PCT, new FundExposureSummaryTO(GRP_GROSS_EXP_PCT, 52));
            fundExposureDict.Add(GRP_GROSS_EX_CRYPTO_EXP_PCT, new FundExposureSummaryTO(GRP_GROSS_EX_CRYPTO_EXP_PCT, 53));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="asofDate"></param>
        /// <param name="fundName"></param>
        /// <returns></returns>
        public IList<FundFuturesExposureTO> GetFundFuturesExposureReport(DateTime asofDate, string fundName)
        {
            IDictionary<string, FundFuturesExposureTO> dict = new Dictionary<string, FundFuturesExposureTO>();
            int groupId = 1;

            try
            {
                IList<FundFuturesExposureTO> list = _holdingsDao.GetFuturesHoldings(asofDate, fundName);
                foreach (FundFuturesExposureTO data in list)
                {
                    data.NotionalAmt = data.Position.GetValueOrDefault() * data.Multiplier.GetValueOrDefault();
                    if (data.Duration.HasValue)
                        data.DurationContr = data.NotionalAmt.GetValueOrDefault() * data.Duration.GetValueOrDefault();

                    //group by yellow key
                    string ykey = data.Ykey;
                    if (dict.TryGetValue(ykey, out FundFuturesExposureTO group))
                    {
                        group.NotionalAmt += data.NotionalAmt;
                        group.DurationContr += data.DurationContr;
                    }
                    else
                    {
                        group = new FundFuturesExposureTO();
                        group.IsGroup = 1;
                        group.GroupId = groupId++;
                        group.GroupName = ykey;
                        group.NotionalAmt = data.NotionalAmt;
                        group.DurationContr = data.DurationContr;
                        dict.Add(group.GroupName, group);
                    }

                    //add security
                    data.IsGroup = 0;
                    data.GroupId = group.GroupId;
                    data.GroupName = data.RowId.ToString();
                    dict.Add(data.GroupName, data);
                }
                IList<FundFuturesExposureTO> result = dict.Values.ToList<FundFuturesExposureTO>();
                return result.OrderBy(x => x.GroupId).ThenBy(y => y.Position).ToList<FundFuturesExposureTO>();
            }
            catch (Exception ex)
            {
                _logger.LogError("Error generating Futures Exposure Report", ex);
            }
            return null;
        }
    }

    struct ExposureGroup
    {
        public string GrpName;
        public int SortId;

        public ExposureGroup(string grpName, int sortId)
        {
            this.GrpName = grpName;
            this.SortId = sortId;
        }
    }
}
