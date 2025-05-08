using aACTrader.DAO.Repository;
using aCommons;
using aCommons.Utils;
using aCommons.Web;
using LazyCache;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace aACTrader.Operations.Reports
{
    public class HoldingExposureReport
    {
        private readonly ILogger<HoldingExposureReport> _logger;
        private readonly CachingService _cache;
        private readonly HoldingsDao _holdingsDao;
        private readonly IConfiguration _configuration;

        public HoldingExposureReport(ILogger<HoldingExposureReport> logger
            , CachingService cache
            , HoldingsDao holdingsDao
            , IConfiguration configuration)
        {
            _logger = logger;
            _cache = cache;
            _holdingsDao = holdingsDao;
            _configuration = configuration;
        }

        public IList<HoldingExposureTO> GetExposureReport(DateTime asOfDate, string longShortInd, string topHoldings, string groupBy)
        {
            bool groupData = true;
            int longShortIndicator = 0;
            int topHoldingsIndicator = 0;
            int includeHolding = 0;
            if (!string.IsNullOrEmpty(longShortInd))
                longShortIndicator = 1;

            if (!string.IsNullOrEmpty(topHoldings))
                topHoldingsIndicator = Int16.Parse(topHoldings);

            IDictionary<string, string> positionIdentifierMap = _cache.Get<IDictionary<string, string>>(CacheKeys.POSITION_IDENTIFIER_MAP);
            IDictionary<string, SecurityMasterExt> securityMasterExtDict = _cache.Get<IDictionary<string, SecurityMasterExt>>(CacheKeys.SECURITY_EXT_DETAILS);
            IDictionary<string, FundCurrExpTO> fundCurrencyExpDict = _cache.Get<IDictionary<string, FundCurrExpTO>>(CacheKeys.FUND_CURRENCY_EXPOSURES);

            IList<HoldingExposureTO> holdings = _holdingsDao.GetHoldingsByDate(asOfDate);
            IDictionary<string, HoldingExposureTO> dict = new Dictionary<string, HoldingExposureTO>();

            string groupByField = "Symbol";
            string groupValue = string.Empty;

            if (!string.IsNullOrEmpty(groupBy))
                groupByField = groupBy;

            foreach (HoldingExposureTO data in holdings)
            {
                switch (groupByField)
                {
                    case "Symbol":
                        groupValue = data.Symbol;
                        groupData = false;
                        break;
                    case "ParentCompany":
                        groupValue = data.ParentCompName;
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
                    case "GeoLevel1":
                        groupValue = data.GeoLvl1;
                        break;
                    case "AssetClassLevel1":
                        groupValue = data.AssetClsLvl1;
                        break;
                    default:
                        groupValue = data.Symbol;
                        break;
                }

                if (string.IsNullOrEmpty(groupValue))
                    groupValue = "Other";

                includeHolding = 0;

                //filter based on Long/Short indicator
                if (longShortIndicator == 1)
                {
                    if (longShortInd.Equals(data.PosInd))
                        includeHolding = 1;
                }
                else
                {
                    includeHolding = 1;
                }

                // filter based on top holdings setting
                if (topHoldingsIndicator > 1 && includeHolding == 1)
                {
                    if (data.Rank <= topHoldingsIndicator)
                        includeHolding = 1;
                    else
                        includeHolding = 0;
                }

                if (includeHolding == 1)
                {
                    SecurityMasterExt securityMasterExt = CommonOperationsUtil.GetSecurityMasterExt(data.Ticker, data.Ticker, data.Sedol, data.ISIN, securityMasterExtDict, positionIdentifierMap);
                    if (securityMasterExt != null)
                    {
                        data.Beta = securityMasterExt.RiskBeta;
                        data.Dur = securityMasterExt.Duration;
                        data.Sector = securityMasterExt.FundGroup;
                    }

                    if (data.ShOut.HasValue)
                        data.ShOwnPct = data.Pos / data.ShOut;

                    CalculateCurrencyExposures(fundCurrencyExpDict, data);
                    AddHoldingToGroupNew(dict, data, groupValue, groupData);
                }
            }

            IList<HoldingExposureTO> list = dict.Values.ToList<HoldingExposureTO>();
            return list.OrderBy(p => p.GrpName).ThenByDescending(p => p.IsGroup).ToList<HoldingExposureTO>();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="dict"></param>
        /// <param name="data"></param>
        /// <param name="groupName"></param>
        /// <param name="groupData"></param>
        private void AddHoldingToGroupNew(IDictionary<string, HoldingExposureTO> dict, HoldingExposureTO data, string groupName, bool groupData)
        {
            //if group by is not selected then do not group data
            if (!groupData)
            {
                if (!dict.TryGetValue(groupName, out HoldingExposureTO record))
                {
                    record = data;
                    record.GrpName = groupName;
                    record.IsGroup = 0;
                    dict.Add(data.Symbol, record);
                }
            }
            else
            {
                if (!dict.TryGetValue(groupName, out HoldingExposureTO record))
                {
                    record = new HoldingExposureTO();
                    record.Symbol = groupName;
                    record.GrpName = groupName;
                    record.IsGroup = 1;
                    record.MV = data.MV;
                    record.Wt = data.Wt;
                    record.FundOppWt = data.FundOppWt;
                    record.FundTacWt = data.FundTacWt;
                    record.AsOfDt = data.AsOfDt;

                    dict.Add(record.Symbol, record);
                }
                else
                {
                    record.MV = CommonUtils.AddNullableDoubles(record.MV, data.MV);
                    record.Wt = CommonUtils.AddNullableDoubles(record.Wt, data.Wt);
                    record.FundOppWt = CommonUtils.AddNullableDoubles(record.FundOppWt, data.FundOppWt);
                    record.FundTacWt = CommonUtils.AddNullableDoubles(record.FundTacWt, data.FundTacWt);
                }

                // add position
                if (!dict.TryGetValue(data.Symbol, out HoldingExposureTO position))
                {
                    position = new HoldingExposureTO();
                    position = data;
                    position.GrpName = groupName;
                    position.IsGroup = 0;
                    dict.Add(position.Symbol, position);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="dict"></param>
        /// <param name="data"></param>
        /// <param name="groupName"></param>
        /// <param name="groupData"></param>
        private void AddHoldingToGroup(IDictionary<string, HoldingExposureTO> dict, HoldingExposureTO data, string groupName, bool groupData)
        {
            //if group by is not selected then do not group data
            if (!groupData)
            {
                if (!dict.TryGetValue(groupName, out HoldingExposureTO record))
                {
                    record = data;
                    record.GrpName = groupName;
                    dict.Add(groupName, record);
                }
            }
            else
            {
                if (!dict.TryGetValue(groupName, out HoldingExposureTO record))
                {
                    record = new HoldingExposureTO();
                    record.GrpName = groupName;
                    record.Pos = data.Pos;
                    record.MV = data.MV;
                    record.Wt = data.Wt;
                    record.FundOppWt = data.FundOppWt;
                    record.FundTacWt = data.FundTacWt;
                    record.CashPos = data.CashPos;
                    record.SwapPos = data.SwapPos;
                    record.FundOppCashPos = data.FundOppCashPos;
                    record.FundOppSwapPos = data.FundOppSwapPos;
                    record.FundTacCashPos = data.FundTacCashPos;
                    record.FundTacSwapPos = data.FundTacSwapPos;
                    record.ShOut = data.ShOut;
                    record.AsOfDt = data.AsOfDt;

                    dict.Add(groupName, record);
                }
                else
                {
                    record.Pos = CommonUtils.AddNullableDoubles(record.Pos, data.Pos);
                    record.MV = CommonUtils.AddNullableDoubles(record.MV, data.MV);
                    record.Wt = CommonUtils.AddNullableDoubles(record.Wt, data.Wt);
                    record.FundOppWt = CommonUtils.AddNullableDoubles(record.FundOppWt, data.FundOppWt);
                    record.FundTacWt = CommonUtils.AddNullableDoubles(record.FundTacWt, data.FundTacWt);
                    record.CashPos = CommonUtils.AddNullableDoubles(record.CashPos, data.CashPos);
                    record.SwapPos = CommonUtils.AddNullableDoubles(record.SwapPos, data.SwapPos);
                    record.FundOppCashPos = CommonUtils.AddNullableDoubles(record.FundOppCashPos, data.FundOppCashPos);
                    record.FundOppSwapPos = CommonUtils.AddNullableDoubles(record.FundOppSwapPos, data.FundOppSwapPos);
                    record.FundTacCashPos = CommonUtils.AddNullableDoubles(record.FundTacCashPos, data.FundTacCashPos);
                    record.FundTacSwapPos = CommonUtils.AddNullableDoubles(record.FundTacSwapPos, data.FundTacSwapPos);
                    record.ShOut = CommonUtils.AddNullableDoubles(record.ShOut, data.ShOut);
                }

                //add child records
                IList<HoldingExposureDetTO> details = record.Details;
                HoldingExposureDetTO detail = new HoldingExposureDetTO()
                {
                    Symbol = data.Symbol,
                    SecDesc = data.SecDesc,
                    Sector = data.Sector,
                    TrdRationale = data.TrdRationale,
                    TrdStatus = data.TrdStatus,
                    Curr = data.Curr,
                    Price = data.Price,
                    PriceLocal = data.PriceLocal,
                    Fx = data.Fx,
                    MV = data.MV,
                    Beta = data.Beta,
                    Dur = data.Dur,
                    DscntBeta = data.DscntBeta,
                    Pos = data.Pos,
                    CashPos = data.CashPos,
                    SwapPos = data.SwapPos,
                    Wt = data.Wt,
                    ShOut = data.ShOut,
                    ShOwnPct = data.ShOwnPct,
                    FundOppPos = data.FundOppPos,
                    FundOppCashPos = data.FundOppCashPos,
                    FundOppSwapPos = data.FundOppSwapPos,
                    FundOppWt = data.FundOppWt,
                    FundTacPos = data.FundTacPos,
                    FundTacCashPos = data.FundTacCashPos,
                    FundTacSwapPos = data.FundTacSwapPos,
                    FundTacWt = data.FundTacWt,
                    Cntry = data.Cntry,
                    SecType = data.SecType,
                    AssetType = data.AssetType,
                    PaymentRank = data.PaymentRank,
                    GeoLvl1 = data.GeoLvl1,
                    GeoLvl2 = data.GeoLvl2,
                    GeoLvl3 = data.GeoLvl3,
                    AssetClsLvl1 = data.AssetClsLvl1,
                    AssetClsLvl2 = data.AssetClsLvl2,
                    AssetClsLvl3 = data.AssetClsLvl3,
                    Sec13F = data.Sec13F,
                    CashSynthetic = data.CashSynthetic,
                    YKey = data.YKey,
                    ISIN = data.ISIN,
                    Cusip = data.Cusip,
                    Sedol = data.Sedol,
                    ParentCompName = data.ParentCompName,
                    FxExpUSD = data.FxExpUSD,
                    FxExpCAD = data.FxExpCAD,
                    FxExpGBP = data.FxExpGBP,
                    FxExpEUR = data.FxExpEUR,
                    FxExpJPY = data.FxExpJPY,
                    FxExpRON = data.FxExpRON,
                    FxExpILS = data.FxExpILS,
                    FxExpAUD = data.FxExpAUD,
                    FxExpINR = data.FxExpINR,
                    FxExpHKD = data.FxExpHKD,
                    FxExpCHF = data.FxExpCHF,
                    FxExpMXN = data.FxExpMXN,
                    FxExpSGD = data.FxExpSGD,
                    FxExpCNY = data.FxExpCNY,
                    FxExpBRL = data.FxExpBRL,
                    FxExpOthers = data.FxExpOthers,
                };

                details.Add(detail);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fundCurrencyExpDict"></param>
        /// <param name="data"></param>
        private void CalculateCurrencyExposures(IDictionary<string, FundCurrExpTO> fundCurrencyExpDict, HoldingExposureTO data)
        {
            try
            {
                if (fundCurrencyExpDict.TryGetValue(data.Ticker, out FundCurrExpTO currExp))
                {
                    data.FxExpUSD = currExp.USDExp;
                    data.FxExpCAD = currExp.CADExp;
                    data.FxExpGBP = currExp.GBPExp;
                    data.FxExpEUR = currExp.EURExp;
                    data.FxExpJPY = currExp.JPYExp;
                    data.FxExpRON = currExp.RONExp;
                    data.FxExpILS = currExp.ILSExp;
                    data.FxExpAUD = currExp.AUDExp;
                    data.FxExpINR = currExp.INRExp;
                    data.FxExpHKD = currExp.HKDExp;
                    data.FxExpCHF = currExp.CHFExp;
                    data.FxExpMXN = currExp.MXNExp;
                    data.FxExpSGD = currExp.SGDExp;
                    data.FxExpCNY = currExp.CNYExp;
                    data.FxExpBRL = currExp.BRLExp;
                    data.FxExpOthers = currExp.OtherExp;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("Error populating Currency Exposures for Ticker: " + data.Ticker, ex);
            }
        }
    }
}
