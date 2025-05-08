using aACTrader.DAO.Repository;
using aCommons;
using aCommons.Cef;
using aCommons.DTO;
using aCommons.Utils;
using LazyCache;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;

namespace aACTrader.Operations.Impl
{
    public class FundHoldingsOperations
    {
        private readonly ILogger<FundHistoryOperations> _logger;
        private readonly HoldingsDao _holdingsDao;
        private readonly CachingService _cache;

        private readonly DateTime currentDate = DateTime.Now;
        private readonly int DEFAULT_RECALL_DAYS = 10;
        private readonly double DEFAULT_QUALIFYING_DIVIDENDS = 0.37;
        private readonly string DEFAULT_RECALL_DATE = "Special";

        public FundHoldingsOperations(ILogger<FundHistoryOperations> logger, HoldingsDao holdingsDao, CachingService cache)
        {
            _logger = logger;
            _holdingsDao = holdingsDao;
            _cache = cache;
        }

        public IList<HoldingExposureDetail> GenerateExposureReport(CachingService cache, string portfolio, string broker, string[] groups, string showDetails)
        {
            IDictionary<string, Holding> almHoldingsDict = cache.Get<IDictionary<string, Holding>>(CacheKeys.ALM_HOLDINGS_BY_PORT);
            IDictionary<string, FundMaster> securityMasterDict = cache.Get<IDictionary<string, FundMaster>>(CacheKeys.SECURITY_MASTER);
            IDictionary<string, FundForecast> fundForecastDict = cache.Get<IDictionary<string, FundForecast>>(CacheKeys.FUND_FORECASTS);
            IDictionary<string, UserDataOverride> userOverrideDict = cache.Get<IDictionary<string, UserDataOverride>>(CacheKeys.USER_OVERRIDES);

            IList<HoldingExposureDetail> holdingList = new List<HoldingExposureDetail>();

            CompositeGroup root = new CompositeGroup("Total")
            {
                Name = "Total",
                Indent = 0,
                IsGroup = "Y"
            };

            foreach (KeyValuePair<string, Holding> kvp in almHoldingsDict)
            {
                Holding holding = kvp.Value;

                //if ("DNI US".Equals(holding.HoldingTicker))
                //    _logger.LogDebug(holding.HoldingTicker);

                if (FundFilter(holding, portfolio) && BrokerFilter(holding, broker))
                {
                    //lookup by figi or holding ticker
                    FundMaster fundMaster = null;
                    if (!string.IsNullOrEmpty(holding.FIGI) && securityMasterDict.TryGetValue(holding.FIGI, out fundMaster))
                    {
                    }
                    else if (securityMasterDict.TryGetValue(holding.HoldingTicker, out fundMaster))
                    {
                    }

                    FundForecast fundForecast = null;
                    if (fundForecastDict.TryGetValue(holding.HoldingTicker, out fundForecast))
                    {
                    }

                    UserDataOverride userOverride = null;
                    if (userOverrideDict.TryGetValue(holding.HoldingTicker, out userOverride))
                    {
                    }

                    string fundName = string.Empty;
                    if ("Y".Equals(holding.InOpportunityFund, StringComparison.CurrentCultureIgnoreCase))
                    {
                        fundName = "OPP";
                    }
                    else if ("Y".Equals(holding.InTacticalFund, StringComparison.CurrentCultureIgnoreCase))
                    {
                        fundName = "TAC";
                    }

                    //add groups
                    Group group = null;
                    Group parentGroup = null;
                    int indentLevel = groups.Length + 1;
                    for (int i = 0; i < groups.Length; i++)
                    {
                        string groupBy = groups[i];
                        Tuple<string, int> groupName = GetGroupName(groupBy, fundMaster, fundForecast, userOverride, holding);

                        if (i == 0)
                        {
                            //if group is empty, then link to previous parent group or root
                            if (string.IsNullOrEmpty(groupName.Item1))
                            {
                                group = root;
                            }
                            else
                            {
                                group = root.Find(groupName.Item1);
                                if (group == null)
                                {
                                    group = new CompositeGroup(groupName.Item1)
                                    {
                                        Name = groupName.Item1,
                                        Indent = i + 1,
                                        IsGroup = "Y",
                                        SortOrder = groupName.Item2
                                    };

                                    root.AddNode(group);
                                }
                                group.AddDetails(holding.MarketValue, fundName);
                            }
                            parentGroup = group;
                        }
                        else
                        {
                            if (string.IsNullOrEmpty(groupName.Item1))
                            {
                                if (parentGroup != null)
                                    group = parentGroup;
                                else
                                    group = root;
                            }
                            else
                            {
                                group = parentGroup.Find(groupName.Item1);
                                if (group == null)
                                {
                                    group = new CompositeGroup(groupName.Item1)
                                    {
                                        Name = groupName.Item1,
                                        Indent = i + 1,
                                        IsGroup = "Y",
                                        SortOrder = groupName.Item2
                                    };

                                    parentGroup.AddNode(group);
                                }
                                group.AddDetails(holding.MarketValue, fundName);
                            }
                            parentGroup = group;
                        }
                        indentLevel = group.Indent;
                    }

                    //add to root
                    root.AddDetails(holding.MarketValue, fundName);

                    //add details
                    if ("Y".Equals(showDetails))
                    {
                        PrimitiveGroup detail;
                        if (group != null)
                        {
                            detail = (PrimitiveGroup)group.Find(holding.HoldingTicker);
                        }
                        else
                        {
                            detail = (PrimitiveGroup)root.Find(holding.HoldingTicker);
                        }

                        if (detail == null)
                        {
                            detail = new PrimitiveGroup(holding.HoldingTicker)
                            {
                                Name = holding.HoldingTicker,
                                Indent = indentLevel + 1,
                                IsGroup = "N",
                                Currency = holding.Currency,
                                FXRate = holding.FX,
                                Price = holding.Price,
                                Security13FFlag = holding.Security13FFlag,
                                YellowKey = holding.YellowKey,
                                SortOrder = holding.GroupingSortOrder.GetValueOrDefault()
                            };

                            if (fundMaster != null)
                            {
                                detail.SecurityName = fundMaster.Name;
                                detail.FIGI = fundMaster.FIGI;
                                detail.Country = fundMaster.Cntry;
                                detail.SecurityType = fundMaster.SecTyp;
                                detail.AssetType = fundMaster.AssetTyp;
                                detail.PaymentRank = fundMaster.PayRank;
                                detail.FundCategory = FundForecastOperations.GetFundCategory(fundForecast, userOverride);
                                detail.GeoLevel1 = fundMaster.GeoLvl1;
                                detail.GeoLevel2 = fundMaster.GeoLvl2;
                                detail.GeoLevel3 = fundMaster.GeoLvl3;
                                detail.AssetClassLevel1 = fundMaster.AssetLvl1;
                                detail.AssetClassLevel2 = fundMaster.AssetLvl2;
                                detail.AssetClassLevel3 = fundMaster.AssetLvl3;

                                detail.CustomGroup1 = GetCustomLevel1Grouping(fundMaster, holding).Item1;
                                detail.CustomGroup2 = GetCustomLevel2Grouping(fundMaster, holding).Item1;
                                detail.CustomGroup3 = GetCustomLevel3Grouping(fundMaster, holding).Item1;
                                detail.CustomGroup4 = GetCustomLevel4Grouping(fundMaster, holding).Item1;
                                detail.CustomGroup5 = GetCustomLevel5Grouping(holding).Item1;
                            }

                            if (fundForecast != null)
                            {
                                if (fundForecast.EAFinalAlpha != null)
                                    detail.ExpectedAlpha = fundForecast.EAFinalAlpha;

                                if (fundForecast.IRRLastPrc != null)
                                    detail.IRR = fundForecast.IRRLastPrc;

                                if (fundForecast.LastDvdAdjNav.HasValue)
                                    detail.PublishedNav = fundForecast.LastDvdAdjNav;

                                if (fundForecast.LastPD.HasValue)
                                    detail.PublishedDiscount = fundForecast.LastPD;

                                if (fundForecast.EstNav.HasValue)
                                    detail.EstimatedNav = fundForecast.EstNav;

                                if (fundForecast.PDLastPrc.HasValue)
                                    detail.EstimatedDiscount = fundForecast.PDLastPrc;

                                if (detail.IRR.HasValue)
                                    detail.ExpectedAnnualizedRtn = detail.IRR;
                                else if (detail.ExpectedAlpha.HasValue)
                                    detail.ExpectedAnnualizedRtn = detail.ExpectedAlpha;
                            }

                            if (group != null)
                                group.AddNode(detail);
                            else
                                root.AddNode(detail);
                        }

                        //add positions
                        if (detail != null)
                        {
                            detail.Position = CommonUtils.AddNullableDoubles(detail.Position, holding.Position.GetValueOrDefault());
                            detail.MarketValue = CommonUtils.AddNullableDoubles(detail.MarketValue, holding.MarketValue.GetValueOrDefault());
                            detail.MarketValueLocal = CommonUtils.AddNullableDoubles(detail.MarketValueLocal, holding.MarketValueLocal.GetValueOrDefault());

                            if ("Y".Equals(holding.InOpportunityFund, StringComparison.CurrentCultureIgnoreCase))
                            {
                                detail.FundOppFXRate = holding.FX;
                                detail.FundOppPrice = holding.Price;
                                detail.FundOppPosition = CommonUtils.AddNullableDoubles(detail.FundOppPosition, holding.Position.GetValueOrDefault());
                                detail.FundOppMarketValue = CommonUtils.AddNullableDoubles(detail.FundOppMarketValue, holding.MarketValue.GetValueOrDefault());
                                detail.FundOppMarketValueLocal = CommonUtils.AddNullableDoubles(detail.FundOppMarketValueLocal, holding.MarketValueLocal.GetValueOrDefault());
                            }
                            else if ("Y".Equals(holding.InTacticalFund, StringComparison.CurrentCultureIgnoreCase))
                            {
                                detail.FundTacFXRate = holding.FX;
                                detail.FundTacPrice = holding.Price;
                                detail.FundTacPosition = CommonUtils.AddNullableDoubles(detail.FundTacPosition, holding.Position.GetValueOrDefault());
                                detail.FundTacMarketValue = CommonUtils.AddNullableDoubles(detail.FundTacMarketValue, holding.MarketValue.GetValueOrDefault());
                                detail.FundTacMarketValueLocal = CommonUtils.AddNullableDoubles(detail.FundTacMarketValueLocal, holding.MarketValueLocal.GetValueOrDefault());
                            }
                        }
                    }
                }
            }

            //summarize data
            IList<Group> groupList = root.GetDetails();

            HoldingExposureDetail exposureDetail = new HoldingExposureDetail
            {
                Name = root.Name,
                SortOrder = 0, //root.SortOrder;
                Indent = root.Indent,
                IsGroup = root.IsGroup,
                Currency = root.Currency,
                Position = root.Position,
                Price = root.Price,
                FXRate = root.FXRate,
                MarketValue = root.MarketValue,
                FundOppMarketValue = root.FundOppMarketValue,
                FundTacMarketValue = root.FundTacMarketValue
            };

            holdingList.Add(exposureDetail);

            foreach (Group group in groupList)
            {
                exposureDetail = new HoldingExposureDetail
                {
                    Name = group.Name,
                    SortOrder = group.SortOrder,
                    FIGI = group.FIGI,
                    Indent = group.Indent,
                    IsGroup = group.IsGroup,
                    Currency = group.Currency,
                    Position = group.Position,
                    Price = group.Price,
                    FXRate = group.FXRate,
                    MarketValue = group.MarketValue,
                    MarketValueLocal = group.MarketValueLocal,
                    ExpectedAlpha = group.ExpectedAlpha,
                    IRR = group.IRR,
                    ExpectedAnnualizedRtn = group.ExpectedAnnualizedRtn,
                    PublishedNav = group.PublishedNav,
                    PublishedDiscount = group.PublishedDiscount,
                    EstimatedNav = group.EstimatedNav,
                    EstimatedDiscount = group.EstimatedDiscount,

                    SecurityName = group.SecurityName,
                    Country = group.Country,
                    SecurityType = group.SecurityType,
                    AssetType = group.AssetType,
                    PaymentRank = group.PaymentRank,
                    FundCategory = group.FundCategory,
                    GeoLevel1 = group.GeoLevel1,
                    GeoLevel2 = group.GeoLevel2,
                    GeoLevel3 = group.GeoLevel3,
                    AssetClassLevel1 = group.AssetClassLevel1,
                    AssetClassLevel2 = group.AssetClassLevel2,
                    AssetClassLevel3 = group.AssetClassLevel3,
                    YellowKey = group.YellowKey,
                    Security13FFlag = group.Security13FFlag,

                    FundOppFXRate = group.FundOppFXRate,
                    FundOppPrice = group.FundOppPrice,
                    FundOppPosition = group.FundOppPosition,
                    FundOppMarketValue = group.FundOppMarketValue,
                    FundOppMarketValueLocal = group.FundOppMarketValueLocal
                };

                if (root.FundOppMarketValue.HasValue && root.FundOppMarketValue.GetValueOrDefault() > 0)
                {
                    exposureDetail.FundOppMarketValuePct = exposureDetail.FundOppMarketValue / root.FundOppMarketValue;
                }

                exposureDetail.FundTacFXRate = group.FundTacFXRate;
                exposureDetail.FundTacPrice = group.FundTacPrice;
                exposureDetail.FundTacPosition = group.FundTacPosition;
                exposureDetail.FundTacMarketValue = group.FundTacMarketValue;
                exposureDetail.FundTacMarketValueLocal = group.FundTacMarketValueLocal;

                if (root.FundTacMarketValue.HasValue && root.FundTacMarketValue.GetValueOrDefault() > 0)
                {
                    exposureDetail.FundTacMarketValuePct = exposureDetail.FundTacMarketValue / root.FundTacMarketValue;
                }

                exposureDetail.CustomGroup1 = group.CustomGroup1;
                exposureDetail.CustomGroup2 = group.CustomGroup2;
                exposureDetail.CustomGroup3 = group.CustomGroup3;
                exposureDetail.CustomGroup4 = group.CustomGroup4;
                exposureDetail.CustomGroup5 = group.CustomGroup5;

                holdingList.Add(exposureDetail);
            }

            return holdingList.OrderBy(h => h.SortOrder).ToList<HoldingExposureDetail>();
        }

        public IList<AlmHolding> GenerateSharesRecallReport(string portfolio, string broker)
        {
            IList<AlmHolding> filteredHoldings = new List<AlmHolding>();
            try
            {
                IList<AlmHolding> holdings = _holdingsDao.GetAlmHoldingsSharesRecall();
                foreach (AlmHolding holding in holdings)
                {
                    if (!string.IsNullOrEmpty(holding.Portfolio)
                        && FundFilter(holding, portfolio)
                        && BrokerFilter(holding, broker))
                    {
                        CalculateTaxes(holding);
                        filteredHoldings.Add(holding);
                        //if (holding.Savings.GetValueOrDefault() != 0)
                        //    filteredHoldings.Add(holding);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error generating shares recall report");
            }
            return filteredHoldings;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="holding"></param>
        private void CalculateTaxes(AlmHolding holding)
        {
            try
            {
                //after tax cost of lending
                if (!holding.QualifyingDividends.HasValue)
                    holding.QualifyingDividends = DEFAULT_QUALIFYING_DIVIDENDS;

                double afterTaxCostOfLending = 0.0;
                double? dvdYield = (holding.OvrDividendYield.HasValue ? holding.OvrDividendYield : holding.DividendYield);
                if (dvdYield.HasValue)
                {
                    afterTaxCostOfLending = (DEFAULT_QUALIFYING_DIVIDENDS - holding.QualifyingDividends.GetValueOrDefault()) * dvdYield.GetValueOrDefault();
                    holding.AfterTaxCostOfLending = afterTaxCostOfLending;
                }

                //estimate next ex dividend date
                Nullable<DateTime> exDividendDate = (holding.OvrDividendExDate.HasValue ? holding.OvrDividendExDate : holding.DividendExDate);
                if (exDividendDate.HasValue)
                {
                    if (exDividendDate > currentDate)
                    {
                        if (!string.IsNullOrEmpty(holding.DividendFrequency) && (holding.DividendFrequency.Equals("Monthly", StringComparison.CurrentCultureIgnoreCase)
                            || holding.DividendFrequency.Equals("Quarter", StringComparison.CurrentCultureIgnoreCase)))
                        {
                            holding.NextDividendExDate = exDividendDate;
                            holding.NextDividendExDateString = DateUtils.ConvertDate(holding.NextDividendExDate, "yyyy-MM-dd");
                        }
                        else
                        {
                            holding.NextDividendExDate = null;
                            holding.NextDividendExDateString = DEFAULT_RECALL_DATE;
                        }
                    }
                    else
                    {
                        if (!string.IsNullOrEmpty(holding.DividendFrequency) && holding.DividendFrequency.Equals("Monthly", StringComparison.CurrentCultureIgnoreCase))
                        {
                            holding.NextDividendExDate = DateUtils.AddDays(exDividendDate.GetValueOrDefault(), 30);
                            holding.NextDividendExDateString = DateUtils.ConvertDate(holding.NextDividendExDate, "yyyy-MM-dd");
                        }
                        else if (!string.IsNullOrEmpty(holding.DividendFrequency) && holding.DividendFrequency.Equals("Quarter", StringComparison.CurrentCultureIgnoreCase))
                        {
                            holding.NextDividendExDate = DateUtils.AddDays(exDividendDate.GetValueOrDefault(), 90);
                            holding.NextDividendExDateString = DateUtils.ConvertDate(holding.NextDividendExDate, "yyyy-MM-dd");
                        }
                        else
                        {
                            holding.NextDividendExDate = null;
                            holding.NextDividendExDateString = DEFAULT_RECALL_DATE;
                        }
                    }
                }

                //calculate recall date
                holding.RecallDateString = DEFAULT_RECALL_DATE;
                Nullable<DateTime> nextExDividendDate = (holding.OvrNextDividendExDate.HasValue ? holding.OvrNextDividendExDate : holding.NextDividendExDate);
                if (nextExDividendDate.HasValue)
                {
                    int recallDays = DEFAULT_RECALL_DAYS;
                    if (holding.RecallDays.HasValue)
                    {
                        recallDays = holding.RecallDays.GetValueOrDefault();
                    }

                    if (nextExDividendDate.HasValue)
                    {
                        DateTime recallDate = DateUtils.AddBusinessDays(nextExDividendDate.GetValueOrDefault(), -1 * recallDays);
                        holding.RecallDate = recallDate;
                        holding.RecallDateString = DateUtils.ConvertDate(recallDate, "yyyy-MM-dd");
                    }
                }

                //calculate Type 1 or Type 2
                holding.RecallType = "Type 1";
                if (holding.AfterTaxCostOfLending < 0.0025)
                {
                    holding.RecallType = "Type 2";
                }
                else if (holding.RecallDate.HasValue && holding.RecallDate > currentDate)
                {
                    holding.RecallType = "Type 2";
                }

                //calculate savings
                //So the formula for dollar savings would be dividend yield / payment frequency * market value * (37 % -Qualifying dvds[column H]).
                holding.Savings = CalculateSavings(holding);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error calculating shares recall: " + holding.Ticker);
            }
        }

        private double? CalculateSavings(AlmHolding holding)
        {
            double? savings = null;
            double dividendFrequency = 0;

            try
            {
                if (!string.IsNullOrEmpty(holding.DividendFrequency) && holding.DividendFrequency.Equals("Monthly", StringComparison.CurrentCultureIgnoreCase))
                    dividendFrequency = 12.0;
                else if (!string.IsNullOrEmpty(holding.DividendFrequency) && holding.DividendFrequency.Equals("Quarter", StringComparison.CurrentCultureIgnoreCase))
                    dividendFrequency = 4.0;
                else if (!string.IsNullOrEmpty(holding.DividendFrequency) && holding.DividendFrequency.Equals("Semi-Anl", StringComparison.CurrentCultureIgnoreCase))
                    dividendFrequency = 2.0;
                else if (!string.IsNullOrEmpty(holding.DividendFrequency) && holding.DividendFrequency.Equals("Annual", StringComparison.CurrentCultureIgnoreCase))
                    dividendFrequency = 1.0;

                if (dividendFrequency > 0)
                {
                    double? dvdYield = (holding.OvrDividendYield.HasValue ? holding.OvrDividendYield : holding.DividendYield);
                    if (dvdYield.HasValue)
                    {
                        savings = (dvdYield.GetValueOrDefault() / dividendFrequency) * holding.MktValueUSD * (0.37 - holding.QualifyingDividends);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error calculating savings for ticker: " + holding.Ticker);
            }

            return savings;
        }

        private bool FundFilter(Holding holding, string fundName)
        {
            bool result = false;
            try
            {
                if ("All".Equals(fundName, StringComparison.CurrentCultureIgnoreCase))
                    result = true;
                else if ("OPP".Equals(fundName, StringComparison.CurrentCultureIgnoreCase) && "Y".Equals(holding.InOpportunityFund, StringComparison.CurrentCultureIgnoreCase))
                    result = true;
                else if ("TAC".Equals(fundName, StringComparison.CurrentCultureIgnoreCase) && "Y".Equals(holding.InTacticalFund, StringComparison.CurrentCultureIgnoreCase))
                    result = true;
            }
            catch (Exception)
            {
            }
            return result;
        }

        private bool BrokerFilter(Holding holding, string broker)
        {
            bool result = false;
            try
            {
                if ("All".Equals(broker, StringComparison.CurrentCultureIgnoreCase))
                    result = true;
                else if ("Jeff".Equals(broker, StringComparison.CurrentCultureIgnoreCase) && holding.Portfolio.IndexOf("Jeff", StringComparison.CurrentCultureIgnoreCase) >= 0)
                    result = true;
                else if ("JPM".Equals(broker, StringComparison.CurrentCultureIgnoreCase) && holding.Portfolio.IndexOf("JPM", StringComparison.CurrentCultureIgnoreCase) >= 0)
                    result = true;
                else if ("IB".Equals(broker, StringComparison.CurrentCultureIgnoreCase) && holding.Portfolio.IndexOf("IB", StringComparison.CurrentCultureIgnoreCase) >= 0)
                    result = true;
                else if ("Fidelity".Equals(broker, StringComparison.CurrentCultureIgnoreCase) && holding.Portfolio.IndexOf("Fido", StringComparison.CurrentCultureIgnoreCase) >= 0)
                    result = true;
            }
            catch (Exception)
            {
            }
            return result;
        }

        private bool FundFilter(AlmHolding holding, string fundName)
        {
            bool result = false;
            try
            {
                if ("All".Equals(fundName, StringComparison.CurrentCultureIgnoreCase))
                    result = true;
                else if (holding.Portfolio.Equals(fundName, StringComparison.CurrentCultureIgnoreCase))
                    //&& "Y".Equals(holding.InOpportunityFund, StringComparison.CurrentCultureIgnoreCase))
                    result = true;
                else if (holding.Portfolio.Equals(fundName, StringComparison.CurrentCultureIgnoreCase))
                    //&& "Y".Equals(holding.InTacticalFund, StringComparison.CurrentCultureIgnoreCase))
                    result = true;
            }
            catch (Exception)
            {
            }
            return result;
        }

        private bool BrokerFilter(AlmHolding holding, string broker)
        {
            bool result = false;
            try
            {
                if ("All".Equals(broker, StringComparison.CurrentCultureIgnoreCase))
                    result = true;
                else if (holding.Broker.Equals(broker, StringComparison.CurrentCultureIgnoreCase))
                    //&& holding.Portfolio.IndexOf("Jeff", StringComparison.CurrentCultureIgnoreCase) >= 0)
                    result = true;
                //else if (holding.Broker.Equals(broker, StringComparison.CurrentCultureIgnoreCase)) 
                //    //&& holding.Portfolio.IndexOf("JPM", StringComparison.CurrentCultureIgnoreCase) >= 0)
                //    result = true;
                //else if (holding.Broker.Equals(broker, StringComparison.CurrentCultureIgnoreCase))
                //    //&& holding.Portfolio.IndexOf("IB", StringComparison.CurrentCultureIgnoreCase) >= 0)
                //    result = true;
                //else if (holding.Broker.Equals(broker, StringComparison.CurrentCultureIgnoreCase))
                //    //&& holding.Portfolio.IndexOf("Fido", StringComparison.CurrentCultureIgnoreCase) >= 0)
                //    result = true;
            }
            catch (Exception)
            {
            }
            return result;
        }

        private Tuple<string, int> GetGroupName(string groupBy, FundMaster fundMaster, FundForecast fundForecast, UserDataOverride userOverride, Holding holding)
        {
            string groupName = "Other";
            int sortOrder = sortOrder = holding.GroupingSortOrder.GetValueOrDefault();

            if ("Discount vs IRR Trades".Equals(groupBy))
            {
                if (fundForecast != null)
                {
                    if (fundForecast.IRRLastPrc != null)
                    {
                        groupName = "IRR Trades";
                        sortOrder = 2;
                    }
                    else if (fundForecast.EAFinalAlpha != null)
                    {
                        groupName = "Discount Trades";
                        sortOrder = 1;
                    }
                }
            }
            else if ("Geo Level 1".Equals(groupBy))
            {
                if (fundMaster != null && !string.IsNullOrEmpty(fundMaster.GeoLvl1))
                    groupName = fundMaster.GeoLvl1;
            }
            else if ("Geo Level 2".Equals(groupBy))
            {
                if (fundMaster != null && !string.IsNullOrEmpty(fundMaster.GeoLvl2))
                    groupName = fundMaster.GeoLvl2;
            }
            else if ("Geo Level 3".Equals(groupBy))
            {
                if (fundMaster != null && !string.IsNullOrEmpty(fundMaster.GeoLvl3))
                    groupName = fundMaster.GeoLvl3;
            }
            else if ("Asset Class Level 1".Equals(groupBy))
            {
                if (fundMaster != null && !string.IsNullOrEmpty(fundMaster.AssetLvl1))
                    groupName = fundMaster.AssetLvl1;
            }
            else if ("Asset Class Level 2".Equals(groupBy))
            {
                if (fundMaster != null && !string.IsNullOrEmpty(fundMaster.AssetLvl2))
                    groupName = fundMaster.AssetLvl2;
            }
            else if ("Asset Class Level 3".Equals(groupBy))
            {
                if (fundMaster != null && !string.IsNullOrEmpty(fundMaster.AssetLvl3))
                    groupName = fundMaster.AssetLvl3;
            }
            else if ("Asset Type".Equals(groupBy))
            {
                if (fundMaster != null && !string.IsNullOrEmpty(fundMaster.SecTyp))
                    groupName = fundMaster.SecTyp;
            }
            else if ("Payment Rank".Equals(groupBy))
            {
                if (fundMaster != null && !string.IsNullOrEmpty(fundMaster.PayRank))
                    groupName = fundMaster.PayRank;
            }
            else if ("Fund Category".Equals(groupBy))
            {
                groupName = FundForecastOperations.GetFundCategory(fundForecast, userOverride);
                sortOrder = holding.GroupingSortOrder.GetValueOrDefault();
            }
            else if ("Country".Equals(groupBy))
            {
                if (fundMaster != null && !string.IsNullOrEmpty(fundMaster.Cntry))
                    groupName = fundMaster.Cntry;
            }
            else if ("Currency".Equals(groupBy))
            {
                if (fundMaster != null && !string.IsNullOrEmpty(fundMaster.Curr))
                    groupName = fundMaster.Curr;
            }
            else if ("Custom Level 1".Equals(groupBy))
            {
                Tuple<string, int> grouping = GetCustomLevel1Grouping(fundMaster, holding);
                groupName = grouping.Item1;
                sortOrder = grouping.Item2;
            }
            else if ("Custom Level 2".Equals(groupBy))
            {
                Tuple<string, int> grouping = GetCustomLevel2Grouping(fundMaster, holding);
                groupName = grouping.Item1;
                sortOrder = grouping.Item2;
            }
            else if ("Custom Level 3".Equals(groupBy))
            {
                Tuple<string, int> grouping = GetCustomLevel3Grouping(fundMaster, holding);
                groupName = grouping.Item1;
                sortOrder = grouping.Item2;
            }
            else if ("Custom Level 4".Equals(groupBy))
            {
                Tuple<string, int> grouping = GetCustomLevel4Grouping(fundMaster, holding);
                groupName = grouping.Item1;
                sortOrder = grouping.Item2;
            }
            else if ("Custom Level 5".Equals(groupBy))
            {
                Tuple<string, int> grouping = GetCustomLevel5Grouping(holding);
                groupName = grouping.Item1;
                sortOrder = grouping.Item2;
            }

            if ("Other".Equals(groupName))
                sortOrder = 999;
            else if (sortOrder == 0)
                sortOrder = 1;

            return new Tuple<string, int>(groupName, sortOrder);
        }

        private Tuple<string, int> GetCustomLevel1Grouping(FundMaster fundMaster, Holding holding)
        {
            string groupName = string.Empty;
            int sortOrder = 0;

            try
            {
                if (holding != null)
                {
                    groupName = holding.GroupingLevel1;
                    sortOrder = holding.GroupingSortOrder.GetValueOrDefault();
                }
                else
                {
                    groupName = fundMaster.AssetLvl1;

                    if (fundMaster.AssetLvl1.Equals("Currency", StringComparison.CurrentCultureIgnoreCase))
                    {
                        groupName = "Currency";
                        sortOrder = 99997;
                    }
                    else if (fundMaster.AssetLvl1.Equals("Fixed Income", StringComparison.CurrentCultureIgnoreCase))
                    {
                        groupName = "Fixed Income";
                        sortOrder = 99999;
                    }
                    else if (fundMaster.AssetLvl1.Equals("Equity", StringComparison.CurrentCultureIgnoreCase)
                        && fundMaster.PayRank.Equals("Equity", StringComparison.CurrentCultureIgnoreCase))
                    {
                        groupName = "Equity";
                        sortOrder = 99998;
                    }
                    else if (fundMaster.AssetLvl1.Equals("Equity", StringComparison.CurrentCultureIgnoreCase)
                        && !fundMaster.PayRank.Equals("Equity", StringComparison.CurrentCultureIgnoreCase))
                    {
                        groupName = "Fixed Income";
                        sortOrder = 99999;
                    }
                    else if (fundMaster.AssetLvl1.Equals("Alternatives", StringComparison.CurrentCultureIgnoreCase)
                        && fundMaster.PayRank.Equals("Equity", StringComparison.CurrentCultureIgnoreCase))
                    {
                        groupName = "Equity";
                        sortOrder = 99998;
                    }
                    else if (fundMaster.AssetLvl1.Equals("Alternatives", StringComparison.CurrentCultureIgnoreCase)
                        && !fundMaster.PayRank.Equals("Equity", StringComparison.CurrentCultureIgnoreCase))
                    {
                        groupName = "Fixed Income";
                        sortOrder = 99999;
                    }
                }
            }
            catch (Exception)
            {
            }

            return new Tuple<string, int>(groupName, sortOrder);
        }

        private Tuple<string, int> GetCustomLevel2Grouping(FundMaster fundMaster, Holding holding)
        {
            string groupName = string.Empty;
            int sortOrder = 0;

            try
            {
                if (holding != null)
                {
                    groupName = holding.GroupingLevel2;
                    sortOrder = holding.GroupingSortOrder.GetValueOrDefault();
                }
                else
                {
                    groupName = fundMaster.SecTyp;

                    if (fundMaster.SecTyp.Contains("Future"))
                    {
                        groupName = fundMaster.SecTyp;
                        sortOrder = 9998;
                    }
                    else if (fundMaster.AssetLvl1.Equals("Fixed Income", StringComparison.CurrentCultureIgnoreCase))
                    {
                        groupName = fundMaster.AssetLvl1 + " - " + "Cash";
                        sortOrder = 9999;
                    }
                    else if (fundMaster.AssetLvl1.Equals("Equity", StringComparison.CurrentCultureIgnoreCase))
                    {
                        groupName = fundMaster.AssetLvl1 + " - " + "Cash";
                        sortOrder = 9997;
                    }
                }
            }
            catch (Exception)
            {
            }

            return new Tuple<string, int>(groupName, sortOrder);
        }

        private Tuple<string, int> GetCustomLevel3Grouping(FundMaster fundMaster, Holding holding)
        {
            string groupName = string.Empty;
            int sortOrder = 0;

            try
            {
                if (holding != null)
                {
                    groupName = holding.GroupingLevel3;
                    sortOrder = holding.GroupingSortOrder.GetValueOrDefault();
                }
                else
                {
                    groupName = fundMaster.SecTyp;

                    if (fundMaster.SecTyp.Equals("ABS", StringComparison.CurrentCultureIgnoreCase) ||
                        fundMaster.SecTyp.Equals("MBS", StringComparison.CurrentCultureIgnoreCase))
                    {
                        groupName = "Structured Products";
                        sortOrder = 991;
                    }
                    else if (fundMaster.PayRank.Equals("Sr Unsecured", StringComparison.CurrentCultureIgnoreCase) ||
                        fundMaster.PayRank.Equals("Unsecured", StringComparison.CurrentCultureIgnoreCase) ||
                        fundMaster.PayRank.Equals("Collateralized Debt", StringComparison.CurrentCultureIgnoreCase))
                    {
                        groupName = "Debt Securities";
                        sortOrder = 999;
                    }
                    else if (fundMaster.PayRank.Equals("Preferred", StringComparison.CurrentCultureIgnoreCase))
                    {
                        groupName = fundMaster.PayRank;
                        sortOrder = 998;
                    }
                    else if (fundMaster.SecTyp.Equals("ETF", StringComparison.CurrentCultureIgnoreCase))
                    {
                        groupName = fundMaster.SecTyp + " - " + fundMaster.PayRank;
                        sortOrder = 990;
                    }
                    else if (fundMaster.SecTyp.Equals("BDC-Debt", StringComparison.CurrentCultureIgnoreCase) ||
                        fundMaster.SecTyp.Equals("BDC-Equity", StringComparison.CurrentCultureIgnoreCase))
                    {
                        groupName = fundMaster.SecTyp;
                    }
                    //TODO:
                    else if (fundMaster.SecTyp.Equals("Closed End Fund", StringComparison.CurrentCultureIgnoreCase))
                    {
                        if (fundMaster.Cntry.Equals("CANADA", StringComparison.CurrentCultureIgnoreCase))
                        {
                            groupName = "CAD " + fundMaster.SecTyp + " - " + fundMaster.PayRank;
                            sortOrder = 995;
                        }
                        else if (fundMaster.Cntry.Equals("United States", StringComparison.CurrentCultureIgnoreCase))
                        {
                            groupName = "US " + fundMaster.SecTyp + " - " + fundMaster.PayRank;
                            sortOrder = 996;
                        }
                        else
                        {
                            groupName = "UK " + fundMaster.SecTyp + " - " + fundMaster.PayRank;
                            sortOrder = 994;
                        }
                    }
                    else if (fundMaster.SecTyp.Equals("Holding Companies", StringComparison.CurrentCultureIgnoreCase))
                    {
                        if (fundMaster.Cntry.Equals("CANADA", StringComparison.CurrentCultureIgnoreCase))
                        {
                            groupName = "CAD " + fundMaster.SecTyp;
                        }
                        else if (fundMaster.Cntry.Equals("United States", StringComparison.CurrentCultureIgnoreCase))
                        {
                            groupName = "US " + fundMaster.SecTyp;
                        }
                        else
                        {
                            groupName = "UK " + fundMaster.SecTyp;
                        }
                    }
                    else if (fundMaster.SecTyp.Equals("Corporation", StringComparison.CurrentCultureIgnoreCase))
                    {
                        if (fundMaster.Cntry.Equals("CANADA", StringComparison.CurrentCultureIgnoreCase))
                        {
                            groupName = "CAD " + fundMaster.PayRank;
                        }
                        else if (fundMaster.Cntry.Equals("United States", StringComparison.CurrentCultureIgnoreCase))
                        {
                            groupName = "US " + fundMaster.PayRank;
                        }
                        else
                        {
                            groupName = "UK " + fundMaster.PayRank;
                        }
                    }
                    else if (fundMaster.SecTyp.Contains("Reit"))
                    {
                        groupName = "Mortgage Reits";
                        sortOrder = 997;
                    }
                }
            }
            catch (Exception)
            {
            }

            return new Tuple<string, int>(groupName, sortOrder);
        }

        private Tuple<string, int> GetCustomLevel4Grouping(FundMaster fundMaster, Holding holding)
        {
            string groupName = string.Empty;
            int sortOrder = 0;

            try
            {
                if (holding != null)
                {
                    groupName = holding.GroupingLevel4;
                    sortOrder = holding.GroupingSortOrder.GetValueOrDefault();
                }
                else
                {
                    groupName = fundMaster.SecTyp;

                    if (fundMaster.PayRank.Equals("Preferred", StringComparison.CurrentCultureIgnoreCase))
                    {
                        if (fundMaster.Cntry.Equals("CANADA", StringComparison.CurrentCultureIgnoreCase))
                        {
                            groupName = "CAD " + fundMaster.PayRank;
                        }
                        else if (fundMaster.Cntry.Equals("United States", StringComparison.CurrentCultureIgnoreCase))
                        {
                            groupName = "US " + fundMaster.PayRank;
                        }
                        else
                        {
                            groupName = "UK " + fundMaster.PayRank;
                        }
                    }
                    else if (fundMaster.SecTyp.Equals("ABS", StringComparison.CurrentCultureIgnoreCase))
                    {
                        groupName = "Structured - " + fundMaster.SecTyp;
                    }
                    else if (fundMaster.SecTyp.Equals("MBS", StringComparison.CurrentCultureIgnoreCase))
                    {
                        groupName = "Structured - " + fundMaster.SecTyp;
                    }
                    else if (fundMaster.SecTyp.Equals("Closed End Fund", StringComparison.CurrentCultureIgnoreCase))
                    {
                        if (fundMaster.Cntry.Equals("United States", StringComparison.CurrentCultureIgnoreCase))
                        {
                            if (fundMaster.AssetLvl1.Equals("Equity", StringComparison.CurrentCultureIgnoreCase) &&
                                fundMaster.GeoLvl1.Equals("Developed International", StringComparison.CurrentCultureIgnoreCase))
                            {
                                groupName = "US CEF - Dev Intl Equity";
                            }
                            else if (fundMaster.AssetLvl1.Equals("Equity", StringComparison.CurrentCultureIgnoreCase) &&
                                fundMaster.GeoLvl1.Equals("Emerging International", StringComparison.CurrentCultureIgnoreCase))
                            {
                                groupName = "US CEF - Emerging Intl Equity";
                            }
                            else if (fundMaster.AssetLvl1.Equals("Equity", StringComparison.CurrentCultureIgnoreCase) &&
                                fundMaster.AssetLvl3.Equals("MLP", StringComparison.CurrentCultureIgnoreCase))
                            {
                                groupName = "US CEF - MLP";
                            }
                            else if (fundMaster.AssetLvl1.Equals("Equity", StringComparison.CurrentCultureIgnoreCase))
                            {
                                groupName = "US CEF - General Equity";
                            }
                            else if (fundMaster.AssetLvl1.Equals("Fixed Income", StringComparison.CurrentCultureIgnoreCase) &&
                                fundMaster.AssetLvl2.Contains("Taxable Fixed Income"))
                            {
                                groupName = "US CEF - FI Taxable";
                            }
                            else if (fundMaster.AssetLvl1.Equals("Fixed Income", StringComparison.CurrentCultureIgnoreCase) &&
                                fundMaster.AssetLvl2.Contains("Tax-Exempt Fixed Income"))
                            {
                                groupName = "US CEF - FI Tax Exempt";
                            }
                        }
                    }
                }
            }
            catch (Exception)
            {
            }

            return new Tuple<string, int>(groupName, sortOrder);
        }

        private Tuple<string, int> GetCustomLevel5Grouping(Holding holding)
        {
            string groupName = string.Empty;
            int sortOrder = 0;

            try
            {
                if (holding != null)
                {
                    groupName = holding.GroupingLevel5;
                    sortOrder = holding.GroupingSortOrder.GetValueOrDefault();
                }
            }
            catch (Exception)
            {
            }

            return new Tuple<string, int>(groupName, sortOrder);
        }

        public void SaveSharesRecallOverrides(IList<AlmHolding> holdings)
        {
            try
            {
                IList<AlmHolding> filteredHoldings = new List<AlmHolding>();
                foreach (AlmHolding holding in holdings)
                {
                    if (!string.IsNullOrEmpty(holding.Update) && "Y".Equals(holding.Update, StringComparison.CurrentCultureIgnoreCase))
                    {
                        filteredHoldings.Add(holding);
                    }
                }

                if (filteredHoldings.Count > 0)
                {
                    _holdingsDao.SaveSharesRecallOverrides(holdings);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving shares recall overrides");
            }
        }

        public void SaveAlmHoldings(IList<AlmHolding> holdings)
        {
            try
            {
                _holdingsDao.SaveAlmHoldings(holdings);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving alm holdings");
            }
        }

        public IDictionary<string, PositionExposure> GenerateExposureReportWithCurrency(CachingService cache)
        {
            string ticker;

            try
            {
                IDictionary<string, PositionExposure> positionExposureDict = new Dictionary<string, PositionExposure>();
                IDictionary<string, PositionMaster> almPositionMasterDict = cache.Get<IDictionary<string, PositionMaster>>(CacheKeys.POSITION_MASTER);
                IDictionary<string, FundHoldingSummary> fundHoldingSummaryDict = cache.Get<IDictionary<string, FundHoldingSummary>>(CacheKeys.FUND_HOLDING_SUMMARY);

                foreach (KeyValuePair<string, PositionMaster> kvp in almPositionMasterDict)
                {
                    PositionMaster positionMaster = kvp.Value;

                    if (!string.IsNullOrEmpty(positionMaster.SecTicker))
                        ticker = positionMaster.SecTicker;
                    else
                        ticker = positionMaster.Ticker;

                    PositionExposure positionExposure;
                    if (!positionExposureDict.TryGetValue(ticker, out positionExposure))
                    {
                        positionExposure = new PositionExposure
                        {
                            Ticker = positionMaster.Ticker,
                            SecTicker = positionMaster.SecTicker,
                            PositionMaster = positionMaster
                        };

                        positionExposureDict.Add(ticker, positionExposure);
                    }

                    FundHoldingSummary fundHoldingSummary;
                    if (fundHoldingSummaryDict.TryGetValue(ticker, out fundHoldingSummary))
                    {
                        positionExposure.Currency1 = fundHoldingSummary.Currency1;
                        positionExposure.Currency2 = fundHoldingSummary.Currency2;
                        positionExposure.Currency3 = fundHoldingSummary.Currency3;
                        positionExposure.Currency4 = fundHoldingSummary.Currency4;
                        positionExposure.Currency5 = fundHoldingSummary.Currency5;

                        positionExposure.Currency1Exp = fundHoldingSummary.Currency1Exp;
                        positionExposure.Currency2Exp = fundHoldingSummary.Currency2Exp;
                        positionExposure.Currency3Exp = fundHoldingSummary.Currency3Exp;
                        positionExposure.Currency4Exp = fundHoldingSummary.Currency4Exp;
                        positionExposure.Currency5Exp = fundHoldingSummary.Currency5Exp;
                    }
                }

                return positionExposureDict;

            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error generating position exposure report");
                return null;
            }
        }

        public IList<string> GenerateCurrencyExposureList(CachingService cache)
        {
            string ticker;

            try
            {
                IDictionary<string, string> currencyDict = new Dictionary<string, string>();
                IDictionary<string, PositionMaster> almPositionMasterDict = cache.Get<IDictionary<string, PositionMaster>>(CacheKeys.POSITION_MASTER);
                IDictionary<string, FundHoldingSummary> fundHoldingSummaryDict = cache.Get<IDictionary<string, FundHoldingSummary>>(CacheKeys.FUND_HOLDING_SUMMARY);

                foreach (KeyValuePair<string, PositionMaster> kvp in almPositionMasterDict)
                {
                    PositionMaster positionMaster = kvp.Value;

                    if (!string.IsNullOrEmpty(positionMaster.SecTicker))
                        ticker = positionMaster.SecTicker;
                    else
                        ticker = positionMaster.Ticker;

                    if (positionMaster.FundAll.PosHeld.HasValue)
                    {
                        FundHoldingSummary fundHoldingSummary;
                        if (fundHoldingSummaryDict.TryGetValue(ticker, out fundHoldingSummary))
                        {
                            string currency = fundHoldingSummary.Currency1;
                            double? currencyExposure = fundHoldingSummary.Currency1Exp;
                            if (!string.IsNullOrEmpty(currency) && currencyExposure.HasValue)
                            {
                                if (!currencyDict.ContainsKey(currency))
                                    currencyDict.Add(currency, currency);
                            }

                            currency = fundHoldingSummary.Currency2;
                            currencyExposure = fundHoldingSummary.Currency2Exp;
                            if (!string.IsNullOrEmpty(currency) && currencyExposure.HasValue)
                            {
                                if (!currencyDict.ContainsKey(currency))
                                    currencyDict.Add(currency, currency);
                            }

                            currency = fundHoldingSummary.Currency3;
                            currencyExposure = fundHoldingSummary.Currency3Exp;
                            if (!string.IsNullOrEmpty(currency) && currencyExposure.HasValue)
                            {
                                if (!currencyDict.ContainsKey(currency))
                                    currencyDict.Add(currency, currency);
                            }

                            currency = fundHoldingSummary.Currency4;
                            currencyExposure = fundHoldingSummary.Currency4Exp;
                            if (!string.IsNullOrEmpty(currency) && currencyExposure.HasValue)
                            {
                                if (!currencyDict.ContainsKey(currency))
                                    currencyDict.Add(currency, currency);
                            }

                            currency = fundHoldingSummary.Currency5;
                            currencyExposure = fundHoldingSummary.Currency5Exp;
                            if (!string.IsNullOrEmpty(currency) && currencyExposure.HasValue)
                            {
                                if (!currencyDict.ContainsKey(currency))
                                    currencyDict.Add(currency, currency);
                            }
                        }
                        else
                        {
                            if (!string.IsNullOrEmpty(positionMaster.Curr))
                            {
                                if (!currencyDict.ContainsKey(positionMaster.Curr))
                                    currencyDict.Add(positionMaster.Curr, positionMaster.Curr);
                            }
                        }
                    }
                }

                return currencyDict.Values.ToList<string>();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error generating currency list");
                return null;
            }
        }

        public IList<PositionTO> GetAlmHoldingsByDate(DateTime asOfDate, string fundName, string longShortInd, string topHoldings)
        {
            int longShortIndicator = 0;
            int topHoldingsIndicator = 0;
            int includeHolding = 0;
            if (!string.IsNullOrEmpty(longShortInd))
                longShortIndicator = 1;

            if (!string.IsNullOrEmpty(topHoldings))
                topHoldingsIndicator = Int16.Parse(topHoldings);

            IDictionary<string, string> positionIdentifierMap = _cache.Get<IDictionary<string, string>>(CacheKeys.POSITION_IDENTIFIER_MAP);
            IDictionary<string, SecurityMasterExt> securityMasterExtDict = _cache.Get<IDictionary<string, SecurityMasterExt>>(CacheKeys.SECURITY_EXT_DETAILS);
            IList<PositionTO> list = _holdingsDao.GetAlmHoldingsByDate(asOfDate, fundName);
            IList<PositionTO> filteredList = new List<PositionTO>();
            foreach (PositionTO data in list)
            {
                data.Symbol = data.Ticker;
                if (!string.IsNullOrEmpty(data.PfdExchSym))
                    data.Symbol = data.PfdExchSym;
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

                //filter based on top holdings setting
                if (topHoldingsIndicator > 1 && includeHolding == 1)
                {
                    if (data.Rank <= topHoldingsIndicator)
                        includeHolding = 1;
                    else
                        includeHolding = 0;
                }

                if (includeHolding == 1)
                {
                    filteredList.Add(data);
                    SecurityMasterExt securityMasterExt = CommonOperationsUtil.GetSecurityMasterExt(data.Ticker, data.Ticker, data.Sedol, data.ISIN, securityMasterExtDict, positionIdentifierMap);
                    if (securityMasterExt != null)
                    {
                        data.Beta = securityMasterExt.RiskBeta;
                        data.Dur = securityMasterExt.Duration;
                        data.FundGrp = securityMasterExt.FundGroup;
                    }
                }
            }
            return filteredList.OrderBy(p => p.Ticker).ThenBy(p => p.Fund).ToList<PositionTO>();
            //return filteredList.OrderBy(p => p.Rank).ThenBy(p => p.Ticker).ThenBy(p => p.Fund).ToList<PositionTO>();
        }
    }
}