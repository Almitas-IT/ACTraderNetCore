using aACTrader.Model;
using aCommons;
using aCommons.Cef;
using LazyCache;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;

namespace aACTrader.Operations.Impl
{
    public class TradeFilterOperations
    {
        private readonly ILogger<TradeFilterOperations> _logger;
        private readonly CachingService _cache;

        public TradeFilterOperations(ILogger<TradeFilterOperations> logger, CachingService cache)
        {
            _logger = logger;
            _cache = cache;
        }

        public void GetACHoldingsList(CEFParameters cefParams)
        {
            IList<FundMaster> filteredFundMasterList = new List<FundMaster>();
            IList<FundGroup> missingFundGroup = new List<FundGroup>();

            try
            {
                IDictionary<string, FundMaster> fundMasterDict = _cache.Get<IDictionary<string, FundMaster>>(CacheKeys.FUND_MASTER);
                IDictionary<string, FundForecast> fundForecastDict = _cache.Get<IDictionary<string, FundForecast>>(CacheKeys.FUND_FORECASTS);
                IDictionary<string, UserDataOverride> userOverrideDict = _cache.Get<IDictionary<string, UserDataOverride>>(CacheKeys.USER_OVERRIDES);
                IDictionary<string, PositionMaster> almPositionMasterDict = _cache.Get<IDictionary<string, PositionMaster>>(CacheKeys.POSITION_MASTER);
                IDictionary<string, FundRedemption> fundRedemptionDict = _cache.Get<IDictionary<string, FundRedemption>>(CacheKeys.FUND_REDEMPTIONS);

                bool matched = false;
                foreach (KeyValuePair<string, PositionMaster> kvp in almPositionMasterDict)
                {
                    PositionMaster positionMaster = kvp.Value;
                    string holdingsTicker = positionMaster.Ticker;
                    string securityTicker = positionMaster.SecTicker;
                    string countryCode = "Unknown";

                    if (positionMaster.FundAll.MV.HasValue)
                    {
                        FundMaster fundMaster;
                        fundMasterDict.TryGetValue(holdingsTicker, out fundMaster);
                        if (fundMaster == null && securityTicker != null)
                            fundMasterDict.TryGetValue(securityTicker, out fundMaster);

                        matched = false;
                        if (fundMaster != null)
                        {
                            countryCode = fundMaster.CntryCd;
                            if (ApplyAssetTypeFilter(fundMaster, cefParams.AssetType, fundRedemptionDict))
                            {
                                if (ApplyPaymentRankFilter(fundMaster, cefParams.PaymentRank))
                                    matched = true;
                            }
                        }
                        else
                        {
                            FundGroup fundGroup = new FundGroup
                            {
                                Ticker = securityTicker != null ? securityTicker : holdingsTicker,
                                GroupName = "Other"
                            };
                            missingFundGroup.Add(fundGroup);
                        }

                        if (matched)
                        {
                            if (cefParams.Market.Equals("AC Holdings Only"))
                            {
                                filteredFundMasterList.Add(fundMaster);
                            }
                            else if (cefParams.Market.Equals("Eur Holdings Only"))
                            {
                                if (!("CA".Equals(countryCode, StringComparison.CurrentCultureIgnoreCase)
                                    || "US".Equals(countryCode, StringComparison.CurrentCultureIgnoreCase)
                                    || "Australia".Equals(countryCode, StringComparison.CurrentCultureIgnoreCase)
                                    || "Bangladesh".Equals(countryCode, StringComparison.CurrentCultureIgnoreCase)))
                                {
                                    filteredFundMasterList.Add(fundMaster);
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
            }
        }

        /// <summary>
        /// Applies Asset Type Filter
        /// </summary>
        /// <param name="fundMaster"></param>
        /// <param name="selectedAssetType"></param>
        /// <param name="fundRedemptionDict"></param>
        /// <returns></returns>
        private bool ApplyAssetTypeFilter(FundMaster fundMaster, string selectedAssetType, IDictionary<string, FundRedemption> fundRedemptionDict)
        {
            bool assetType = false;

            try
            {
                switch (selectedAssetType)
                {
                    case "All":
                        assetType = true;
                        break;
                    case "Non CEF":
                        if (!"CEF".Equals(fundMaster.AssetTyp))
                            assetType = true;
                        break;
                    case "BDC & Reit":
                        if ("BDC".Equals(fundMaster.AssetTyp) || "Reit".Equals(fundMaster.AssetTyp))
                            assetType = true;
                        break;
                }


                if ("All".Equals(selectedAssetType, StringComparison.CurrentCultureIgnoreCase))
                {
                    assetType = true;
                }
                else if ("Non CEF".Equals(selectedAssetType, StringComparison.CurrentCultureIgnoreCase))
                {
                    if (!"CEF".Equals(fundMaster.AssetTyp, StringComparison.CurrentCultureIgnoreCase))
                    {
                        assetType = true;
                    }
                }
                else if ("BDC & Reit".Equals(selectedAssetType, StringComparison.CurrentCultureIgnoreCase))
                {
                    if ("BDC".Equals(fundMaster.AssetTyp, StringComparison.CurrentCultureIgnoreCase)
                        || "Reit".Equals(fundMaster.AssetTyp, StringComparison.CurrentCultureIgnoreCase))
                    {
                        assetType = true;
                    }
                }
                else if (selectedAssetType.Equals(fundMaster.AssetTyp, StringComparison.CurrentCultureIgnoreCase))
                {
                    assetType = true;
                }
                else if ("IRR".Equals(selectedAssetType, StringComparison.CurrentCultureIgnoreCase))
                {
                    string countryCode = fundMaster.CntryCd;

                    if (fundRedemptionDict.TryGetValue(fundMaster.Ticker, out FundRedemption fundRedemption)
                        && fundRedemption.NextRedemptionDate.HasValue
                        && !("Bangladesh".Equals(countryCode, StringComparison.CurrentCultureIgnoreCase)))
                        assetType = true;
                }
            }
            catch (Exception)
            {
            }
            return assetType;
        }

        private bool ApplyCountryFilter(FundMaster fundMaster
            , string selectedMarket
            , IDictionary<string, PositionMaster> almPositionMasterDict
            , IDictionary<string, FundForecast> fundForecastDict)
        {
            bool marketFilter = false;
            try
            {
                string countryCode = fundMaster.CntryCd;

                if ("US".Equals(selectedMarket, StringComparison.CurrentCultureIgnoreCase)
                    && "US".Equals(countryCode, StringComparison.CurrentCultureIgnoreCase))
                {
                    marketFilter = true;
                }
                else if ("UK".Equals(selectedMarket, StringComparison.CurrentCultureIgnoreCase)
                    && "UK".Equals(countryCode, StringComparison.CurrentCultureIgnoreCase))
                {
                    marketFilter = true;
                }
                else if ("Canada".Equals(selectedMarket, StringComparison.CurrentCultureIgnoreCase)
                    && "CA".Equals(countryCode, StringComparison.CurrentCultureIgnoreCase))
                {
                    marketFilter = true;
                }
                else if ("AUS".Equals(selectedMarket, StringComparison.CurrentCultureIgnoreCase)
                    && "Australia".Equals(countryCode, StringComparison.CurrentCultureIgnoreCase))
                {
                    marketFilter = true;
                }
                else if ("Asia Pacific".Equals(selectedMarket, StringComparison.CurrentCultureIgnoreCase)
                    && ("Australia".Equals(countryCode, StringComparison.CurrentCultureIgnoreCase)
                        || "Bangladesh".Equals(countryCode, StringComparison.CurrentCultureIgnoreCase)))
                {
                    marketFilter = true;
                }
                else if ("North America".Equals(selectedMarket, StringComparison.CurrentCultureIgnoreCase)
                    && ("CA".Equals(countryCode, StringComparison.CurrentCultureIgnoreCase)
                        || "US".Equals(countryCode, StringComparison.CurrentCultureIgnoreCase)))
                {
                    marketFilter = true;
                }
                else if ("Major European".Equals(selectedMarket, StringComparison.CurrentCultureIgnoreCase)
                    && !("CA".Equals(countryCode, StringComparison.CurrentCultureIgnoreCase)
                        || "US".Equals(countryCode, StringComparison.CurrentCultureIgnoreCase)
                        || "Australia".Equals(countryCode, StringComparison.CurrentCultureIgnoreCase)
                        || "Bangladesh".Equals(countryCode, StringComparison.CurrentCultureIgnoreCase)))
                {
                    marketFilter = true;
                }
                else if ("World".Equals(selectedMarket, StringComparison.CurrentCultureIgnoreCase))
                {
                    marketFilter = true;
                }
                else if ("AC Holdings Only".Equals(selectedMarket, StringComparison.CurrentCultureIgnoreCase))
                {
                    if (almPositionMasterDict.TryGetValue(fundMaster.Ticker, out PositionMaster positionMaster))
                        marketFilter = true;
                }
                else if ("SPACs".Equals(selectedMarket, StringComparison.CurrentCultureIgnoreCase))
                {
                    if (fundForecastDict.TryGetValue(fundMaster.Ticker, out FundForecast fundForecast))
                    {
                        if (fundForecast.FundCat.Equals("SPACs", StringComparison.CurrentCultureIgnoreCase))
                            marketFilter = true;
                    }
                }
            }
            catch (Exception)
            {
            }

            return marketFilter;
        }

        private bool ApplyPaymentRankFilter(FundMaster fundMaster, string selectedPaymentRank)
        {
            bool paymentRankFilter = false;
            try
            {
                if ("All".Equals(selectedPaymentRank, StringComparison.CurrentCultureIgnoreCase))
                {
                    paymentRankFilter = true;
                }
                else if ("Non Equity".Equals(selectedPaymentRank, StringComparison.CurrentCultureIgnoreCase))
                {
                    if (!"Equity".Equals(fundMaster.PayRank, StringComparison.CurrentCultureIgnoreCase))
                    {
                        paymentRankFilter = true;
                    }
                }
                else if (selectedPaymentRank.Equals(fundMaster.PayRank, StringComparison.CurrentCultureIgnoreCase))
                {
                    paymentRankFilter = true;
                }
            }
            catch (Exception)
            {
            }
            return paymentRankFilter;
        }
    }
}