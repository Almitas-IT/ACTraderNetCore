using aCommons;
using aCommons.Fund;
using aCommons.Utils;
using LazyCache;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;

namespace aACTrader.Operations.Impl
{
    public class BrokerReportOperations
    {
        private readonly ILogger<BrokerReportOperations> _logger;
        private readonly CachingService _cache;

        public BrokerReportOperations(ILogger<BrokerReportOperations> logger, CachingService cache)
        {
            _logger = logger;
            _cache = cache;
        }

        public FundDetail GetFundDetails(string broker, string fundName)
        {
            IList<FundDetail> fundDetailList = null;

            if (broker.Equals("Fidelity", StringComparison.CurrentCultureIgnoreCase))
                fundDetailList = _cache.Get<IList<FundDetail>>(CacheKeys.FIDELITY_FUND_DETAILS);
            else if (broker.Equals("JPM", StringComparison.CurrentCultureIgnoreCase))
                fundDetailList = _cache.Get<IList<FundDetail>>(CacheKeys.JPM_FUND_DETAILS);
            else if (broker.Equals("IB", StringComparison.CurrentCultureIgnoreCase))
                fundDetailList = _cache.Get<IList<FundDetail>>(CacheKeys.IB_FUND_DETAILS);
            else if (broker.Equals("Jefferies", StringComparison.CurrentCultureIgnoreCase))
                fundDetailList = _cache.Get<IList<FundDetail>>(CacheKeys.JEFFERIES_FUND_DETAILS);
            else if (broker.Equals("EDF", StringComparison.CurrentCultureIgnoreCase))
                fundDetailList = _cache.Get<IList<FundDetail>>(CacheKeys.EDF_FUND_DETAILS);

            if (fundDetailList != null && fundDetailList.Count > 0)
            {
                foreach (FundDetail fundDetail in fundDetailList)
                {
                    if (fundDetail.Id.Equals(fundName, StringComparison.CurrentCultureIgnoreCase))
                        return fundDetail;
                }
            }
            return null;
        }

        public FundCashDetail GetFundCashDetails(string broker, string fundName, string currency, string assetType)
        {
            IList<FundCashDetail> fundCashDetailList = null;
            string lookupKey = string.Empty;

            if (broker.Equals("Fidelity", StringComparison.CurrentCultureIgnoreCase))
            {
                fundCashDetailList = _cache.Get<IList<FundCashDetail>>(CacheKeys.FIDELITY_MARGIN_DETAILS);
                lookupKey = fundName + "|" + currency + "|" + assetType;
            }
            else if (broker.Equals("JPM", StringComparison.CurrentCultureIgnoreCase))
            {
                fundCashDetailList = _cache.Get<IList<FundCashDetail>>(CacheKeys.JPM_MARGIN_DETAILS);
                lookupKey = fundName + "|" + currency;
            }
            else if (broker.Equals("IB", StringComparison.CurrentCultureIgnoreCase))
            {
                fundCashDetailList = _cache.Get<IList<FundCashDetail>>(CacheKeys.IB_MARGIN_DETAILS);
                lookupKey = fundName + "|" + currency;
            }
            else if (broker.Equals("Jefferies", StringComparison.CurrentCultureIgnoreCase))
            {
                fundCashDetailList = _cache.Get<IList<FundCashDetail>>(CacheKeys.JEFFERIES_MARGIN_DETAILS);
                lookupKey = fundName + "|" + currency + "|" + assetType;
            }

            if (fundCashDetailList != null && fundCashDetailList.Count > 0)
            {
                foreach (FundCashDetail fundCashDetail in fundCashDetailList)
                {
                    if (fundCashDetail.Id.Equals(lookupKey, StringComparison.CurrentCultureIgnoreCase))
                        return fundCashDetail;
                }
            }
            return null;
        }

        public FundMarginAttributionDetail GetFundMarginAttributionDetails(string broker, string fundName)
        {
            IList<FundMarginAttributionDetail> fundMarginAttributionDetailList = null;

            if (broker.Equals("FIDELITY", StringComparison.CurrentCultureIgnoreCase))
                fundMarginAttributionDetailList = _cache.Get<IList<FundMarginAttributionDetail>>(CacheKeys.FIDELITY_MARGIN_ATTRIBUTION_DETAILS);

            if (fundMarginAttributionDetailList != null && fundMarginAttributionDetailList.Count > 0)
            {
                foreach (FundMarginAttributionDetail fundMarginAttributionDetail in fundMarginAttributionDetailList)
                {
                    if (fundMarginAttributionDetail.Id.Equals(fundName, StringComparison.CurrentCultureIgnoreCase))
                        return fundMarginAttributionDetail;
                }
            }
            return null;
        }

        public FundSwapDetail GetFundSwapDetails(string broker, string fundName, string ticker, string side)
        {
            IList<FundSwapDetail> fundSwapDetailList = null;
            string lookupKey = string.Empty;

            if (broker.Equals("JPM", StringComparison.CurrentCultureIgnoreCase))
            {
                fundSwapDetailList = _cache.Get<IList<FundSwapDetail>>(CacheKeys.JPM_SWAP_DETAILS);
                lookupKey = fundName + "|" + ticker;
            }
            else if (broker.Equals("JPM_BY_FUND", StringComparison.CurrentCultureIgnoreCase))
            {
                fundSwapDetailList = _cache.Get<IList<FundSwapDetail>>(CacheKeys.JPM_SWAP_DETAILS_BY_FUND);
                lookupKey = fundName;
            }
            else if (broker.Equals("JPM_BY_FUND_BY_TYPE", StringComparison.CurrentCultureIgnoreCase))
            {
                fundSwapDetailList = _cache.Get<IList<FundSwapDetail>>(CacheKeys.JPM_SWAP_DETAILS_BY_FUND_BY_TYPE);
                lookupKey = fundName + "|" + side;
            }

            if (fundSwapDetailList != null && fundSwapDetailList.Count > 0)
            {
                foreach (FundSwapDetail fundSwapDetail in fundSwapDetailList)
                {
                    if (fundSwapDetail.Id.Equals(lookupKey, StringComparison.CurrentCultureIgnoreCase))
                        return fundSwapDetail;
                }
            }
            return null;
        }

        public FundFinanceDetail GetFundFinanceDetails(string broker, string fundName, string currency)
        {
            IList<FundFinanceDetail> fundFinanceDetailList = null;
            string lookupKey = fundName + "|" + currency;

            if (broker.Equals("JPM", StringComparison.CurrentCultureIgnoreCase))
                fundFinanceDetailList = _cache.Get<IList<FundFinanceDetail>>(CacheKeys.JPM_FINANCE_DETAILS);

            if (fundFinanceDetailList != null && fundFinanceDetailList.Count > 0)
            {
                foreach (FundFinanceDetail fundFinanceDetail in fundFinanceDetailList)
                {
                    if (fundFinanceDetail.Id.Equals(lookupKey, StringComparison.CurrentCultureIgnoreCase))
                        return fundFinanceDetail;
                }
            }
            return null;
        }

        public FundInterestEarningDetail GetFundInterestEarningDetails(string broker, string fundName, string currency)
        {
            IList<FundInterestEarningDetail> fundInterestEarningDetailList = null;
            string lookupKey = fundName + "|" + currency;

            if (broker.Equals("Fidelity", StringComparison.CurrentCultureIgnoreCase))
                fundInterestEarningDetailList = _cache.Get<IList<FundInterestEarningDetail>>(CacheKeys.FIDELITY_INTEREST_EARNING_DETAILS);

            if (fundInterestEarningDetailList != null && fundInterestEarningDetailList.Count > 0)
            {
                foreach (FundInterestEarningDetail fundInterestEarningDetail in fundInterestEarningDetailList)
                {
                    if (fundInterestEarningDetail.Id.Equals(lookupKey, StringComparison.CurrentCultureIgnoreCase))
                        return fundInterestEarningDetail;
                }
            }
            return null;
        }

        public FundCurrencyDetail GetFundCurrencyDetails(string broker, string fundName, string currency, string contractType)
        {
            IList<FundCurrencyDetail> fundCurrencyDetailList = null;
            string lookupKey = fundName + "|" + currency + "|" + contractType;

            if (broker.Equals("JPM", StringComparison.CurrentCultureIgnoreCase))
                fundCurrencyDetailList = _cache.Get<IList<FundCurrencyDetail>>(CacheKeys.JPM_CURRENCY_DETAILS);

            if (fundCurrencyDetailList != null && fundCurrencyDetailList.Count > 0)
            {
                foreach (FundCurrencyDetail fundCurrencyDetail in fundCurrencyDetailList)
                {
                    if (fundCurrencyDetail.Id.Equals(lookupKey, StringComparison.CurrentCultureIgnoreCase))
                        return fundCurrencyDetail;
                }
            }
            return null;
        }

        public Nullable<double> GetIBPositionMarketValue(string broker, string fundName, string assetType, string side, string assetGroup)
        {
            IList<FundPositionDetail> fundPositionDetailList = null;
            Nullable<double> marketValue = null;

            if (broker.Equals("IB", StringComparison.CurrentCultureIgnoreCase))
                fundPositionDetailList = _cache.Get<IList<FundPositionDetail>>(CacheKeys.IB_POSITION_DETAILS);

            if (fundPositionDetailList != null && fundPositionDetailList.Count > 0)
            {
                foreach (FundPositionDetail fundPositionDetail in fundPositionDetailList)
                {
                    bool addPos = false;

                    if (!string.IsNullOrEmpty(fundName) && fundPositionDetail.FundName.Equals(fundName, StringComparison.CurrentCultureIgnoreCase))
                        addPos = true;
                    else
                        addPos = false;

                    if (addPos && !string.IsNullOrEmpty(assetType) &&
                        fundPositionDetail.AssetClass.Equals(assetType, StringComparison.CurrentCultureIgnoreCase))
                        addPos = true;
                    else
                        addPos = false;

                    if (addPos && !string.IsNullOrEmpty(side) &&
                        fundPositionDetail.Side.Equals(side, StringComparison.CurrentCultureIgnoreCase))
                        addPos = true;
                    else
                        addPos = false;

                    if (addPos && !string.IsNullOrEmpty(assetGroup))
                    {
                        if (assetGroup.Equals("Currency"))
                        {
                            if (fundPositionDetail.UnderlyingSymbol.Equals("CAD")
                                || fundPositionDetail.UnderlyingSymbol.Equals("EUR")
                                || fundPositionDetail.UnderlyingSymbol.Equals("GBP")
                                || fundPositionDetail.UnderlyingSymbol.Equals("ILS"))
                                addPos = true;
                            else
                                addPos = false;
                        }
                        else if (assetGroup.Equals("Bonds"))
                        {
                            if (fundPositionDetail.UnderlyingSymbol.Equals("ZN")
                                || fundPositionDetail.UnderlyingSymbol.Equals("GBL")
                                || fundPositionDetail.UnderlyingSymbol.Equals("OAT")
                                || fundPositionDetail.UnderlyingSymbol.Equals("JGB"))
                                addPos = true;
                            else
                                addPos = false;
                        }
                        else if (assetGroup.Equals("Crypto"))
                        {
                            if (fundPositionDetail.UnderlyingSymbol.Equals("BRR"))
                                addPos = true;
                            else
                                addPos = false;
                        }
                        else
                        {
                            addPos = false;
                        }
                    }

                    if (addPos)
                        marketValue = CommonUtils.AddNullableDoubles(marketValue, fundPositionDetail.MarketValue);
                }
            }
            return marketValue;
        }
    }
}