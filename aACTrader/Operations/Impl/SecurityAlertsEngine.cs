using aACTrader.DAO.Repository;
using aCommons;
using aCommons.Alerts;
using aCommons.Cef;
using LazyCache;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;

namespace aACTrader.Operations.Impl
{
    public class SecurityAlertsEngine
    {
        private readonly ILogger<SecurityAlertsEngine> _logger;
        private readonly SecurityAlertDao _securityAlertDao;
        private readonly CachingService _cache;

        public SecurityAlertsEngine(ILogger<SecurityAlertsEngine> logger
            , SecurityAlertDao securityAlertDao
            , CachingService cache)
        {
            _logger = logger;
            _securityAlertDao = securityAlertDao;
            _cache = cache;
        }

        public void ProcessSecurityAlerts()
        {
            IList<SecurityAlertDetail> securityAlertDetails = new List<SecurityAlertDetail>();

            IList<SecurityAlert> securityAlerts = _cache.Get<IList<SecurityAlert>>(CacheKeys.SECURITY_ALERT_TARGETS);
            if (securityAlerts != null && securityAlerts.Count > 0)
            {
                IDictionary<string, string> priceTickerMap = _cache.Get<IDictionary<string, string>>(CacheKeys.PRICE_TICKER_MAP);
                IDictionary<string, SecurityPrice> securityPriceDict = _cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);
                IDictionary<string, FundForecast> fundForecastDict = _cache.Get<IDictionary<string, FundForecast>>(CacheKeys.FUND_FORECASTS);

                foreach (SecurityAlert data in securityAlerts)
                {
                    string alertyType = data.AlertType;
                    if (alertyType.Equals("PriceChange", StringComparison.CurrentCultureIgnoreCase))
                        PriceChangeAlert(data, securityAlertDetails, priceTickerMap, securityPriceDict);
                    if (alertyType.Equals("PriceTarget", StringComparison.CurrentCultureIgnoreCase))
                        PriceTargetAlert(data, securityAlertDetails, priceTickerMap, securityPriceDict);
                    else if (alertyType.Equals("DiscountChange", StringComparison.CurrentCultureIgnoreCase))
                        DiscountChangeAlert(data, securityAlertDetails, fundForecastDict);
                    else if (alertyType.Equals("DiscountTarget", StringComparison.CurrentCultureIgnoreCase))
                        DiscountTargetAlert(data, securityAlertDetails, fundForecastDict);
                    else if (alertyType.Equals("PriceDifferential", StringComparison.CurrentCultureIgnoreCase))
                        PriceDifferentialAlert(data, securityAlertDetails, priceTickerMap, securityPriceDict);
                    else if (alertyType.Equals("DiscountDifferential", StringComparison.CurrentCultureIgnoreCase))
                        DiscountDifferentialAlert(data, securityAlertDetails, fundForecastDict);
                }
            }

            _cache.Remove(CacheKeys.SECURITY_ALERTS);
            _cache.Add(CacheKeys.SECURITY_ALERTS, securityAlertDetails, DateTimeOffset.MaxValue);
        }

        private void PriceTargetAlert(
            SecurityAlert securityAlert
            , IList<SecurityAlertDetail> securityAlertDetails
            , IDictionary<string, string> priceTickerMap
            , IDictionary<string, SecurityPrice> securityPriceDict)

        {
            try
            {
                string alertFlag = "N";
                SecurityPrice securityPrice = SecurityPriceLookupOperations.GetSecurityPrice(securityAlert.Ticker1, priceTickerMap, securityPriceDict);
                if (securityPrice != null && securityPrice.LastPrc.HasValue)
                {
                    double lastPrice = securityPrice.LastPrc.GetValueOrDefault();
                    double targetPrice = securityAlert.TargetValue.GetValueOrDefault();
                    double priceDiff = targetPrice - lastPrice;

                    if ("BUY".Equals(securityAlert.TransType, StringComparison.CurrentCultureIgnoreCase))
                    {
                        if (lastPrice <= targetPrice)
                            alertFlag = "Y";
                    }
                    else if ("SELL".Equals(securityAlert.TransType, StringComparison.CurrentCultureIgnoreCase))
                    {
                        if (lastPrice >= targetPrice)
                            alertFlag = "Y";
                    }

                    //
                    if (alertFlag.Equals("Y"))
                    {
                        SecurityAlertDetail securityAlertDetail = new SecurityAlertDetail();
                        securityAlertDetail.AlertFlag = alertFlag;
                        securityAlertDetail.SecurityAlert = securityAlert;
                        securityAlertDetail.LivePrice = lastPrice;
                        securityAlertDetail.TgtPrice = securityAlert.TargetValue;
                        securityAlertDetail.TgtPriceDiff = priceDiff;
                        securityAlertDetails.Add(securityAlertDetail);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing Price Target for Id: " + securityAlert.Id + "; Ticker1: " + securityAlert.Ticker1, ex);
            }
        }

        private void PriceChangeAlert(
            SecurityAlert securityAlert
            , IList<SecurityAlertDetail> securityAlertDetails
            , IDictionary<string, string> priceTickerMap
            , IDictionary<string, SecurityPrice> securityPriceDict)

        {
            try
            {
                string alertFlag = "N";
                SecurityPrice securityPrice = SecurityPriceLookupOperations.GetSecurityPrice(securityAlert.Ticker1, priceTickerMap, securityPriceDict);
                if (securityPrice != null
                    && securityPrice.PrcChng.HasValue
                    && securityPrice.PrcChng.GetValueOrDefault() != 0)
                {
                    double priceChange = securityPrice.PrcChng.GetValueOrDefault();
                    double targetPriceChange = securityAlert.TargetValue.GetValueOrDefault();
                    double priceChangeDiff = Math.Abs(targetPriceChange) - Math.Abs(priceChange);

                    if ("UP".Equals(securityAlert.TargetChangeSide, StringComparison.CurrentCultureIgnoreCase))
                    {
                        if (priceChange >= targetPriceChange)
                            alertFlag = "Y";
                    }
                    else if ("DOWN".Equals(securityAlert.TargetChangeSide, StringComparison.CurrentCultureIgnoreCase))
                    {
                        if (priceChange <= -1.0 * targetPriceChange)
                            alertFlag = "Y";
                    }
                    else if ("BOTH".Equals(securityAlert.TargetChangeSide, StringComparison.CurrentCultureIgnoreCase))
                    {
                        if (priceChange >= targetPriceChange || priceChange <= -1.0 * targetPriceChange)
                            alertFlag = "Y";
                    }

                    //
                    if (alertFlag.Equals("Y"))
                    {
                        SecurityAlertDetail securityAlertDetail = new SecurityAlertDetail();
                        securityAlertDetail.AlertFlag = alertFlag;
                        securityAlertDetail.SecurityAlert = securityAlert;
                        securityAlertDetail.PriceChng = priceChange;
                        securityAlertDetail.TgtPriceChng = securityAlert.TargetValue;
                        securityAlertDetail.TgtPriceChngDiff = priceChangeDiff;
                        securityAlertDetails.Add(securityAlertDetail);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing Price Target for Id: " + securityAlert.Id + "; Ticker1: " + securityAlert.Ticker1, ex);
            }
        }

        private void PriceDifferentialAlert(
            SecurityAlert securityAlert
            , IList<SecurityAlertDetail> securityAlertDetails
            , IDictionary<string, string> priceTickerMap
            , IDictionary<string, SecurityPrice> securityPriceDict)

        {
            try
            {
                string alertFlag = "N";
                SecurityPrice ticker1Price = SecurityPriceLookupOperations.GetSecurityPrice(securityAlert.Ticker1, priceTickerMap, securityPriceDict);
                SecurityPrice ticker2Price = SecurityPriceLookupOperations.GetSecurityPrice(securityAlert.Ticker2, priceTickerMap, securityPriceDict);
                if (ticker1Price != null && ticker1Price.LastPrc.HasValue &&
                    ticker2Price != null && ticker2Price.LastPrc.HasValue)
                {
                    double ticker1LastPrice = ticker1Price.LastPrc.GetValueOrDefault();
                    double ticker2LastPrice = ticker2Price.LastPrc.GetValueOrDefault();
                    double targetPrice = securityAlert.TargetValue.GetValueOrDefault();
                    double priceDiff = ticker1LastPrice - ticker2LastPrice;

                    if ("BUY".Equals(securityAlert.TransType, StringComparison.CurrentCultureIgnoreCase))
                    {
                        if (priceDiff <= targetPrice)
                            alertFlag = "Y";
                    }
                    else if ("SELL".Equals(securityAlert.TransType, StringComparison.CurrentCultureIgnoreCase))
                    {
                        if (priceDiff >= targetPrice)
                            alertFlag = "Y";
                    }

                    if (alertFlag.Equals("Y"))
                    {
                        SecurityAlertDetail securityAlertDetail = new SecurityAlertDetail();
                        securityAlertDetail.AlertFlag = alertFlag;
                        securityAlertDetail.SecurityAlert = securityAlert;
                        securityAlertDetail.LivePrice1 = ticker1LastPrice;
                        securityAlertDetail.LivePrice2 = ticker2LastPrice;
                        securityAlertDetail.TgtPriceDifferential = securityAlert.TargetValue;
                        securityAlertDetail.TgtPriceDifferentialDiff = priceDiff;
                        securityAlertDetails.Add(securityAlertDetail);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error calculating Price Target for Id: " + securityAlert.Id + "; Ticker1: " + securityAlert.Ticker1, ex);
            }
        }

        private void DiscountTargetAlert(
           SecurityAlert securityAlert
            , IList<SecurityAlertDetail> securityAlertDetails
            , IDictionary<string, FundForecast> fundForecastDict)
        {
            try
            {
                if (fundForecastDict.TryGetValue(securityAlert.Ticker1, out FundForecast fundForecast))
                {
                    double? discount = fundForecast.PDLastPrc;

                    if (!string.IsNullOrEmpty(securityAlert.TargetField))
                    {
                        if ("PubDscnt".Equals(securityAlert.TargetField, StringComparison.CurrentCultureIgnoreCase))
                            discount = fundForecast.LastPD;
                    }

                    if (discount.HasValue)
                    {
                        string alertFlag = "N";
                        double discountToLastPrice = discount.GetValueOrDefault();
                        double targetDiscount = securityAlert.TargetValue.GetValueOrDefault();
                        double discountDiff = targetDiscount - discountToLastPrice;

                        if ("BUY".Equals(securityAlert.TransType, StringComparison.CurrentCultureIgnoreCase))
                        {
                            if (discountToLastPrice <= targetDiscount)
                                alertFlag = "Y";
                        }
                        else if ("SELL".Equals(securityAlert.TransType, StringComparison.CurrentCultureIgnoreCase))
                        {
                            if (discountToLastPrice >= targetDiscount)
                                alertFlag = "Y";
                        }

                        if (alertFlag.Equals("Y"))
                        {
                            SecurityAlertDetail securityAlertDetail = new SecurityAlertDetail();
                            securityAlertDetail.AlertFlag = alertFlag;
                            securityAlertDetail.SecurityAlert = securityAlert;
                            securityAlertDetail.PubDscnt = fundForecast.LastPD;
                            securityAlertDetail.LiveDscnt = discountToLastPrice;
                            securityAlertDetail.TgtDscnt = securityAlert.TargetValue;
                            securityAlertDetail.TgtDscntDiff = discountDiff;
                            securityAlertDetails.Add(securityAlertDetail);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error calculating Discount Target for Id: " + securityAlert.Id + "; Ticker1: " + securityAlert.Ticker1, ex);
            }
        }

        private void DiscountChangeAlert(
           SecurityAlert securityAlert
            , IList<SecurityAlertDetail> securityAlertDetails
            , IDictionary<string, FundForecast> fundForecastDict)
        {
            try
            {
                if (fundForecastDict.TryGetValue(securityAlert.Ticker1, out FundForecast fundForecast))
                {
                    double? discountChange = fundForecast.PDChng;
                    //if (!string.IsNullOrEmpty(securityAlert.TargetField))
                    //{
                    //    if ("Pub".Equals(securityAlert.TargetField, StringComparison.CurrentCultureIgnoreCase))
                    //        discountChange = fundForecast.LastPctPremium;
                    //}

                    if (discountChange.HasValue)
                    {
                        string alertFlag = "N";
                        double targetDiscountChange = securityAlert.TargetValue.GetValueOrDefault();
                        double discountChangeDiff = Math.Abs(targetDiscountChange) - Math.Abs(discountChange.GetValueOrDefault());

                        if ("UP".Equals(securityAlert.TargetChangeSide, StringComparison.CurrentCultureIgnoreCase))
                        {
                            if (discountChange >= targetDiscountChange)
                                alertFlag = "Y";
                        }
                        else if ("DOWN".Equals(securityAlert.TargetChangeSide, StringComparison.CurrentCultureIgnoreCase))
                        {
                            if (discountChange <= -1.0 * targetDiscountChange)
                                alertFlag = "Y";
                        }
                        else if ("BOTH".Equals(securityAlert.TargetChangeSide, StringComparison.CurrentCultureIgnoreCase))
                        {
                            if (discountChange >= targetDiscountChange || discountChange <= -1.0 * targetDiscountChange)
                                alertFlag = "Y";
                        }

                        if (alertFlag.Equals("Y"))
                        {
                            SecurityAlertDetail securityAlertDetail = new SecurityAlertDetail();
                            securityAlertDetail.AlertFlag = alertFlag;
                            securityAlertDetail.SecurityAlert = securityAlert;
                            securityAlertDetail.DscntChng = discountChange;
                            securityAlertDetail.TgtDscntChng = securityAlert.TargetValue;
                            securityAlertDetail.TgtDscntChngDiff = discountChangeDiff;
                            securityAlertDetails.Add(securityAlertDetail);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error calculating Discount Target for Id: " + securityAlert.Id + "; Ticker1: " + securityAlert.Ticker1, ex);
            }
        }

        private void DiscountDifferentialAlert(
            SecurityAlert securityAlert
            , IList<SecurityAlertDetail> securityAlertDetails
            , IDictionary<string, FundForecast> fundForecastDict)
        {
            try
            {
                if (fundForecastDict.TryGetValue(securityAlert.Ticker1, out FundForecast fundForecast1)
                    && fundForecastDict.TryGetValue(securityAlert.Ticker2, out FundForecast fundForecast2))
                {
                    double? discount1 = fundForecast1.PDLastPrc;
                    double? discount2 = fundForecast2.PDLastPrc;

                    if (!string.IsNullOrEmpty(securityAlert.TargetField))
                    {
                        if ("PubDscnt".Equals(securityAlert.TargetField, StringComparison.CurrentCultureIgnoreCase))
                        {
                            discount1 = fundForecast1.LastPD;
                            discount2 = fundForecast2.LastPD;
                        }
                    }

                    if (discount1.HasValue && discount2.HasValue)
                    {
                        string alertFlag = "N";
                        double discount1ToLastPrice = discount1.GetValueOrDefault();
                        double discount2ToLastPrice = discount2.GetValueOrDefault();
                        double targetDiscount = securityAlert.TargetValue.GetValueOrDefault();
                        double discountDiff = discount1ToLastPrice - discount2ToLastPrice;

                        if ("BUY".Equals(securityAlert.TransType, StringComparison.CurrentCultureIgnoreCase))
                        {
                            if (discountDiff <= targetDiscount)
                                alertFlag = "Y";
                        }
                        else if ("SELL".Equals(securityAlert.TransType, StringComparison.CurrentCultureIgnoreCase))
                        {
                            if (discountDiff >= targetDiscount)
                                alertFlag = "Y";
                        }

                        if (alertFlag.Equals("Y"))
                        {
                            SecurityAlertDetail securityAlertDetail = new SecurityAlertDetail();
                            securityAlertDetail.AlertFlag = alertFlag;
                            securityAlertDetail.SecurityAlert = securityAlert;
                            securityAlertDetail.PubDscnt1 = fundForecast1.LastPD;
                            securityAlertDetail.PubDscnt2 = fundForecast2.LastPD;
                            securityAlertDetail.LiveDscnt1 = discount1ToLastPrice;
                            securityAlertDetail.LiveDscnt2 = discount2ToLastPrice;
                            securityAlertDetail.TgtDscntDifferential = securityAlert.TargetValue;
                            securityAlertDetail.TgtDscntDifferentialDiff = discountDiff;
                            securityAlertDetails.Add(securityAlertDetail);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing Discount Differential Target for Id: " + securityAlert.Id + "; Ticker1: " + securityAlert.Ticker1 + "; Ticker2: " + securityAlert.Ticker2, ex);
            }
        }
    }
}