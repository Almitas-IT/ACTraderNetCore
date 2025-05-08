using aACTrader.DAO.Repository;
using aACTrader.Model;
using aACTrader.Operations.Interface;
using aACTrader.Services.Admin;
using aCommons;
using aCommons.Cef;
using aCommons.Crypto;
using aCommons.DTO;
using aCommons.Security;
using aCommons.Utils;
using LazyCache;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;

namespace aACTrader.Operations.Impl
{
    public class SecurityPriceOperation : ISecurityPriceOperation
    {
        private readonly ILogger<SecurityPriceOperation> _logger;
        private readonly CachingService _cache;
        private readonly SecurityPriceDao _securityPriceDao;
        private readonly LogDataService _logDataService;
        private readonly IConfiguration _configuration;

        private readonly string _tableName = "almitasc_ACTradingBBGLink.StgRTPriceLatest";

        public SecurityPriceOperation(ILogger<SecurityPriceOperation> logger
            , CachingService cache
            , SecurityPriceDao securityPriceDao
            , LogDataService logDataService
            , IConfiguration configuration)
        {
            _logger = logger;
            _cache = cache;
            _securityPriceDao = securityPriceDao;
            _logDataService = logDataService;
            _configuration = configuration;
        }

        public IList<SecurityPrice> GetLatestPrices()
        {
            return _securityPriceDao.GetLatestPrices();
        }

        public IList<SecurityPrice> GetAllPrices()
        {
            IDictionary<string, SecurityPrice> dict = _cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);
            IList<SecurityPrice> list = new List<SecurityPrice>(dict.Values);
            return list;
        }

        public void MovePricesToTgtTable()
        {
            _securityPriceDao.MovePricesToTgtTable();
        }

        public void SavePricesToStgTable(string filePath, string tableName)
        {
            _securityPriceDao.SavePricesToStgTable(filePath, tableName);
        }

        public void SavePricesToStgTable(IDictionary<string, SecurityPrice> securityPriceDict)
        {
            _securityPriceDao.SavePricesToStgTableNew(securityPriceDict);
        }

        public void TruncateTable(string tableName)
        {
            _securityPriceDao.TruncateTable(tableName);
        }

        public IDictionary<string, SecurityPrice> GetSecurityPriceMaster()
        {
            return _securityPriceDao.GetSecurityPriceMaster();
        }

        public IDictionary<string, FXRate> GetLatestFXRates()
        {
            return _securityPriceDao.GetLatestFXRates();
        }

        public void SaveFXRatesToStg(IDictionary<string, FXRate> fxRateDict)
        {
            _securityPriceDao.SaveFXRatesToStg(fxRateDict);
        }

        public void MoveFXRatesToTgtTable()
        {
            _securityPriceDao.MoveFXRatesToTgtTable();
        }

        public void SaveDailyPrices(IDictionary<string, SecurityPrice> securityPriceDict)
        {
            _securityPriceDao.SaveDailyPrices(securityPriceDict);
        }

        public IDictionary<string, SharesImbalance> GetSharesImbalanceList()
        {
            return _securityPriceDao.GetSharesImbalanceList();
        }

        public void SaveSharesImbalanceList(IDictionary<string, SharesImbalance> dict)
        {
            if (dict != null && dict.Count > 0)
                _securityPriceDao.SaveSharesImbalanceList(dict);
        }

        public void UpdateFlagForClosedMarkets()
        {
            _logger.LogInformation("Update Market Closed Securities Flag - STARTED");
            try
            {
                IList<string> closedMarketSecurities = _securityPriceDao.GetSecuritiesByExchange();
                if (closedMarketSecurities != null && closedMarketSecurities.Count > 0)
                {
                    _logger.LogInformation("Updating Market Closed Securities Flags, Securities Count: " + closedMarketSecurities.Count);
                    IDictionary<string, SecurityPrice> securityPriceDict = _cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);
                    foreach (string ticker in closedMarketSecurities)
                    {
                        if (securityPriceDict.TryGetValue(ticker, out SecurityPrice securityPrice))
                            securityPrice.MktCls = 1;  //0 - Open, 1 - Closed
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error updating Market Closed Securities Flag");
            }

            try
            {
                IDictionary<string, SecurityPrice> dict = _cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);
                foreach (SecurityPrice data in dict.Values)
                {
                    if (data.RTFlag == 1 && data.Ticker.StartsWith("Swap"))
                        data.MktCls = 1;  //0 - Open, 1 - Closed
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error updating Market Closed Securities Flag for Swaps");
            }

            _logger.LogInformation("Update Market Closed Securities Flag - DONE");
        }

        public void ResetClosedMarketsFlag()
        {
            _logger.LogInformation("Reset Market Closed Securities Flag - STARTED");
            try
            {
                IDictionary<string, SecurityPrice> securityPriceDict = _cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);
                foreach (KeyValuePair<string, SecurityPrice> kvp in securityPriceDict)
                {
                    SecurityPrice securityPrice = kvp.Value;
                    securityPrice.MktCls = 0;  //0 - Open, 1 - Closed
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error resetting Market Closed Securities Flag");
            }
            _logger.LogInformation("Reset Market Closed Securities Flag - DONE");
        }

        public void SavePrices()
        {
            try
            {
                IDictionary<string, SecurityPrice> dict = _cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);
                if (dict != null && dict.Count > 0)
                {
                    TruncateTable(_tableName);
                    SavePricesToStgTable(dict);
                    MovePricesToTgtTable();
                    //_logger.LogDebug("Saved and moved prices to target table");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving data");
                _logDataService.SaveLog("SecurityPriceOperation", "SavePrices", null, ex.Message, "INFO");
            }
        }

        //public IList<SecurityPrice> GetLivePrices()
        //{
        //    //_logger.LogInformation("GetLivePrices - STARTED");
        //    IDictionary<string, SecurityPrice> dict = _cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);
        //    IList<SecurityPrice> list = new List<SecurityPrice>();
        //    foreach (KeyValuePair<string, SecurityPrice> kvp in dict)
        //    {
        //        SecurityPrice securityPrice = kvp.Value;
        //        if (securityPrice.RTFlag == 1)
        //            list.Add(securityPrice);
        //    }
        //    //_logger.LogInformation("GetLivePrices - DONE");
        //    return list;
        //}

        public IList<SecurityPrice> GetLivePrices()
        {
            //_logger.LogInformation("GetLivePrices - STARTED");
            IDictionary<string, SecurityPrice> dict = _cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);
            IList<SecurityPrice> list = new List<SecurityPrice>();
            foreach (SecurityPrice data in dict.Values)
            {
                if (data.RTFlag == 1)
                    list.Add(data);
            }
            //_logger.LogInformation("GetLivePrices - DONE");
            return list;
        }

        public IList<SecurityPrice> GetLivePricesByExchange()
        {
            //_logger.LogInformation("GetLivePricesByExchange - STARTED");
            IDictionary<string, SecurityPrice> dict = _cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);
            IList<SecurityPrice> list = new List<SecurityPrice>();
            foreach (SecurityPrice data in dict.Values)
            {
                if (data.RTFlag == 1 && data.MktCls == 0) //0 - Open, 1 - Closed
                    list.Add(data);
            }
            // _logger.LogInformation("GetLivePricesByExchange - DONE");
            return list;
        }

        public IList<SecurityPrice> GetDelayedPrices()
        {
            //_logger.LogInformation("GetDelayedPrices - STARTED");
            IDictionary<string, SecurityPrice> dict = _cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);
            IList<SecurityPrice> list = new List<SecurityPrice>();
            foreach (SecurityPrice data in dict.Values)
            {
                if (data.RTFlag == 0)
                    list.Add(data);
            }
            //_logger.LogInformation("GetDelayedPrices - DONE");
            return list;
        }


        public IList<SecurityPrice> GetDelayedPricesByExchange()
        {
            //_logger.LogInformation("GetDelayedPricesByExchange - STARTED");
            IDictionary<string, SecurityPrice> dict = _cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);
            IList<SecurityPrice> list = new List<SecurityPrice>();
            foreach (SecurityPrice data in dict.Values)
            {
                if (data.RTFlag == 0 && data.MktCls == 0) //0 - Open, 1 - Closed
                    list.Add(data);
            }
            //_logger.LogInformation("GetDelayedPricesByExchange - DONE");
            return list;
        }

        public IList<FXRateTO> GetLiveFXRates()
        {
            //_logger.LogInformation("GetLiveFXRates - STARTED");
            IDictionary<string, FXRate> fxRatesBBG = _cache.Get<IDictionary<string, FXRate>>(CacheKeys.FX_RATES);
            IDictionary<string, FXRate> fxRatesNV = _cache.Get<IDictionary<string, FXRate>>(CacheKeys.FX_RATES_LIVE);

            foreach (FXRate data in fxRatesNV.Values)
            {
                if (data.TradeDate.HasValue)
                {
                    if (!data.BaseCurrency.Equals("USD")
                        && fxRatesBBG.TryGetValue(data.BaseCurrency, out FXRate fxRate))
                        fxRate.FXRateLatest = data.LastPrice;
                    else if (fxRatesBBG.TryGetValue(data.TargetCurrency, out fxRate))
                        fxRate.FXRateLatest = (1.0 / data.LastPrice);
                }
            }

            IList<FXRateTO> list = new List<FXRateTO>();
            foreach (FXRate data in fxRatesBBG.Values)
            {
                list.Add(new FXRateTO
                {
                    Currency = data.CurrencyCode,
                    FXRate = data.FXRateLatest,
                    Source = data.Source
                }
                );
            }
            //_logger.LogInformation("GetLiveFXRates - DONE");
            return list;
        }

        public IList<SecurityPriceMap> GetPriceTickerMap()
        {
            return _cache.Get<IList<SecurityPriceMap>>(CacheKeys.PRICE_TICKER_LIST);
        }

        public IList<SecurityPriceMap> GetNVPriceTickerMap()
        {
            return _cache.Get<IList<SecurityPriceMap>>(CacheKeys.NV_PRICE_TICKER_LIST);
        }

        public IList<SecurityPrice> GetPriceAlerts()
        {
            _logger.LogInformation("GetPriceAlerts - STARTED");
            IDictionary<string, SecurityPrice> dict = _cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);
            IDictionary<string, string> sourceTickerToAlmTickerDict = _cache.Get<IDictionary<string, string>>(CacheKeys.SOURCE_PRICE_TICKER_MAP);

            IList<SecurityPrice> list = new List<SecurityPrice>();
            foreach (KeyValuePair<string, SecurityPrice> kvp in dict)
            {
                SecurityPrice securityPrice = kvp.Value;
                if (securityPrice.PrcRtn.HasValue
                    && Math.Abs(securityPrice.PrcRtn.GetValueOrDefault()) > 0.05)
                {
                    list.Add(securityPrice);
                    if (sourceTickerToAlmTickerDict.TryGetValue(securityPrice.Ticker, out string almSymbol))
                        securityPrice.SrcTicker = almSymbol;
                }
            }
            _logger.LogInformation("GetPriceAlerts - DONE");
            return list;
        }

        public IList<SecurityPrice> GetPriceAlertsWithFilter(PriceFilterParameters parameters)
        {
            _logger.LogInformation("GetPriceAlertsWithFilter - STARTED");
            IDictionary<string, SecurityPrice> dict = _cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);
            IDictionary<string, string> sourceTickerToAlmTickerDict = _cache.Get<IDictionary<string, string>>(CacheKeys.SOURCE_PRICE_TICKER_MAP);

            double priceThreshold = 0.05;
            if (parameters != null && !string.IsNullOrEmpty(parameters.PriceThreshold))
                priceThreshold = Convert.ToDouble(parameters.PriceThreshold) / 100.0;

            IList<SecurityPrice> list = new List<SecurityPrice>();
            foreach (KeyValuePair<string, SecurityPrice> kvp in dict)
            {
                SecurityPrice securityPrice = kvp.Value;
                if (securityPrice.RTFlag == 1
                    && securityPrice.PrcRtn.HasValue
                    && Math.Abs(securityPrice.PrcRtn.GetValueOrDefault()) >= priceThreshold)
                {
                    list.Add(securityPrice);
                    if (sourceTickerToAlmTickerDict.TryGetValue(securityPrice.Ticker, out string almSymbol))
                        securityPrice.SrcTicker = almSymbol;
                }
            }
            _logger.LogInformation("GetPriceAlertsWithFilter - DONE");
            return list.OrderBy(s => s.SrcTicker).ToList<SecurityPrice>();
        }

        public IList<CryptoSecMst> GetCryptoNavs()
        {
            //_logger.LogInformation.Info("GetCryptoNavs - STARTED");
            IDictionary<string, CryptoSecMst> dict = _cache.Get<IDictionary<string, CryptoSecMst>>(CacheKeys.CRYPTO_SECURITY_MST);
            IList<CryptoSecMst> list = new List<CryptoSecMst>();
            foreach (KeyValuePair<string, CryptoSecMst> kvp in dict)
                list.Add(kvp.Value);
            //_logger.LogInformation.Info("GetCryptoNavs - DONE");
            return list;
        }

        public IList<SharesImbalanceTO> GetSharesImbalanceDetails()
        {
            IDictionary<string, SharesImbalance> dict = _cache.Get<IDictionary<string, SharesImbalance>>(CacheKeys.SHARES_IMBALANCE);
            IDictionary<string, SecurityPrice> priceDict = _cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);
            IDictionary<string, string> priceTickerMap = _cache.Get<IDictionary<string, string>>(CacheKeys.PRICE_TICKER_MAP);
            IDictionary<string, PositionMaster> positionDict = _cache.Get<IDictionary<string, PositionMaster>>(CacheKeys.POSITION_MASTER);
            IDictionary<string, FundMaster> fundMasterDict = _cache.Get<IDictionary<string, FundMaster>>(CacheKeys.FUND_MASTER);
            IDictionary<string, FundForecast> fundForecastDict = _cache.Get<IDictionary<string, FundForecast>>(CacheKeys.FUND_FORECASTS);
            IDictionary<string, IDictionary<string, PositionDetailTO>> tradeDetailsDict = _cache.Get<IDictionary<string, IDictionary<string, PositionDetailTO>>>(CacheKeys.LIVE_TRADE_DETAILS);

            //IDictionary<string, OrderSummary> orderSummaryDict;
            //if (_configuration["ConnectionStrings:ENV"].Equals("PROD"))
            //    orderSummaryDict = _cache.Get<IDictionary<string, OrderSummary>>(CacheKeys.ORDER_SUMMARY);
            //else
            //    orderSummaryDict = _cache.Get<IDictionary<string, OrderSummary>>(CacheKeys.SIM_ORDER_SUMMARY);

            IList<SharesImbalanceTO> list = new List<SharesImbalanceTO>();
            foreach (KeyValuePair<string, SharesImbalance> kvp in dict)
            {
                SharesImbalance si = kvp.Value;

                // Shares Imbalance
                SharesImbalanceTO data = new SharesImbalanceTO
                {
                    Ticker = si.Ticker,
                    BBGTicker = si.BBGTicker,
                    ALMTicker = si.ALMTicker,
                    Paired = si.Paired,
                    Imbalance = si.Imbalance,
                    ImbalanceSide = si.ImbalanceSide,
                    CurrentRefPrice = si.CurrentRefPrice,
                    FarIndPrice = si.FarIndPrice,
                    NearIndPrice = si.NearIndPrice,
                    NumIndPrice = si.NumIndPrice,
                    PriceVar = si.PriceVar,
                    IsLiveUpdate = si.IsLiveUpdate,
                    DfTime = si.DfTime,
                    Avg20Vol = si.Avg20Vol,
                    //IAction = null,
                    //ISide = null,
                    //ILocate = null,
                    //IPrice = null,
                    //IQty = null,
                };

                // Security Price
                SecurityPrice securityPrice = SecurityPriceLookupOperations.GetSecurityPrice(data.ALMTicker, priceTickerMap, priceDict);
                if (securityPrice != null)
                {
                    data.LastPrc = securityPrice.LastPrc;
                    data.BidPrc = securityPrice.BidPrc;
                    data.AskPrc = securityPrice.AskPrc;
                    data.MidPrc = securityPrice.MidPrc;
                    data.ClsPrc = securityPrice.ClsPrc;
                    data.BidSz = securityPrice.BidSz;
                    data.AskSz = securityPrice.AskSz;
                    data.Vol = securityPrice.Vol;
                    data.DvdAmt = securityPrice.DvdAmt;
                    data.PrcRtn = securityPrice.PrcRtn;
                    data.NetPrcChng = securityPrice.NetPrcChng;
                }

                // Positions
                string ticker = !string.IsNullOrEmpty(data.ALMTicker) ? data.ALMTicker : data.BBGTicker;
                if (!positionDict.TryGetValue(ticker, out PositionMaster positionMaster))
                {
                    //search by CT ticker if composite ticker CN is provided
                    if (ticker.EndsWith(" CN", StringComparison.CurrentCultureIgnoreCase))
                    {
                        string newTicker = ticker.Replace(" CN", " CT");
                        positionDict.TryGetValue(newTicker, out positionMaster);
                    }
                }

                // Discounts
                if (fundForecastDict.TryGetValue(ticker, out FundForecast fundForecast))
                {
                    data.PDPub = fundForecast.LastPD;
                    data.PDLast = fundForecast.PDLastPrc;
                    data.PDBid = fundForecast.PDBidPrc;
                    data.PDAsk = fundForecast.PDAskPrc;
                    if (data.NearIndPrice != null && data.NearIndPrice.GetValueOrDefault() > 0)
                        data.PDNearIndPrc = DataConversionUtils.CalculateReturn(data.NearIndPrice, fundForecast.EstNav);
                }

                if (positionMaster != null)
                {
                    data.TotPos = positionMaster.FundAll.PosHeld;
                    data.OppPos = positionMaster.FundOpp.PosHeld;
                    data.TacPos = positionMaster.FundTac.PosHeld;
                }

                if (fundMasterDict.TryGetValue(ticker, out FundMaster fundMaster))
                    data.SecType = fundMaster.SecTyp;

                // Calculate
                try
                {
                    data.OpenExpVol = CommonUtils.AddNullableInts(data.Paired, data.Imbalance);
                    if (data.Avg20Vol != null && data.Avg20Vol.GetValueOrDefault() > 0)
                    {
                        data.OpenExp20DVolPct = data.OpenExpVol.GetValueOrDefault() / data.Avg20Vol.GetValueOrDefault();
                        data.Imalance20DVolPct = String.Format("{0:0.00}", data.Imbalance.GetValueOrDefault() / data.Avg20Vol.GetValueOrDefault()) + "x";
                    }
                    if (data.OpenExpVol != null && data.OpenExpVol.GetValueOrDefault() > 0)
                        data.ImbalanceOpenExpVolPct = (double)(data.Imbalance.GetValueOrDefault() / data.OpenExpVol.GetValueOrDefault());

                }
                catch (Exception e)
                {
                    _logger.LogError(e.Message);
                }

                // Trade Details
                if (tradeDetailsDict.TryGetValue(ticker, out IDictionary<string, PositionDetailTO> positionDetailDict))
                {
                    IList<PositionDetailTO> tradeList = new List<PositionDetailTO>(positionDetailDict.Values);
                    data.TrdDetails = tradeList;
                }

                list.Add(data);
            }

            return list;
        }

        public void CalculateFXReturns()
        {
            _logger.LogInformation("Calculating FX returns");
            _securityPriceDao.CalculateFXReturns();
        }
    }
}