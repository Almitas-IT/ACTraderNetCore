using aACTrader.DAO.Repository;
using aCommons;
using aCommons.Cef;
using aCommons.Web;
using LazyCache;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;

namespace aACTrader.Operations.Impl
{
    public class WebDataOperations
    {
        private readonly ILogger<WebDataOperations> _logger;
        private readonly WebDao _webDao;
        private readonly CachingService _cache;

        public WebDataOperations(ILogger<WebDataOperations> logger
            , CachingService cache
            , WebDao webDao)
        {
            _logger = logger;
            _cache = cache;
            _webDao = webDao;
        }

        public IList<SecurityDataErrorTO> GetSecurityDataCheckReport(string country, string ticker)
        {
            IDictionary<string, string> dict = _cache.Get<IDictionary<string, string>>(CacheKeys.SECURITY_DATA_VALIDATION_CHECKS);
            IList<SecurityDataErrorTO> list = _webDao.GetSecurityDataCheckReport(country, ticker);
            foreach (SecurityDataErrorTO security in list)
            {
                if (dict.TryGetValue(security.Ticker, out string validationString))
                    security.NavComments = validationString;
            }
            return list;
        }

        public IList<FundCurrExpTO> GetFundCurrencyExposures()
        {
            IDictionary<string, SecurityPrice> securityPriceDict = _cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);
            IDictionary<string, string> priceTickerMap = _cache.Get<IDictionary<string, string>>(CacheKeys.PRICE_TICKER_MAP);
            IDictionary<string, PositionMaster> positionDict = _cache.Get<IDictionary<string, PositionMaster>>(CacheKeys.POSITION_MASTER);
            IDictionary<string, FXRate> fxRateDict = _cache.Get<IDictionary<string, FXRate>>(CacheKeys.FX_RATES);

            IDictionary<string, FundCurrExpTO> dict = _cache.Get<IDictionary<string, FundCurrExpTO>>(CacheKeys.FUND_CURRENCY_EXPOSURES);
            foreach (KeyValuePair<string, FundCurrExpTO> kvp in dict)
            {
                FundCurrExpTO data = kvp.Value;
                try
                {
                    if (data.IsGrpRow == 0)
                    {
                        //Total Position
                        if (positionDict.TryGetValue(data.Ticker, out PositionMaster position))
                        {
                            data.OppPos = position.FundOpp.PosHeld;
                            data.TacPos = position.FundTac.PosHeld;
                            data.OppCashPos = data.OppPos;
                            data.TacCashPos = data.TacPos;
                        }

                        //Swap Position
                        string swapTicker = "SwapUnderlying#" + data.Ticker;
                        if (positionDict.TryGetValue(swapTicker, out PositionMaster swapPosition))
                        {
                            data.OppSwapPos = swapPosition.FundOpp.PosHeld;
                            data.TacSwapPos = swapPosition.FundTac.PosHeld;
                            data.OppCashPos -= data.OppSwapPos;
                            data.TacCashPos -= data.TacSwapPos;
                        }

                        SecurityPrice securityPrice = SecurityPriceLookupOperations.GetSecurityPrice(data.Ticker, priceTickerMap, securityPriceDict);
                        if (securityPrice != null && !String.IsNullOrEmpty(securityPrice.Curr))
                        {
                            data.Prc = securityPrice.LastPrc;
                            data.PrcCurr = securityPrice.Curr;

                            double fx = 1.0;
                            if ("GBp".Equals(data.PrcCurr) || "GBX".Equals(data.PrcCurr))
                            {
                                fx /= 100.0;
                                data.PrcCurr = "GBP";
                            }

                            if (fxRateDict.TryGetValue(data.PrcCurr, out FXRate fxRate))
                                fx *= fxRate.FXRateLatest.GetValueOrDefault();

                            double lastPrice = data.Prc.GetValueOrDefault();
                            double lastPriceUSD = lastPrice * fx;
                            data.Fx = fx;
                            data.PrcUSD = lastPriceUSD;

                            //Fund Currency
                            if (data.Curr.Equals("USD", StringComparison.CurrentCultureIgnoreCase))
                            {
                                //OPP
                                double oppPos = data.OppPos.GetValueOrDefault();
                                data.OppCADFwd = -1.0 * data.CADExp.GetValueOrDefault() * oppPos * lastPriceUSD;
                                data.OppGBPFwd = -1.0 * data.GBPExp.GetValueOrDefault() * oppPos * lastPriceUSD;
                                data.OppEURFwd = -1.0 * data.EURExp.GetValueOrDefault() * oppPos * lastPriceUSD;
                                data.OppJPYFwd = -1.0 * data.JPYExp.GetValueOrDefault() * oppPos * lastPriceUSD;
                                data.OppAUDFwd = -1.0 * data.AUDExp.GetValueOrDefault() * oppPos * lastPriceUSD;

                                //TAC
                                double tacPos = data.TacPos.GetValueOrDefault();
                                data.TacCADFwd = -1.0 * data.CADExp.GetValueOrDefault() * tacPos * lastPriceUSD;
                                data.TacGBPFwd = -1.0 * data.GBPExp.GetValueOrDefault() * tacPos * lastPriceUSD;
                                data.TacEURFwd = -1.0 * data.EURExp.GetValueOrDefault() * tacPos * lastPriceUSD;
                                data.TacJPYFwd = -1.0 * data.JPYExp.GetValueOrDefault() * tacPos * lastPriceUSD;
                                data.TacAUDFwd = -1.0 * data.AUDExp.GetValueOrDefault() * tacPos * lastPriceUSD;
                            }
                            else
                            {
                                //OPP
                                double oppCashPos = data.OppCashPos.GetValueOrDefault();
                                data.OppCADFwd = -1.0 * data.CADExp.GetValueOrDefault() * oppCashPos * lastPriceUSD;
                                data.OppGBPFwd = -1.0 * data.GBPExp.GetValueOrDefault() * oppCashPos * lastPriceUSD;
                                data.OppEURFwd = -1.0 * data.EURExp.GetValueOrDefault() * oppCashPos * lastPriceUSD;
                                data.OppJPYFwd = -1.0 * data.JPYExp.GetValueOrDefault() * oppCashPos * lastPriceUSD;
                                data.OppAUDFwd = -1.0 * data.AUDExp.GetValueOrDefault() * oppCashPos * lastPriceUSD;

                                //TAC
                                double tacCashPos = data.TacCashPos.GetValueOrDefault();
                                data.TacCADFwd = -1.0 * data.CADExp.GetValueOrDefault() * tacCashPos * lastPriceUSD;
                                data.TacGBPFwd = -1.0 * data.GBPExp.GetValueOrDefault() * tacCashPos * lastPriceUSD;
                                data.TacEURFwd = -1.0 * data.EURExp.GetValueOrDefault() * tacCashPos * lastPriceUSD;
                                data.TacJPYFwd = -1.0 * data.JPYExp.GetValueOrDefault() * tacCashPos * lastPriceUSD;
                                data.TacAUDFwd = -1.0 * data.AUDExp.GetValueOrDefault() * tacCashPos * lastPriceUSD;

                                double usdExp = data.USDExp.GetValueOrDefault();
                                double oppSwapPos = data.OppSwapPos.GetValueOrDefault();
                                double tacSwapPos = data.TacSwapPos.GetValueOrDefault();
                                if (data.Curr.Equals("CAD", StringComparison.CurrentCultureIgnoreCase) && data.USDExp.HasValue)
                                {
                                    data.OppCADFwd += (usdExp * oppSwapPos * lastPriceUSD);
                                    data.TacCADFwd += (usdExp * tacSwapPos * lastPriceUSD);
                                }
                                else if (data.Curr.Equals("GBP", StringComparison.CurrentCultureIgnoreCase) && data.USDExp.HasValue)
                                {
                                    data.OppGBPFwd += (usdExp * oppSwapPos * lastPriceUSD);
                                    data.TacGBPFwd += (usdExp * tacSwapPos * lastPriceUSD);
                                }
                                else if (data.Curr.Equals("EUR", StringComparison.CurrentCultureIgnoreCase) && data.USDExp.HasValue)
                                {
                                    data.OppEURFwd += (usdExp * oppSwapPos * lastPriceUSD);
                                    data.TacEURFwd += (usdExp * tacSwapPos * lastPriceUSD);
                                }
                                else if (data.Curr.Equals("JPY", StringComparison.CurrentCultureIgnoreCase) && data.USDExp.HasValue)
                                {
                                    data.OppJPYFwd += (usdExp * oppSwapPos * lastPrice);
                                    data.TacJPYFwd += (usdExp * tacSwapPos * lastPrice);
                                }
                                else if (data.Curr.Equals("AUD", StringComparison.CurrentCultureIgnoreCase) && data.USDExp.HasValue)
                                {
                                    data.OppAUDFwd += (usdExp * oppSwapPos * lastPriceUSD);
                                    data.TacAUDFwd += (usdExp * tacSwapPos * lastPriceUSD);
                                }
                            }
                        }
                        //_logger.LogInformation("Ticker: " + data.Ticker + "/" + data.OppCADFwd);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError("Error calculating currency exposures for ticker: " + data.Ticker, ex);
                }
            }
            return dict.Values.ToList<FundCurrExpTO>();
        }
    }
}