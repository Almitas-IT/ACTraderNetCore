using aACTrader.DAO.Repository;
using aCommons;
using aCommons.Cef;
using LazyCache;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;

namespace aACTrader.Operations.Impl
{
    public class PortHoldingsOperations
    {
        private readonly ILogger<PortHoldingsOperations> _logger;
        private readonly CachingService _cache;
        private readonly CommonDao _commonDao;

        public PortHoldingsOperations(ILogger<PortHoldingsOperations> logger
            , CachingService cache
            , CommonDao commonDao)
        {
            _logger = logger;
            _cache = cache;
            _commonDao = commonDao;
        }

        public void Start()
        {
            _logger.LogInformation("Port Holding Operations - STARTED");
            CalculateFundHoldingNavs();
            CalculateLiveMarketValues();
            _logger.LogInformation("Port Holding Operations - DONE");
        }

        /// <summary>
        /// Estimates fund navs from Portfolio Holding returns
        /// Calculate Equity returns (from Price and Dividend changes) since last nav date
        /// Calculate Fixed Income returns (from Curve and Spread changes)
        /// Calculate Cash and FX returns (from exchange rate changes)
        /// Using calculated return, compute fund's estimated nav adjusted for leverage
        /// 
        /// </summary>
        /// <param name="cache"></param>
        /// <param name="dao"></param>
        public void CalculateFundHoldingNavs()
        {
            IDictionary<string, FundForecast> fundForecastDict = _cache.Get<IDictionary<string, FundForecast>>(CacheKeys.FUND_FORECASTS);
            IList<FundHoldingsReturn> fundHoldingsReturnList = _commonDao.GetFundHoldingsReturn();
            foreach (FundHoldingsReturn fundHoldingsReturn in fundHoldingsReturnList)
            {
                string ticker = fundHoldingsReturn.Ticker;

                //if (("EVT US".Equals(ticker, StringComparison.CurrentCultureIgnoreCase))
                //    || ("ACZ-U CT".Equals(ticker, StringComparison.CurrentCultureIgnoreCase)))
                //    _logger.LogInformation("Calculating holdings return for ticker: " + ticker);

                try
                {
                    if (fundForecastDict.TryGetValue(ticker, out FundForecast fundForecast))
                    {
                        if (fundForecast.LastDvdAdjNav.HasValue)
                        {
                            if (!string.IsNullOrEmpty(fundForecast.PortFIProxyForm))
                            {
                                //portfolio return excluding fixed income
                                double portReturn = fundHoldingsReturn.TotalRtnExFIRtn.GetValueOrDefault();
                                //add fixed income return
                                //portReturn = (1 + portReturn) * (1 + fundForecast.PortFIProxyRtn.GetValueOrDefault()) - 1.0;
                                portReturn += fundForecast.PortFIProxyRtn.GetValueOrDefault();
                                //adjust portfolio return for leverage
                                portReturn *= (1.0 + fundForecast.LevRatio.GetValueOrDefault());

                                //add currency hedge return
                                portReturn += fundHoldingsReturn.FXHedgeReturn.GetValueOrDefault();

                                //calculate estimated nav
                                double estimatedNav = fundForecast.LastDvdAdjNav.GetValueOrDefault() * (1.0 + portReturn);

                                fundForecast.PortNav = estimatedNav;
                                fundForecast.PortRtn = fundHoldingsReturn.TotalReturn;
                                fundForecast.AdjPortRtn = portReturn;
                            }
                            else
                            {
                                //portfolio return (equity, fixed income and cash)
                                double portReturn = fundHoldingsReturn.TotalReturn.GetValueOrDefault();
                                //adjust portfolio return for leverage
                                portReturn *= (1.0 + fundForecast.LevRatio.GetValueOrDefault());

                                //add currency hedge return
                                portReturn += fundHoldingsReturn.FXHedgeReturn.GetValueOrDefault();

                                //calculate estimated nav
                                double estimatedNav = fundForecast.LastDvdAdjNav.GetValueOrDefault() * (1.0 + portReturn);

                                fundForecast.PortNav = estimatedNav;
                                fundForecast.PortRtn = fundHoldingsReturn.TotalReturn;
                                fundForecast.AdjPortRtn = portReturn;

                                //ignore extreme values (where there is 15% change in portfolio return)
                                //if (Math.Abs(portReturn) > 0.15)
                                //    fundForecast.PortNav = null;

                                //holdings return using proxy
                                if (fundHoldingsReturn.TotalReturnProxy.HasValue)
                                {
                                    // adjust portfolio return for leverage
                                    double portReturnProxy = fundHoldingsReturn.TotalReturnProxy.GetValueOrDefault();
                                    portReturnProxy *= (1.0 + fundForecast.LevRatio.GetValueOrDefault());

                                    //add currency hedge return
                                    portReturnProxy += fundHoldingsReturn.FXHedgeReturn.GetValueOrDefault();

                                    //calculate estimated nav
                                    double estimatedNavProxy = fundForecast.LastDvdAdjNav.GetValueOrDefault() * (1.0 + portReturnProxy);

                                    fundForecast.PortNavProxy = estimatedNavProxy;
                                    fundForecast.PortRtnProxy = fundHoldingsReturn.TotalReturnProxy;
                                    fundForecast.AdjPortRtnProxy = portReturnProxy;
                                }
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error calculating Navs using Portfolio Holdings for ticker: " + ticker);
                }
            }
        }

        /// <summary>
        /// Calculates live market values of ALM fund(s)
        /// </summary>
        /// <param name="cache"></param>
        public void CalculateLiveMarketValues()
        {
            IDictionary<string, PositionMaster> almPositionMasterDict = _cache.Get<IDictionary<string, PositionMaster>>(CacheKeys.POSITION_MASTER);
            IDictionary<string, SecurityPrice> securityPriceDict = _cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);
            IDictionary<string, FXRate> fxRatesDict = _cache.Get<IDictionary<string, FXRate>>(CacheKeys.FX_RATES);
            IDictionary<string, double> fundMarketValues = _cache.Get<IDictionary<string, double>>(CacheKeys.ALM_FUND_MARKET_VALUES);
            IDictionary<string, string> priceTickerMap = _cache.Get<IDictionary<string, string>>(CacheKeys.PRICE_TICKER_MAP);

            //alm fund market values
            double totalFundMarketValue = 0;
            double totalOppFundMarketValue = 0;
            double totalTacFundMarketValue = 0;

            if (fundMarketValues != null && fundMarketValues.ContainsKey("All"))
                totalFundMarketValue = fundMarketValues["All"];
            if (fundMarketValues != null && fundMarketValues.ContainsKey("OPP"))
                totalOppFundMarketValue = fundMarketValues["OPP"];
            if (fundMarketValues != null && fundMarketValues.ContainsKey("TAC"))
                totalTacFundMarketValue = fundMarketValues["TAC"];

            foreach (KeyValuePair<string, PositionMaster> kvp in almPositionMasterDict)
            {
                string ticker = kvp.Key;
                PositionMaster positionMaster = kvp.Value;

                try
                {
                    //alm holdings market value
                    //convert holdings market value (from holdings's base currency) to USD
                    if (positionMaster.FundAll.PosHeld.HasValue)
                    {
                        //get fx rate
                        FXRate fxRate = null;
                        if (!string.IsNullOrEmpty(positionMaster.Curr))
                            fxRatesDict.TryGetValue(positionMaster.Curr, out fxRate);

                        //get security price
                        SecurityPrice securityPrice = SecurityPriceLookupOperations.GetSecurityPrice(ticker, priceTickerMap, securityPriceDict);

                        //calculate live market values
                        //Fund ALL, OPP, TAC
                        CalculateLiveMarketValuesByFund(
                            ticker
                            , positionMaster.FundAll
                            , securityPrice
                            , fxRate
                            , positionMaster.ShOut
                            , totalFundMarketValue
                            , positionMaster.Avg20DayVol);
                        CalculateLiveMarketValuesByFund(
                            ticker
                            , positionMaster.FundOpp
                            , securityPrice
                            , fxRate
                            , positionMaster.ShOut
                            , totalOppFundMarketValue
                            , positionMaster.Avg20DayVol);
                        CalculateLiveMarketValuesByFund(
                            ticker
                            , positionMaster.FundTac
                            , securityPrice
                            , fxRate
                            , positionMaster.ShOut
                            , totalTacFundMarketValue
                            , positionMaster.Avg20DayVol);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error calculating Live Market Values for ticker: " + ticker);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ticker"></param>
        /// <param name="fundSummary"></param>
        /// <param name="securityPrice"></param>
        /// <param name="fxRate"></param>
        /// <param name="sharesOutstanding"></param>
        /// <param name="fundMarketValue"></param>
        /// <param name="average20DayVolume"></param>
        private void CalculateLiveMarketValuesByFund(
            string ticker
            , FundSummaryDetail fundSummary
            , SecurityPrice securityPrice
            , FXRate fxRate
            , double? sharesOutstanding
            , double fundMarketValue
            , double? average20DayVolume)
        {
            try
            {
                if (sharesOutstanding.HasValue && sharesOutstanding.GetValueOrDefault() > 0)
                {
                    fundSummary.PosOwnPct = fundSummary.PosHeld / sharesOutstanding;
                    fundSummary.BkrJPM.PosOwnPct = fundSummary.BkrJPM.PosHeld / sharesOutstanding;
                    fundSummary.BkrFido.PosOwnPct = fundSummary.BkrFido.PosHeld / sharesOutstanding;
                    fundSummary.BkrJeff.PosOwnPct = fundSummary.BkrJeff.PosHeld / sharesOutstanding;
                    fundSummary.BkrIB.PosOwnPct = fundSummary.BkrIB.PosHeld / sharesOutstanding;
                    fundSummary.BkrTD.PosOwnPct = fundSummary.BkrTD.PosHeld / sharesOutstanding;
                    fundSummary.BkrEDF.PosOwnPct = fundSummary.BkrEDF.PosHeld / sharesOutstanding;
                    fundSummary.BkrScotia.PosOwnPct = fundSummary.BkrScotia.PosHeld / sharesOutstanding;
                    fundSummary.BkrBMO.PosOwnPct = fundSummary.BkrBMO.PosHeld / sharesOutstanding;
                    fundSummary.BkrUBS.PosOwnPct = fundSummary.BkrUBS.PosHeld / sharesOutstanding;
                    fundSummary.BkrMS.PosOwnPct = fundSummary.BkrMS.PosHeld / sharesOutstanding;
                    fundSummary.BkrBAML.PosOwnPct = fundSummary.BkrBAML.PosHeld / sharesOutstanding;
                    fundSummary.BkrSTON.PosOwnPct = fundSummary.BkrSTON.PosHeld / sharesOutstanding;
                }

                double fxReturn = 1.0;
                if (fxRate != null)
                    fxReturn = (1.0 + fxRate.FXReturn.GetValueOrDefault());

                double securityReturn = 1.0;
                if (securityPrice != null)
                    securityReturn = (1.0 + securityPrice.PrcRtn.GetValueOrDefault());

                fundSummary.MV = fundSummary.ClsMV * securityReturn * fxReturn;
                fundSummary.BkrJPM.MV = fundSummary.BkrJPM.ClsMV * securityReturn * fxReturn;
                fundSummary.BkrFido.MV = fundSummary.BkrFido.ClsMV * securityReturn * fxReturn;
                fundSummary.BkrJeff.MV = fundSummary.BkrJeff.ClsMV * securityReturn * fxReturn;
                fundSummary.BkrIB.MV = fundSummary.BkrIB.ClsMV * securityReturn * fxReturn;
                fundSummary.BkrTD.MV = fundSummary.BkrTD.ClsMV * securityReturn * fxReturn;
                fundSummary.BkrEDF.MV = fundSummary.BkrEDF.ClsMV * securityReturn * fxReturn;
                fundSummary.BkrScotia.MV = fundSummary.BkrScotia.ClsMV * securityReturn * fxReturn;
                fundSummary.BkrBMO.MV = fundSummary.BkrBMO.ClsMV * securityReturn * fxReturn;
                fundSummary.BkrUBS.MV = fundSummary.BkrUBS.ClsMV * securityReturn * fxReturn;
                fundSummary.BkrMS.MV = fundSummary.BkrMS.ClsMV * securityReturn * fxReturn;
                fundSummary.BkrBAML.MV = fundSummary.BkrBAML.ClsMV * securityReturn * fxReturn;
                fundSummary.BkrSTON.MV = fundSummary.BkrSTON.ClsMV * securityReturn * fxReturn;

                if (fundMarketValue > 0)
                {
                    fundSummary.ClsMVPct = fundSummary.ClsMV / fundMarketValue;
                    fundSummary.MVPct = fundSummary.MV / fundMarketValue;
                    fundSummary.BkrJPM.MVPct = fundSummary.BkrJPM.MV / fundMarketValue;
                    fundSummary.BkrFido.MVPct = fundSummary.BkrFido.MV / fundMarketValue;
                    fundSummary.BkrJeff.MVPct = fundSummary.BkrJeff.MV / fundMarketValue;
                    fundSummary.BkrIB.MVPct = fundSummary.BkrIB.MV / fundMarketValue;
                    fundSummary.BkrTD.MVPct = fundSummary.BkrTD.MV / fundMarketValue;
                    fundSummary.BkrEDF.MVPct = fundSummary.BkrEDF.MV / fundMarketValue;
                    fundSummary.BkrScotia.MVPct = fundSummary.BkrScotia.MV / fundMarketValue;
                    fundSummary.BkrBMO.MVPct = fundSummary.BkrBMO.MV / fundMarketValue;
                    fundSummary.BkrUBS.MVPct = fundSummary.BkrUBS.MV / fundMarketValue;
                    fundSummary.BkrMS.MVPct = fundSummary.BkrMS.MV / fundMarketValue;
                    fundSummary.BkrBAML.MVPct = fundSummary.BkrBAML.MV / fundMarketValue;
                    fundSummary.BkrSTON.MVPct = fundSummary.BkrSTON.MV / fundMarketValue;
                }

                if (average20DayVolume.HasValue && average20DayVolume.GetValueOrDefault() > 0)
                    fundSummary.PosByAvg20DayVol = fundSummary.PosHeld / average20DayVolume;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error updating Live Market Values: " + ticker);
            }
        }
    }
}