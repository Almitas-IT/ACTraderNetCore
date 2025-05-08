using aCommons;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;

namespace aACTrader.Operations.Impl
{
    public class SecurityPriceLookupOperations
    {
        private readonly ILogger<SecurityPriceLookupOperations> _logger;

        public SecurityPriceLookupOperations(ILogger<SecurityPriceLookupOperations> logger)
        {
            _logger = logger;
        }

        /// <summary>
        /// Gets security price for a ticker
        /// Looks up the source ticker for given input ticker and gets live security price
        /// Input ticker 4475 JP mapped to 4475 JT Equity Neovest ticker
        /// </summary>
        /// <param name="ticker"></param>
        /// <param name="priceTickerMap"></param>
        /// <param name="securityPriceDict"></param>
        /// <returns></returns>
        public static SecurityPrice GetSecurityPrice(string ticker, IDictionary<string, string> priceTickerMap, IDictionary<string, SecurityPrice> securityPriceDict)
        {
            SecurityPrice securityPrice = null;

            try
            {
                //get source ticker (Neovest ticker)
                //sometimes multiple tickers map to single source ticker
                //example:- EVT CN and EVT CT tickers map to same RealTick EVT.CAT exchange ticker
                if (!priceTickerMap.TryGetValue(ticker, out string sourceTicker))
                    sourceTicker = ticker;

                //get security price object
                securityPriceDict.TryGetValue(sourceTicker, out securityPrice);
            }
            catch (Exception ex)
            {
                //_logger.LogInformation(ex, "Could not find a price for ticker: " + ticker);
            }
            return securityPrice;
        }
    }
}