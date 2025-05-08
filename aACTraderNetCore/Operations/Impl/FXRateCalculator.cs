using aCommons;
using LazyCache;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;

namespace aACTrader.Operations.Impl
{
    public class FXRateCalculator
    {
        private readonly ILogger<FXRateCalculator> _logger;
        private readonly CachingService _cache;

        public FXRateCalculator(ILogger<FXRateCalculator> logger
            , CachingService cache)
        {
            _logger = logger;
            _cache = cache;
        }

        public void GetFXReturn(string fromCurrency, string toCurrency)
        {
            //_logger.LogInformation("GetFXReturn - STARTED");
            double fcRatePD = 1.0, tcRatePD = 1.0, fcRate = 1.0, tcRate = 1.0, fxRatePD = 1.0, fxRate = 1.0, fxReturn = 0;

            IDictionary<string, FXRate> fxRatesBBG = _cache.Get<IDictionary<string, FXRate>>(CacheKeys.FX_RATES);
            IDictionary<string, FXRate> fxRatesNV = _cache.Get<IDictionary<string, FXRate>>(CacheKeys.FX_RATES_LIVE);

            //previous day fx rate
            if (!fromCurrency.Equals("USD") && fxRatesBBG.TryGetValue(fromCurrency, out FXRate fcPD))
                fcRatePD = fcPD.FXRatePD.GetValueOrDefault();

            if (!toCurrency.Equals("USD") && fxRatesBBG.TryGetValue(toCurrency, out FXRate tcPD))
                tcRatePD = tcPD.FXRatePD.GetValueOrDefault();

            fxRatePD = fcRatePD / tcRatePD;

            //live fx rate

            //foreach (FXRate data in fxRatesNV.Values)
            //{
            //    if (data.TradeDate.HasValue)
            //    {
            //        if (!data.BaseCurrency.Equals("USD")
            //            && fxRatesBBG.TryGetValue(data.BaseCurrency, out FXRate fxRate))
            //            fxRate.FXRateLatest = data.LastPrice;
            //        else if (fxRatesBBG.TryGetValue(data.TargetCurrency, out fxRate))
            //            fxRate.FXRateLatest = (1.0 / data.LastPrice);
            //    }
            //}

            //IList<FXRateTO> list = new List<FXRateTO>();
            //foreach (FXRate data in fxRatesBBG.Values)
            //{
            //    list.Add(new FXRateTO
            //    {
            //        Currency = data.CurrencyCode,
            //        FXRate = data.FXRateLatest,
            //        Source = data.Source
            //    }
            //    );
            //}
            //_logger.LogInformation("GetFXReturn - DONE");
        }
    }
}