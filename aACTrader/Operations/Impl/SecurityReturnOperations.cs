using aCommons;
using LazyCache;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;

namespace aACTrader.Operations.Impl
{
    public class SecurityReturnOperations
    {
        private readonly ILogger<SecurityReturnOperations> _logger;
        private readonly CachingService _cache;

        public SecurityReturnOperations(ILogger<SecurityReturnOperations> logger, CachingService cache)
        {
            _logger = logger;
            _cache = cache;
        }

        public Nullable<double> GetSecurityReturn(string ticker, DateTime fromDate, DateTime toDate)
        {
            double? result = null;

            try
            {
                if (string.IsNullOrEmpty(ticker))
                    return null;

                //if (fromDate == null || toDate == null)
                //    return null;

                IDictionary<string, IList<HistSecurityReturn>> securityReturnDict = _cache.Get<IDictionary<string, IList<HistSecurityReturn>>>(CacheKeys.HIST_SECURITY_RETURNS);

                IList<HistSecurityReturn> securityReturnList;
                if (securityReturnDict.TryGetValue(ticker, out securityReturnList))
                {
                    IList<HistSecurityReturn> filteredSecurityReturnList = securityReturnList.Where(s => s.EffectiveDate > fromDate && s.EffectiveDate <= toDate).ToList<HistSecurityReturn>();
                    foreach (HistSecurityReturn data in filteredSecurityReturnList)
                    {
                        if (result.HasValue)
                            result = result.GetValueOrDefault() * (1.0 + data.SecurityRtn.GetValueOrDefault());
                        else
                            result = (1.0 + data.SecurityRtn.GetValueOrDefault());
                    }

                    if (result.HasValue)
                        result = result.GetValueOrDefault() - 1.0;
                }
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error calculating security returns for ticker: " + ticker);
            }

            return result;
        }

        public Nullable<double> GetFXReturn(string fromCurrency, string toCurrency, DateTime fromDate, DateTime toDate)
        {
            double? result = null;

            try
            {
                if (string.IsNullOrEmpty(fromCurrency) || string.IsNullOrEmpty(toCurrency))
                    return null;

                if (fromCurrency.Equals(toCurrency))
                    return null;

                if (fromDate == null || toDate == null)
                    return null;

                IDictionary<string, IDictionary<DateTime, HistFXRate>> fxRateDict = _cache.Get<IDictionary<string, IDictionary<DateTime, HistFXRate>>>(CacheKeys.HIST_FX_RATES);
                if (toCurrency.Equals("USD", StringComparison.CurrentCultureIgnoreCase))
                {
                    IDictionary<DateTime, HistFXRate> fxRateFromDict;
                    if (fxRateDict.TryGetValue(fromCurrency, out fxRateFromDict))
                    {
                        IList<HistFXRate> filteredFXRateFromList = fxRateFromDict.Values.Where(s => s.EffectiveDate > fromDate && s.EffectiveDate <= toDate).ToList<HistFXRate>();
                        foreach (HistFXRate data in filteredFXRateFromList)
                        {
                            if (result.HasValue)
                                result = result.GetValueOrDefault() * (1.0 + data.FXRate.GetValueOrDefault());
                            else
                                result = (1.0 + data.FXRate.GetValueOrDefault());
                        }

                        if (result.HasValue)
                            result = result.GetValueOrDefault() - 1.0;
                    }
                }
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error calculating fx returns for currency: " + fromCurrency);
            }

            return result;
        }
    }
}