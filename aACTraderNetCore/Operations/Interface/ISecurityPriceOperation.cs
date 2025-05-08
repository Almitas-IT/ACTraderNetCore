using aACTrader.Model;
using aCommons;
using System.Collections.Generic;

namespace aACTrader.Operations.Interface
{
    public interface ISecurityPriceOperation
    {
        public IList<SecurityPrice> GetLatestPrices();
        public IList<SecurityPrice> GetLivePrices();
        public IList<SecurityPrice> GetLivePricesByExchange();
        public IList<SecurityPrice> GetDelayedPrices();
        public IList<SecurityPrice> GetDelayedPricesByExchange();
        public IList<FXRateTO> GetLiveFXRates();
        public IList<SecurityPriceMap> GetPriceTickerMap();
        public IList<SecurityPriceMap> GetNVPriceTickerMap();
        public IList<SecurityPrice> GetPriceAlerts();
        public IList<SecurityPrice> GetPriceAlertsWithFilter(PriceFilterParameters parameters);

        public void SavePricesToStgTable(string filePath, string tableName);
        public void SavePricesToStgTable(IDictionary<string, SecurityPrice> securityPriceDict);
        public void SaveDailyPrices(IDictionary<string, SecurityPrice> securityPriceDict);
        public void MovePricesToTgtTable();
        public void TruncateTable(string tableName);
        public IDictionary<string, SecurityPrice> GetSecurityPriceMaster();
        public void SavePrices();

        public IDictionary<string, FXRate> GetLatestFXRates();
        public void SaveFXRatesToStg(IDictionary<string, FXRate> fxRateDict);
        public void MoveFXRatesToTgtTable();
    }
}