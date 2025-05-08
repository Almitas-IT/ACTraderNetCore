using aCommons;
using aCommons.Derivatives;
using aCommons.Security;
using System.Collections.Generic;

namespace aACTrader.DAO.Interface
{
    public interface ISecurityPriceDao
    {
        public IList<SecurityPrice> GetLatestPrices();
        public void SavePricesToStgTable(string filePath, string tableName);
        public void SavePricesToStgTableNew(IDictionary<string, SecurityPrice> securityPriceDict);
        public void SaveDailyPrices(IDictionary<string, SecurityPrice> securityPriceDict);
        public void MovePricesToTgtTable();
        public void TruncateTable(string tableName);
        public IDictionary<string, SecurityPrice> GetSecurityPriceMaster();
        public IDictionary<string, string> GetPriceTickerMap();
        public IDictionary<string, string> GetNVPriceTickerMap();
        public IList<SecurityPriceDetail> GetSecurityPriceDetails();
        public IList<string> GetSecuritiesByExchange();

        public IDictionary<string, FXRate> GetLatestFXRates();
        public void SaveFXRatesToStg(IDictionary<string, FXRate> fxRateDict);
        public void MoveFXRatesToTgtTable();
        public void CalculateFXReturns();

        public IList<Option> GetOptionSymbols();
        public void PopulateNeovestOptionSymbols(IList<Option> list);

        public void SaveMonthEndPrices(IList<MonthEndSecurityPrice> list);

        //////////////////////////////////////////////////////////////////////////////////////////////////
        public IDictionary<string, SharesImbalance> GetSharesImbalanceList();
        public void SaveSharesImbalanceList(IDictionary<string, SharesImbalance> dict);
    }
}
