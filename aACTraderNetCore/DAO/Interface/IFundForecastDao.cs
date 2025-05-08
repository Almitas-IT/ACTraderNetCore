using aCommons;
using aCommons.Cef;
using System.Collections.Generic;

namespace aACTrader.DAO.Interface
{
    public interface IFundForecastDao
    {
        public void SaveFundForecasts(IDictionary<string, FundForecast> fundForecastDict
           , IDictionary<string, FundMaster> fundMasterDict
           , IDictionary<string, FundAlphaModelScores> fundAlphaModelScoresDict
           , IDictionary<string, SecurityPrice> securityPriceDict
           , IDictionary<string, string> priceTickerMap);

        public void SaveCAFundForecasts(IList<CAFundForecast> fundForecastList);
        public void SavePortHoldingReturns(IList<FundHoldingsReturn> fundHoldingsReturnList);
        public void SaveFundPDStats(IDictionary<string, FundPDStats> fundPDStatsDict);

        public IDictionary<string, FundCondProxy> GetFundConditionalProxies();
    }
}
