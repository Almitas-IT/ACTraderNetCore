using aACTrader.Model;
using aCommons;
using aCommons.Cef;
using System.Collections.Generic;

namespace aACTrader.Operations.Interface
{
    public interface ICommonOperations
    {
        public IDictionary<string, FundForecast> GetFundForecasts();
        //public IList<FundHoldingsReturn> GetFundHoldingsReturn();
        public IList<MSFundHistory> GetMSFundHistory(string Ticker, string StartDate, string EndDate);
        public IList<MSExpenseRatio> GetMSExpenseHistory(string Ticker);
        public IDictionary<string, FundHistStats> GetFundStats();
        public IList<FundGroup> GetCEFFundTickers(CEFParameters cefParams);
        public IList<FundHoldingReturn> GetFundHoldingReturn(InputParameters reqParams);
        public IList<FundHoldingReturn> GetFundHoldingReturnsNew(InputParameters reqParams);

        public void SaveUserOverrides(IList<UserDataOverride> overrides);
        public void SaveGlobalOverrides(IList<GlobalDataOverride> overrides);
        public void SaveFundRedemptionRules(IList<FundRedemption> fundRedemptionList);
        public void SaveSecurityRiskFactors(IList<SecurityRiskFactor> list);

        public void UpdateFundNavs();

        public void SaveFundRightsOfferDetails(IList<FundRightsOffer> list);
        public void SaveFundTenderOfferDetails(IList<FundTenderOffer> list);
    }
}