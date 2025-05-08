using aCommons;
using aCommons.Cef;
using System;
using System.Collections.Generic;

namespace aACTrader.DAO.Interface
{
    public interface IFundSupplementalDataDao
    {
        public IDictionary<string, FundNavUpdate> GetFundNavUpdateFrequency();
        public IDictionary<string, FundRightsOffer> GetFundRightsOffers();
        public IList<FundDividend> GetFundDividends(string ticker, DateTime startDate, DateTime endDate);
        public IList<FundDividend> GetFundDividendsFinal(string ticker, DateTime startDate, DateTime endDate);
        public IDictionary<string, FundTenderOffer> GetFundTenderOffers();
        public IDictionary<string, FundLeverage> GetFundLeverageRatios();
        public IList<FundBuyback> GetFundBuybackCounts();
        public IList<FundBuyback> GetFundBuybackDetails(string ticker);
        public IList<FundBuyback> GetFundBuybackHistory(string ticker);
        public IList<FundBuybackTO> GetBuybackHistory(string ticker);
        public IDictionary<string, FundPortDate> GetFundPortDates();
        public IDictionary<string, FundRedemptionTrigger> GetFundRedemptionTriggers();
        public IList<FundRedemptionTriggerDetail> GetFundRedemptionDiscountTriggerDetails(string ticker);
        public IList<FundRedemptionTriggerDetail> GetFundRedemptionNavReturnTriggerDetails(string ticker, string triggerType, string securityType);
        public IList<FundDiscountHist> GetFundDiscountHistory(string ticker, string source, DateTime startDate, DateTime endDate);
        public IList<FundDataHist> GetFundDataHistory(string ticker, string securityType, string dataType, string source, DateTime startDate, DateTime endDate);
        public void SaveFundRedemptionTriggerDetails(IList<FundRedemptionTriggerDetail> list);
        public void SaveFundRightsOffers(IList<FundRightsOffer> list);
        public void SaveFundTenderOffers(IList<FundTenderOffer> list);
    }
}
