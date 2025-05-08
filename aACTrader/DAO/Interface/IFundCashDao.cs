using aCommons;
using aCommons.Fund;
using aCommons.Web;
using System.Collections.Generic;

namespace aACTrader.DAO.Interface
{
    public interface IFundCashDao
    {
        public IList<FundDetail> GetFidelityFundDetails(string broker);
        public IList<FundDetail> GetJPMFundDetails(string broker);
        public IList<FundDetail> GetIBFundDetails(string broker);
        public IList<FundDetail> GetJefferiesFundDetails(string broker);
        public IList<FundDetail> GetEDFFundDetails(string broker);

        public IList<FundCashDetail> GetFidelityFundCashDetails(string broker);
        public IList<FundCashDetail> GetJPMFundCashDetails(string broker);
        public IList<FundCashDetail> GetIBFundCashDetails(string broker);
        public IList<FundCashDetail> GetJefferiesFundCashDetails(string broker);

        public IList<FundFinanceDetail> GetJPMFundFinanceDetails(string broker);
        public IList<FundInterestEarningDetail> GetFidelityFundInterestEarningsDetails(string broker);

        public IDictionary<string, SecurityRebateRate> GetSecurityRebateRates();
        public IList<SecurityActualRebateRate> GetSecurityActualRebateRates();

        public IList<SecurityMargin> GetJPMSecurityMarginRates(string broker);
        public IDictionary<string, SecurityMarginDetail> GetJPMSecurityMarginDetails();
        public IDictionary<string, JPMSecurityMarginDetail> GetLatestJPMSecurityMarginDetails();
        public IDictionary<string, SecurityMargin> GetFidelitySecurityMarginRates(string broker);
        public IDictionary<string, FundCurrencyExposureTO> GetFundCurrencyExposures(string broker);

        public IList<FundMarginAttributionDetail> GetFidelityMarginAttributionDetails(string broker);

        public IList<FundCurrencyDetail> GetJPMCurrencyDetails(string broker);
        public IList<FundSwapDetail> GetJPMFundSwapDetails(string broker);
        public IList<FundSwapDetail> GetJPMFundSwapDetails();

        public IList<FundPositionDetail> GetIBPositionDetails(string broker);

        //Fund Summary and Detail Exposures (Report)
        public IList<FundSummaryExpTO> GetFundSummaryExposures();
        public IList<FundDetailExpTO> GetFundDetailExposures();
        public void SaveFundSummaryExposures(IList<FundSummaryExp> list);
        public void SaveFundDetailExposures(IList<FundDetailExp> list);
    }
}
