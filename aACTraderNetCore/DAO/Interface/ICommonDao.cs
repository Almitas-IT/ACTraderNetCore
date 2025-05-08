using aCommons;
using aCommons.Cef;
using System;
using System.Collections.Generic;

namespace aACTrader.DAO.Interface
{
    public interface ICommonDao
    {
        public IList<RegressionCoefficient> GetETFRegressionCoefficients(string ticker);
        public IDictionary<string, FundNav> GetFundNavs();
        public IList<FundHoldingsReturn> GetFundHoldingsReturn();
        public IList<MSFundHistory> GetMSFundHistory(string Ticker, string StartDate, string EndDate);
        public IList<MSExpenseRatio> GetMSExpenseHistory(string Ticker);
        public IDictionary<string, FundHistStats> GetFundStats();
        public IDictionary<string, FundMaster> GetFundMaster();
        public IDictionary<string, Holding> GetHoldings();
        public IDictionary<string, Holding> GetHoldingDetailsByPort();
        public IDictionary<string, Holding> GetHoldingHistoryByPort(DateTime asofDate);
        public IList<Holding> GetHoldingsHistoryByPort(string fundName, string broker, string ticker, DateTime startDate, DateTime endDate);
        public IDictionary<string, Holding> GetEODHoldingDetailsByPort();
        public IDictionary<string, FundHolderSummary> GetFundHolderSummary();
        public IDictionary<string, FXRate> GetFXRates();
        public IDictionary<string, FXRate> GetFXRatesPD();
        public IDictionary<string, FundDividend> GetFundDividends();
        public IDictionary<string, FundETFReturn> GetFundRegETFReturns();
        public IDictionary<string, FundETFReturn> GetFundProxyETFReturns();
        public IDictionary<string, FundETFReturn> GetFundAltProxyETFReturns();
        public IDictionary<string, FundETFReturn> GetFundPortProxyETFReturns();
        public IDictionary<string, FundRedemption> GetFundRedemptionDetails();
        public IDictionary<string, FundAlphaModelParams> GetFundAlphaModelParams();
        public IDictionary<string, FundAlphaModelScores> GetFundAlphaModelScores();
        public IDictionary<string, FundDividendSchedule> GetFundDividendSchedule();
        public IDictionary<string, UserDataOverride> GetUserDataOverrides();
        public IList<FundHoldingReturn> GetFundHoldingReturn(string ticker);
        public IList<FundHoldingReturn> GetFundHoldingReturnsNew(string ticker);
        public IDictionary<string, Dividend> GetDividends();
        public IDictionary<string, double> GetFundMarketValues();
        public IList<UserDataOverrideRpt> GetUserDataOverrideRpt();

        public void InitializeDailyBatch();

        public void SaveFundRedemptionDates(IDictionary<string, FundRedemption> fundRedemptionDict);
        public void SaveUserOverrides(IList<UserDataOverride> overrides);
        public void SaveFundProxyTickers(IDictionary<string, FundProxyFormula> fundProxyTickersDict);
        public void SaveFundAltProxyTickers(IDictionary<string, FundProxyFormula> fundProxyTickersDict);
        public void SaveFundPortProxyTickers(IDictionary<string, FundProxyFormula> fundProxyTickersDict);

        public void UpdateFundNavOverrides();
        public void UpdateFundPortDates();
    }
}
