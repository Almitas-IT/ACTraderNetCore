using aCommons;
using aCommons.Cef;
using aCommons.DTO;
using System;
using System.Collections.Generic;

namespace aACTrader.DAO.Interface
{
    public interface IBaseDao
    {
        public IList<FundHolding> GetFundHoldings(string ticker, string source);
        public IList<FundHolding> GetFundHoldings(string country);
        public IList<SecurityMaster> GetSecurityMaster(string ticker, string status);
        public IList<CustomView> GetCustomViews();
        public IList<CustomView> GetCustomViewDetails(string viewName);
        public IList<BDCNavHistory> GetBDCNavHistory(string ticker);
        public IList<ReitBookValueHistory> GetReitBookValueHistory(string ticker);
        public IList<FundCurrencyHedge> GetFundCurrencyHedges(string ticker);
        public IList<FundAlert> GetFundAlerts(string category, string startDate, string endDate);
        public IList<FundAlertTarget> GetFundAlertTargets();
        public IList<TradeTaxLot> GetTaxLots(string ticker, string account, string broker, string AgeRangeFrom, string AgeRangeTo);
        public IList<TradeTaxLotSummary> GetTaxLotSummary(string account, string broker);
        public IList<FundNavHistory> GetFundNavHistory(string ticker, DateTime startDate, DateTime endDate);
        public IList<FundEarningHist> GetFundEarningsHistory(string ticker, string profilePeriod);
        public IList<FundHistory> GetFundHistory(string ticker, string startDate, string endDate);
        public IList<FundHistory> GetFundHistory(string figi);
        public IDictionary<string, FundMaster> GetSecurityMaster();
        public IDictionary<string, BrokerTradingVolume> GetBrokerTradingVolume();
        public IDictionary<string, SecurityMaster> GetPositionSecurityDetails();
        public IDictionary<string, SecurityPerformance> GetSecurityPerformance();
        public IDictionary<string, SecurityPerformance> GetSecurityPerformance(DateTime startDate, DateTime endDate);
        public IDictionary<string, string> GetPfdCommonSharesMap();
        public IDictionary<string, FundReturn> GetBDCFundReturns();
        public IDictionary<string, SecurityMaster> GetWatchlistSecurities();
        public IDictionary<string, TradingTarget> GetTradingTargets();
        public IDictionary<string, ExpectedAlphaModelParams> GetExpectedAlphaModelParams();
        public IList<JobDetail> GetJobDetails(string jobName);
        public Nullable<DateTime> GetPfdCurvesUpdateTime();
        public IList<FundHolding> GetPortHoldingsForTicker(string holdingTicker);
        public IList<TaxLotSummaryTO> GetTaxLotSummary();

        //New Functions
        public IList<FundNavHistoryNew> GetFundHistoricalData(string ticker, DateTime startDate, DateTime endDate, string hasNav);
        public IList<FundNavHistoryNew> GetSectorHistoricalData(string country, string securityType, string sector, string fundCategory, DateTime startDate, DateTime endDate);
        public FundGroupStatsSummary GetFundHistoricalStats(string ticker, string measureType);
        public FundGroupStatsSummary GetSectorHistoricalStats(string country, string securityType, string sector, string fundCategory, string measureType);
        public IList<FundNavHistory> GetSectorHistory(string country, string securityType, string cefInstrumentType, string sector, string fundCategory, DateTime startDate, DateTime endDate);

        //BDC
        public IList<BDCData> GetBDCHistoricalData(string ticker, DateTime startDate, DateTime endDate, string hasNav);

        public void SaveBDCNavHistory(IList<BDCNavHistory> bdcNavHistory);
        public void SaveReitBookValueHistory(IList<ReitBookValueHistory> reitBVHistory);
        public void SaveCustomView(IList<CustomView> customViewList);
        public void SaveFundCurrencyHedges(IList<FundCurrencyHedge> currencyHedgeList);
        public void SaveFundAlertTargets(IList<FundAlertTarget> fundAlertTargetList);
        public void SaveFundRedemptions(IList<aCommons.Cef.FundRedemption> fundRedemptionList);
        public void SaveFundHoldings(IList<FundHolding> fundHoldings, string deleteAndInsert);
        public void SaveUserFundHoldings(IList<FundHolding> fundHoldings, string deleteAndInsert);
        public void SaveSectorDiscounts(string assetClass, IList<FundGroupStats> fundGroupStatsList);
        public void SaveSectorFundMap(string assetClass, HashSet<string> fundList);
        public void SaveSecurityMasterDetails(IList<SecurityMaster> securityMasterList);
        public void SaveTradingTargets(IList<TradingTarget> tradingTargetList);

        public void PopulateFundAlerts();
    }
}
