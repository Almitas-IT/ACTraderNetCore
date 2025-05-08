using aCommons;
using aCommons.Cef;
using aCommons.DTO;
using aCommons.Fund;
using System;
using System.Collections.Generic;

namespace aACTrader.DAO.Interface
{
    public interface IHoldingsDao
    {
        public IList<AlmHolding> GetAlmHoldings(DateTime asofDate);
        public IList<CompanyHoldings> GetActivistHoldings(string activistName, DateTime startDate, DateTime endDate);
        public IList<FundHolder> GetFundHolders(string reportType);
        public IList<FundHolder> GetFundHoldersSummary(string reportType);
        public IList<AlmHolding> GetAlmHoldingsSharesRecall();
        public IList<ActivistHolding> GetActivistHoldings(string holder, string ticker);
        public IList<ActivistHolding> GetActivistHoldingsHistory(string holder, string ticker);
        public IList<ActivistScore> GetActivistScores(string country);
        public IDictionary<string, ActivistHolding> GetActivistHoldings();
        public IList<Trade> GetTradeHistory(string startDate, string endDate, string portfolio, string broker, string ticker, string currency, string country, string geoLevel1, string geoLevel2, string geoLevel3,
            string assetClassLevel1, string assetClassLevel2, string assetClassLevel3, string fundCategory, string securityType);

        public IDictionary<string, Trade> GetTradeHistory(DateTime asofDate);
        public IDictionary<string, FundHoldingSummary> GetFundHoldingSummary();
        public IList<PositionLongShortDetail> GetPositionLongShortDetails();
        public IDictionary<string, FundSharesTendered> GetSharesTendered();
        IList<PositionReconTO> GetPositionRecon(DateTime asofDate);
        public IList<PositionTO> GetAlmHoldingsByDate(DateTime asofDate, string fundName);
        public IList<HoldingExposureTO> GetHoldingsByDate(DateTime asofDate);
        public IList<AlmHoldingPerfTO> GetAlmPositionsByDate(DateTime asofDate);
        public IList<EODPositionTO> GetEODHoldingsByDate(DateTime asofDate);
        public IList<FundFuturesExposureTO> GetFuturesHoldings(DateTime asofDate, string fundName);

        public void SaveSharesRecallOverrides(IList<AlmHolding> holdings);
        public void SaveAlmHoldings(IList<AlmHolding> holdings);
        public void SaveActivistScores(IList<ActivistScore> activistScores);
    }
}
