using aCommons;
using aCommons.DTO.CEFA;
using System;
using System.Collections.Generic;

namespace aACTrader.DAO.Interface
{
    public interface IFundHistoryDao
    {
        public IList<FundHistory> GetFundHistory(string ticker);
        public IList<CAFundHistory> GetCAFundHistory(string ticker);
        public IList<FundHistoryTO> GetFundHistoryDetails(string ticker, DateTime startDate, DateTime endDate, string navFreq);
        public IList<FundHistoryTO> GetREITHistoryDetails(string ticker, DateTime startDate, DateTime endDate);
        public IList<FundNavReportTO> GetFundNavReportDetails(string ticker, DateTime startDate, DateTime endDate);
        public IList<FundNavReportTO> GetLatestFundNavReportDetails(string country, DateTime asofDate);
        public FundHistoryScalar GetFundHistoryScalar(string ticker, DateTime effectiveDate, int lookbackDays);
        public FundDividendScalar GetFundDividendScalar(string ticker, DateTime startDate, DateTime endDate, string dvdDateField);
        public IDictionary<string, IDictionary<DateTime, FundHistoryScalar>> GetFundHistoryFull();
        public IList<FundHistoryTO> GetBBGFundHistory(string ticker, DateTime startDate, DateTime endDate);

        public IList<CEFAFieldMstTO> GetCEFAFieldMaster(string fieldName);
        public IList<CEFAFieldDetTO> GetCEFAFieldHistory(string ticker, string fieldName);
        public IDictionary<string, IList<FundPDHistory>> GetCEFFundHistoryFull();
    }
}
