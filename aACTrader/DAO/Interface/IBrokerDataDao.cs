using aCommons;
using aCommons.Fund;
using aCommons.Trading;
using System;
using System.Collections.Generic;

namespace aACTrader.DAO.Interface
{
    public interface IBrokerDataDao
    {
        public IList<SecurityMarginDetail> GetJPMSecurityMarginDetails(
            string fundName, string ticker, DateTime startDate, DateTime endDate);

        public IList<SecurityMargin> GetFidelitySecurityMarginDetails(
            string fundName, string ticker, DateTime startDate, DateTime endDate);

        public IList<SecurityBorrowRate> GetJPMSecurityBorrowRates(string ticker);

        public IDictionary<string, BrokerCommission> GetBrokerCommissionRates();
        public IList<BrokerTO> GetExecutedBrokers();
        public IList<BrokerTO> GetASExecutingBrokers();
        public IList<FundSummaryTO> GetFundSummary(string fundName, DateTime asofDate);
    }
}
