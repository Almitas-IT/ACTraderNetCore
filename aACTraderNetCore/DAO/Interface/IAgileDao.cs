using aCommons.Agile;
using System;
using System.Collections.Generic;

namespace aACTrader.DAO.Interface
{
    public interface IAgileDao
    {
        public IList<AgilePosition> GetPositions(string ticker, DateTime startDate, DateTime endDate);
        public IList<AgileTrade> GetTrades(string ticker, DateTime startDate, DateTime endDate);
        public IList<AgileDailyPerf> GetDailyPerf(string ticker, DateTime startDate, DateTime endDate);
    }
}
