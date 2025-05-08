using aCommons.DTO;
using System;
using System.Collections.Generic;

namespace aACTrader.DAO.Interface
{
    public interface IUKFundDetailsDao
    {
        public IList<JefferiesDetailTO> GetJefferiesDataFull(string ticker, string source, DateTime? startDate, DateTime? endDate);
        public IList<NumisDetailTO> GetNumisDataFull(string ticker, string source, DateTime? startDate, DateTime? endDate);
        public IList<PeelHuntDetailTO> GetPeelHuntDataFull(string ticker, string source, DateTime? startDate, DateTime? endDate);
        public IList<JPMDetailTO> GetJPMDataFull(string ticker, string source, DateTime? startDate, DateTime? endDate);

        public IList<JefferiesDetailTO> GetJefferiesData(string ticker, string source, DateTime? startDate, DateTime? endDate);
        public IList<NumisDetailTO> GetNumisData(string ticker, string source, DateTime? startDate, DateTime? endDate);
        public IList<PeelHuntDetailTO> GetPeelHuntData(string ticker, string source, DateTime? startDate, DateTime? endDate);
        public IList<BBGFundDetailTO> GetBloombergData(string ticker, string source, DateTime? startDate, DateTime? endDate);
        public IList<JPMDetailTO> GetJPMData(string ticker, string source, DateTime? startDate, DateTime? endDate);
    }
}
