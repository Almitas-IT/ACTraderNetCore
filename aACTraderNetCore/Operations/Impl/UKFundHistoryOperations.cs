using aACTrader.DAO.Repository;
using aCommons.DTO;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;

namespace aACTrader.Operations.Impl
{
    public class UKFundHistoryOperations
    {
        private readonly ILogger<UKFundHistoryOperations> _logger;
        private readonly UKFundDetailDao _ukFundDetailDao;
        private readonly CommonDao _commonDao;

        public UKFundHistoryOperations(ILogger<UKFundHistoryOperations> logger
            , UKFundDetailDao ukFundDetailDao
            , CommonDao commonDao)
        {
            _logger = logger;
            _ukFundDetailDao = ukFundDetailDao;
            _commonDao = commonDao;
            _logger.LogInformation("Initializing UKFundHistory Operations...");
        }

        public IList<NumisDetailTO> GetNumisDataFull(string ticker, string source, DateTime? startDate, DateTime? endDate)
        {
            return _ukFundDetailDao.GetNumisDataFull(ticker, source, startDate, endDate);
        }

        public IList<NumisDetailTO> GetNumisData(string ticker, string source, DateTime? startDate, DateTime? endDate)
        {
            return _ukFundDetailDao.GetNumisData(ticker, source, startDate, endDate);
        }

        public IList<PeelHuntDetailTO> GetPeelHuntDataFull(string ticker, string source, DateTime? startDate, DateTime? endDate)
        {
            return _ukFundDetailDao.GetPeelHuntDataFull(ticker, source, startDate, endDate);
        }

        public IList<PeelHuntDetailTO> GetPeelHuntData(string ticker, string source, DateTime? startDate, DateTime? endDate)
        {
            return _ukFundDetailDao.GetPeelHuntData(ticker, source, startDate, endDate);
        }

        public IList<JefferiesDetailTO> GetJefferiesDataFull(string ticker, string source, DateTime? startDate, DateTime? endDate)
        {
            return _ukFundDetailDao.GetJefferiesDataFull(ticker, source, startDate, endDate);
        }

        public IList<JefferiesDetailTO> GetJefferiesData(string ticker, string source, DateTime? startDate, DateTime? endDate)
        {
            return _ukFundDetailDao.GetJefferiesData(ticker, source, startDate, endDate);
        }

        public IList<JPMDetailTO> GetJPMDataFull(string ticker, string source, DateTime? startDate, DateTime? endDate)
        {
            return _ukFundDetailDao.GetJPMDataFull(ticker, source, startDate, endDate);
        }

        public IList<JPMDetailTO> GetJPMData(string ticker, string source, DateTime? startDate, DateTime? endDate)
        {
            return _ukFundDetailDao.GetJPMData(ticker, source, startDate, endDate);
        }

        public IList<BBGFundDetailTO> GetBloombergData(string ticker, string source, DateTime? startDate, DateTime? endDate)
        {
            return _ukFundDetailDao.GetBloombergData(ticker, source, startDate, endDate);
        }

    }
}
