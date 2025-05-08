using aACTrader.DAO.Repository;
using aACTrader.Model;
using aACTrader.Operations.Impl;
using aCommons.DTO;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;

namespace aACTrader.Controllers
{
    [ApiController]
    public class UKFundsController : ControllerBase
    {
        private readonly ILogger<UKFundsController> _logger;
        private readonly UKFundHistoryOperations _ukFundHistoryOperation;
        private readonly UKFundDetailDao _ukFundDetailDao;

        public UKFundsController(ILogger<UKFundsController> logger
           , UKFundHistoryOperations ukFundHistoryOperation
           , UKFundDetailDao uKFundDetailDao)
        {
            _logger = logger;
            _ukFundHistoryOperation = ukFundHistoryOperation;
            _ukFundDetailDao = uKFundDetailDao;
        }

        [Route("/UKFunds/GetNumisDataFull")]
        [HttpPost]
        public IList<NumisDetailTO> GetNumisDataFull(InputParameters parameters)
        {
            return _ukFundHistoryOperation.GetNumisDataFull(parameters.Ticker
                , parameters.Source, Convert.ToDateTime(parameters.StartDate), Convert.ToDateTime(parameters.EndDate));
        }

        [Route("/UKFunds/GetNumisData")]
        [HttpPost]
        public IList<NumisDetailTO> GetNumisData(InputParameters parameters)
        {
            return _ukFundHistoryOperation.GetNumisData(parameters.Ticker
                , "Numis", Convert.ToDateTime(parameters.StartDate), Convert.ToDateTime(parameters.EndDate));
        }

        [Route("/UKFunds/GetPeelHuntDataFull")]
        [HttpPost]
        public IList<PeelHuntDetailTO> GetPeelHuntDataFull(InputParameters parameters)
        {
            return _ukFundHistoryOperation.GetPeelHuntDataFull(parameters.Ticker
                , parameters.Source, Convert.ToDateTime(parameters.StartDate), Convert.ToDateTime(parameters.EndDate));
        }

        [Route("/UKFunds/GetPeelHuntData")]
        [HttpPost]
        public IList<PeelHuntDetailTO> GetPeelHuntData(InputParameters parameters)
        {
            return _ukFundHistoryOperation.GetPeelHuntData(parameters.Ticker
                , "PeelHunt", Convert.ToDateTime(parameters.StartDate), Convert.ToDateTime(parameters.EndDate));
        }

        [Route("/UKFunds/GetJefferiesDataFull")]
        [HttpPost]
        public IList<JefferiesDetailTO> GetJefferiesDataFull(InputParameters parameters)
        {
            return _ukFundHistoryOperation.GetJefferiesDataFull(parameters.Ticker
                , parameters.Source, Convert.ToDateTime(parameters.StartDate), Convert.ToDateTime(parameters.EndDate));
        }

        [Route("/UKFunds/GetJefferiesData")]
        [HttpPost]
        public IList<JefferiesDetailTO> GetJefferiesData(InputParameters parameters)
        {
            return _ukFundHistoryOperation.GetJefferiesData(parameters.Ticker
                , "Jefferies", Convert.ToDateTime(parameters.StartDate), Convert.ToDateTime(parameters.EndDate));
        }

        [Route("/UKFunds/GetJPMDataFull")]
        [HttpPost]
        public IList<JPMDetailTO> GetJPMDataFull(InputParameters parameters)
        {
            return _ukFundHistoryOperation.GetJPMDataFull(parameters.Ticker
                , parameters.Source, Convert.ToDateTime(parameters.StartDate), Convert.ToDateTime(parameters.EndDate));
        }

        [Route("/UKFunds/GetJPMData")]
        [HttpPost]
        public IList<JPMDetailTO> GetJPMData(InputParameters parameters)
        {
            return _ukFundHistoryOperation.GetJPMData(parameters.Ticker
                , "JPM", Convert.ToDateTime(parameters.StartDate), Convert.ToDateTime(parameters.EndDate));
        }

        [Route("/UKFunds/GetBloombergData")]
        [HttpPost]
        public IList<BBGFundDetailTO> GetBloombergData(InputParameters parameters)
        {
            return _ukFundHistoryOperation.GetBloombergData(parameters.Ticker
                , "BBG", Convert.ToDateTime(parameters.StartDate), Convert.ToDateTime(parameters.EndDate));
        }
    }
}
