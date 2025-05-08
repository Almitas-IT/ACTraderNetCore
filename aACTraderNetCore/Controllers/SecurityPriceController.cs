using aACTrader.DAO.Repository;
using aACTrader.Model;
using aACTrader.Operations.Impl;
using aCommons;
using aCommons.Crypto;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;

namespace aACTrader.Controllers
{
    [ApiController]
    public class SecurityPriceController : ControllerBase
    {
        private readonly ILogger<SecurityPriceController> _logger;
        private readonly SecurityPriceOperation _securityPriceOperation;
        private readonly SecurityPriceDao _securityPriceDao;

        public SecurityPriceController(ILogger<SecurityPriceController> logger
            , SecurityPriceOperation securityPriceOperation
            , SecurityPriceDao securityPriceDao)
        {
            _logger = logger;
            _securityPriceOperation = securityPriceOperation;
            _securityPriceDao = securityPriceDao;
            //_logger.LogInformation("Initializing SecurityPriceController...");
        }

        [Route("/SecurityPrices/GetLivePrices")]
        [HttpGet]
        public IList<SecurityPrice> GetLivePrices()
        {
            //_logger.LogInformation("In /SecurityPrices/GetLivePrices");
            return _securityPriceOperation.GetLivePrices();
        }

        //[Route("/SecurityPrices/GetLivePrices")]
        //[HttpGet]
        //public async Task<IList<SecurityPrice>> GetLivePrices()
        //{
        //    return await Task.Run(() => _securityPriceOperation.GetLivePrices());
        //}

        [Route("/SecurityPrices/GetLivePricesByExchange")]
        [HttpGet]
        public IList<SecurityPrice> GetLivePricesByExchange()
        {
            return _securityPriceOperation.GetLivePricesByExchange();
        }

        [Route("/SecurityPrices/GetDelayedPrices")]
        [HttpGet]
        public IList<SecurityPrice> GetDelayedPrices()
        {
            return _securityPriceOperation.GetDelayedPrices();
        }

        //[Route("/SecurityPrices/GetDelayedPrices")]
        //[HttpGet]
        //public async Task<IList<SecurityPrice>> GetDelayedPrices()
        //{
        //    return await Task.Run(() => _securityPriceOperation.GetDelayedPrices());
        //}

        [Route("/SecurityPrices/GetDelayedPricesByExchange")]
        [HttpGet]
        public IList<SecurityPrice> GetDelayedPricesByExchange()
        {
            return _securityPriceOperation.GetDelayedPricesByExchange();
        }

        [Route("/SecurityPrices/GetAllPrices")]
        [HttpGet]
        public IList<SecurityPrice> GetAllPrices()
        {
            return _securityPriceOperation.GetAllPrices();
        }

        [Route("/SecurityPrices/GetPriceTickerMap")]
        [HttpGet]
        public IList<SecurityPriceMap> GetPriceTickerMap()
        {
            return _securityPriceOperation.GetPriceTickerMap();
        }

        [Route("/SecurityPrices/GetNVPriceTickerMap")]
        [HttpGet]
        public IList<SecurityPriceMap> GetNVPriceTickerMap()
        {
            return _securityPriceOperation.GetNVPriceTickerMap();
        }

        [Route("/SecurityPrices/GetLiveFXRates")]
        [HttpGet]
        public IList<FXRateTO> GetLiveFXRates()
        {
            return _securityPriceOperation.GetLiveFXRates();
        }

        [Route("/SecurityPrices/GetPriceAlerts")]
        [HttpGet]
        public IList<SecurityPrice> GetPriceAlerts()
        {
            return _securityPriceOperation.GetPriceAlerts();
        }

        [Route("/SecurityPrices/GetPriceAlertsWithFilter")]
        [HttpPost]
        public IList<SecurityPrice> GetPriceAlertsWithFilter(PriceFilterParameters parameters)
        {
            return _securityPriceOperation.GetPriceAlertsWithFilter(parameters);
        }

        [Route("/SecurityPrices/GetCryptoNavs")]
        [HttpGet]
        public IList<CryptoSecMst> GetCryptoNavs()
        {
            return _securityPriceOperation.GetCryptoNavs();
        }

        [Route("/SecurityPrices/SaveMonthEndPrices")]
        [HttpPost]
        public void SaveMonthEndPrices(IList<MonthEndSecurityPrice> list)
        {
            _securityPriceDao.SaveMonthEndPrices(list);
        }
    }
}
