using aACTrader.Model;
using aACTrader.Operations.Impl.NavEstimation;
using aCommons;
using aCommons.Cef;
using aCommons.Crypto;
using LazyCache;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;

namespace aACTrader.Controllers
{
    [ApiController]
    public class FundForecastController : ControllerBase
    {
        private readonly ILogger<FundForecastController> _logger;
        private readonly CachingService _cache;
        private readonly ConditionalProxyEvaluator _conditionalProxyEvaluator;

        public FundForecastController(ILogger<FundForecastController> logger, CachingService cache, ConditionalProxyEvaluator conditionalProxyEvaluator)
        {
            _logger = logger;
            _cache = cache;
            _conditionalProxyEvaluator = conditionalProxyEvaluator;
            //_logger.LogInformation("Initializing FundForecastController...");
        }

        [Route("/FundForecasts/GetFundForecasts")]
        [HttpGet]
        public IList<FundForecast> GetFundForecasts()
        {
            //_logger.LogInformation("GetFundForecasts - STARTED");
            IDictionary<string, FundForecast> dict = _cache.Get<IDictionary<string, FundForecast>>(CacheKeys.FUND_FORECASTS);
            IList<FundForecast> list = new List<FundForecast>(dict.Values);
            return list;
        }

        [Route("/FundForecasts/GetFundMaster")]
        [HttpGet]
        public IList<FundMaster> GetFundMaster()
        {
            //_logger.LogInformation("GetFundMaster - STARTED");
            IDictionary<string, FundMaster> dict = _cache.Get<IDictionary<string, FundMaster>>(CacheKeys.FUND_MASTER);
            IList<FundMaster> list = new List<FundMaster>(dict.Values);
            return list;
        }

        [Route("/FundForecasts/GetCryptoNavs")]
        [HttpGet]
        public IList<CryptoSecMst> GetCryptoNavs()
        {
            //_logger.LogInformation("GetCryptoNavs - STARTED");
            IDictionary<string, CryptoSecMst> dict = _cache.Get<IDictionary<string, CryptoSecMst>>(CacheKeys.CRYPTO_SECURITY_MST);
            IList<CryptoSecMst> list = new List<CryptoSecMst>(dict.Values);
            return list;
        }

        [Route("/FundForecasts/GetConditionalProxyReturns")]
        [HttpPost]
        public IList<FundProxy> GetConditionalProxyReturns(InputParameters parameters)
        {
            return _conditionalProxyEvaluator.GetProxyFormula(parameters.Ticker);
        }

        [Route("/FundForecasts/GetLiveFundStats")]
        [HttpGet]
        public IList<FundPDStats> GetLiveFundStats()
        {
            //_logger.LogInformation("GetFundForecasts - STARTED");
            IDictionary<string, FundPDStats> dict = _cache.Get<IDictionary<string, FundPDStats>>(CacheKeys.LIVE_FUND_PD_STATS);
            IList<FundPDStats> list = new List<FundPDStats>(dict.Values);
            return list;
        }
    }
}