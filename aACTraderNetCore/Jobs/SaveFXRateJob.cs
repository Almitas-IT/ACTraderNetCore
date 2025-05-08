using aACTrader.Operations.Impl;
using aCommons;
using LazyCache;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Quartz;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace aACTrader.Jobs
{
    [DisallowConcurrentExecution]
    public class SaveFXRateJob : IJob
    {
        private readonly ILogger<SaveFXRateJob> _logger;
        private readonly CachingService _cache;
        private readonly SecurityPriceOperation _securityPriceOperation;
        private readonly IConfiguration _configuration;
        private readonly string _tableName = "almitasc_ACTradingBBGLink.StgFXRateLive";

        public SaveFXRateJob(ILogger<SaveFXRateJob> logger
            , CachingService cache, SecurityPriceOperation securityPriceOperation, IConfiguration configuration)
        {
            _logger = logger;
            _cache = cache;
            _securityPriceOperation = securityPriceOperation;
            _configuration = configuration;
        }

        public Task Execute(IJobExecutionContext context)
        {
            try
            {
                if (_configuration["ConnectionStrings:ENV"].Equals("PROD"))
                {
                    _logger.LogInformation("Saving Live FX Rates - STARTED");
                    IDictionary<string, FXRate> fxRateDict = _cache.Get<IDictionary<string, FXRate>>(CacheKeys.FX_RATES_LIVE);
                    _securityPriceOperation.TruncateTable(_tableName);
                    _securityPriceOperation.SaveFXRatesToStg(fxRateDict);
                    _securityPriceOperation.MoveFXRatesToTgtTable();
                    _securityPriceOperation.CalculateFXReturns();
                    _logger.LogInformation("Saved Live FX Rates - DONE");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving FX Rates");
            }
            return Task.CompletedTask;
        }
    }
}