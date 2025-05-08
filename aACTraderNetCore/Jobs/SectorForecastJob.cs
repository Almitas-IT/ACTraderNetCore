using aACTrader.Operations.Impl;
using Microsoft.Extensions.Logging;
using Quartz;
using System;
using System.Threading.Tasks;

namespace aACTrader.Jobs
{
    [DisallowConcurrentExecution]
    public class SectorForecastJob : IJob
    {
        private readonly ILogger<SectorForecastJob> _logger;
        private readonly PortHoldingsOperations _portHoldingsOperations;
        private readonly SectorForecastOperations _sectorForecastOperations;
        private readonly TradeOrderOperations _tradeOrderOperations;

        public SectorForecastJob(ILogger<SectorForecastJob> logger
            , PortHoldingsOperations portHoldingsOperations
            , SectorForecastOperations sectorForecastOperations
            , TradeOrderOperations tradeOrderOperations)
        {
            _logger = logger;
            _portHoldingsOperations = portHoldingsOperations;
            _sectorForecastOperations = sectorForecastOperations;
            _tradeOrderOperations = tradeOrderOperations;
        }

        public Task Execute(IJobExecutionContext context)
        {
            try
            {
                _logger.LogInformation("Sector Forecasts/Port Holding Operations - STARTED");
                //Update Port Holding Navs
                ////////////////////////////////////////////////////////////////
                _portHoldingsOperations.CalculateFundHoldingNavs();
                _portHoldingsOperations.CalculateLiveMarketValues();
                //Update Sector Forecasts
                _sectorForecastOperations.Calculate();
                _sectorForecastOperations.CalculateFundSectorZDScores();
                //Check for new trades and send email
                ////////////_tradeOrderOperations.ProcessTradeList();
                _logger.LogInformation("Sector Forecasts/Port Holding Operations - DONE");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error running Sector Forecasts");
            }
            return Task.CompletedTask;
        }
    }
}