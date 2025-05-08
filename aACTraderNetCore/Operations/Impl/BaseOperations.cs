using aACTrader.Compliance;
using aACTrader.Operations.Impl.NavEstimation;
using aACTrader.Services;
using Microsoft.Extensions.Logging;

namespace aACTrader.Operations.Impl
{
    public class BaseOperations
    {
        private readonly ILogger<BaseOperations> _logger;
        private readonly CommonOperations _commonOperations;
        private readonly SectorForecastOperations _sectorForecastOperations;
        private readonly PricingService _pricingService;
        private readonly EMSXService _emsxervice;
        private readonly FundForecastEngine _fundForecastEngine;
        private readonly PortHoldingsOperations _portHoldingsOperations;
        private readonly PfdCommonOperations _pfdCommonOperations;
        private readonly RulesManager _rulesManager;
        private readonly SecurityFilingThresholdOperations _securityFilingThresholdOperations;
        private readonly ConditionalProxyProcessor _conditionalProxyProcessor;

        public BaseOperations(ILogger<BaseOperations> logger
            , CommonOperations commonOperations
            , SectorForecastOperations sectorForecastOperations
            , PricingService pricingService
            , EMSXService emsxervice
            , FundForecastEngine fundForecastEngine
            , PortHoldingsOperations portHoldingsOperations
            , PfdCommonOperations pfdCommonOperations
            , RulesManager rulesManager
            , SecurityFilingThresholdOperations securityFilingThresholdOperations
            , ConditionalProxyProcessor conditionalProxyProcessor)
        {
            _logger = logger;
            _commonOperations = commonOperations;
            _sectorForecastOperations = sectorForecastOperations;
            _pricingService = pricingService;
            _emsxervice = emsxervice;
            _fundForecastEngine = fundForecastEngine;
            _portHoldingsOperations = portHoldingsOperations;
            _pfdCommonOperations = pfdCommonOperations;
            _rulesManager = rulesManager;
            _securityFilingThresholdOperations = securityFilingThresholdOperations;
            _conditionalProxyProcessor = conditionalProxyProcessor;
            _logger.LogInformation("Initializing BaseOperations...");
            Start();
        }

        public void Start()
        {
            _commonOperations.Start();
            _pricingService.Start();
            _emsxervice.Start();
            _fundForecastEngine.Start();
            _sectorForecastOperations.Start();
            _portHoldingsOperations.Start();
            _conditionalProxyProcessor.Initialize();
            _fundForecastEngine.Calculate();
            _pfdCommonOperations.Start();
            _rulesManager.Initialize();
            _rulesManager.RunDefaultRules();
            _securityFilingThresholdOperations.CalculateSecurityOwnershipDetails();
        }
    }
}