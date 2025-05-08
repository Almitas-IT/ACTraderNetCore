using aACTrader.gRPCServices.Interface;
using aACTrader.Protos;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;

namespace aACTrader.gRPCServices.Impl
{
    public class SecurityPriceGrpcService : SecurityPriceTest.SecurityPriceTestBase
    {
        private readonly ILogger<SecurityPriceGrpcService> _logger;
        private readonly ISecurityPriceTestService _securityPriceService;

        public SecurityPriceGrpcService(ILogger<SecurityPriceGrpcService> logger, ISecurityPriceTestService securityPriceService)
        {
            _logger = logger;
            _securityPriceService = securityPriceService;
        }

        public override Task<SecurityPriceReply> GetLivePrices(Empty request, ServerCallContext context)
        {
            return _securityPriceService.GetLivePrices(context);
        }
    }
}
