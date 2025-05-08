using aACTrader.Protos;
using Grpc.Core;
using System.Threading.Tasks;

namespace aACTrader.gRPCServices.Interface
{
    public interface ISecurityPriceTestService
    {
        Task<SecurityPriceReply> GetLivePrices(ServerCallContext context);
    }
}
