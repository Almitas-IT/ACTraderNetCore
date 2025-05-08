using aACTrader.Protos;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using System.Threading.Tasks;

namespace aACTrader.gRPCServices.Interface
{
    public interface IWeatherForecastTestService
    {
        Task<WeatherForecastReply> GetWeatherForecast(ServerCallContext context);
        Task<WeatherForecastReply> GetWeatherForecastForDate(Timestamp date, ServerCallContext context);
    }
}
