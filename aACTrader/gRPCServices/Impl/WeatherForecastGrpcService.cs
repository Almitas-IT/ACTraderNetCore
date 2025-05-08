using aACTrader.gRPCServices.Interface;
using aACTrader.Protos;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;

namespace aACTrader.gRPCServices.Impl
{
    public class WeatherForecastGrpcService : WeatherForecastTest.WeatherForecastTestBase
    {
        private static readonly string[] Summaries = new[]
        {
            "Freezing", "Bracing", "Chilly", "Cool", "Mild", "Warm", "Balmy", "Hot", "Sweltering", "Scorching"
        };

        private readonly ILogger<WeatherForecastGrpcService> _logger;
        private readonly IWeatherForecastTestService _weatherForecastService;

        public WeatherForecastGrpcService(ILogger<WeatherForecastGrpcService> logger, IWeatherForecastTestService weatherForecastService)
        {
            _logger = logger;
            _weatherForecastService = weatherForecastService;
        }

        public override Task<WeatherForecastReply> GetWeatherForecast(Empty request, ServerCallContext context)
        {
            return _weatherForecastService.GetWeatherForecast(context);
        }

        public override Task<WeatherForecastReply> GetWeatherForecastForDate(Timestamp date, ServerCallContext context)
        {
            return _weatherForecastService.GetWeatherForecastForDate(date, context);
        }
    }
}