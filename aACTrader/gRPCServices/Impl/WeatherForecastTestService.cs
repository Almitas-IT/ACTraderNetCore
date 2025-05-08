using aACTrader.gRPCServices.Interface;
using aACTrader.Protos;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;

namespace aACTrader.gRPCServices.Impl
{
    public class WeatherForecastTestService : IWeatherForecastTestService
    {
        private static readonly string[] Summaries = new[]
        {
            "Freezing", "Bracing", "Chilly", "Cool", "Mild", "Warm", "Balmy", "Hot", "Sweltering", "Scorching"
        };
        private readonly ILogger<WeatherForecastTestService> _logger;
        public WeatherForecastTestService(ILogger<WeatherForecastTestService> logger)
        {
            _logger = logger;
        }
        public Task<WeatherForecastReply> GetWeatherForecast(ServerCallContext context)
        {
            return Task.FromResult<WeatherForecastReply>(GetWeather());
        }
        public Task<WeatherForecastReply> GetWeatherForecastForDate(Timestamp date, ServerCallContext context)
        {
            return Task.FromResult<WeatherForecastReply>(GetWeather(date));
        }

        private WeatherForecastReply GetWeather()
        {
            var result = new WeatherForecastReply();
            for (var index = 1; index <= 5; index++)
            {
                result.Result.Add(
                    new WeatherForecastTO
                    {
                        Date = Timestamp.FromDateTime(DateTime.UtcNow.AddDays(index)),
                        TemperatureC = Random.Shared.Next(-20, 55),
                        Summary = Summaries[Random.Shared.Next(Summaries.Length)],
                        TemperatureF = (int)(32 + (Random.Shared.Next(-20, 55) / 0.5556))
                    }
                    );
            }
            return result;
        }

        private WeatherForecastReply GetWeather(Timestamp date)
        {
            var result = new WeatherForecastReply();
            result.Result.Add(
                new WeatherForecastTO
                {
                    Date = date,
                    TemperatureC = Random.Shared.Next(-20, 55),
                    Summary = Summaries[Random.Shared.Next(Summaries.Length)],
                    TemperatureF = (int)(32 + (Random.Shared.Next(-20, 55) / 0.5556))
                }
                );
            return result;
        }
    }
}