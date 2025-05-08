using aACTrader.Operations.Impl;
using aCommons;
using aCommons.Cef;
using aCommons.Crypto;
using aCommons.Trading;
using LazyCache;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace aACTrader.SignalR.Hubs
{
    public class PriceStreamingHub : Hub
    {
        private readonly ILogger<PriceStreamingHub> _logger;
        private readonly CachingService _cache;
        private readonly SecurityPriceOperation _securityPriceOperation;
        private readonly static ConnectionMapping<string> _connections = new ConnectionMapping<string>();

        public PriceStreamingHub(ILogger<PriceStreamingHub> logger, CachingService cache, SecurityPriceOperation securityPriceOperation)
        {
            _logger = logger;
            _cache = cache;
            _securityPriceOperation = securityPriceOperation;
        }

        public override Task OnConnectedAsync()
        {
            string name = Context.User.Identity.Name;
            if (string.IsNullOrEmpty(name))
                name = Context.ConnectionId;
            _connections.Add(name, Context.ConnectionId);
            _logger.LogInformation("[PriceStreamingHub] User Connected: " + name + ", # of Users " + _connections.Count);
            return base.OnConnectedAsync();
        }

        public override Task OnDisconnectedAsync(Exception? exception)
        {
            string name = Context.User.Identity.Name;
            if (string.IsNullOrEmpty(name))
                name = Context.ConnectionId;
            _connections.Remove(name, Context.ConnectionId);
            _logger.LogInformation("[PriceStreamingHub] User Disconnected: " + name + ", # of Users " + _connections.Count);
            return base.OnDisconnectedAsync(exception);
        }

        public ChannelReader<SomeData> GetSomeDataWithChannelReader(int count, int delay, CancellationToken cancellationToken)
        {
            var channel = Channel.CreateUnbounded<SomeData>();
            _ = WriteItemsAsync(channel.Writer, count, delay, cancellationToken);
            return channel.Reader;
        }

        public async IAsyncEnumerable<SomeData> GetSomeDataWithAsyncStreams(int count, int delay, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            for (int i = 0; i < count; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await Task.Delay(delay);
                yield return new SomeData { Value = i };
            }
        }

        public async IAsyncEnumerable<SecurityPrice> GetLivePrices(int count, int delay, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            ApplicationData appData = _cache.Get<ApplicationData>(CacheKeys.APPLICATION_DATA_FLAG);
            _logger.LogInformation("Streaming GetLivePrices..." + appData.DataUpdateFlag);
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await Task.Delay(delay);
                appData = _cache.Get<ApplicationData>(CacheKeys.APPLICATION_DATA_FLAG);
                //_logger.LogInformation("Streaming GetLivePrices..." + appData.DataUpdateFlag);
                if (appData.DataUpdateFlag.Equals("Y"))
                {
                    IDictionary<string, SecurityPrice> dict = _cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);
                    foreach (KeyValuePair<string, SecurityPrice> kvp in dict)
                    {
                        SecurityPrice securityPrice = kvp.Value;
                        if (securityPrice.RTFlag == 1 && securityPrice.MktCls == 0) //0 - Open, 1 - Closed
                            yield return securityPrice;
                    }
                }
            }
        }

        public async IAsyncEnumerable<FundForecast> GetFundForecasts(int count, int delay, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            ApplicationData appData = _cache.Get<ApplicationData>(CacheKeys.APPLICATION_DATA_FLAG);
            _logger.LogInformation("Streaming GetFundForecasts..." + appData.DataUpdateFlag);
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await Task.Delay(delay);
                appData = _cache.Get<ApplicationData>(CacheKeys.APPLICATION_DATA_FLAG);
                //_logger.LogInformation("Streaming GetFundForecasts..." + appData.DataUpdateFlag);
                if (appData.DataUpdateFlag.Equals("Y"))
                {
                    IDictionary<string, FundForecast> dict = _cache.Get<IDictionary<string, FundForecast>>(CacheKeys.FUND_FORECASTS);
                    foreach (KeyValuePair<string, FundForecast> kvp in dict)
                        yield return kvp.Value;
                }
            }
        }

        public async IAsyncEnumerable<SectorForecast> GetSectorForecasts(int count, int delay, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            ApplicationData appData = _cache.Get<ApplicationData>(CacheKeys.APPLICATION_DATA_FLAG);
            _logger.LogInformation("Streaming GetSectorForecasts..." + appData.DataUpdateFlag);
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await Task.Delay(delay);
                appData = _cache.Get<ApplicationData>(CacheKeys.APPLICATION_DATA_FLAG);
                //_logger.LogInformation("Streaming GetSectorForecasts..." + appData.DataUpdateFlag);
                if (appData.DataUpdateFlag.Equals("Y"))
                {
                    IDictionary<string, SectorForecast> dict = _cache.Get<IDictionary<string, SectorForecast>>(CacheKeys.SECTOR_FORECASTS);
                    foreach (KeyValuePair<string, SectorForecast> kvp in dict)
                        yield return kvp.Value;
                }
            }
        }

        public async IAsyncEnumerable<TradePosition> GetTradePositions(int count, int delay, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            ApplicationData appData = _cache.Get<ApplicationData>(CacheKeys.APPLICATION_DATA_FLAG);
            _logger.LogInformation("Streaming GetTradePositions..." + appData.DataUpdateFlag);
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await Task.Delay(delay);
                appData = _cache.Get<ApplicationData>(CacheKeys.APPLICATION_DATA_FLAG);
                //_logger.LogInformation("Streaming GetTradePositions..." + appData.DataUpdateFlag);
                if (appData.DataUpdateFlag.Equals("Y"))
                {
                    IDictionary<string, TradePosition> dict = _cache.Get<IDictionary<string, TradePosition>>(CacheKeys.TRADE_EXECUTIONS);
                    foreach (KeyValuePair<string, TradePosition> kvp in dict)
                        yield return kvp.Value;
                }
            }
        }

        public async IAsyncEnumerable<FXRateTO> GetFxRates(int count, int delay, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            ApplicationData appData = _cache.Get<ApplicationData>(CacheKeys.APPLICATION_DATA_FLAG);
            _logger.LogInformation("Streaming GetFxRates..." + appData.DataUpdateFlag);
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await Task.Delay(delay);
                appData = _cache.Get<ApplicationData>(CacheKeys.APPLICATION_DATA_FLAG);
                //_logger.LogInformation("Streaming GetFxRates..." + appData.DataUpdateFlag);
                if (appData.DataUpdateFlag.Equals("Y"))
                {
                    IList<FXRateTO> list = _securityPriceOperation.GetLiveFXRates();
                    foreach (FXRateTO data in list)
                        yield return data;
                }
            }
        }

        public async IAsyncEnumerable<FundSupplementalData> GetFundSupplementalData(int count, int delay, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            ApplicationData appData = _cache.Get<ApplicationData>(CacheKeys.APPLICATION_DATA_FLAG);
            _logger.LogInformation("Streaming GetFundSupplementalData..." + appData.DataUpdateFlag);
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await Task.Delay(delay);
                appData = _cache.Get<ApplicationData>(CacheKeys.APPLICATION_DATA_FLAG);
                //_logger.LogInformation("Streaming GetFundSupplementalData..." + appData.DataUpdateFlag);
                if (appData.DataUpdateFlag.Equals("Y"))
                {
                    IDictionary<string, FundSupplementalData> dict = _cache.Get<IDictionary<string, FundSupplementalData>>(CacheKeys.FUND_SUPPLEMENTAL_DETAILS);
                    if (dict != null && dict.Count > 0)
                        foreach (KeyValuePair<string, FundSupplementalData> kvp in dict)
                            yield return kvp.Value;
                }
            }
        }

        public async IAsyncEnumerable<CryptoSecMst> GetCryptoNavs(int count, int delay, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            ApplicationData appData = _cache.Get<ApplicationData>(CacheKeys.APPLICATION_DATA_FLAG);
            _logger.LogInformation("Streaming GetCryptoNavs..." + appData.DataUpdateFlag);
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await Task.Delay(delay);
                appData = _cache.Get<ApplicationData>(CacheKeys.APPLICATION_DATA_FLAG);
                //_logger.LogInformation("Streaming GetCryptoNavs..." + appData.DataUpdateFlag);
                if (appData.DataUpdateFlag.Equals("Y"))
                {
                    IDictionary<string, CryptoSecMst> dict = _cache.Get<IDictionary<string, CryptoSecMst>>(CacheKeys.CRYPTO_SECURITY_MST);
                    foreach (KeyValuePair<string, CryptoSecMst> kvp in dict)
                        yield return kvp.Value;
                }
            }
        }

        public async IAsyncEnumerable<PositionMaster> GetPositions(int count, int delay, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            ApplicationData appData = _cache.Get<ApplicationData>(CacheKeys.APPLICATION_DATA_FLAG);
            _logger.LogInformation("Streaming GetPositions..." + appData.DataUpdateFlag);
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await Task.Delay(delay);
                appData = _cache.Get<ApplicationData>(CacheKeys.APPLICATION_DATA_FLAG);
                //_logger.LogInformation("Streaming GetPositions..." + appData.DataUpdateFlag);
                if (appData.DataUpdateFlag.Equals("Y"))
                {
                    IDictionary<string, PositionMaster> dict = _cache.Get<IDictionary<string, PositionMaster>>(CacheKeys.POSITION_MASTER);
                    foreach (KeyValuePair<string, PositionMaster> kvp in dict)
                        yield return kvp.Value;
                }
            }
        }

        private async Task WriteItemsAsync(
          ChannelWriter<SomeData> writer,
          int count,
          int delay,
          CancellationToken cancellationToken)
        {
            try
            {
                for (var i = 0; i < count; i++)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    await writer.WriteAsync(new SomeData() { Value = i });
                    await Task.Delay(delay, cancellationToken);
                }
            }
            catch (Exception ex)
            {
                writer.TryComplete(ex);
            }
            writer.TryComplete();
        }
    }
}
