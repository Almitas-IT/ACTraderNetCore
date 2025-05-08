using aACTrader.Operations.Impl;
using aCommons;
using LazyCache;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace aACTrader.SignalR.Hubs
{
    public class DelayedPriceStreamingHub : Hub
    {
        private readonly ILogger<DelayedPriceStreamingHub> _logger;
        private readonly CachingService _cache;
        private readonly SecurityPriceOperation _securityPriceOperation;
        private readonly static ConnectionMapping<string> _connections = new ConnectionMapping<string>();

        public DelayedPriceStreamingHub(ILogger<DelayedPriceStreamingHub> logger, CachingService cache, SecurityPriceOperation securityPriceOperation)
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
            _logger.LogInformation("[DelayedPriceStreamingHub] User Connected: " + name + ", # of Users " + _connections.Count);
            return base.OnConnectedAsync();
        }

        public override Task OnDisconnectedAsync(Exception? exception)
        {
            string name = Context.User.Identity.Name;
            if (string.IsNullOrEmpty(name))
                name = Context.ConnectionId;
            _connections.Remove(name, Context.ConnectionId);
            _logger.LogInformation("[DelayedPriceStreamingHub] User Disconnected: " + name + ", # of Users " + _connections.Count);
            return base.OnDisconnectedAsync(exception);
        }

        public async IAsyncEnumerable<SecurityPrice> GetDelayedPrices(int count, int delay, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            ApplicationData appData = _cache.Get<ApplicationData>(CacheKeys.APPLICATION_DATA_FLAG);
            _logger.LogInformation("Streaming GetDelayedPrices..." + appData.DataUpdateFlag);
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await Task.Delay(delay);
                appData = _cache.Get<ApplicationData>(CacheKeys.APPLICATION_DATA_FLAG);
                //_logger.LogInformation("Streaming GetDelayedPrices..." + appData.DataUpdateFlag);
                if (appData.DataUpdateFlag.Equals("Y"))
                {
                    IDictionary<string, SecurityPrice> dict = _cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);
                    foreach (KeyValuePair<string, SecurityPrice> kvp in dict)
                    {
                        SecurityPrice securityPrice = kvp.Value;
                        if (securityPrice.RTFlag == 0 && securityPrice.MktCls == 0) //0 - Open, 1 - Closed
                            yield return securityPrice;
                    }
                }
            }
        }
    }
}
