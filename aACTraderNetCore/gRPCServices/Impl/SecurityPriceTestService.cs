using aACTrader.gRPCServices.Interface;
using aACTrader.Operations.Impl;
using aACTrader.Protos;
using aCommons;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using LazyCache;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace aACTrader.gRPCServices.Impl
{
    public class SecurityPriceTestService : ISecurityPriceTestService
    {
        private readonly ILogger<SecurityPriceTestService> _logger;
        private readonly CachingService _cache;
        private readonly SecurityPriceOperation _securityPriceOperation;

        public SecurityPriceTestService(ILogger<SecurityPriceTestService> logger
            , CachingService cache
            , SecurityPriceOperation securityPriceOperation)
        {
            _logger = logger;
            _cache = cache;
            _securityPriceOperation = securityPriceOperation;
        }

        public Task<SecurityPriceReply> GetLivePrices(ServerCallContext context)
        {
            return Task.FromResult<SecurityPriceReply>(GetPrices());
        }

        private SecurityPriceReply GetPrices()
        {
            _logger.LogInformation("GetPrices - STARTED");
            var result = new SecurityPriceReply();
            IDictionary<string, SecurityPrice> dict = _cache.Get<IDictionary<string, SecurityPrice>>(CacheKeys.SECURITY_PRICES);
            foreach (SecurityPrice data in dict.Values)
            {
                if (data.RTFlag == 1)
                {
                    SecurityPriceTO sp = new SecurityPriceTO
                    {
                        Ticker = data.Ticker,
                        Src = data.Src,
                        ClsPrc = data.ClsPrc,
                        LastPrc = data.LastPrc,
                        BidPrc = data.BidPrc,
                        AskPrc = data.AskPrc,
                        MidPrc = data.MidPrc,
                        BidSz = data.BidSz,
                        AskSz = data.AskSz,
                        Vol = data.Vol,
                        PrcRtn = data.PrcRtn,
                        PrcChng = data.PrcChng,
                        DvdAmt = data.DvdAmt,
                        //TrdDt = Timestamp.FromDateTimeOffset(data.TrdDt.GetValueOrDefault()),
                        TrdTm = data.TrdTm,
                        VolAvg20PctW = data.VolAvg20PctW
                    };
                    if (data.TrdDt.HasValue)
                        sp.TrdDt = Timestamp.FromDateTimeOffset(data.TrdDt.GetValueOrDefault());
                    result.Result.Add(sp);
                }
            }
            _logger.LogInformation("GetPrices - END");
            return result;
        }
    }
}