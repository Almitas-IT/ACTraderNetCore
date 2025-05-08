using aCommons;
using aCommons.Cef;
using LazyCache;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;

namespace aACTrader.Operations.Impl
{
    public class FundTenderOperations
    {
        private readonly ILogger<FundTenderOperations> _logger;
        private readonly CachingService _cache;

        public FundTenderOperations(ILogger<FundTenderOperations> logger, CachingService cache)
        {
            _logger = logger;
            _cache = cache;
        }

        public void Process()
        {
            IDictionary<string, FundMaster> fundMasterDict = _cache.Get<IDictionary<string, FundMaster>>(CacheKeys.FUND_MASTER);
            IDictionary<string, FundForecast> fundForecastDict = _cache.Get<IDictionary<string, FundForecast>>(CacheKeys.FUND_FORECASTS);

            foreach (KeyValuePair<string, FundMaster> kvp in fundMasterDict)
            {
                FundMaster fundMaster = kvp.Value;
                if ("Perpetual".Equals(fundMaster.TenderOfferTyp, StringComparison.CurrentCultureIgnoreCase))
                {
                }
            }
        }

        public void ProcessPerpetualTenderOffer(string ticker, FundForecast fundForecat, SecurityPrice securityPrice)
        {
            DateTime todaysDate = DateTime.Now;
            double nav = fundForecat.EstNav.GetValueOrDefault();
            double price = securityPrice.LastPrc.GetValueOrDefault();
            double discount = fundForecat.PDLastPrc.GetValueOrDefault();
            double sharesOutstanding = fundForecat.ShOut.GetValueOrDefault();

            IList<FundTenderCashflows> cashflows = new List<FundTenderCashflows>();

            //initial cash flow
            FundTenderCashflows cashflow = new FundTenderCashflows
            {
                CashflowDate = todaysDate,
                Nav = nav,
                SharesOutstanding = sharesOutstanding,
                NetAssets = sharesOutstanding * nav,
                Cashflow = sharesOutstanding * price,
                Price = nav * (1 + discount),
                Shares = 1000
            };
            cashflow.TotalCashflow = cashflow.Shares * cashflow.Price;
            cashflows.Add(cashflow);

            //for (int i = 1; i < 20; i++)
            //{
            //	DateTime cashflowDate = Data
            //}
        }
    }
}