using aACTrader.DAO.Repository;
using aCommons;
using LazyCache;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;

namespace aACTrader.Operations.Impl
{
    public class CompanyHoldingsReportGenerator
    {
        private readonly ILogger<CompanyHoldingsReportGenerator> _logger;
        private readonly HoldingsDao _holdingsDao;

        public CompanyHoldingsReportGenerator(ILogger<CompanyHoldingsReportGenerator> logger, HoldingsDao holdingsDao)
        {
            _logger = logger;
            _holdingsDao = holdingsDao;
        }

        public IList<CompanyHoldingsChangeReport> GenerateCompanyHoldingsChangeReport(CachingService cache, string companyName, DateTime? startDate, DateTime? endDate)
        {
            IList<CompanyHoldingsChangeReport> results = new List<CompanyHoldingsChangeReport>();
            IList<CompanyHoldings> holdings = _holdingsDao.GetActivistHoldings(companyName, startDate.GetValueOrDefault(), endDate.GetValueOrDefault());

            if (holdings != null && holdings.Count > 0)
            {
                IDictionary<string, CompanyHoldingsChangeReport> holdingsChangeReportDict = new Dictionary<string, CompanyHoldingsChangeReport>();

                DateTime? previousReportDate = null;
                int quarterIndex = 0;
                foreach (CompanyHoldings holding in holdings)
                {
                    string ticker = holding.DisplayTicker;
                    DateTime? reportDate = holding.ReportDate;

                    CompanyHoldingsChangeReport data = null;
                    if (!holdingsChangeReportDict.ContainsKey(ticker))
                    {

                        data = new CompanyHoldingsChangeReport
                        {
                            DisplayTicker = ticker,
                            Ticker = holding.Ticker,
                            Cusip = holding.Cusip,
                            IssuerName = holding.IssuerName,
                            SharesOutstanding = holding.SharesOutstanding
                        };

                        holdingsChangeReportDict.Add(ticker, data);
                    }
                    else
                    {
                        data = holdingsChangeReportDict[ticker];
                    }

                    if (previousReportDate == null)
                    {
                        quarterIndex++;
                        previousReportDate = reportDate;
                    }
                    else if (!reportDate.GetValueOrDefault().Date.Equals(previousReportDate.GetValueOrDefault().Date))
                    {
                        quarterIndex++;
                        previousReportDate = reportDate;
                    }

                    if (quarterIndex == 1)
                    {
                        if (string.IsNullOrEmpty(data.Quarter1))
                            data.Quarter1 = holding.ReportQuarter;
                        data.Quarter1Holdings = holding.SharesHeld;
                        if (data.Quarter1Holdings.HasValue && data.SharesOutstanding.HasValue)
                        {
                            data.Quarter1HoldingsPct = data.Quarter1Holdings / (double)data.SharesOutstanding;
                        }
                    }
                    else if (quarterIndex == 2)
                    {
                        if (string.IsNullOrEmpty(data.Quarter2))
                            data.Quarter2 = holding.ReportQuarter;
                        data.Quarter2Holdings = holding.SharesHeld;
                        if (data.Quarter2Holdings.HasValue && data.SharesOutstanding.HasValue)
                        {
                            data.Quarter2HoldingsPct = data.Quarter2Holdings / (double)data.SharesOutstanding;
                        }
                    }
                    else if (quarterIndex == 3)
                    {
                        if (string.IsNullOrEmpty(data.Quarter3))
                            data.Quarter3 = holding.ReportQuarter;
                        data.Quarter3Holdings = holding.SharesHeld;
                        if (data.Quarter3Holdings.HasValue && data.SharesOutstanding.HasValue)
                        {
                            data.Quarter3HoldingsPct = data.Quarter3Holdings / (double)data.SharesOutstanding;
                        }
                    }
                    else if (quarterIndex == 4)
                    {
                        if (string.IsNullOrEmpty(data.Quarter4))
                            data.Quarter4 = holding.ReportQuarter;
                        data.Quarter4Holdings = holding.SharesHeld;
                        if (data.Quarter4Holdings.HasValue && data.SharesOutstanding.HasValue)
                        {
                            data.Quarter4HoldingsPct = data.Quarter4Holdings / (double)data.SharesOutstanding;
                        }
                    }
                    else if (quarterIndex == 5)
                    {
                        if (string.IsNullOrEmpty(data.Quarter5))
                            data.Quarter5 = holding.ReportQuarter;
                        data.Quarter5Holdings = holding.SharesHeld;
                        if (data.Quarter5Holdings.HasValue && data.SharesOutstanding.HasValue)
                        {
                            data.Quarter5HoldingsPct = data.Quarter5Holdings / (double)data.SharesOutstanding;
                        }
                    }
                    else if (quarterIndex == 6)
                    {
                        if (string.IsNullOrEmpty(data.Quarter6))
                            data.Quarter6 = holding.ReportQuarter;
                        data.Quarter6Holdings = holding.SharesHeld;
                        if (data.Quarter6Holdings.HasValue && data.SharesOutstanding.HasValue)
                        {
                            data.Quarter6HoldingsPct = data.Quarter6Holdings / (double)data.SharesOutstanding;
                        }
                    }

                    if (data.Quarter1Holdings.HasValue && data.Quarter2Holdings.HasValue)
                    {
                        data.ChangeInSharesOwned = data.Quarter1Holdings - data.Quarter2Holdings;
                    }

                    if (data.Quarter1HoldingsPct.HasValue && data.Quarter2HoldingsPct.HasValue)
                    {
                        data.ChangeInOwnership = data.Quarter1HoldingsPct - data.Quarter2HoldingsPct;
                    }
                }

                results = holdingsChangeReportDict.Values.OrderBy(h => h.DisplayTicker).ToList<CompanyHoldingsChangeReport>();

                IDictionary<string, Holding> almHoldingsDict = cache.Get<IDictionary<string, Holding>>(CacheKeys.ALM_HOLDINGS);

                foreach (CompanyHoldingsChangeReport data in results)
                {
                    if (data.Quarter1Holdings.HasValue && !data.Quarter2Holdings.HasValue)
                    {
                        data.NewOrSold = "New Holding";
                    }
                    else if (!data.Quarter1Holdings.HasValue && data.Quarter2Holdings.HasValue)
                    {
                        data.NewOrSold = "Sold Off Holding";
                    }
                    else if (data.Quarter1Holdings.HasValue && data.Quarter2Holdings.HasValue)
                    {
                        data.NewOrSold = "Retained Holdings";
                    }

                    //populate ALM holdings
                    Holding almHolding;
                    if (almHoldingsDict.TryGetValue(data.Ticker, out almHolding))
                    {
                        data.ALMHoldings = almHolding.Position;
                        if (data.ALMHoldings.HasValue && data.SharesOutstanding.HasValue)
                        {
                            data.ALMHoldingsPct = almHolding.Position / (double)data.SharesOutstanding;
                        }
                    }

                    data.Display = 1;
                    if (!data.Quarter1Holdings.HasValue
                        && !data.Quarter2Holdings.HasValue
                        && !data.Quarter3Holdings.HasValue
                        && !data.Quarter4Holdings.HasValue
                        && !data.Quarter5Holdings.HasValue
                        && !data.Quarter6Holdings.HasValue)
                        data.Display = 0;

                }
            }

            return results;
        }
    }
}