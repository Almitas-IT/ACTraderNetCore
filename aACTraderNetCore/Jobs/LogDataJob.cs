using aACTrader.DAO.Repository;
using aACTrader.Operations.Impl;
using aACTrader.Operations.Reports;
using aACTrader.Services.Admin;
using aCommons;
using aCommons.Admin;
using LazyCache;
using Microsoft.Extensions.Logging;
using Quartz;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace aACTrader.Jobs
{
    [DisallowConcurrentExecution]
    public class LogDataJob : IJob
    {
        private readonly ILogger<LogDataJob> _logger;
        private readonly CachingService _cache;
        private readonly AdminDao _adminDao;
        private readonly LogDataService _logDataService;
        private readonly AllocationOperations _allocationOperations;
        private readonly TradeSummaryReportNew _tradeSummaryReportNew;

        public LogDataJob(ILogger<LogDataJob> logger
            , CachingService cache
            , AdminDao adminDao
            , LogDataService logDataService
            , AllocationOperations allocationOperations
            , TradeSummaryReportNew tradeSummaryReportNew)
        {
            _logger = logger;
            _cache = cache;
            _adminDao = adminDao;
            _logDataService = logDataService;
            _allocationOperations = allocationOperations;
            _tradeSummaryReportNew = tradeSummaryReportNew;
        }

        public Task Execute(IJobExecutionContext context)
        {
            try
            {
                //_logger.LogInformation("LogDataJob - STARTED");
                IList<LogData> logDataList = _cache.Get<IList<LogData>>(CacheKeys.LOG_DATA).Where(l => l.Notified == false).ToList<LogData>();
                _logDataService.ProcessLogs(logDataList, _adminDao);
                _allocationOperations.PopulateLivePositions();
                _tradeSummaryReportNew.PopulateTradeSummary();
                //_logger.LogInformation("LogDataJob - DONE");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error running LogDataJob");
            }
            return Task.CompletedTask;
        }
    }
}