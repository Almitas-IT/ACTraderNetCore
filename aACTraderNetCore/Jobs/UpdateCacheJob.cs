using aACTrader.DAO.Repository;
using aACTrader.Operations.Impl;
using aACTrader.Operations.Reports;
using Microsoft.Extensions.Logging;
using Quartz;
using System;
using System.Threading.Tasks;

namespace aACTrader.Jobs
{
    [DisallowConcurrentExecution]
    public class UpdateCacheJob : IJob
    {
        private readonly ILogger<UpdateCacheJob> _logger;
        private readonly CommonOperations _commonOperations;
        private TradeSummaryReport _tradeSummaryReport { get; set; }
        private readonly AdminDao _adminDao;

        public UpdateCacheJob(ILogger<UpdateCacheJob> logger, CommonOperations commonOperations, TradeSummaryReport tradeSummaryReport, AdminDao adminDao)
        {
            _logger = logger;
            _commonOperations = commonOperations;
            _tradeSummaryReport = tradeSummaryReport;
            _adminDao = adminDao;
        }

        public Task Execute(IJobExecutionContext context)
        {
            try
            {
                _logger.LogInformation("Updating Cache - STARTED");
                _logger.LogInformation("Updating FX Rates - STARTED");
                _commonOperations.UpdateFXRates();
                //_commonOperations.UpdateFundNavs();
                _logger.LogInformation("Updating FX Rates - DONE");
                //_logger.LogInformation("Saving Trading Summary - STARTED");
                //_tradeSummaryReport.SaveTradeSummary();
                //_logger.LogInformation("Saving Trading Summary - DONE");
                _adminDao.ExecuteStoredProcedure("call almitasc_ACTradingBBGData.spMorningDataChecks");
                _logger.LogInformation("Updating Cache - DONE");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error updating Cache");
            }
            return Task.CompletedTask;
        }
    }
}