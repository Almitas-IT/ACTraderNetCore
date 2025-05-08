using aACTrader.DAO.Repository;
using aACTrader.Operations.Impl;
using Microsoft.Extensions.Logging;
using Quartz;
using System;
using System.Threading.Tasks;

namespace aACTrader.Jobs
{
    [DisallowConcurrentExecution]
    public class SecurityAlertJob : IJob
    {
        private readonly ILogger<SecurityAlertJob> _logger;
        private readonly SecurityAlertsEngine _securityAlertsEngine;
        private readonly AdminDao _adminDao;

        public SecurityAlertJob(ILogger<SecurityAlertJob> logger, SecurityAlertsEngine securityAlertsEngine, AdminDao adminDao)
        {
            _logger = logger;
            _securityAlertsEngine = securityAlertsEngine;
            _adminDao = adminDao;
        }

        public Task Execute(IJobExecutionContext context)
        {
            try
            {
                _logger.LogInformation("SecurityAlertJob - STARTED");
                _securityAlertsEngine.ProcessSecurityAlerts();
                _adminDao.ExecuteStoredProcedure("call Primebrokerfiles.spPopulatePrimeBrokerData");
                _logger.LogInformation("SecurityAlertJob - DONE");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error running SecurityAlertJob");
            }
            return Task.CompletedTask;
        }
    }
}