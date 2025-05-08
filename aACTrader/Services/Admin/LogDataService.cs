using aACTrader.DAO.Repository;
using aACTrader.Operations.Impl;
using aCommons;
using aCommons.Admin;
using LazyCache;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;

namespace aACTrader.Services.Admin
{
    public class LogDataService
    {
        private readonly ILogger<LogDataService> _logger;
        private readonly CachingService _cache;
        private readonly EmailOperations _emailOperations;
        public bool _sendLogs { get; set; }

        public LogDataService(ILogger<LogDataService> logger, CachingService cache, EmailOperations emailOperations)
        {
            _logger = logger;
            _cache = cache;
            _emailOperations = emailOperations;
            _sendLogs = true;
            Initialize();
            _logger.LogInformation("Initializing LogDataService...");
        }

        public void Initialize()
        {
            _cache.Remove(CacheKeys.LOG_DATA);
            _cache.Add(CacheKeys.LOG_DATA, new List<LogData>(), DateTimeOffset.MaxValue);
        }

        public void SaveLog(string serviceName, string functionName, string ticker, string errorMessage, string logLevel)
        {
            IList<LogData> logDataList = _cache.Get<IList<LogData>>(CacheKeys.LOG_DATA);
            LogData data = new LogData();
            data.ServiceName = serviceName;
            data.FunctionName = functionName;
            data.Ticker = ticker;
            data.Message = errorMessage;
            data.LogLevel = logLevel;
            data.EntryTime = DateTime.Now;
            data.Notified = false;
            logDataList.Add(data);
        }

        public void ProcessLogs(IList<LogData> logDataList, AdminDao adminDao)
        {
            try
            {
                if (_sendLogs)
                {
                    if (logDataList != null && logDataList.Count > 0)
                    {
                        SaveLogsToDatabase(adminDao, logDataList);
                        SendEmail(logDataList);
                        foreach (LogData logData in logDataList)
                            logData.Notified = true;
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing Logs/Price Alerts");
            }
        }

        private void SaveLogsToDatabase(AdminDao adminDao, IList<LogData> logDataList)
        {
            adminDao.SaveLogData(logDataList);
        }

        private void SendEmail(IList<LogData> logDataList)
        {
            string message = _emailOperations.GenerateMessage(logDataList);
            _emailOperations.SendEmail(message, "AC Trader Alert");
        }
    }
}