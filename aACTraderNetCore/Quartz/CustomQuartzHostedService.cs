using aACTrader.Jobs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Quartz;
using Quartz.Spi;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace aACTrader.Quartz
{
    public class CustomQuartzHostedService : IHostedService
    {
        private readonly ISchedulerFactory _schedulerFactory;
        private readonly IJobFactory _jobFactory;
        private IConfiguration _configuration { get; set; }
        private readonly ILogger<CustomQuartzHostedService> _logger;

        public CustomQuartzHostedService(ILogger<CustomQuartzHostedService> logger
            , ISchedulerFactory schedulerFactory
            , IJobFactory jobFactory
            , IConfiguration configuration
            )
        {
            this._logger = logger;
            this._schedulerFactory = schedulerFactory;
            this._jobFactory = jobFactory;
            this._configuration = configuration;
        }

        public IScheduler _scheduler { get; set; }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("CustomQuartzHostedService - STARTED");
            _scheduler = await _schedulerFactory.GetScheduler();
            _scheduler.JobFactory = _jobFactory;
            await SetUpJobs(_scheduler);
            await _scheduler.Start(cancellationToken);
            _logger.LogInformation("CustomQuartzHostedService - DONE");
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            await _scheduler?.Shutdown(cancellationToken);
        }

        private ITrigger CreateTrigger(JobMetadata jobMetadata)
        {
            return TriggerBuilder.Create()
            .WithIdentity(jobMetadata.JobId.ToString())
            .WithCronSchedule(jobMetadata.CronExpression)
            .WithDescription($"{jobMetadata.JobName}")
            .Build();
        }

        private IJobDetail CreateJob(JobMetadata jobMetadata)
        {
            return JobBuilder
            .Create(jobMetadata.JobType)
            .WithIdentity(jobMetadata.JobId.ToString())
            .WithDescription($"{jobMetadata.JobName}")
            .Build();
        }

        public async Task SetUpJobs(IScheduler scheduler)
        {
            await CreateFundForecastEngineJob(scheduler);
            //await CreateCryptoForecastEngineJob(scheduler);
            //await CreateSectorForecastJob(scheduler);
            //await CreateApplicationDataUpdateJob(scheduler);
            //await CreateApplicationDataCheckJob(scheduler);
            //await CreateTradeDataJob(scheduler);
            //await CreateTradeFillsDataJob(scheduler);
            //await CreateEMSXDataJob(scheduler);
            //await CreateEMSXAutoDiscountOrdersJob(scheduler);
            //await CreateAutoRefIndexOrdersJob(scheduler);
            //await CreateAutoDiscountOrdersJob(scheduler);
            ////////////////////////////////////////////////////////////await CreateOrderQueueJob(scheduler);
            //await CreateOrderExecutionJob(scheduler);
            ////////////////////////////////////////////////////////////await CreatePairOrderExecutionJob(scheduler);
            //await CreateSaveSecurityPriceJob(scheduler);
            //await CreatePriceDataUpdateJob(scheduler);
            //await CreateSaveFXRateJob(scheduler);
            //await CreateUpdatePositionsJob(scheduler);
            //await CreateUpdateCacheJob(scheduler);
            ////////////////////////////////////////////////////////////await CreateBBGDataUpdateCacheJob(scheduler);
            //await CreateUpdateWatchlistSecuritiesJob(scheduler);
            //await CreateSaveFundForecastJob(scheduler);
            //await CreateSaveFundForecastIntraDayJob(scheduler);
            //await CreateSavePfdForecastJob(scheduler);
            //await CreateFundRedemptionTriggerJob(scheduler);
            //await CreateSecurityAlertsJob(scheduler);
            //await CreateLogDataUpdateJob(scheduler);
            //await CreateGlobalMarketMonitorJob(scheduler);
            //await CreatePnLReportingJob(scheduler);
            //await CreateFundStatsJob(scheduler);
        }

        /// <summary>
        /// Job to generate forecasts for Closed End Funds, BDCs and REITs
        /// Calculates estimated navs, live discounts, expected alphas, live Z and D-scores
        /// </summary>
        /// <param name="scheduler"></param>
        private async Task CreateFundForecastEngineJob(IScheduler scheduler)
        {
            string jobTriggerCriteria = "0/4 * 0-23 ? * MON-SUN";
            if (_configuration["ConnectionStrings:ENV"].Equals("DEV", StringComparison.CurrentCultureIgnoreCase))
                jobTriggerCriteria = "0 0/2 0-23 ? * MON-SUN";

            IJobDetail job = JobBuilder.Create<FundForecastEngineJob>()
                .WithIdentity("FundForecastEngineJob", "FundForecastEngineJob")
                .Build();

            ITrigger jobTrigger = TriggerBuilder.Create()
                    .WithIdentity("FundForecastEngineJobTrigger", "FundForecastEngineJob")
                    .WithCronSchedule(jobTriggerCriteria)
                    .Build();

            await scheduler.ScheduleJob(job, jobTrigger);
        }

        /// <summary>
        /// Job to generate forecasts for Crypto
        /// Calculates estimated navs, live discounts
        /// </summary>
        /// <param name="scheduler"></param>
        private async Task CreateCryptoForecastEngineJob(IScheduler scheduler)
        {
            string jobTriggerCriteria = "0/1 * 5-13 ? * MON-SUN";
            if (_configuration["ConnectionStrings:ENV"].Equals("DEV", StringComparison.CurrentCultureIgnoreCase))
                jobTriggerCriteria = "0/30 * 0-23 ? * MON-SUN";

            IJobDetail job = JobBuilder.Create<CryptoForecastJob>()
                .WithIdentity("CryptoForecastJob", "CryptoForecastJob")
                .Build();

            ITrigger jobTrigger = TriggerBuilder.Create()
                    .WithIdentity("CryptoForecastJobTrigger", "CryptoForecastJob")
                    .WithCronSchedule(jobTriggerCriteria)
                    .Build();

            await scheduler.ScheduleJob(job, jobTrigger);
        }

        /// <summary>
        /// Job to generate sector forecasts for Closed End Funds, BDCs and REITs
        /// Calculates median discounts, Z and D-scores
        /// </summary>
        /// <param name="scheduler"></param>
        private async Task CreateSectorForecastJob(IScheduler scheduler)
        {
            string jobTriggerCriteria = "0/15 * 0-13 ? * MON-SUN";
            if (_configuration["ConnectionStrings:ENV"].Equals("DEV", StringComparison.CurrentCultureIgnoreCase))
                jobTriggerCriteria = "0 0/1 0-23 ? * MON-SUN";

            IJobDetail job = JobBuilder.Create<SectorForecastJob>()
                .WithIdentity("SectorForecastJob", "SectorForecastJob")
                .Build();

            ITrigger jobTrigger = TriggerBuilder.Create()
                    .WithIdentity("SectorForecastJobTrigger", "SectorForecastJob")
                    .WithCronSchedule(jobTriggerCriteria)
                    .Build();

            await scheduler.ScheduleJob(job, jobTrigger);
        }

        /// <summary>
        /// Job to save live security prices to database
        /// </summary>
        /// <param name="scheduler"></param>
        private async Task CreateSaveSecurityPriceJob(IScheduler scheduler)
        {
            string jobTriggerCriteria = "0/35 * 0-13 ? * MON-FRI";
            if (_configuration["ConnectionStrings:ENV"].Equals("DEV", StringComparison.CurrentCultureIgnoreCase))
                jobTriggerCriteria = "0 0/2 0-23 ? * MON-SUN";

            IJobDetail job = JobBuilder.Create<SaveSecurityPriceJob>()
                .WithIdentity("SaveSecurityPriceJob", "SaveSecurityPriceJob")
                .Build();

            ITrigger jobTrigger = TriggerBuilder.Create()
                .WithIdentity("SaveSecurityPriceJobTrigger", "SaveSecurityPriceJob")
                .WithCronSchedule(jobTriggerCriteria)
                .Build();

            await scheduler.ScheduleJob(job, jobTrigger);
        }

        /// <summary>
        /// Job to save Nevest Trades to database
        /// </summary>
        /// <param name="scheduler"></param>
        private async Task CreateTradeDataJob(IScheduler scheduler)
        {
            string jobTriggerCriteria = "0 0/2 5-13 ? * MON-FRI";
            //string jobTriggerCriteria = "0 0/1 0-23 ? * MON-SUN";
            if (_configuration["ConnectionStrings:ENV"].Equals("DEV", StringComparison.CurrentCultureIgnoreCase))
                jobTriggerCriteria = "0 0/1 0-23 ? * MON-SUN";

            IJobDetail job = JobBuilder.Create<TradeDataJob>()
                .WithIdentity("TradeDataJob", "TradeDataJob")
                .Build();

            ITrigger jobTrigger = TriggerBuilder.Create()
                .WithIdentity("TradeDataJobTrigger", "TradeDataJob")
                .WithCronSchedule(jobTriggerCriteria)
                .Build();

            await scheduler.ScheduleJob(job, jobTrigger);
        }

        /// <summary>
        /// Job to save Neovest Trade Fills to database
        /// </summary>
        /// <param name="scheduler"></param>
        private async Task CreateTradeFillsDataJob(IScheduler scheduler)
        {
            //string jobTriggerCriteria = "0/45 * 3-13 ? * MON-FRI";
            //if (_configuration["ConnectionStrings:ENV"].Equals("DEV", StringComparison.CurrentCultureIgnoreCase))
            //    jobTriggerCriteria = "0/45 * 0-23 ? * MON-SUN";

            IJobDetail job = JobBuilder.Create<TradeFillsDataJob>()
                .WithIdentity("TradeFillsDataJob", "TradeFillsDataJob")
                .Build();

            ITrigger jobTrigger = TriggerBuilder.Create()
                .WithIdentity("TradeFillsDataJobTrigger", "TradeFillsDataJob")
                //.WithCronSchedule(jobTriggerCriteria)
                .StartAt(DateBuilder.DateOf(4, 30, 0))
                .EndAt(DateBuilder.DateOf(13, 15, 0))
                .WithSimpleSchedule(x => x
                    .WithIntervalInSeconds(45)
                    .RepeatForever())
                .Build();

            await scheduler.ScheduleJob(job, jobTrigger);
        }

        /// <summary>
        /// Job to save EMSX trading orders to database
        /// </summary>
        /// <param name="scheduler"></param>
        private async Task CreateEMSXDataJob(IScheduler scheduler)
        {
            //string jobTriggerCriteria = "0 0/2 6-13 ? * MON-FRI";
            //if (_configuration["ConnectionStrings:ENV"].Equals("DEV", StringComparison.CurrentCultureIgnoreCase))
            //    jobTriggerCriteria = "0 0/2 0-23 ? * MON-SUN";

            //IJobDetail job = JobBuilder.Create<EMSXDataJob>()
            //    .WithIdentity("EMSXDataJob", "EMSXDataJob")
            //    .Build();

            //ITrigger jobTrigger = TriggerBuilder.Create()
            //    .WithIdentity("EMSXDataJobTrigger", "EMSXDataJob")
            //    .WithCronSchedule(jobTriggerCriteria)
            //    .Build();

            if (_configuration["ConnectionStrings:ENV"].Equals("PROD", StringComparison.CurrentCultureIgnoreCase))
            {
                IJobDetail job = JobBuilder.Create<EMSXDataJob>()
                .WithIdentity("EMSXDataJob", "EMSXDataJob")
                .Build();

                ITrigger jobTrigger = TriggerBuilder.Create()
                    .WithIdentity("EMSXDataJobTrigger", "EMSXDataJob")
                    .StartAt(DateBuilder.DateOf(6, 30, 0))
                    .EndAt(DateBuilder.DateOf(13, 15, 0))
                    .WithSimpleSchedule(x => x
                        .WithIntervalInSeconds(50)
                        .RepeatForever())
                    .Build();

                await scheduler.ScheduleJob(job, jobTrigger);
            }
            else
            {
                IJobDetail job = JobBuilder.Create<EMSXDataJob>()
                .WithIdentity("EMSXDataJob", "EMSXDataJob")
                .Build();

                ITrigger jobTrigger = TriggerBuilder.Create()
                    .WithIdentity("EMSXDataJobTrigger", "EMSXDataJob")
                    .StartAt(DateBuilder.DateOf(3, 15, 0))
                    .EndAt(DateBuilder.DateOf(5, 15, 0))
                    .WithSimpleSchedule(x => x
                        .WithIntervalInSeconds(50)
                        .RepeatForever())
                    .Build();

                await scheduler.ScheduleJob(job, jobTrigger);
            }
        }

        /// <summary>
        /// Job to track orders
        /// Identifies best bid/offer orders, pair trade spreads
        /// </summary>
        /// <param name="scheduler"></param>
        private async Task CreateOrderExecutionJob(IScheduler scheduler)
        {
            string jobTriggerCriteria = "0/25 * 4-13 ? * MON-FRI";
            if (_configuration["ConnectionStrings:ENV"].Equals("DEV", StringComparison.CurrentCultureIgnoreCase))
                jobTriggerCriteria = "0/45 * 0-23 ? * MON-SUN";

            IJobDetail job = JobBuilder.Create<OrderExecutionJob>()
                .WithIdentity("OrderExecutionJob", "OrderExecutionJob")
                .Build();

            ITrigger jobTrigger = TriggerBuilder.Create()
                .WithIdentity("OrderExecutionJobTrigger", "OrderExecutionJob")
                .WithCronSchedule(jobTriggerCriteria)
                .Build();

            await scheduler.ScheduleJob(job, jobTrigger);
        }

        /// <summary>
        /// Job to track pair orders
        /// Identifies best bid/offer orders, pair trade spreads
        /// </summary>
        /// <param name="scheduler"></param>
        private async Task CreatePairOrderExecutionJob(IScheduler scheduler)
        {
            string jobTriggerCriteria = "0/2 * 6-12 ? * MON-FRI";
            if (_configuration["ConnectionStrings:ENV"].Equals("DEV", StringComparison.CurrentCultureIgnoreCase))
                jobTriggerCriteria = "0/30 * 0-23 ? * MON-SUN";

            IJobDetail job = JobBuilder.Create<PairOrderExecutionJob>()
                .WithIdentity("PairOrderExecutionJob", "PairOrderExecutionJob")
                .Build();

            ITrigger jobTrigger = TriggerBuilder.Create()
                .WithIdentity("PairOrderExecutionJobTrigger", "PairOrderExecutionJob")
                .WithCronSchedule(jobTriggerCriteria)
                .Build();

            await scheduler.ScheduleJob(job, jobTrigger);
        }

        /// <summary>
        /// Job to save live fx rates to database
        /// Rates from Neovest are updated live for 30+ currencies
        /// </summary>
        /// <param name="scheduler"></param>
        private async Task CreateSaveFXRateJob(IScheduler scheduler)
        {
            string jobTriggerCriteria = "0 10/1 0-13 ? * MON-FRI";
            if (_configuration["ConnectionStrings:ENV"].Equals("DEV", StringComparison.CurrentCultureIgnoreCase))
                jobTriggerCriteria = "0 0/2 0-23 ? * MON-SUN";

            IJobDetail job = JobBuilder.Create<SaveFXRateJob>()
                .WithIdentity("SaveFXRateJob", "SaveFXRateJob")
                .Build();

            ITrigger jobTrigger = TriggerBuilder.Create()
                .WithIdentity("SaveFXRateJobTrigger", "SaveFXRateJob")
                .WithCronSchedule(jobTriggerCriteria)
                .Build();

            await scheduler.ScheduleJob(job, jobTrigger);
        }

        /// <summary>
        /// Job to update ALM funds positions & broker data in data cache
        /// </summary>
        /// <param name="scheduler"></param>
        private async Task CreateUpdatePositionsJob(IScheduler scheduler)
        {
            string jobTriggerCriteria = "0 0/20 5-10 ? * MON-FRI";
            if (_configuration["ConnectionStrings:ENV"].Equals("DEV", StringComparison.CurrentCultureIgnoreCase))
                jobTriggerCriteria = "0 0/2 0-23 ? * MON-SUN";

            IJobDetail job = JobBuilder.Create<PositionUpdateJob>()
                .WithIdentity("PositionUpdateJob", "PositionUpdateJob")
                .Build();

            ITrigger jobTrigger = TriggerBuilder.Create()
                .WithIdentity("PositionUpdateJobTrigger", "PositionUpdateJob")
                .WithCronSchedule(jobTriggerCriteria)
                .Build();

            await scheduler.ScheduleJob(job, jobTrigger);
        }

        /// <summary>
        /// Job to update data cache
        /// Updates BBG Fx rates
        /// </summary>
        /// <param name="scheduler"></param>
        private async Task CreateUpdateCacheJob(IScheduler scheduler)
        {
            string jobTriggerCriteria = "0 0/10 0-13 ? * MON-FRI";
            if (_configuration["ConnectionStrings:ENV"].Equals("DEV", StringComparison.CurrentCultureIgnoreCase))
                jobTriggerCriteria = "0 0/1 0-23 ? * MON-SUN";

            IJobDetail job = JobBuilder.Create<UpdateCacheJob>()
                .WithIdentity("UpdateCacheJob", "UpdateCacheJob")
                .Build();

            ITrigger jobTrigger = TriggerBuilder.Create()
                .WithIdentity("UpdateCacheJobTrigger", "UpdateCacheJob")
                .WithCronSchedule(jobTriggerCriteria)
                .Build();

            await scheduler.ScheduleJob(job, jobTrigger);
        }

        /// <summary>
        /// Job to update data cache
        /// Updates navs, ETF, Proxy returns
        /// Updates Pfd interest rate curves, cashflows
        /// </summary>
        /// <param name="scheduler"></param>
        private async Task CreateBBGDataUpdateCacheJob(IScheduler scheduler)
        {
            string jobTriggerCriteria = "0 30/18 0-13 ? * MON-FRI";
            if (_configuration["ConnectionStrings:ENV"].Equals("DEV", StringComparison.CurrentCultureIgnoreCase))
                jobTriggerCriteria = "0 0/2 0-23 ? * MON-SUN";

            IJobDetail job = JobBuilder.Create<UpdateBBGDataCacheJob>()
                .WithIdentity("UpdateBBGDataCacheJob", "UpdateBBGDataCacheJob")
                .Build();

            ITrigger jobTrigger = TriggerBuilder.Create()
                .WithIdentity("UpdateBBGDataCacheJobTrigger", "UpdateBBGDataCacheJob")
                .WithCronSchedule(jobTriggerCriteria)
                .Build();

            await scheduler.ScheduleJob(job, jobTrigger);
        }

        /// <summary>
        /// Job to update watchlist securities
        /// </summary>
        /// <param name="scheduler"></param>
        private async Task CreateUpdateWatchlistSecuritiesJob(IScheduler scheduler)
        {
            string jobTriggerCriteria = "0 0/45 5-13 ? * MON-FRI";
            if (_configuration["ConnectionStrings:ENV"].Equals("DEV", StringComparison.CurrentCultureIgnoreCase))
                jobTriggerCriteria = "0/45 * 0-23 ? * MON-SUN";

            IJobDetail job = JobBuilder.Create<WatchlistSecuritiesJob>()
                .WithIdentity("WatchlistSecuritiesJob", "WatchlistSecuritiesJob")
                .Build();

            ITrigger jobTrigger = TriggerBuilder.Create()
                .WithIdentity("WatchlistSecuritiesJobTrigger", "WatchlistSecuritiesJob")
                .WithCronSchedule(jobTriggerCriteria)
                .Build();

            await scheduler.ScheduleJob(job, jobTrigger);
        }

        /// <summary>
        /// Job to save fund forecasts at market close
        /// </summary>
        /// <param name="scheduler"></param>
        private async Task CreateSaveFundForecastJob(IScheduler scheduler)
        {
            string jobTriggerCriteria = "0 02 13 ? * MON-FRI";
            if (_configuration["ConnectionStrings:ENV"].Equals("DEV", StringComparison.CurrentCultureIgnoreCase))
                jobTriggerCriteria = "0 0/2 0-23 ? * MON-SUN";

            IJobDetail job = JobBuilder.Create<SaveFundForecastJob>()
                .WithIdentity("SaveFundForecastJob", "SaveFundForecastJob")
                .Build();

            ITrigger jobTrigger = TriggerBuilder.Create()
                .WithIdentity("SaveFundForecastJobTrigger", "SaveFundForecastJob")
                .WithCronSchedule(jobTriggerCriteria)
                .Build();

            await scheduler.ScheduleJob(job, jobTrigger);
        }

        /// <summary>
        /// Job to save fund forecasts intra-day
        /// </summary>
        /// <param name="scheduler"></param>
        private async Task CreateSaveFundForecastIntraDayJob(IScheduler scheduler)
        {
            //string jobTriggerCriteria = "0 0/15 6-12 ? * MON-FRI";
            string jobTriggerCriteria = "0 0/15 3-12 ? * MON-FRI";
            if (_configuration["ConnectionStrings:ENV"].Equals("DEV", StringComparison.CurrentCultureIgnoreCase))
                jobTriggerCriteria = "0 0/2 0-23 ? * MON-SUN";

            IJobDetail job = JobBuilder.Create<SaveFundForecastIntraDayJob>()
                .WithIdentity("SaveFundForecastIntraDayJob", "SaveFundForecastIntraDayJob")
                .Build();

            ITrigger jobTrigger = TriggerBuilder.Create()
                .WithIdentity("SaveFundForecastIntraDayJobTrigger", "SaveFundForecastIntraDayJob")
                .WithCronSchedule(jobTriggerCriteria)
                .Build();

            await scheduler.ScheduleJob(job, jobTrigger);
        }

        /// <summary>
        /// Job to save (Canadian) pfd security forecasts at market close
        /// Saves prices, spreads and yields
        /// </summary>
        /// <param name="scheduler"></param>
        private async Task CreateSavePfdForecastJob(IScheduler scheduler)
        {
            string jobTriggerCriteria = "0 01 13 ? * MON-FRI";
            if (_configuration["ConnectionStrings:ENV"].Equals("DEV", StringComparison.CurrentCultureIgnoreCase))
                jobTriggerCriteria = "0 0/3 0-23 ? * MON-SUN";

            IJobDetail job = JobBuilder.Create<SavePfdForecastJob>()
                .WithIdentity("SavePfdForecastJob", "SavePfdForecastJob")
                .Build();

            ITrigger jobTrigger = TriggerBuilder.Create()
                .WithIdentity("SavePfdForecastJobTrigger", "SavePfdForecastJob")
                .WithCronSchedule(jobTriggerCriteria)
                .Build();

            await scheduler.ScheduleJob(job, jobTrigger);
        }

        /// <summary>
        /// Job to process Fund Redemption Triggers
        /// </summary>
        /// <param name="scheduler"></param>
        private async Task CreateFundRedemptionTriggerJob(IScheduler scheduler)
        {
            string jobTriggerCriteria = "0 30 05 ? * MON-FRI";
            if (_configuration["ConnectionStrings:ENV"].Equals("DEV", StringComparison.CurrentCultureIgnoreCase))
                jobTriggerCriteria = "0 0/3 0-23 ? * MON-SUN";

            IJobDetail job = JobBuilder.Create<FundRedemptionJob>()
                .WithIdentity("FundRedemptionJob", "FundRedemptionJob")
                .Build();

            ITrigger jobTrigger = TriggerBuilder.Create()
                .WithIdentity("FundRedemptionJobTrigger", "FundRedemptionJob")
                .WithCronSchedule(jobTriggerCriteria)
                .Build();

            await scheduler.ScheduleJob(job, jobTrigger);
        }

        /// <summary>
        /// Job to process Security Alerts
        /// </summary>
        /// <param name="scheduler"></param>
        private async Task CreateSecurityAlertsJob(IScheduler scheduler)
        {
            string jobTriggerCriteria = "0 0/45 5-7 ? * MON-FRI";
            if (_configuration["ConnectionStrings:ENV"].Equals("DEV", StringComparison.CurrentCultureIgnoreCase))
                jobTriggerCriteria = "0/30 * 0-23 ? * MON-SUN";

            IJobDetail job = JobBuilder.Create<SecurityAlertJob>()
                .WithIdentity("SecurityAlertJob", "SecurityAlertJob")
                .Build();

            ITrigger jobTrigger = TriggerBuilder.Create()
                .WithIdentity("SecurityAlertJobTrigger", "SecurityAlertJob")
                .WithCronSchedule(jobTriggerCriteria)
                .Build();

            await scheduler.ScheduleJob(job, jobTrigger);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="scheduler"></param>
        private async Task CreateApplicationDataUpdateJob(IScheduler scheduler)
        {
            string jobTriggerCriteria = "0 02 14 ? * MON-FRI";

            IJobDetail job = JobBuilder.Create<ApplicationDataJob>()
                .WithIdentity("ApplicationDataJob", "ApplicationDataJob")
                .Build();

            ITrigger jobTrigger = TriggerBuilder.Create()
                .WithIdentity("ApplicationDataJobTrigger", "ApplicationDataJob")
                .WithCronSchedule(jobTriggerCriteria)
                .Build();

            await scheduler.ScheduleJob(job, jobTrigger);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="scheduler"></param>
        private async Task CreateLogDataUpdateJob(IScheduler scheduler)
        {
            string jobTriggerCriteria = "0/55 * 5-13 ? * MON-FRI";
            if (_configuration["ConnectionStrings:ENV"].Equals("DEV", StringComparison.CurrentCultureIgnoreCase))
                jobTriggerCriteria = "0/55 * 0-23 ? * MON-SUN";

            IJobDetail job = JobBuilder.Create<LogDataJob>()
                .WithIdentity("LogDataJob", "LogDataJob")
                .Build();

            ITrigger jobTrigger = TriggerBuilder.Create()
                .WithIdentity("LogDataJobTrigger", "LogDataJob")
                .WithCronSchedule(jobTriggerCriteria)
                .Build();

            await scheduler.ScheduleJob(job, jobTrigger);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="scheduler"></param>
        private async Task CreatePriceDataUpdateJob(IScheduler scheduler)
        {
            string jobTriggerCriteria = "0 0/3 5-13 ? * MON-SUN";
            if (_configuration["ConnectionStrings:ENV"].Equals("DEV", StringComparison.CurrentCultureIgnoreCase))
                jobTriggerCriteria = "0 0/1 0-23 ? * MON-FRI";

            IJobDetail job = JobBuilder.Create<PricingJob>()
                .WithIdentity("PricingJob", "PricingJob")
                .Build();

            ITrigger jobTrigger = TriggerBuilder.Create()
                .WithIdentity("PricingJobTrigger", "PricingJob")
                .WithCronSchedule(jobTriggerCriteria)
                .Build();

            await scheduler.ScheduleJob(job, jobTrigger);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="scheduler"></param>
        private async Task CreateApplicationDataCheckJob(IScheduler scheduler)
        {
            string jobTriggerCriteria = "0/30 * 7-12 ? * MON-FRI";
            if (_configuration["ConnectionStrings:ENV"].Equals("DEV", StringComparison.CurrentCultureIgnoreCase))
                jobTriggerCriteria = "0/45 * 0-23 ? * MON-FRI";

            IJobDetail job = JobBuilder.Create<ApplicationDataCheckJob>()
                .WithIdentity("ApplicationDataCheckJob", "ApplicationDataCheckJob")
                .Build();

            ITrigger jobTrigger = TriggerBuilder.Create()
                .WithIdentity("ApplicationDataCheckJobTrigger", "ApplicationDataCheckJob")
                .WithCronSchedule(jobTriggerCriteria)
                .Build();

            await scheduler.ScheduleJob(job, jobTrigger);
        }

        /// <summary>
        /// Job to process reference orders
        /// </summary>
        /// <param name="scheduler"></param>
        private async Task CreateAutoRefIndexOrdersJob(IScheduler scheduler)
        {
            string jobTriggerCriteria = "0/2 * 6-12 ? * MON-FRI";
            if (_configuration["ConnectionStrings:ENV"].Equals("DEV", StringComparison.CurrentCultureIgnoreCase))
                jobTriggerCriteria = "0/1 * 0-23 ? * MON-SUN";

            IJobDetail job = JobBuilder.Create<AutoRefIndexOrdersJob>()
                .WithIdentity("AutoRefIndexOrdersJob", "AutoRefIndexOrdersJob")
                .Build();

            ITrigger jobTrigger = TriggerBuilder.Create()
                .WithIdentity("AutoRefIndexOrdersJobTrigger", "AutoRefIndexOrdersJob")
                .WithCronSchedule(jobTriggerCriteria)
                .Build();

            await scheduler.ScheduleJob(job, jobTrigger);
        }

        /// <summary>
        /// Job to process discount orders
        /// </summary>
        /// <param name="scheduler"></param>
        private async Task CreateAutoDiscountOrdersJob(IScheduler scheduler)
        {
            string jobTriggerCriteria = "0/2 * 6-12 ? * MON-FRI";
            if (_configuration["ConnectionStrings:ENV"].Equals("DEV", StringComparison.CurrentCultureIgnoreCase))
                jobTriggerCriteria = "0/1 * 0-23 ? * MON-SUN";

            IJobDetail job = JobBuilder.Create<AutoDiscountOrdersJob>()
                .WithIdentity("AutoDiscountOrdersJob", "AutoDiscountOrdersJob")
                .Build();

            ITrigger jobTrigger = TriggerBuilder.Create()
                .WithIdentity("AutoDiscountOrdersJobTrigger", "AutoDiscountOrdersJob")
                .WithCronSchedule(jobTriggerCriteria)
                .Build();

            await scheduler.ScheduleJob(job, jobTrigger);
        }

        /// <summary>
        /// Job to process order queue
        /// </summary>
        /// <param name="scheduler"></param>
        private async Task CreateOrderQueueJob(IScheduler scheduler)
        {
            string jobTriggerCriteria = "0 0/1 6-12 ? * MON-FRI";
            if (_configuration["ConnectionStrings:ENV"].Equals("DEV", StringComparison.CurrentCultureIgnoreCase))
                jobTriggerCriteria = "0 0/2 0-23 ? * MON-SUN";

            IJobDetail job = JobBuilder.Create<OrderQueueJob>()
                .WithIdentity("OrderQueueJob", "OrderQueueJob")
                .Build();

            ITrigger jobTrigger = TriggerBuilder.Create()
                .WithIdentity("OrderQueueJobTrigger", "OrderQueueJob")
                .WithCronSchedule(jobTriggerCriteria)
                .Build();

            await scheduler.ScheduleJob(job, jobTrigger);
        }

        /// <summary>
        /// Job to process GMM data
        /// </summary>
        /// <param name="scheduler"></param>
        private async Task CreateGlobalMarketMonitorJob(IScheduler scheduler)
        {
            if (_configuration["ConnectionStrings:ENV"].Equals("DEV", StringComparison.CurrentCultureIgnoreCase))
            {
                string jobTriggerCriteria = "0 0/2 0-23 ? * MON-SUN";

                IJobDetail job = JobBuilder.Create<GlobalMarketMonitorJob>()
                    .WithIdentity("GlobalMarketMonitorJob", "GlobalMarketMonitorJob")
                    .Build();

                ITrigger jobTrigger = TriggerBuilder.Create()
                    .WithIdentity("GlobalMarketMonitorJobTrigger", "GlobalMarketMonitorJob")
                    .WithCronSchedule(jobTriggerCriteria)
                    .Build();

                await scheduler.ScheduleJob(job, jobTrigger);
            }
            else
            {
                IJobDetail job = JobBuilder.Create<GlobalMarketMonitorJob>()
                .WithIdentity("GlobalMarketMonitorJob", "GlobalMarketMonitorJob")
                .Build();

                ITrigger jobTrigger = TriggerBuilder.Create()
                    .WithIdentity("GlobalMarketMonitorJobTrigger", "GlobalMarketMonitorJob")
                    .StartAt(DateBuilder.DateOf(5, 30, 0))
                    .EndAt(DateBuilder.DateOf(6, 00, 0))
                    .WithSimpleSchedule(x => x
                        .WithIntervalInMinutes(15)
                        .RepeatForever())
                    .Build();

                await scheduler.ScheduleJob(job, jobTrigger);
            }
        }

        /// <summary>
        /// Job to process discount orders
        /// </summary>
        /// <param name="scheduler"></param>
        private async Task CreateEMSXAutoDiscountOrdersJob(IScheduler scheduler)
        {
            string jobTriggerCriteria = "0/2 * 6-12 ? * MON-FRI";
            if (_configuration["ConnectionStrings:ENV"].Equals("DEV", StringComparison.CurrentCultureIgnoreCase))
                jobTriggerCriteria = "0/45 * 0-23 ? * MON-SUN";

            IJobDetail job = JobBuilder.Create<EMSXAutoDiscountOrdersJob>()
                .WithIdentity("EMSXAutoDiscountOrdersJob", "EMSXAutoDiscountOrdersJob")
                .Build();

            ITrigger jobTrigger = TriggerBuilder.Create()
                .WithIdentity("EMSXAutoDiscountOrdersJobTrigger", "EMSXAutoDiscountOrdersJob")
                .WithCronSchedule(jobTriggerCriteria)
                .Build();

            await scheduler.ScheduleJob(job, jobTrigger);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="scheduler"></param>
        private async Task CreatePnLReportingJob(IScheduler scheduler)
        {
            string jobTriggerCriteria = "0 0/5 6-12 ? * MON-FRI";
            if (_configuration["ConnectionStrings:ENV"].Equals("DEV", StringComparison.CurrentCultureIgnoreCase))
                jobTriggerCriteria = "0/45 * 0-23 ? * MON-SUN";

            IJobDetail job = JobBuilder.Create<PnLReportingJob>()
                .WithIdentity("PnLReportingJob", "PnLReportingJob")
                .Build();

            ITrigger jobTrigger = TriggerBuilder.Create()
                .WithIdentity("PnLReportingJob", "PnLReportingJob")
                .WithCronSchedule(jobTriggerCriteria)
                .Build();

            await scheduler.ScheduleJob(job, jobTrigger);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="scheduler"></param>
        /// <returns></returns>
        private async Task CreateFundStatsJob(IScheduler scheduler)
        {
            string jobTriggerCriteria = "0 0/2 6-12 ? * MON-FRI";
            if (_configuration["ConnectionStrings:ENV"].Equals("DEV", StringComparison.CurrentCultureIgnoreCase))
                jobTriggerCriteria = "0 0/1 0-23 ? * MON-SUN";

            IJobDetail job = JobBuilder.Create<FundStatsJob>()
                .WithIdentity("FundStatsJob", "FundStatsJob")
                .Build();

            ITrigger jobTrigger = TriggerBuilder.Create()
                .WithIdentity("FundStatsJob", "FundStatsJob")
                .WithCronSchedule(jobTriggerCriteria)
                .Build();

            await scheduler.ScheduleJob(job, jobTrigger);
        }
    }
}
