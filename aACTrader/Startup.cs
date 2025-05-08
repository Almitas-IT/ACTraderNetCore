using aACTrader.Compliance;
using aACTrader.DAO.Repository;
using aACTrader.gRPCServices.Impl;
using aACTrader.gRPCServices.Interface;
using aACTrader.Jobs;
using aACTrader.JSONParsers.Utf8Json;
using aACTrader.Operations;
using aACTrader.Operations.Impl;
using aACTrader.Operations.Impl.BatchOrders;
using aACTrader.Operations.Impl.NavEstimation;
using aACTrader.Operations.Reports;
using aACTrader.Quartz;
using aACTrader.Samples;
using aACTrader.Services;
using aACTrader.Services.Admin;
using aACTrader.Services.Crypto;
using aACTrader.Services.EMSX;
using aACTrader.Services.EMSX.Simulation;
using aACTrader.Services.Messaging;
using aACTrader.Services.Neovest;
using aACTrader.SignalR.Hubs;
using aACTrader.SignalR.Services;
using LazyCache;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MySql.Data.MySqlClient;
using Quartz;
using Quartz.Impl;
using Quartz.Spi;
using System;
using Utf8Json.Resolvers;

namespace aACTrader
{
    public class Startup
    {
        private ILogger _logger;

        public Startup(IConfiguration configuration, IWebHostEnvironment env)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {
            try
            {
                //Classes
                services.AddSingleton<MySqlConnection>(_ => new MySqlConnection(Configuration["ConnectionStrings:DATABASE"]));

                //Cache
                services.AddSingleton<CachingService, CachingService>();

                AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
                AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2Support", true);

                //Data Access Classes
                services.AddSingleton<AdminDao, AdminDao>();
                services.AddSingleton<BaseDao, BaseDao>();
                services.AddSingleton<CommonDao, CommonDao>();
                services.AddSingleton<CryptoDao, CryptoDao>();
                services.AddSingleton<DataOverrideDao, DataOverrideDao>();
                services.AddSingleton<FundCashDao, FundCashDao>();
                services.AddSingleton<FundDao, FundDao>();
                services.AddSingleton<FundForecastDao, FundForecastDao>();
                services.AddSingleton<FundHistoryDao, FundHistoryDao>();
                services.AddSingleton<FundSupplementalDataDao, FundSupplementalDataDao>();
                services.AddSingleton<HoldingsDao, HoldingsDao>();
                services.AddSingleton<OptionDao, OptionDao>();
                services.AddSingleton<PfdBaseDao, PfdBaseDao>();
                services.AddSingleton<SecurityAlertDao, SecurityAlertDao>();
                services.AddSingleton<SecurityPriceDao, SecurityPriceDao>();
                services.AddSingleton<SecurityRiskDao, SecurityRiskDao>();
                services.AddSingleton<SPACDao, SPACDao>();
                services.AddSingleton<StatsDao, StatsDao>();
                services.AddSingleton<TradingDao, TradingDao>();
                services.AddSingleton<WebDao, WebDao>();
                services.AddSingleton<BrokerDataDao, BrokerDataDao>();
                services.AddSingleton<PairTradingDao, PairTradingDao>();
                services.AddSingleton<AgileDao, AgileDao>();
                services.AddSingleton<UKFundDetailDao, UKFundDetailDao>();
                services.AddSingleton<PairTradingNVDao, PairTradingNVDao>();
                services.AddSingleton<EMSXDao, EMSXDao>();
                services.AddSingleton<ComplianceDao, ComplianceDao>();

                //Operation Classes
                services.AddSingleton<ActivistHoldingsOperations, ActivistHoldingsOperations>();
                services.AddSingleton<BaseOperations, BaseOperations>();
                services.AddSingleton<BBGOperations, BBGOperations>();
                services.AddSingleton<BrokerReportOperations, BrokerReportOperations>();
                services.AddSingleton<CashflowGenerator, CashflowGenerator>();
                services.AddSingleton<CommonOperations, CommonOperations>();
                services.AddSingleton<CompanyHoldingsReportGenerator, CompanyHoldingsReportGenerator>();
                services.AddSingleton<DataValidationChecks, DataValidationChecks>();
                services.AddSingleton<EmailOperations, EmailOperations>();
                services.AddSingleton<ExpectedAlphaCalculatorNew, ExpectedAlphaCalculatorNew>();
                services.AddSingleton<ExpressionEvaluator, ExpressionEvaluator>();
                services.AddSingleton<FundAlertManager, FundAlertManager>();
                services.AddSingleton<FundAlertManagerNew, FundAlertManagerNew>();
                services.AddSingleton<FundForecastEngine, FundForecastEngine>();
                services.AddSingleton<FundHistoryOperations, FundHistoryOperations>();
                services.AddSingleton<FundHoldingsOperations, FundHoldingsOperations>();
                services.AddSingleton<FundIRRCalculator, FundIRRCalculator>();
                services.AddSingleton<FundRedemptionTriggerOperations, FundRedemptionTriggerOperations>();
                services.AddSingleton<FundTenderOperations, FundTenderOperations>();
                services.AddSingleton<NavProxyEvaluator, NavProxyEvaluator>();
                services.AddSingleton<AltNavProxyEvaluator, AltNavProxyEvaluator>();
                services.AddSingleton<PortProxyEvaluator, PortProxyEvaluator>();
                services.AddSingleton<PairTradeOperations, PairTradeOperations>();
                services.AddSingleton<PfdCommonOperations, PfdCommonOperations>();
                services.AddSingleton<PortHoldingsOperations, PortHoldingsOperations>();
                services.AddSingleton<PositionReconOperations, PositionReconOperations>();
                services.AddSingleton<SectorForecastOperations, SectorForecastOperations>();
                services.AddSingleton<SecurityAlertsEngine, SecurityAlertsEngine>();
                services.AddSingleton<SecurityPriceLookupOperations, SecurityPriceLookupOperations>();
                services.AddSingleton<SecurityPriceOperation, SecurityPriceOperation>();
                services.AddSingleton<SecurityReturnOperations, SecurityReturnOperations>();
                services.AddSingleton<TradeFilterOperations, TradeFilterOperations>();
                services.AddSingleton<TradeOrderOperations, TradeOrderOperations>();
                services.AddSingleton<BatchOrderOperations, BatchOrderOperations>();
                services.AddSingleton<AllocationOperations, AllocationOperations>();
                services.AddSingleton<DiscountTargetOrder, DiscountTargetOrder>();
                services.AddSingleton<RefIndexOrder, RefIndexOrder>();
                services.AddSingleton<OrderValidator, OrderValidator>();
                services.AddSingleton<FundExposureReport, FundExposureReport>();
                services.AddSingleton<GlobalMarketMonitorOperations, GlobalMarketMonitorOperations>();
                services.AddSingleton<JPMMarginOperations, JPMMarginOperations>();
                services.AddSingleton<WebDataOperations, WebDataOperations>();
                services.AddSingleton<PairOrderOperations, PairOrderOperations>();
                services.AddSingleton<UKFundHistoryOperations, UKFundHistoryOperations>();
                services.AddSingleton<NVPairOrderOperations, NVPairOrderOperations>();
                services.AddSingleton<BatchOrderQueueOperations, BatchOrderQueueOperations>();
                services.AddSingleton<BrokerCommissionOperations, BrokerCommissionOperations>();
                services.AddSingleton<TradeSummaryReport, TradeSummaryReport>();
                services.AddSingleton<TradeSummaryReportNew, TradeSummaryReportNew>();
                services.AddSingleton<HoldingExposureReport, HoldingExposureReport>();
                services.AddSingleton<SecurityPerformanceOperations, SecurityPerformanceOperations>();
                services.AddSingleton<EMSXBatchOrderOperations, EMSXBatchOrderOperations>();
                services.AddSingleton<EMSXDiscountTargetOrder, EMSXDiscountTargetOrder>();
                services.AddSingleton<EMSXRefIndexOrder, EMSXRefIndexOrder>();
                services.AddSingleton<EMSXOrderValidator, EMSXOrderValidator>();
                services.AddSingleton<RulesManager, RulesManager>();
                services.AddSingleton<RulesEngine, RulesEngine>();
                services.AddSingleton<SecurityFilingThresholdOperations, SecurityFilingThresholdOperations>();
                services.AddSingleton<DailyPnLReport, DailyPnLReport>();
                services.AddSingleton<ConditionalProxyProcessor, ConditionalProxyProcessor>();
                services.AddSingleton<ConditionalProxyEvaluator, ConditionalProxyEvaluator>();
                services.AddSingleton<FundStatsCalculator, FundStatsCalculator>();

                //Messaging Services
                //Consumer
                services.AddSingleton<NeovestStatusConsumer, NeovestStatusConsumer>();
                services.AddSingleton<NeovestFundPricingConsumer, NeovestFundPricingConsumer>();
                services.AddSingleton<NeovestFXConsumer, NeovestFXConsumer>();
                services.AddSingleton<NeovestOrderStatusConsumer, NeovestOrderStatusConsumer>();
                services.AddSingleton<NeovestOrderSummaryConsumer, NeovestOrderSummaryConsumer>();
                services.AddSingleton<NeovestPricingConsumer, NeovestPricingConsumer>();
                services.AddSingleton<SimNeovestOrderSummaryConsumer, SimNeovestOrderSummaryConsumer>();
                services.AddSingleton<CoinAPIPricingConsumer, CoinAPIPricingConsumer>();
                services.AddSingleton<CoinAPIQuoteConsumer, CoinAPIQuoteConsumer>();
                services.AddSingleton<BBGPricingConsumer, BBGPricingConsumer>();
                services.AddSingleton<BBGDataUpdateConsumer, BBGDataUpdateConsumer>();
                services.AddSingleton<NeovestSharesImbalanceConsumer, NeovestSharesImbalanceConsumer>();
                services.AddSingleton<NeovestTradeVolumeConsumer, NeovestTradeVolumeConsumer>();
                services.AddSingleton<EMSXOrderPublisher, EMSXOrderPublisher>();
                services.AddSingleton<SimEMSXOrderPublisher, SimEMSXOrderPublisher>();
                services.AddSingleton<EMSXOrderStatusConsumer, EMSXOrderStatusConsumer>();
                services.AddSingleton<EMSXRouteStatusConsumer, EMSXRouteStatusConsumer>();
                services.AddSingleton<EMSXOrderErrorConsumer, EMSXOrderErrorConsumer>();

                //Services
                services.AddSingleton<CoinAPIPricingService, CoinAPIPricingService>();
                services.AddSingleton<CoinAPIQuoteService, CoinAPIQuoteService>();
                services.AddSingleton<BBGJobUpdatePublisher, BBGJobUpdatePublisher>();
                services.AddSingleton<BBGSecurityUpdatePublisher, BBGSecurityUpdatePublisher>();
                services.AddSingleton<NeovestFundPricingService, NeovestFundPricingService>();
                services.AddSingleton<NeovestFXService, NeovestFXService>();
                services.AddSingleton<NeovestOrderPublisher, NeovestOrderPublisher>();
                services.AddSingleton<NeovestOrderStatusService, NeovestOrderStatusService>();
                services.AddSingleton<NeovestPricingService, NeovestPricingService>();
                services.AddSingleton<SimNeovestOrderPublisher, SimNeovestOrderPublisher>();
                services.AddSingleton<NeovestOrderSummaryService, NeovestOrderSummaryService>();
                services.AddSingleton<SimNeovestOrderSummaryService, SimNeovestOrderSummaryService>();
                services.AddSingleton<NeovestSharesImbalanceService, NeovestSharesImbalanceService>();
                services.AddSingleton<BBGPricingService, BBGPricingService>();
                services.AddSingleton<PricingService, PricingService>();
                services.AddSingleton<EMSXService, EMSXService>();
                services.AddSingleton<LogDataService, LogDataService>();
                services.AddSingleton<NeovestPairOrderPublisher, NeovestPairOrderPublisher>();
                services.AddSingleton<SimNeovestPairOrderPublisher, SimNeovestPairOrderPublisher>();
                services.AddSingleton<NeovestTradeVolumeService, NeovestTradeVolumeService>();
                services.AddSingleton<EMSXOrderStatusService, EMSXOrderStatusService>();
                services.AddSingleton<EMSXRouteStatusService, EMSXRouteStatusService>();
                services.AddSingleton<EMSXOrderErrorService, EMSXOrderErrorService>();
                services.AddSingleton<NeovestStatusService, NeovestStatusService>();

                //gRPC Services
                //services.AddSingleton<SecurityPriceTestService, SecurityPriceTestService>();

                //Jobs
                services.AddSingleton<ApplicationDataCheckJob>();
                services.AddSingleton<ApplicationDataJob>();
                services.AddSingleton<CryptoForecastJob>();
                services.AddSingleton<FundAlertsJob>();
                services.AddSingleton<FundForecastEngineJob>();
                services.AddSingleton<FundRedemptionJob>();
                services.AddSingleton<LogDataJob>();
                services.AddSingleton<OrderExecutionJob>();
                services.AddSingleton<PositionUpdateJob>();
                services.AddSingleton<PricingJob>();
                services.AddSingleton<SaveFundForecastIntraDayJob>();
                services.AddSingleton<SaveFundForecastJob>();
                services.AddSingleton<SaveFXRateJob>();
                services.AddSingleton<SavePfdForecastJob>();
                services.AddSingleton<SaveSecurityPriceJob>();
                services.AddSingleton<SectorForecastJob>();
                services.AddSingleton<SecurityAlertJob>();
                services.AddSingleton<TradeDataJob>();
                services.AddSingleton<UpdateBBGDataCacheJob>();
                services.AddSingleton<UpdateCacheJob>();
                services.AddSingleton<WatchlistSecuritiesJob>();
                services.AddSingleton<AutoRefIndexOrdersJob>();
                services.AddSingleton<AutoDiscountOrdersJob>();
                services.AddSingleton<GlobalMarketMonitorJob>();
                services.AddSingleton<PairOrderExecutionJob>();
                services.AddSingleton<OrderQueueJob>();
                services.AddSingleton<EMSXDataJob>();
                services.AddSingleton<EMSXAutoDiscountOrdersJob>();
                services.AddSingleton<TradeFillsDataJob>();
                services.AddSingleton<PnLReportingJob>();
                services.AddSingleton<FundStatsJob>();

                //Controllers
                services.AddControllers();
                services.AddMvcCore(option =>
                  {
                      option.OutputFormatters.Clear();
                      option.OutputFormatters.Add(new Utf8JsonOutputFormatter(StandardResolver.Default));
                      option.InputFormatters.Clear();
                      option.InputFormatters.Add(new Utf8JsonInputFormatter());
                  });

                //SignalR
                //services.AddSignalR(hubOptions => {
                //    hubOptions.EnableDetailedErrors = true;
                //    hubOptions.HandshakeTimeout = TimeSpan.FromSeconds(120);
                //});
                services.AddSignalR()
                    .AddHubOptions<PriceStreamingHub>(s =>
                        {
                            s.MaximumParallelInvocationsPerClient = 10;
                            s.EnableDetailedErrors = true;
                        })
                    .AddHubOptions<NotificationHub>(s =>
                    {
                        s.MaximumParallelInvocationsPerClient = 10;
                        s.EnableDetailedErrors = true;
                    });
                services.AddSingleton<StockTicker>();
                services.AddSingleton<NotificationService>();

                //Quartz
                services.AddSingleton<IJobFactory, CustomQuartzJobFactory>();
                services.AddSingleton<ISchedulerFactory, StdSchedulerFactory>();

                //gRPC
                services.AddGrpc(options =>
                {
                    options.EnableDetailedErrors = true;
                    options.MaxReceiveMessageSize = 2 * 1024 * 1024; // 2 MB
                    options.MaxSendMessageSize = 5 * 1024 * 1024; // 5 MB
                });
                services.AddTransient<IWeatherForecastTestService, WeatherForecastTestService>();
                services.AddTransient<ISecurityPriceTestService, SecurityPriceTestService>();


                //services.AddGrpcClient<TcpClient>(factoryOptions =>
                //    {
                //        factoryOptions.Address = new Uri("dns://");
                //     })
                //    .ConfigureChannel(channelOptions =>
                //    {
                //        channelOptions.Credentials = ChannelCredentials.Insecure;
                //        channelOptions.ServiceConfig = new ServiceConfig
                //        {
                //            LoadBalancingConfigs =
                //    {
                //        new RoundRobinConfig()
                //    }
                //        };
                //        channelOptions.HttpHandler = new SocketsHttpHandler()
                //        {
                //            EnableMultipleHttp2Connections = true,
                //            SslOptions = new SslClientAuthenticationOptions
                //            {
                //                RemoteCertificateValidationCallback = delegate { return true; }
                //            }
                //        };
                //    });

                services.AddCors(options =>
                {
                    options.AddDefaultPolicy(builder =>
                        {
                            builder.AllowAnyOrigin()
                                .AllowAnyHeader()
                                .AllowAnyMethod();
                        });
                });
            }
            catch (System.Exception)
            {
                throw;
            }
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env, ILoggerFactory loggerFactory)
        {
            loggerFactory.AddLog4Net();
            _logger = loggerFactory.CreateLogger<Startup>();
            _logger.LogInformation("In Configure... - START");

            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
                _logger.LogInformation("In Development...");
            }
            else
            {
                app.UseExceptionHandler("/Error");
                app.UseHsts();
                _logger.LogInformation("In Production...");
            }

            app.UseHttpsRedirection();
            app.UseStaticFiles();
            app.UseRouting();
            app.UseCors();
            //app.UseAuthorization();
            app.UseEndpoints(endpoints =>
            {
                endpoints.MapControllers();
                //endpoints.MapHub<StockPriceHub>("/OpenMarket");
                //endpoints.MapHub<StockPriceHub>("/stocks");
                //endpoints.MapHub<NotificationHub>("/Notification");
                //endpoints.MapHub<PriceStreamingHub>("/PriceStreaming");
                //endpoints.MapHub<DelayedPriceStreamingHub>("/DelayedPriceStreaming");

                endpoints.MapGrpcService<WeatherForecastGrpcService>();
                endpoints.MapGrpcService<SecurityPriceGrpcService>();
            });

            app.ApplicationServices.GetService<BaseOperations>();
            _logger.LogInformation("In Configure... - END");
        }
    }
}
