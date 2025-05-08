using aACTrader.Quartz;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System.IO;

namespace aACTrader
{
    public class Program
    {
        public static void Main(string[] args)
        {
            CreateHostBuilder(args).Build().Run();
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureWebHostDefaults(webBuilder =>
                {
                    webBuilder.UseContentRoot(Directory.GetCurrentDirectory());
                    webBuilder.UseIISIntegration();
                    //webBuilder.UseSetting("https_port", "5001");
                    webBuilder.CaptureStartupErrors(true);
                    webBuilder.UseSetting(WebHostDefaults.DetailedErrorsKey, "true");
                    webBuilder.UseStartup<Startup>();
                    //webBuilder.UseUrls("https://localhost:5001;http://localhost:5000");
                })
            .ConfigureServices(services =>
            {
                services.AddHostedService<CustomQuartzHostedService>();
                services.AddSignalR();
                services.AddGrpc();
            });
    }
}
