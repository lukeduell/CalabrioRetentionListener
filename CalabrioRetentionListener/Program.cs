using Microsoft.Extensions.Configuration;
using CalabrioRetentionListener.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NLog.Extensions.Logging;
using RockLib.Logging;
using RockLib.Logging.DependencyInjection;
using Axiom.Configuration.RestApi;
using Axiom.Configuration.RestApi.OAuth;
using Axiom.Configuration.Switching;
using RockLib.Messaging.Kafka.DependencyInjection;
using RockLib.Messaging.DependencyInjection;
using System;

namespace CalabrioRetentionListener
{
    class Program
    {
        public Program(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        static async Task Main()
        {
            IHost host = Host.CreateDefaultBuilder()
            .ConfigureAppConfiguration((context, config) =>
            {
                var env = context.HostingEnvironment;
                config.AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
                      .AddJsonFile($"appsettings.{env.EnvironmentName}.json", optional: true, reloadOnChange: true);
                config.AddRestApi("KafkaDR", restApi =>
                {
                    restApi.AddOAuth("KafkaDR:OAuth");
                });
            })
            .ConfigureServices((context, services) =>
            {
                var configuration = context.Configuration;
                var rockLibEnvironment = context.Configuration["AppSettings:RockLib.Environment"];

                services.AddSingleton<IApplicationSettings, ApplicationSettings>();
                services.AddSingleton<IKafkaConsumerService, KafkaConsumerService>();
                services.AddSingleton<IConsumerBuilderWrapper, ConsumerBuilderWrapper>();
                services.AddSingleton<IKafkaAckProducerService, KafkaAckProducerService>();
                services.Configure<KafkaReceiverOptions>("Topics", configuration.GetSection("Kafka").SwitchingOn("Region"));
                services.AddLogger(processingMode: Logger.ProcessingMode.Background)
                        .AddCoreLogProvider(applicationId: 220019);
                services.AddLogging(loggingBuilder =>
                {
                    loggingBuilder.ClearProviders();
                    loggingBuilder.AddNLog();
                });
            })
            .Build();

            Application svc = ActivatorUtilities.CreateInstance<Application>(host.Services);
            await svc.Run();
        }
    }
}