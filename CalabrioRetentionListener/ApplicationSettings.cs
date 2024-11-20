using Microsoft.Extensions.Configuration;
using NLog;
using NLog.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CalabrioRetentionListener
{
    public class ApplicationSettings : IApplicationSettings
    {
        readonly IConfiguration _configuration;

        public ApplicationSettings()
        {
            IConfigurationRoot environmentConfiguration = new ConfigurationBuilder()
                .AddEnvironmentVariables()
                .Build();

            string environment = environmentConfiguration["RUNTIME_ENVIRONMENT"];
            _configuration = new ConfigurationBuilder()
                .SetBasePath(Path.Combine(AppContext.BaseDirectory))
                .AddJsonFile("appsettings.json", false, true)
                .AddJsonFile($"appsettings.{environment}.json", false, true)
                .AddEnvironmentVariables()
                .Build();

            LogManager.Configuration = new NLogLoggingConfiguration(_configuration.GetSection("NLog"));
        }

        public IConfiguration GetConfiguration()
        {
            IConfiguration settings = _configuration;
            return settings;
        }
    }
}
