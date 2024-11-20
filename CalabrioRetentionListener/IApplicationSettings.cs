using Microsoft.Extensions.Configuration;

namespace CalabrioRetentionListener
{
    public interface IApplicationSettings
    {
        IConfiguration GetConfiguration();
    }
}