using ArgentSea.Orleans;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Configuration;

namespace ArgentSea.Orleans.Sql;

public static class ClusteringExtensions
{
    /// <summary>
    /// Configures ArgentSea as the clustering provider.
    /// </summary>
    public static ISiloBuilder UseArgentSeaClustering(this ISiloBuilder builder, Action<IConfiguration> configuration)
    {
        return builder.ConfigureServices(services =>
        {
            if (configuration != null)
            {
                services.Configure(configuration);
            }

            services.AddArgentSeaClustering();
        });
    }

    /// <summary>
    /// Configures ArgentSea as the clustering provider.
    /// </summary>
    public static ISiloBuilder UseArgentSeaClustering(this ISiloBuilder builder)
    {
        return builder.ConfigureServices(services => services
            .Configure<ClusterOptions>(builder.Configuration)
            .AddArgentSeaClustering());
    }

    internal static IServiceCollection AddArgentSeaClustering(this IServiceCollection services)
    {
        services.AddSingleton<IMembershipTable, ArgentSeaOrleansMembershipTable>();
        services.AddSingleton<IConfigurationValidator, ClusterOptionsValidator>();
        return services;
    }

    public static IClientBuilder UseArgentSeaClustering(this IClientBuilder builder, Action<IConfiguration> configuration)
    {
        return builder.ConfigureServices(services =>
        {
            if (configuration != null)
            {
                services.Configure(configuration);
            }

            services.AddArgentSeaClustering();
        });
    }
}