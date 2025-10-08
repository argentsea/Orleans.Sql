using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Configuration;
using Orleans.Providers;

// Our providerType is named "ArgentSea" and our providerName is "Clustering".
[assembly: RegisterProvider("ArgentSea", "Clustering", "Silo", typeof(ArgentSea.Orleans.Sql.ArgentSeaClusteringProviderBuilder))]
[assembly: RegisterProvider("ArgentSea", "Clustering", "Client", typeof(ArgentSea.Orleans.Sql.ArgentSeaClusteringProviderBuilder))]

namespace ArgentSea.Orleans.Sql;

public sealed class ArgentSeaClusteringProviderBuilder : IProviderBuilder<ISiloBuilder>, IProviderBuilder<IClientBuilder>
{
    public void Configure(ISiloBuilder builder, string name, IConfigurationSection configurationSection)
    {
        builder.Configure<ClusterOptions>(configurationSection);
        builder.UseArgentSeaClustering(_ => { });
    }

    public void Configure(IClientBuilder builder, string name, IConfigurationSection configurationSection)
    {
        builder.Configure<ClusterOptions>(configurationSection);
        builder.UseArgentSeaClustering(_ => { });
    }
}