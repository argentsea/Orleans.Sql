using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Configuration;
using Orleans.Providers;


[assembly: RegisterProvider("ArgentSea", "Reminders", "Silo", typeof(ArgentSea.Orleans.Sql.ArgentSeaRemindersProviderBuilder))]

namespace ArgentSea.Orleans.Sql;

internal sealed class ArgentSeaRemindersProviderBuilder : IProviderBuilder<ISiloBuilder>
{
    public void Configure(ISiloBuilder builder, string name, IConfigurationSection configurationSection)
    {
        builder.Services.AddOptions<ClusterOptions>();
        builder.UseArgentSeaReminderService(_ => { });
    }
}