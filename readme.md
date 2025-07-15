# ArgentSea Orleans SQL Implementation

As a persistance provider for Microsoft Orleans, ArgentSea can save grain data into 3rd-normal-form tables. 

This means that grain state can be queried *across* grains by the SQL engine. For example, if you have a grains that correspond to users, you now have the ability to query user data: which users have specific attributes, how many users meet a criteria, etc.

## Setup

Example appsettings.json sections in the Silos project:

```json
    "OrleansConfig": {
        "ClusterId": "default",
        "ServiceId": "default"
    },
    "SqlDbConnections": [
        {
            "DatabaseKey": "Common",
            "InitialCatalog": "Common"
        }
    ],
    "SqlShardSets": [
        {
            "ShardSetName": "Default",
            "DefaultShardId": 0,
            "UserName": "orleansWriter",
            "Read": {
                "ApplicationIntent": "ReadOnly",
                "UserName": "orleansReader"
            },
            "Shards": [
                {
                    "ShardId": 0,
                    "InitialCatalog": "Tenant1"
                },
                {
                    "ShardId": 1,
                    "InitialCatalog": "Tenant2"
                }
            ]
        }
    ],
    "OrleansData": {
        "ShardSetKey": "Default",
        "DatabaseKey": "Common",
        "ValidateGrainKeys": false
    },
```


An example startup file section:

```C#
builder.UseOrleans((context, siloBuilder) =>
{
    siloBuilder.Services.AddSqlServices(context.Configuration); //ArgentSea
    siloBuilder.Services.AddSingleton<IFieldCodec<ShardKey<Guid>>, ShardKeyOrleansSerializer>();
    siloBuilder.Services.AddSingleton<IDeepCopier<ShardKey<Guid>>, ShardKeyOrleansSerializer>();
    siloBuilder.Services.AddOptions();
    siloBuilder.Services.Configure<OrleansShardPersistenceOptions>(DataProviderConstants.Tenant, options =>
    {
        options.Queries.Add("Scale", ScaleGrainQueries.ScaleQueryDefinition);
    });
    siloBuilder.Services.Configure<OrleansDbPersistenceOptions>(DataProviderConstants.Common, options =>
    {
        //options.Queries.Add("Users", UserGrainQueries.UserQueryDefinition);
    });
});

```

## Configuring the Orleans Client (API)

Your Program.cs startup class should include:

```C#
builder.UseOrleansClient(client => {
    client.Configure<ClientMessagingOptions>(opts =>
    {
        opts.ResponseTimeoutWithDebugger = TimeSpan.FromSeconds(10);
    });
    client.Services.AddSingleton<IFieldCodec<ShardKey<Guid>>, ShardKeyOrleansSerializer>();
    client.Services.AddSingleton<IDeepCopier<ShardKey<Guid>>, ShardKeyOrleansSerializer>();
    client.Services.AddSingleton<IGatewayListProvider, ArgentSeaGatewayListProvider>();
});
```

You will also need to add to appsettings.json — or other configuration — information about how to connect to the Common database:
```json
    "ClusteringClientOptions": {
        "MaxRefreshInterval": "0.00:02:00",
        "ConnectionDatabase": "Common",
        "ProcedureName": "rdr.OrleansClusterGatewayListV1"
    }
```

## Contributions

Contributions are very welcome.

## License

[MIT.](https://opensource.org/licenses/MIT)
