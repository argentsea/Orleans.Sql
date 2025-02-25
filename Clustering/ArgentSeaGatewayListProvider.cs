using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Messaging;
using System.Data;
using System.Net;
using Microsoft.Data.SqlClient;


namespace ArgentSea.Orleans.Sql;

public class ArgentSeaGatewayListProvider : IGatewayListProvider
{
    private readonly TimeSpan staleness;
    private readonly ILogger<ArgentSeaGatewayListProvider> logger;
    private readonly string clusterId;
    private readonly string connectionString;
    private readonly string procedureName;

    public ArgentSeaGatewayListProvider(IOptions<ClusteringClientOptions> options, ILogger<ArgentSeaGatewayListProvider> logger)
    {
        var builder = new SqlConnectionStringBuilder();
        builder.InitialCatalog = options.Value.ConnectionDatabase;
        builder["Server"] = options.Value.ConnectionServer;
        builder.UserID = options.Value.ConnectionUsername;
        builder.Password = options.Value.ConnectionPassword;
        builder.TrustServerCertificate = options.Value.TrustServerCertificate;
        procedureName = options.Value.ProcedureName;
        connectionString = builder.ConnectionString;
        staleness = options.Value.MaxRefreshInterval;
        clusterId = options.Value.ClusterId;
        this.logger = logger;

    }

    public TimeSpan MaxStaleness { get => staleness; }

    public bool IsUpdatable => true;

    public async Task<IList<Uri>> GetGateways()
    {
        using var cnn = new SqlConnection(connectionString);
        using var cmd = new SqlCommand(procedureName, cnn);
        cmd.CommandType = CommandType.StoredProcedure;
        var prm = new SqlParameter("@ClusterId", SqlDbType.NVarChar, 150)
        {
            Value = clusterId
        };
        cmd.Parameters.Add(prm);
        await cnn.OpenAsync();
        List<Uri> result = [];
        using var rdr = await cmd.ExecuteReaderAsync();
        while (rdr.Read())
        {
            {
                var ip = new IPAddress((byte[])rdr.GetValue(0));
                var silo = SiloAddress.New(new IPEndPoint(ip, rdr.GetInt32(1)), rdr.GetInt32(2));
                result.Add(silo.ToGatewayUri());
            }
        }
        return result;
    }

    public Task InitializeGatewayListProvider()
    {
        logger.LogDebug("Gateway list provider initialized.");
        return Task.CompletedTask;
    }
}
