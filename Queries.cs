using ArgentSea;
using ArgentSea.Orleans;

namespace ArgentSea.Orleans.Sql;

/// <summary>
/// This class corresponds to a recommended ArgentSea pattern for queries. 
/// However, because Orleans used sealed classes, they could not be extended with ArgentSea metadata attributes.
/// Consequently, these Query objects are not consumed as with ArgentSea. ADO.NET had to be used instead.
/// </summary>
public static class Queries
{
    #region Orleans Cluster Membership
    // The parameter arrays are not necessary, since MembershipEntry is a sealed class and therefore the ArgentSea MapTo parameters can't be invoked. Recommended elsewhere.
    private static readonly string[] prmsOrleansClusterDelete = ["@ClusterId"];
    public static QueryProcedure OrleansClusterDelete => new("wtr.OrleansClusterDeleteV1", prmsOrleansClusterDelete);

    public static QueryProcedure OrleansClusterGatewayQueryKey => new("rdr.OrleansClusterGatewayQueryKeyV1");

    public static QueryProcedure OrleansClusterInsertMemberKey => new("wtr.OrleansClusterInsertMemberKeyV1");

    public static QueryProcedure OrleansClusterInsertMemberVersionKey => new("wtr.OrleansClusterInsertMemberVersionKeyV1");

    public static QueryProcedure OrleansClusterMemberReadAllKey => new("rdr.OrleansClusterMemberReadAllKeyV1");

    public static QueryProcedure OrleansClusterMemberReadRowKey => new("rdr.OrleansClusterMemberReadRowKeyV1");

    public static QueryProcedure OrleansClusterUpdateIAmAliveTimeKey => new("wtr.OrleansClusterUpdateIAmAliveTimeKeyV1");

    public static QueryProcedure OrleansClusterUpdateMemberKey => new("wtr.OrleansClusterUpdateMemberKeyV1");

    public static QueryProcedure OrleansClusterDeleteDefunctMembers => new("wtr.OrleansClusterDeleteDefunctMembersV1");

    //OrleansClusterDeleteDefunctMembersV1

    #endregion
    #region Orleans Reminders
    // The parameter arrays are really not necessary, since ReminderEntry is a sealed class and therefore the ArgentSea MapTo parameters can't be invoked. Included for completeness?
    private static readonly string[] prmsOrleansReminderReadRangeRows = ["@BeginHash", "@EndHash"];
    public static QueryProcedure OrleansReminderReadRangeRows1Key => new("rdr.OrleansReminderReadRangeRows1KeyV1", prmsOrleansReminderReadRangeRows);
    public static QueryProcedure OrleansReminderReadRangeRows2Key => new("rdr.OrleansReminderReadRangeRows2KeyV1", prmsOrleansReminderReadRangeRows);


    private static readonly string[] prmsOrleansReminderReadRowKey = ["@GrainId", "@ReminderName"];
    public static QueryProcedure OrleansReminderReadRowKey => new("rdr.OrleansReminderReadRowKeyV1", prmsOrleansReminderReadRowKey);


    private static readonly string[] prmsOrleansReminderReadRowsKey = ["@GrainId"];
    public static QueryProcedure OrleansReminderReadRowsKey => new("rdr.OrleansReminderReadRowsKeyV1", prmsOrleansReminderReadRowsKey);


    private static readonly string[] prmsOrleansReminderDeleteRowKey = ["@GrainId", "@ReminderName", "@Version", "@IsFound"];
    public static QueryProcedure OrleansReminderDeleteRowKey => new("wtr.OrleansReminderDeleteRowKeyV1", prmsOrleansReminderDeleteRowKey);


    private static readonly string[] prmsOrleansReminderUpsertRowKey = ["@GrainId", "@Origin", "@GrainType", "@ReminderName", "@StartTime", "@Period", "@GrainHash", "@OldVersion", "@NewVersion"];
    public static QueryProcedure OrleansReminderUpsertRowKey => new("wtr.OrleansReminderUpsertRowKeyV1", prmsOrleansReminderUpsertRowKey);


    public static OrleansDbQueryDefinitions orleansDbQueryDefinitions => new("", OrleansReminderReadRowKey, QueryResultFormat.ResultSet, OrleansReminderUpsertRowKey, OrleansReminderDeleteRowKey);
    #endregion

}