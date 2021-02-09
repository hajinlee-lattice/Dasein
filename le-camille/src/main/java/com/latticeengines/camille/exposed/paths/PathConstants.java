package com.latticeengines.camille.exposed.paths;

public final class PathConstants {

    protected PathConstants() {
        throw new UnsupportedOperationException();
    }
    public static final String PODS = "Pods";
    public static final String INTERFACES = "Interfaces";
    public static final String DATA = "Data";
    public static final String CONTRACTS = "Contracts";
    public static final String TENANTS = "Tenants";
    public static final String SPACES = "Spaces";
    public static final String SERVICES = "Services";
    public static final String QUEUES = "Queues";
    public static final String DIVISION = "Divisions";
    public static final String STACKS = "Stacks";
    public static final String TABLES = "Tables";
    public static final String FILES = "Files";
    public static final String TABLE_SCHEMAS = "TableSchemas";
    public static final String EXPORTS = "Exports";
    public static final String METADATA = "Metadata";
    public static final String LOCKS = "Locks";
    public static final String WATCHERS = "Watchers";
    public static final String FABRIC_ENTITIES = "FabricEntities";
    public static final String DANTE = "Dante";
    public static final String S3FILES = "S3Files";
    public static final String CAMPAIGNS = "Campaigns";

    public static final String DEFAULTCONFIG_NODE = "Default";
    public static final String CONFIGSCHEMA_NODE = "Metadata";
    public static final String SPACECONFIGURATION_NODE = "SpaceConfiguration";
    public static final String PRODUCTS_NODE = "Products";

    public static final String TENANT_PA_QUOTA = "PAQuota";
    public static final String TIMEZONE = "Timezone";
    public static final String DEFAULT_TIMEZONE = "DefaultTimezone";
    public static final String MATCH_CONFIGURATION = "MatchConfiguration";

    public static final String PROPERTIES_FILE = "properties.json";
    public static final String FEATURE_FLAGS_FILE = "feature-flags.json";
    public static final String FEATURE_FLAGS_DEFINITIONS_FILE = "feature-flag-definitions.json";
    public static final String BOOTSTRAP_STATE_FILE = "state.json";
    public static final String BOOTSTRAP_LOCK = "lock";

    public static final String DEFAULT_SPACE_FILE = "default-space";

    public static final String TRIGGER_FILTER_FILE = "TriggerEventFilter.json";
    public static final String INVOKE_TIME = "InvokeTime";

    public static final String WORKSPACES = "Workspaces";

    public static final String ERROR_CATEGORY_FILE = "error-category.json";

    /*-
     * list of tenant that will NOT be force rebuild txn (on the first PA)
     * to migrate off CustomerAccountId
     * TODO remove after all tenants are migrated
     */
    public static final String SKIP_FORCE_TXN_REBUILD_TENANT_LIST = "skipForceTransactionRebuildList";

    public static final String SCHEDULING_GROUP_FILE = "schedulingGroup.json";
    public static final String SCHEDULING_PA_FLAG_FILE = "schedulingPAFlag.json";
    // list of tenants will NOT be considered as large tenant (faster queue time)
    public static final String SCHEDULING_LARGE_TENANT_EXEMPTION_LIST = "schedulingLargeTenantExemptionList";

    public static final String SCHEDULER_CONFIG = "paSchedulerConfig";

    public static final String SCHEDULING_TENANT_GROUP = "paSchedulerTenantGroupConfig";

    public static final String WORKFLOW_THROTTLING_CONFIG_FILE = "workflowThrottlingConfig";

    public static final String WORKFLOW_THROTTLING_FLAG = "workflowThrottlingFlag";

    public static final String ACTIVITY_UPLOAD_QUOTA = "activityUploadQuota";

    public static final String LOOKUP_ID_LIMIT = "lookupIdLimit";

    public static final String PLS = "PLS";
    public static final String CDL = "CDL";

    public static final String CATALOG_QUOTA_LIMIT_FILE = "catalogQuotaLimit.json";
}
