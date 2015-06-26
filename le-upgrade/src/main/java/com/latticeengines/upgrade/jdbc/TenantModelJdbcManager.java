package com.latticeengines.upgrade.jdbc;

import java.util.ArrayList;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import com.latticeengines.upgrade.yarn.YarnPathUtils;

@Component("tenantModelJdbcManager")
public class TenantModelJdbcManager {

    @Autowired
    private JdbcTemplate tenantModelInfoJdbcTemlate;

    private static final String EXTERNAL_TENANT_MODEL_TABLE = "TenantModel_Info";

    private static final String INTERNAL_TENANT_MODEL_TABLE = "TenantModel_Info_Internal";

    public List<String> getTenantsToUpgrade() {
        List<String> tenants = tenantModelInfoJdbcTemlate.queryForList("SELECT TenantName FROM "
                + INTERNAL_TENANT_MODEL_TABLE, String.class);
        return tenants;
    }

    public List<String> getActiveModels(String dlTenantName) {
        List<String> models = tenantModelInfoJdbcTemlate.queryForList("SELECT ModelGUID FROM "
                + INTERNAL_TENANT_MODEL_TABLE + " WHERE TenantName = \'" + dlTenantName + "\'", String.class);
        return models;
    }

    public List<String> getActiveUuids(String dlTenantName) {
        List<String> modelGuids = getActiveModels(dlTenantName);
        List<String> uuids = new ArrayList<>();
        for (String modelGuid : modelGuids) {
            uuids.add(YarnPathUtils.extractUuid(modelGuid));
        }
        return uuids;
    }

    public void populateExternalTenantModelInfo(String dlTenantName, String modelGuid) {
        tenantModelInfoJdbcTemlate.execute("IF NOT EXISTS (SELECT * FROM " + EXTERNAL_TENANT_MODEL_TABLE
                + " where TenantName = \'" + dlTenantName + "\') insert into " + EXTERNAL_TENANT_MODEL_TABLE
                + " values (\'" + dlTenantName + "\', \'" + modelGuid + "\')");
    }

    public void populateInternalTenantModelInfo(String dlTenantName, String modelGuid) {
        tenantModelInfoJdbcTemlate.execute("IF NOT EXISTS (SELECT * FROM " + INTERNAL_TENANT_MODEL_TABLE
                + " where TenantName = \'" + dlTenantName + "\') insert into " + INTERNAL_TENANT_MODEL_TABLE
                + " values (\'" + dlTenantName + "\', \'" + modelGuid + "\')");
    }

    public boolean modelIsActive(String dlTenantName, String uuid) {
        List<String> uuids = getActiveUuids(dlTenantName);
        return uuids.contains(uuid);
    }

}
