package com.latticeengines.upgrade.jdbc;

import java.util.ArrayList;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.upgrade.domain.TenantModelInfo;
import com.latticeengines.upgrade.yarn.YarnPathUtils;

@Component("jdbcManager")
public class TenantModelJdbcManager {

    @Autowired
    private JdbcTemplate tenantModelInfoJdbcTemlate;

    private static final String EXTERNAL_TENANT_MODEL_TABLE = "TenantModel_Info";

    private static final String INTERNAL_TENANT_MODEL_TABLE = "TenantModel_Info_Internal";

    public List<String> getTenantsToUpgrade() {
        List<TenantModelInfo> results = tenantModelInfoJdbcTemlate.query("SELECT TenantName FROM "
                + EXTERNAL_TENANT_MODEL_TABLE, new BeanPropertyRowMapper<TenantModelInfo>(TenantModelInfo.class));
        List<String> tenants = new ArrayList<>();
        for (TenantModelInfo tenantModelInfo : results) {
            tenants.add(tenantModelInfo.getTenant());
        }
        return tenants;
    }

    public List<String> getActiveModels(String dlTenantName) {
        List<TenantModelInfo> results = tenantModelInfoJdbcTemlate.query("SELECT ModelGUID FROM "
                + EXTERNAL_TENANT_MODEL_TABLE + " WHERE TenantName = \'" + dlTenantName + "\'",
                new BeanPropertyRowMapper<TenantModelInfo>(TenantModelInfo.class));
        List<String> models = new ArrayList<>();
        for(TenantModelInfo tenantModelInfo : results) {
            models.add(tenantModelInfo.getModelGuid());
        }
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
        tenantModelInfoJdbcTemlate.execute("IF NOT EXISTS (SELECT * FROM TenantModel_Info where TenantName = \'"
                + dlTenantName + "\') insert into " + EXTERNAL_TENANT_MODEL_TABLE + " values (\'" + dlTenantName
                + "\', \'" + modelGuid + "\')");
    }

    public void populateInternalTenantModelInfo(String dlTenantName, String modelGuid) {
        tenantModelInfoJdbcTemlate.execute("IF NOT EXISTS (SELECT * FROM TenantModel_Info where TenantName = \'"
                + dlTenantName + "\') insert into " + INTERNAL_TENANT_MODEL_TABLE + " values (\'" + dlTenantName
                + "\', \'" + modelGuid + "\')");
    }

    public boolean modelIsActive(String dlTenantName, String modelGuid) {
        String uuid = YarnPathUtils.extractUuid(modelGuid);
        List<String> uuids = getActiveUuids(dlTenantName);
        return uuids.contains(uuid);
    }

}
