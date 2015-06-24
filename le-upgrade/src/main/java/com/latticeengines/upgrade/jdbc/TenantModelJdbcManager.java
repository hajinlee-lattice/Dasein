package com.latticeengines.upgrade.jdbc;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.upgrade.yarn.YarnPathUtils;

@Component("jdbcManager")
public class TenantModelJdbcManager {

    @Autowired
    private JdbcTemplate tenantModelInfoJdbcTemlate;

    public List<String> getTenantsToUpgrade() {
        List<Map<String, Object>> results = tenantModelInfoJdbcTemlate.queryForList("SELECT TenantName FROM TenantModel_Info");
        List<String> tenants = new ArrayList<>();
        for (Map<String, Object> entry: results) {
            tenants.add(( String ) entry.get("TenantName"));
        }
        return tenants;
    }

    public List<String> getActiveModels(String dlTenantName) {
        List<Map<String, Object>> results = tenantModelInfoJdbcTemlate.queryForList(
                "SELECT ModelGUID FROM TenantModel_Info WHERE TenantName = \'" + dlTenantName + "\'"
        );
        List<String> models = new ArrayList<>();
        for (Map<String, Object> entry: results) {
            models.add(( String ) entry.get("ModelGUID"));
        }
        return models;
    }

    public List<String> getActiveUuids(String dlTenantName) {
        List<String> modelGuids = getActiveModels(dlTenantName);
        List<String> uuids = new ArrayList<>();
        for (String modelGuid: modelGuids) {
            uuids.add(YarnPathUtils.extractUuid(modelGuid));
        }
        return uuids;
    }

    public void populateTenantModelInfo(String dlTenantName, String modelGuid) {
        tenantModelInfoJdbcTemlate.execute("IF NOT EXISTS (SELECT * FROM TenantModel_Info where TenantName = \'" + dlTenantName
                + "\') insert into TenantModel_Info values (\'" + dlTenantName + "\', \'" + modelGuid + "\')");
    }

    public boolean modelIsActive(String dlTenantName, String modelGuid) {
        String uuid = YarnPathUtils.extractUuid(modelGuid);
        List<String> uuids = getActiveUuids(dlTenantName);
        return uuids.contains(uuid);
    }

}
