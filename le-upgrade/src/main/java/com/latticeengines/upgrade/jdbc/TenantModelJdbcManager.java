package com.latticeengines.upgrade.jdbc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
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
    private static final String TENANT_UPGRADED_TABLE = "TenantUpgraded";
    private DateTimeFormatter SQL_DATETIME_FMT = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");

    public List<String> getTenantsToUpgrade() {
        return tenantModelInfoJdbcTemlate.queryForList("SELECT TenantName FROM "
                + INTERNAL_TENANT_MODEL_TABLE, String.class);
    }

    public List<String> getActiveModels(String dlTenantName) {
        List<String> lists = tenantModelInfoJdbcTemlate.queryForList("SELECT ModelGUID FROM "
                + INTERNAL_TENANT_MODEL_TABLE + " WHERE TenantName = \'" + dlTenantName + "\'", String.class);
        List<String> models = new ArrayList<>();
        for (String list: lists) {
            if (!list.contains("No Active Model")) {
                models.addAll(Arrays.asList(list.split(",")));
            }

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
        tenantModelInfoJdbcTemlate.execute("IF NOT EXISTS (SELECT * FROM " + EXTERNAL_TENANT_MODEL_TABLE
                + " where TenantName = \'" + dlTenantName + "\') insert into " + EXTERNAL_TENANT_MODEL_TABLE
                + " values (\'" + dlTenantName + "\', \'" + modelGuid + "\')");
    }

    public void populateInternalTenantModelInfo(String dlTenantName, String modelGuid) {
        tenantModelInfoJdbcTemlate.execute("IF NOT EXISTS (SELECT * FROM " + INTERNAL_TENANT_MODEL_TABLE
                + " where TenantName = \'" + dlTenantName + "\') insert into " + INTERNAL_TENANT_MODEL_TABLE
                + " values (\'" + dlTenantName + "\', \'" + modelGuid + "\')");
    }

    public void removeDuplicatedTenantModelInfo() {
        tenantModelInfoJdbcTemlate.execute("WITH dup AS (" +
                " SELECT TenantName, ModelGUID," +
                "    ROW_NUMBER() OVER(PARTITION BY TenantName, ModelGUID ORDER BY TenantName) AS RN" +
                " FROM " + EXTERNAL_TENANT_MODEL_TABLE + ")" +
                " DELETE dup WHERE RN > 1");
    }

    public boolean modelIsActive(String dlTenantName, String modlGuidOrUuid) {
        String uuid = YarnPathUtils.extractUuid(modlGuidOrUuid);
        List<String> uuids = getActiveUuids(dlTenantName);
        return uuids.contains(uuid);
    }

    public boolean modelShouldBeActive(String modlGuidOrUuid) {
        List<String> modelGuids = tenantModelInfoJdbcTemlate.queryForList("SELECT ModelGUID FROM "
                + EXTERNAL_TENANT_MODEL_TABLE, String.class);
        for (String modelGuid: modelGuids) {
            if (modelGuid.contains(YarnPathUtils.extractUuid(modlGuidOrUuid)))
                return true;
        }
        return false;
    }

    public void populateUpgradeSummary(UpgradeSummary summary) {
        if (!hasBeenUpgraded(summary.tenantName)) {
            tenantModelInfoJdbcTemlate.execute("INSERT "
                            + TENANT_UPGRADED_TABLE + " VALUES ("
                            + "\'" + summary.tenantName + "\', "
                            + "\'" + summary.activeModelGuid + "\', "
                            + summary.modelsInHdfs + ", "
                            + summary.modelsummariesInHdfs + ", "
                            + summary.modelsInLp + ", "
                            + "\'" + SQL_DATETIME_FMT.print(summary.upgradedAt) + "\')");
        }
    }

    public boolean hasBeenUpgraded(String tenantName) {
        List<String> tenants = tenantModelInfoJdbcTemlate.queryForList("SELECT TenantName FROM "
                + TENANT_UPGRADED_TABLE + " WHERE TenantName = \'" + tenantName + "\'", String.class);
        return !tenants.isEmpty();
    }

    public void removeUpgradeSummary(String tenantName) {
        tenantModelInfoJdbcTemlate.execute("DELETE " + TENANT_UPGRADED_TABLE
                + " WHERE TenantName = \'" + tenantName + "\'");
    }
}
