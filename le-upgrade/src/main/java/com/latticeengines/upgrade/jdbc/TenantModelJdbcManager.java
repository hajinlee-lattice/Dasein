package com.latticeengines.upgrade.jdbc;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.stereotype.Component;

import com.latticeengines.upgrade.yarn.YarnPathUtils;

@Component("jdbcManager")
public class TenantModelJdbcManager {

    @Autowired
    private JdbcTemplate upgradeJdbcTemlate;

    @Value("${upgrade.dao.tenant.model.info.jdbc}")
    private String tenantModelInfoJDBC;

    @Value("${upgrade.dao.datasource.user}")
    private String user;

    @Value("${upgrade.dao.datasource.password.encrypted}")
    private String pass;

    @PostConstruct
    private void setJDBCDataSource() {
        DataSource infoDataSource = new DriverManagerDataSource(tenantModelInfoJDBC, user, pass);
        upgradeJdbcTemlate.setDataSource(infoDataSource);
    }

    public List<String> getTenantsToUpgrade() {
        List<Map<String, Object>> results = upgradeJdbcTemlate.queryForList("SELECT TenantName FROM TenantModel_Info");
        List<String> tenants = new ArrayList<>();
        for (Map<String, Object> entry: results) {
            tenants.add(( String ) entry.get("TenantName"));
        }
        return tenants;
    }

    public List<String> getActiveModels(String dlTenantName) {
        List<Map<String, Object>> results = upgradeJdbcTemlate.queryForList(
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
        upgradeJdbcTemlate.execute("IF NOT EXISTS (SELECT * FROM TenantModel_Info where TenantName = \'" + dlTenantName
                + "\') insert into TenantModel_Info values (\'" + dlTenantName + "\', \'" + modelGuid + "\')");
    }

    public boolean modelIsActive(String dlTenantName, String modelGuid) {
        String uuid = YarnPathUtils.extractUuid(modelGuid);
        List<String> uuids = getActiveUuids(dlTenantName);
        return uuids.contains(uuid);
    }

}
