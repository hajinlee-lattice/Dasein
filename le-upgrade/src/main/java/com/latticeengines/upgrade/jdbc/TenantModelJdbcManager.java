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

    public void populateTenantModelInfo(String dlTenantName, String modelGuid) {
        upgradeJdbcTemlate.execute("IF NOT EXISTS (SELECT * FROM TenantModel_Info where TenantName = \'" + dlTenantName
                + "\') insert into TenantModel_Info values (\'" + dlTenantName + "\', \'" + modelGuid + "\')");
    }

}
