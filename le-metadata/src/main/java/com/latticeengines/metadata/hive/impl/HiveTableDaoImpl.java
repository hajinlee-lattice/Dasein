package com.latticeengines.metadata.hive.impl;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.hive.HiveTableDao;
import com.latticeengines.metadata.hive.util.HiveUtils;

@Component("hiveTableDao")
public class HiveTableDaoImpl implements HiveTableDao {

    private Logger log = LoggerFactory.getLogger(HiveTableDaoImpl.class);

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    @Qualifier("hiveJdbcTemplate")
    private JdbcTemplate hiveJdbcTemplate;

    @Override
    public void create(Table table) {
        if (table.getExtracts().size() == 0) {
            log.info(String.format("Not creating table %s because there are no available extracts", table.getName()));
            return;
        }
        try {
            String tableName = getTableName(table);
            String sql = HiveUtils.getCreateStatement(yarnConfiguration, tableName, table);
            hiveJdbcTemplate.execute(sql);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to create table %s in hive", table.getName()), e);
        }

        test(table);
    }

    @Override
    public void create(String tableName, String avroDir, String avscPath) {
        try {
            String sql = AvroUtils.generateHiveCreateTableStatement(tableName, avroDir, avscPath);
            hiveJdbcTemplate.execute(sql);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to create table %s in hive", tableName), e);
        }
        test(tableName);
    }

    @Override
    public void test(Table table) {
        String tableName = getTableName(table);
        test(tableName);
    }

    @Override
    public void test(String tableName) {
        try {
            String sql = String.format("SELECT * FROM %s LIMIT 1", tableName);
            hiveJdbcTemplate.execute(sql);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Failure occurred when testing table %s", tableName), e);
        }
    }

    @Override
    public void deleteIfExists(Table table) {
        String tableName = getTableName(table);
        deleteIfExists(tableName);
    }

    @Override
    public void deleteIfExists(String tableName) {
        try {
            String sql = HiveUtils.getDropStatement(tableName, true);
            hiveJdbcTemplate.execute(sql);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to delete table %s in hive", tableName), e);
        }
    }

    private String getTableName(Table table) {
        Tenant tenant = MultiTenantContext.getTenant();
        table.setTenant(tenant);
        return String.format("%s___%s___%s", getSafeTenantId(tenant), table.getName(), table.getTableType());
    }

    private String getSafeTenantId(Tenant tenant) {
        return tenant.getId().replace(".", "_").replace(" ", "_");
    }
}
