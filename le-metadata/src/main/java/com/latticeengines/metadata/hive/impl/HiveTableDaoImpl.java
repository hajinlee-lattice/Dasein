package com.latticeengines.metadata.hive.impl;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.hive.HiveTableDao;
import com.latticeengines.metadata.hive.util.HiveUtils;
import com.latticeengines.security.exposed.util.SecurityContextUtils;

@Component("hiveTableDao")
public class HiveTableDaoImpl implements HiveTableDao {

    private Logger log = Logger.getLogger(HiveTableDaoImpl.class);

    @Autowired
    private DataSource hiveDataSource;

    private Configuration yarnConfiguration = new Configuration();

    @Override
    public void create(Table table) {
        if (table.getExtracts().size() == 0) {
            log.info(String.format("Not creating table %s because there are no available extracts", table.getName()));
            return;
        }
        try (Connection connection = getConnection()) {
            String tableName = getTableName(table);
            String sql = HiveUtils.getCreateStatement(yarnConfiguration, tableName, table);
            executeSql(connection, sql);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to create table %s in hive", table.getName()), e);
        }

        test(table);
    }

    @Override
    public void test(Table table) {
        try (Connection connection = getConnection()) {
            String tableName = getTableName(table);
            String sql = String.format("SELECT * FROM %s LIMIT 1", tableName);
            executeSql(connection, sql);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Failure occurred when testing table %s", table.getName()), e);
        }
    }

    @Override
    public void deleteIfExists(Table table) {
        try (Connection connection = getConnection()) {
            String tableName = getTableName(table);
            String sql = HiveUtils.getDropStatement(tableName, true);
            executeSql(connection, sql);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to delete table %s in hive", table.getName()), e);
        }
    }

    private void executeSql(Connection connection, String sql) throws SQLException {

        try (Statement stmt = connection.createStatement()) {
            log.info(String.format("Executing sql %s", sql));
            stmt.execute(sql);
            stmt.getResultSet();
        }
    }

    private String getTableName(Table table) {
        Tenant tenant = SecurityContextUtils.getTenant();
        table.setTenant(tenant);
        return String.format("%s___%s___%s", getSafeTenantId(tenant), table.getName(), table.getTableType());
    }

    private String getSafeTenantId(Tenant tenant) {
        return tenant.getId().replace(".", "_").replace(" ", "_");
    }

    private Connection getConnection() throws SQLException {
        return hiveDataSource.getConnection();
    }
}
