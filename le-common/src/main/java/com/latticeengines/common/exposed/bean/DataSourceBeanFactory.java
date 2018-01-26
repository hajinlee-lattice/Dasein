package com.latticeengines.common.exposed.bean;

import static com.latticeengines.common.exposed.bean.BeanFactoryEnvironment.Environment.WebApp;

import java.beans.PropertyVetoException;

import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.jndi.JndiTemplate;

import com.mchange.v2.c3p0.ComboPooledDataSource;

public class DataSourceBeanFactory implements FactoryBean<DataSource> {

    private static final Logger log = LoggerFactory.getLogger(DataSourceBeanFactory.class);

    // if use jndi
    private String jndiName;

    // if not using jndi
    private String driverClass;
    private String jdbcUrl;
    private String user;
    private String password;
    private int minPoolSize = -1;
    private int maxPoolSize = -1;
    private int acquireIncrement = -1;

    @Override
    public DataSource getObject() throws Exception {
        DataSource ds = null;
        BeanFactoryEnvironment.Environment currentEnv = BeanFactoryEnvironment.getEnvironment();
        if (currentEnv == null) {
            throw new IllegalStateException(
                    "BeanFactoryEnvironment has not been initialized yet, check context loading sequence");
        }
        if (WebApp.equals(currentEnv) && StringUtils.isNotBlank(jndiName)) {
            ds = readJndiDataSource();
        }
        if (ds == null) {
            ds = constructDataSource();
        }
        return ds;
    }

    @Override
    public Class<?> getObjectType() {
        return DataSource.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    private DataSource constructDataSource() {
        log.info("Constructing c3p0 connection pool for " + jdbcUrl);
        ComboPooledDataSource cpds = new ComboPooledDataSource();
        try {
            cpds.setDriverClass(driverClass); // loads the jdbc driver
        } catch (PropertyVetoException e) {
            throw new RuntimeException(e);
        }
        cpds.setJdbcUrl(jdbcUrl);
        cpds.setUser(user);
        cpds.setPassword(password);

        int minPoolSize = this.minPoolSize >= 0 ? this.minPoolSize : 1;
        int maxPoolSize = this.maxPoolSize > minPoolSize ? this.maxPoolSize : Math.max(minPoolSize, 8);
        int acquireIncrement = this.acquireIncrement > 0 ? this.acquireIncrement : 2;
        cpds.setMinPoolSize(minPoolSize);
        cpds.setInitialPoolSize(minPoolSize);
        cpds.setMinPoolSize(maxPoolSize);
        cpds.setAcquireIncrement(acquireIncrement);

        cpds.setCheckoutTimeout(60000);
        cpds.setMaxIdleTime(30);
        cpds.setMaxIdleTimeExcessConnections(10);
        return cpds;
    }

    private DataSource readJndiDataSource() {
        try {
            log.info("Reading jndi object " + jndiName);
            JndiTemplate jndiTemplate = new JndiTemplate();
            return jndiTemplate.lookup(jndiName, DataSource.class);
        } catch (Exception e) {
            log.warn("Cannot read jndi datasource named " + jndiName, e);
            return null;
        }
    }

    public String getJndiName() {
        return jndiName;
    }

    public void setJndiName(String jndiName) {
        this.jndiName = jndiName;
    }

    public String getDriverClass() {
        return driverClass;
    }

    public void setDriverClass(String driverClass) {
        this.driverClass = driverClass;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getMinPoolSize() {
        return minPoolSize;
    }

    public void setMinPoolSize(int minPoolSize) {
        this.minPoolSize = minPoolSize;
    }

    public int getMaxPoolSize() {
        return maxPoolSize;
    }

    public void setMaxPoolSize(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
    }

    public int getAcquireIncrement() {
        return acquireIncrement;
    }

    public void setAcquireIncrement(int acquireIncrement) {
        this.acquireIncrement = acquireIncrement;
    }

}
