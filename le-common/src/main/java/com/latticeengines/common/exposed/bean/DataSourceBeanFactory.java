package com.latticeengines.common.exposed.bean;

import static com.latticeengines.common.exposed.bean.BeanFactoryEnvironment.Environment.WebApp;

import java.beans.PropertyVetoException;

import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.jndi.JndiTemplate;

import com.latticeengines.common.exposed.bean.BeanFactoryEnvironment.Environment;
import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.common.exposed.util.StackTraceUtils;
import com.mchange.v2.c3p0.ComboPooledDataSource;

public class DataSourceBeanFactory implements FactoryBean<DataSource> {

    private static final Logger log = LoggerFactory.getLogger(DataSourceBeanFactory.class);
    private static final String WRITE_CONNECTION_TEST_QUERY = //
            "SELECT CASE WHEN @@read_only + @@innodb_read_only < 1 THEN 1 "
            + "ELSE (SELECT table_name FROM information_schema.tables LIMIT 2) END AS `1`";

    // if use jndi
    private String jndiName;

    // if not using jndi
    private String driverClass;
    private String jdbcUrl;
    private String user;
    private String password;
    private Boolean enableDebugSlowSql;
    private Boolean writerConnection;
    private int minPoolSize = -1;
    private int maxPoolSize = -1;
    private String poolSizePropKeyPrefix;
    private int maxPoolSizeForWebApp = -1;
    private int maxPoolSizeForAppMaster = -1;
    private int acquireIncrement = -1;
    private int maxIdleTime = -1;
    private int maxIdleTimeExcessConnections = -1;
    private int numHelperThreads = -1;

    @Override
    public DataSource getObject() {
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

    protected DataSource constructDataSource() {
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

        // Give a meaningful name for better troubleshooting
        String dbName;
        try {
            dbName = jdbcUrl.substring(jdbcUrl.lastIndexOf("/"),
                    jdbcUrl.indexOf("?", jdbcUrl.lastIndexOf("/")));
        } catch (Exception e) {
            dbName = jdbcUrl.substring(0, jdbcUrl.lastIndexOf("/"));
        }

        BeanFactoryEnvironment.Environment currentEnv = BeanFactoryEnvironment.getEnvironment();
        String currentSvc = BeanFactoryEnvironment.getService();
        int maxPoolSize;
        int minPoolSize;
        if (StringUtils.isBlank(this.poolSizePropKeyPrefix)) {
            maxPoolSize = assignLegacyMaxPoolSize(currentEnv);
            minPoolSize = assignLegacyMinPoolSize();
        } else {
            maxPoolSize = getMaxPoolFromProp(currentEnv, currentSvc);
            minPoolSize = getMinPoolFromProp(currentEnv, currentSvc);
        }
        // avoid max < min
        maxPoolSize = maxPoolSize > minPoolSize ? maxPoolSize : Math.max(minPoolSize, 8);

        if (log.isInfoEnabled()) {
            if (maxPoolSize > 1) {
                log.info("Setting Max Connections of {} to: {}, for Envrionment: {}, Service: {}", //
                        dbName, maxPoolSize, currentEnv, currentSvc);
            }
            if (minPoolSize > 0) {
                log.info("Setting Min Connections of {} to: {}, for Envrionment: {}, Service: {}", //
                        dbName, minPoolSize, currentEnv, currentSvc);
            }
        }

        if (log.isDebugEnabled()) {
            log.debug("Stack Trace: {} ", StackTraceUtils.getCurrentStackTrace());
        }

        cpds.setDataSourceName(
                String.format("%s-%s", currentEnv, dbName.replaceAll("[^A-Za-z0-9]", "")));
        int acquireIncrement = this.acquireIncrement > 0 ? this.acquireIncrement
                : (Math.max(3, maxPoolSize / 10));
        cpds.setMinPoolSize(minPoolSize);
        cpds.setInitialPoolSize(minPoolSize);
        cpds.setMaxPoolSize(maxPoolSize);
        cpds.setAcquireIncrement(acquireIncrement);

        cpds.setCheckoutTimeout(60000);
        int maxIdleTime = this.maxIdleTime >= 0 ? this.maxIdleTime : 3600;
        cpds.setMaxIdleTime(maxIdleTime);
        int maxIdleTimeExcessConnections = this.maxIdleTimeExcessConnections >= 0
                ? this.maxIdleTimeExcessConnections : 60;
        cpds.setMaxIdleTimeExcessConnections(maxIdleTimeExcessConnections);
        // cpds.setPreferredTestQuery(preferredTestQuery);
        cpds.setNumHelperThreads(
                this.numHelperThreads > 0 ? this.numHelperThreads : Math.max(3, maxPoolSize / 10));

        if (Boolean.TRUE.equals(this.writerConnection)) {
            // For Failover case, we need to evict the old cached connection and
            // get latest writer connection.
            cpds.setTestConnectionOnCheckout(true);
            cpds.setPreferredTestQuery(WRITE_CONNECTION_TEST_QUERY);
        } else if (Environment.AppMaster == currentEnv) {
            // For Yarn jobs, we want to make sure that connection is in good
            // state, because retry of Yarn job will be costly.
            cpds.setTestConnectionOnCheckout(true);
        } else {
            cpds.setIdleConnectionTestPeriod(60);
            cpds.setTestConnectionOnCheckin(true);
        }

        boolean enableDebugSlowSql = this.enableDebugSlowSql == null ? true
                : this.enableDebugSlowSql;
        if (enableDebugSlowSql) {
            int timeoutInSeconds = 30;
            if (Environment.AppMaster == currentEnv) {
                timeoutInSeconds = 120;
            }
            cpds.setUnreturnedConnectionTimeout(timeoutInSeconds);
            cpds.setDebugUnreturnedConnectionStackTraces(true);
        }

        return cpds;
    }

    private DataSource readJndiDataSource() {
        try {
            log.info("Reading jndi object " + jndiName);
            JndiTemplate jndiTemplate = new JndiTemplate();
            return jndiTemplate.lookup(jndiName, DataSource.class);
        } catch (Exception e) {
            // As this is expected warning message on QA and Prod, we no need to
            // log the full exception trace
            log.warn("Cannot read jndi datasource named:{}, Reason: {}", jndiName, e.getMessage());
            return null;
        }
    }

    private int assignLegacyMaxPoolSize(BeanFactoryEnvironment.Environment env) {
        int maxPoolSize;
        switch (env) {
            case WebApp:
                maxPoolSize = this.maxPoolSizeForWebApp;
                break;
            case AppMaster:
                maxPoolSize = this.maxPoolSizeForAppMaster;
                break;
            case TestClient:
            default:
                maxPoolSize = this.maxPoolSize;
        }
        // If MaxPoolSize is not configured at environment level, then use
        // default MaxPoolSize
        maxPoolSize = maxPoolSize > 0 ? maxPoolSize : this.maxPoolSize;
        return maxPoolSize;
    }

    private int assignLegacyMinPoolSize() {
        return this.minPoolSize > 0 ? this.minPoolSize : 0;
    }

    private int getMaxPoolFromProp(BeanFactoryEnvironment.Environment env, String svc) {
        String prefix = this.poolSizePropKeyPrefix + ".max";
        String prop = getPropertyStr(prefix, env, svc);
        if (StringUtils.isBlank(prop)) {
            return 1;
        } else {
            return Integer.valueOf(prop);
        }
    }

    private int getMinPoolFromProp(BeanFactoryEnvironment.Environment env, String svc) {
        String prefix = this.poolSizePropKeyPrefix + ".min";
        String prop = getPropertyStr(prefix, env, svc);
        if (StringUtils.isBlank(prop)) {
            return 0;
        } else {
            return Integer.valueOf(prop);
        }
    }

    private static String getPropertyStr(String prefix, BeanFactoryEnvironment.Environment env,
            String svc) {
        String key = prefix + getFullSuffix(env, svc);
        String prop = PropertyUtils.getProperty(key);
        if (StringUtils.isBlank(prop)) {
            key = prefix + getEnvSuffix(env);
            prop = PropertyUtils.getProperty(key);
        }
        if (StringUtils.isBlank(prop)) {
            key = prefix;
            prop = PropertyUtils.getProperty(key);
        }
        return prop;
    }

    private static String getEnvSuffix(BeanFactoryEnvironment.Environment env) {
        return "." + env.name().toLowerCase();
    }

    private static String getFullSuffix(BeanFactoryEnvironment.Environment env, String svc) {
        String suffix = getEnvSuffix(env);
        if (StringUtils.isNotBlank(svc)) {
            return suffix + "." + svc.toLowerCase();
        } else {
            return suffix;
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

    public String getPoolSizePropKeyPrefix() {
        return poolSizePropKeyPrefix;
    }

    public void setPoolSizePropKeyPrefix(String poolSizePropKeyPrefix) {
        this.poolSizePropKeyPrefix = poolSizePropKeyPrefix;
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

    public int getMaxPoolSizeForWebApp() {
        return maxPoolSizeForWebApp;
    }

    public void setMaxPoolSizeForWebApp(int maxPoolSizeForWebApp) {
        this.maxPoolSizeForWebApp = maxPoolSizeForWebApp;
    }

    public int getMaxPoolSizeForAppMaster() {
        return maxPoolSizeForAppMaster;
    }

    public void setMaxPoolSizeForAppMaster(int maxPoolSizeForAppMaster) {
        this.maxPoolSizeForAppMaster = maxPoolSizeForAppMaster;
    }

    public int getAcquireIncrement() {
        return acquireIncrement;
    }

    public void setAcquireIncrement(int acquireIncrement) {
        this.acquireIncrement = acquireIncrement;
    }

    public Boolean getEnableDebugSlowSql() {
        return enableDebugSlowSql;
    }

    public void setEnableDebugSlowSql(Boolean enableDebugSlowSql) {
        this.enableDebugSlowSql = enableDebugSlowSql;
    }

    public Boolean getWriterConnection() {
        return writerConnection;
    }

    public void setWriterConnection(Boolean writerConnection) {
        this.writerConnection = writerConnection;
    }

    public int getMaxIdleTime() {
        return maxIdleTime;
    }

    public void setMaxIdleTime(int maxIdleTime) {
        this.maxIdleTime = maxIdleTime;
    }

    public int getMaxIdleTimeExcessConnections() {
        return maxIdleTimeExcessConnections;
    }

    public void setMaxIdleTimeExcessConnections(int maxIdleTimeExcessConnections) {
        this.maxIdleTimeExcessConnections = maxIdleTimeExcessConnections;
    }

    public int getNumHelperThreads() {
        return numHelperThreads;
    }

    public void setNumHelperThreads(int numHelperThreads) {
        this.numHelperThreads = numHelperThreads;
    }
}
