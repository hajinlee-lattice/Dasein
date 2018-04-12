package com.latticeengines.workflow.core;

import static com.latticeengines.common.exposed.bean.BeanFactoryEnvironment.Environment.WebApp;

import javax.sql.DataSource;

import com.latticeengines.common.exposed.bean.BeanFactoryEnvironment;
import com.latticeengines.common.exposed.bean.DataSourceBeanFactory;

public class SpringBatchDataSourceBeanFactory extends DataSourceBeanFactory {

    private int webAppMaxPoolSize;
    private int yarnContainerMaxPoolSize;
    private String jdbcUrlReader;

    @Override
    public DataSource getObject() {
        BeanFactoryEnvironment.Environment currentEnv = BeanFactoryEnvironment.getEnvironment();
        if (currentEnv == null) {
            throw new IllegalStateException(
                    "BeanFactoryEnvironment has not been initialized yet, check context loading sequence");
        }
        if (WebApp.equals(currentEnv)) {
            setMaxPoolSize(webAppMaxPoolSize);
//            setJdbcUrl(jdbcUrlReader);
        } else {
            setWebAppMaxPoolSize(yarnContainerMaxPoolSize);
        }
        return constructDataSource();
    }

    public int getWebAppMaxPoolSize() {
        return webAppMaxPoolSize;
    }

    public void setWebAppMaxPoolSize(int webAppMaxPoolSize) {
        this.webAppMaxPoolSize = webAppMaxPoolSize;
    }

    public int getYarnContainerMaxPoolSize() {
        return yarnContainerMaxPoolSize;
    }

    public void setYarnContainerMaxPoolSize(int yarnContainerMaxPoolSize) {
        this.yarnContainerMaxPoolSize = yarnContainerMaxPoolSize;
    }

    public String getJdbcUrlReader() {
        return jdbcUrlReader;
    }

    public void setJdbcUrlReader(String jdbcUrlReader) {
        this.jdbcUrlReader = jdbcUrlReader;
    }
}
