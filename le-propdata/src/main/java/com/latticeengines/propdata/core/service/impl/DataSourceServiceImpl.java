package com.latticeengines.propdata.core.service.impl;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.core.service.DataSourceService;
import com.latticeengines.propdata.core.service.Database;
import com.latticeengines.propdata.core.service.SQLDialect;

@Component
public class DataSourceServiceImpl implements DataSourceService {

    @Autowired
    @Qualifier(value = "propDataManage")
    private DriverManagerDataSource manageDataSource;

    private Map<Database, SQLDialect> dialectMap;

    @PostConstruct
    private void postConstruct() {
        dialectMap = new HashMap<>();
        dialectMap.put(Database.MANAGE,
                manageDataSource.getUrl().contains("mysql") ? SQLDialect.MYSQL : SQLDialect.SQLSERVER);
    }

    @Override
    public SQLDialect getSqlDialect(Database db) { return dialectMap.get(db); }

}
