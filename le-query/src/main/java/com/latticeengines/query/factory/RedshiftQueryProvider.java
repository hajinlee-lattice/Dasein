package com.latticeengines.query.factory;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.JdbcStorage;
import com.latticeengines.domain.exposed.metadata.Table;
import com.querydsl.sql.Configuration;
import com.querydsl.sql.PostgreSQLTemplates;
import com.querydsl.sql.SQLQueryFactory;
import com.querydsl.sql.SQLTemplates;

@Component("redshiftQueryProvider")
public class RedshiftQueryProvider extends QueryProvider {

    @Autowired
    @Qualifier("redshiftDataSource")
    private DataSource redshiftDataSource;

    @Override
    public boolean providesQueryAgainst(DataCollection dataCollection) {
        return dataCollection //
                .getTables() //
                .stream() //
                .map(Table::getStorageMechanism) //
                .allMatch(
                        storageMechanism -> storageMechanism instanceof JdbcStorage
                                && ((JdbcStorage) storageMechanism).getDatabaseName() == JdbcStorage.DatabaseName.REDSHIFT);
    }

    @Override
    protected SQLQueryFactory getSQLQueryFactory() {
        SQLTemplates templates = new PostgreSQLTemplates();
        Configuration configuration = new Configuration(templates);
        return new SQLQueryFactory(configuration, redshiftDataSource);
    }
}
