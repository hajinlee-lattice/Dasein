package com.latticeengines.query.factory;

import javax.inject.Inject;
import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.query.factory.sqlquery.BaseSQLQueryFactory;
import com.latticeengines.query.factory.sqlquery.RedshiftSQLQueryFactory;
import com.latticeengines.redshiftdb.exposed.service.RedshiftPartitionService;
import com.querydsl.sql.Configuration;
import com.querydsl.sql.PostgreSQLTemplates;
import com.querydsl.sql.SQLTemplates;

@Component("redshiftQueryProvider")
public class RedshiftQueryProvider extends QueryProvider {
    public static final String USER_SEGMENT = "segment";
    public static final String USER_BATCH = "batch";

    @Inject
    private RedshiftPartitionService redshiftPartitionService;

    @Override
    public boolean providesQueryAgainst(AttributeRepository repository, String sqlUser) {
        boolean validUser = USER_BATCH.equalsIgnoreCase(sqlUser) || USER_SEGMENT.equalsIgnoreCase(sqlUser);
        return validUser && getRedshiftDataSource(repository, sqlUser) != null;
    }

    @Override
    protected BaseSQLQueryFactory getSQLQueryFactory(AttributeRepository repository, String sqlUser) {
        SQLTemplates templates = new PostgreSQLTemplates();
        Configuration configuration = new Configuration(templates);
        return new RedshiftSQLQueryFactory(configuration, getRedshiftDataSource(repository, sqlUser));
    }

    private DataSource getRedshiftDataSource(AttributeRepository attrRepo, String sqlUser) {
        String partition = getRedshiftPartition(attrRepo);
        return redshiftPartitionService.getDataSource(partition, sqlUser);
    }

    private String getRedshiftPartition(AttributeRepository attrRepo) {
        String partition = attrRepo.getRedshiftPartition();
        if (StringUtils.isBlank(partition)) {
            return redshiftPartitionService.getLegacyPartition();
        } else {
            return partition;
        }
    }

}
