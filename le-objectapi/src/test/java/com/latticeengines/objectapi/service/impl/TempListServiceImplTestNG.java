package com.latticeengines.objectapi.service.impl;

import static com.latticeengines.query.factory.AthenaQueryProvider.ATHENA_USER;
import static com.latticeengines.query.factory.PrestoQueryProvider.PRESTO_USER;
import static com.latticeengines.query.factory.RedshiftQueryProvider.USER_SEGMENT;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;
import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.CollectionLookup;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.ConcreteRestriction;
import com.latticeengines.objectapi.service.TempListService;
import com.latticeengines.prestodb.exposed.service.AthenaService;
import com.latticeengines.prestodb.exposed.service.PrestoConnectionService;
import com.latticeengines.prestodb.exposed.service.PrestoDbService;
import com.latticeengines.redshiftdb.exposed.service.RedshiftPartitionService;

public class TempListServiceImplTestNG extends QueryServiceImplTestNGBase {

    @Inject
    private TempListService tempListService;

    @Inject
    private RedshiftPartitionService redshiftPartitionService;

    @Inject
    private PrestoConnectionService prestoConnectionService;

    @Inject
    private AthenaService athenaService;

    @Inject
    private PrestoDbService prestoDbService;

    private final List<String> tempTableNames = new ArrayList<>();
    private final Class<?> fieldClz = String.class;

    @AfterClass(groups = "functional")
    public void tearDown() {
        tempTableNames.forEach(tempTableName -> ((TempListServiceImpl) tempListService).dropTempList(tempTableName));
    }

    @Test(groups = "functional")
    public void testCreateAndDeleteTempListInAthena() {
        String sqlUser = ATHENA_USER;
        String redshiftPartition = "partition";

        ConcreteRestriction restriction = getRestriction();
        String tempTableName = tempListService.createTempListIfNotExists(restriction, fieldClz, sqlUser, redshiftPartition);
        tempTableNames.add(tempTableName);
        String sql = String.format("SELECT value FROM %s", tempTableName);
        List<String> vals = athenaService.queryForList(sql, String.class);
        Assert.assertEquals(StringUtils.join(vals, ","), "a,b,c");

        String tempTableName2 = tempListService.createTempListIfNotExists(restriction, fieldClz, sqlUser, redshiftPartition);
        Assert.assertEquals(tempTableName2, tempTableName);
        vals = athenaService.queryForList(sql, String.class);
        Assert.assertEquals(StringUtils.join(vals, ","), "a,b,c");

        ((TempListServiceImpl) tempListService).dropTempList(tempTableName);
        String existingTempTable =
                ((TempListServiceImpl) tempListService).getExistingTempTable(restriction, fieldClz, //
                        sqlUser, redshiftPartition);
        Assert.assertTrue(StringUtils.isBlank(existingTempTable));
        Assert.assertFalse(athenaService.tableExists(tempTableName));
    }

    @Test(groups = "functional")
    public void testCreateAndDeleteTempListInPresto() {
        if (!prestoConnectionService.isPrestoDbAvailable()) {
            return;
        }
        String sqlUser = PRESTO_USER;
        String redshiftPartition = "partition";
        DataSource dataSource = prestoConnectionService.getPrestoDataSource();
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);

        ConcreteRestriction restriction = getRestriction();
        String tempTableName = tempListService.createTempListIfNotExists(restriction, fieldClz, sqlUser, redshiftPartition);
        tempTableNames.add(tempTableName);
        String sql = String.format("SELECT value FROM %s", tempTableName);
        List<String> vals = jdbcTemplate.queryForList(sql, String.class);
        Assert.assertEquals(StringUtils.join(vals, ","), "a,b,c");

        String tempTableName2 = tempListService.createTempListIfNotExists(restriction, fieldClz, sqlUser, redshiftPartition);
        Assert.assertEquals(tempTableName2, tempTableName);
        vals = jdbcTemplate.queryForList(sql, String.class);
        Assert.assertEquals(StringUtils.join(vals, ","), "a,b,c");

        ((TempListServiceImpl) tempListService).dropTempList(tempTableName);
        String existingTempTable =
                ((TempListServiceImpl) tempListService).getExistingTempTable(restriction, fieldClz, //
                        sqlUser, redshiftPartition);
        Assert.assertTrue(StringUtils.isBlank(existingTempTable));
        Assert.assertFalse(prestoDbService.tableExists(tempTableName));
    }

    @Test(groups = "functional")
    public void testCreateAndDeleteTempListInRedshift() {
        String sqlUser = USER_SEGMENT;
        String redshiftPartition = redshiftPartitionService.getDefaultPartition();
        JdbcTemplate redshiftJdbcTemplate = redshiftPartitionService.getSegmentUserJdbcTemplate(redshiftPartition);

        ConcreteRestriction restriction = getRestriction();

        String tempTableName = tempListService.createTempListIfNotExists(restriction, fieldClz, sqlUser, redshiftPartition);
        tempTableNames.add(tempTableName);

        String sql = String.format("SELECT value FROM %s", tempTableName);
        List<String> vals = redshiftJdbcTemplate.queryForList(sql, String.class);
        Assert.assertEquals(StringUtils.join(vals, ","), "a,b,c");

        String tempTableName2 = tempListService.createTempListIfNotExists(restriction, fieldClz, sqlUser, redshiftPartition);
        Assert.assertEquals(tempTableName2, tempTableName);
        vals = redshiftJdbcTemplate.queryForList(sql, String.class);
        Assert.assertEquals(StringUtils.join(vals, ","), "a,b,c");

        ((TempListServiceImpl) tempListService).dropTempList(tempTableName);
        String existingTempTable =
                ((TempListServiceImpl) tempListService).getExistingTempTable(restriction, fieldClz, sqlUser, redshiftPartition);
        Assert.assertTrue(StringUtils.isBlank(existingTempTable));
        Assert.assertThrows(BadSqlGrammarException.class, () -> redshiftJdbcTemplate.queryForList(sql, String.class));
    }

    private ConcreteRestriction getRestriction() {
        String attrName = "Attr";
        AttributeLookup lhs = new AttributeLookup(BusinessEntity.Account, attrName);
        ComparisonType op = ComparisonType.NOT_IN_COLLECTION;
        CollectionLookup rhs = new CollectionLookup(Arrays.asList("A", "B", "C"));
        return new ConcreteRestriction(false, lhs, op, rhs);
    }

}
