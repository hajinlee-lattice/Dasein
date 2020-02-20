package com.latticeengines.objectapi.service.impl;

import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

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
import com.latticeengines.redshiftdb.exposed.service.RedshiftPartitionService;

public class TempListServiceImplTestNG extends QueryServiceImplTestNGBase {

    @Inject
    private TempListService tempListService;

    @Inject
    private RedshiftPartitionService redshiftPartitionService;

    private String tempTableName;

    @AfterClass(groups = "functional")
    public void tearDown() {
        tempListService.dropTempList(tempTableName);
    }

    @Test(groups = "functional")
    public void testCreateAndDeleteTempList() {
        String redshiftPartition = redshiftPartitionService.getDefaultPartition();
        JdbcTemplate redshiftJdbcTemplate = redshiftPartitionService.getSegmentUserJdbcTemplate(redshiftPartition);

        String attrName = "Attr";
        AttributeLookup lhs = new AttributeLookup(BusinessEntity.Account, attrName);
        ComparisonType op = ComparisonType.NOT_IN_COLLECTION;
        CollectionLookup rhs = new CollectionLookup(Arrays.asList("A", "B", "C"));
        ConcreteRestriction restriction = new ConcreteRestriction(false, lhs, op, rhs);

        tempTableName = tempListService.createTempListIfNotExists(restriction, redshiftPartition);

        String sql = String.format("SELECT %s FROM %s", attrName, tempTableName);
        List<String> vals = redshiftJdbcTemplate.queryForList(sql, String.class);
        Assert.assertEquals(StringUtils.join(vals, ","), "A,B,C");

        String tempTableName2 = tempListService.createTempListIfNotExists(restriction, redshiftPartition);
        Assert.assertEquals(tempTableName2, tempTableName);
        vals = redshiftJdbcTemplate.queryForList(sql, String.class);
        Assert.assertEquals(StringUtils.join(vals, ","), "A,B,C");

        tempListService.dropTempList(tempTableName);
        String existingTempTable =
                ((TempListServiceImpl) tempListService).getExistingTempTable(restriction, redshiftPartition);
        Assert.assertTrue(StringUtils.isBlank(existingTempTable));
        Assert.assertThrows(BadSqlGrammarException.class, () -> redshiftJdbcTemplate.queryForList(sql, String.class));
    }

}
