package com.latticeengines.app.exposed.service.impl;

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;

import com.latticeengines.app.exposed.service.DataLakeService;
import com.latticeengines.app.testframework.AppDeploymentTestNGBase;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.testframework.exposed.service.CDLTestDataService;

public abstract class DataLakeServiceImplDeploymentTestNGBase extends AppDeploymentTestNGBase {

    protected abstract Set<Pair<String, Category>> getExpectedAttrs();

    protected abstract Set<Pair<String, Category>> getUnexpectedAttrs();

    @Inject
    protected CDLTestDataService cdlTestDataService;

    @Inject
    protected EntityProxy entityProxy;

    @Inject
    protected DataLakeService dataLakeService;

    protected String actualInternalAccountId;
    protected String actualSfdcAccountId;

    protected void setupRedshiftData() {
        cdlTestDataService.populateData(mainTestTenant.getId(), 3);

        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        frontEndQuery.addLookups(BusinessEntity.Account, InterfaceName.AccountId.name(),
                InterfaceName.SalesforceAccountID.name());
        PageFilter pageFilter = new PageFilter(0L, 1L);
        frontEndQuery.setPageFilter(pageFilter);

        DataPage result = entityProxy.getDataFromObjectApi(mainTestTenant.getId(), frontEndQuery);
        Assert.assertNotNull(result);
        Assert.assertTrue(CollectionUtils.isNotEmpty(result.getData()));
        Assert.assertEquals(result.getData().size(), 1);
        Map<String, Object> row = result.getData().get(0);
        Assert.assertTrue(row.containsKey(InterfaceName.AccountId.name()));
        Assert.assertTrue(row.containsKey(InterfaceName.SalesforceAccountID.name()));

        actualInternalAccountId = row.get(InterfaceName.AccountId.name()) == null ? null
                : row.get(InterfaceName.AccountId.name()).toString();
        actualSfdcAccountId = row.get(InterfaceName.SalesforceAccountID.name()) == null ? null
                : row.get(InterfaceName.SalesforceAccountID.name()).toString();
        Assert.assertNotNull(actualInternalAccountId);
        Assert.assertNotNull(actualSfdcAccountId);
    }

    protected void testAndVerifyGetAttributes() {
        List<ColumnMetadata> cms = dataLakeService.getAttributes(null, null);
        int expectedCnt = 0, unexpectedCnt = 0;
        Set<Pair<String, Category>> expectedAttrs = getExpectedAttrs();
        Set<Pair<String, Category>> unexpectedAttrs = getUnexpectedAttrs();
        for (ColumnMetadata cm : cms) {
            if (expectedAttrs.contains(Pair.of(cm.getAttrName(), cm.getCategory()))) {
                expectedCnt++;
            }
            if (unexpectedAttrs.contains(Pair.of(cm.getAttrName(), cm.getCategory()))) {
                unexpectedCnt++;
            }
        }
        Assert.assertEquals(expectedCnt, expectedAttrs.size());
        Assert.assertEquals(unexpectedCnt, 0);
    }
}
