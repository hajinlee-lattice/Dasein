package com.latticeengines.objectapi.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.objectapi.service.EntityQueryService;

public class EntityQueryServiceImplBigListTestNG extends QueryServiceImplTestNGBase {

    @SuppressWarnings("checkstyle:HideUtilityClassConstructor")
    private final class AccountAttr {
        static final String CompanyName = "CompanyName";
    }

    @Inject
    private EntityQueryService entityQueryService;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestData(3);
    }

    @Test(groups = "functional")
    public void testBigList() {
        Collection<Object> bigList = getCompanyNamesList();
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setEvaluationDateStr(maxTransactionDate);
        Restriction accRes = Restriction.builder().let(BusinessEntity.Account, AccountAttr.CompanyName)
                .inCollection(bigList).build();
        FrontEndRestriction frontEndRestriction = new FrontEndRestriction();
        frontEndRestriction.setRestriction(accRes);
        frontEndQuery.setAccountRestriction(frontEndRestriction);
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        long start = System.currentTimeMillis();
        long count = entityQueryService.getCount(frontEndQuery, DataCollection.Version.Blue, SEGMENT_USER);
        long duration1 = System.currentTimeMillis() - start;
        Assert.assertTrue(count >= bigList.size());
        start = System.currentTimeMillis();
        long count2 = entityQueryService.getCount(frontEndQuery, DataCollection.Version.Blue, SEGMENT_USER);
        Assert.assertEquals(count2, count);
        long duration2 = System.currentTimeMillis() - start;
        Assert.assertTrue(duration2 < duration1, "Second run should be faster");
    }

    private Collection<Object> getCompanyNamesList() {
        List<Object> candidates = new ArrayList<>();
        final InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("CompanyNameBigList");
        Assert.assertNotNull(is);
        Iterable<String> lns = () -> {
            try {
                return IOUtils.lineIterator(is, Charset.defaultCharset());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
        for (String ln : lns) {
            if (StringUtils.isNotBlank(ln)) {
                candidates.add(ln);
            }
        }
        return candidates;
    }

}
