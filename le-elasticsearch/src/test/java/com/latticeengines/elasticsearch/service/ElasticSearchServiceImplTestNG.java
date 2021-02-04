package com.latticeengines.elasticsearch.service;

import static com.latticeengines.domain.exposed.metadata.InterfaceName.ContactId;

import java.util.Collections;
import java.util.Map;

import javax.inject.Inject;

import org.springframework.retry.support.RetryTemplate;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.elasticsearch.Service.ElasticSearchService;
import com.latticeengines.elasticsearch.framework.ElasticSearchFunctionalTestNGBase;
import com.latticeengines.elasticsearch.util.ElasticSearchUtils;

public class ElasticSearchServiceImplTestNG extends ElasticSearchFunctionalTestNGBase {

    @Inject
    private ElasticSearchService elasticSearchService;


    @Test(groups = "functional")
    private void testIndexCRUD() {
        String version = ElasticSearchUtils.generateNewVersion();
        String indexName = String.format("%s_%s_%s", "tenant", TableRoleInCollection.TimelineProfile.name().toLowerCase(),
                version);

        boolean value = elasticSearchService.createIndex(indexName, TableRoleInCollection.TimelineProfile.name());
        Assert.assertTrue(value);

        RetryTemplate retry = RetryUtils.getRetryTemplate(2, Collections.singleton(AssertionError.class), null);
        retry.execute(context ->{
            boolean exists = elasticSearchService.checkFieldExist(indexName, ContactId.name());
            Assert.assertTrue(exists);
            return true;
        });

        retry.execute(context -> {
            Map<String, Object> mappings = elasticSearchService.getSourceMapping(indexName);
            verifyField(mappings, "AccountId", "keyword");
            verifyField(mappings, "ContactId", "keyword");
            verifyField(mappings, "EventTimestamp", "date");
            return true;
        });

        elasticSearchService.updateIndexMapping(indexName, TableRoleInCollection.AccountLookup.name(), "nested");

        retry.execute(context -> {
            boolean exists = elasticSearchService.checkFieldExist(indexName,
                    TableRoleInCollection.AccountLookup.name());
            Assert.assertTrue(exists);
            return true;
        });

        retry.execute(context ->{
            boolean exists = elasticSearchService.checkFieldExist(indexName, ContactId.name());
            Assert.assertTrue(exists);
            return true;

        });


        elasticSearchService.deleteIndex(indexName);
        retry.execute(context -> {
            boolean exists = elasticSearchService.indexExists(indexName);
            Assert.assertFalse(exists);
            return true;
        });


    }

    private void verifyField(Map<String, Object> mappings, String field, String type) {
        Map<?, ?> val = (Map<?, ?>)mappings.get(field);
        Assert.assertNotNull(val);
        Map<String, String> accountMap = JsonUtils.convertMap(val, String.class, String.class);
        Assert.assertEquals(accountMap.get("type"), type);
    }

}
