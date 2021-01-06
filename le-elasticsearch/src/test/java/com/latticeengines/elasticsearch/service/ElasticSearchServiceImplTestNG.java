package com.latticeengines.elasticsearch.service;

import static com.latticeengines.domain.exposed.metadata.InterfaceName.ContactId;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.Test;

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

        boolean exists = elasticSearchService.checkFieldExist(indexName, ContactId.name());
        Assert.assertTrue(exists);

        elasticSearchService.updateIndexMapping(indexName, TableRoleInCollection.AccountLookup.name());

        exists = elasticSearchService.checkFieldExist(indexName, TableRoleInCollection.AccountLookup.name());
        Assert.assertTrue(exists);


        exists = elasticSearchService.checkFieldExist(indexName, ContactId.name());
        Assert.assertTrue(exists);

        elasticSearchService.deleteIndex(indexName);
        exists = elasticSearchService.indexExists(indexName);
        Assert.assertFalse(exists);

    }


}
