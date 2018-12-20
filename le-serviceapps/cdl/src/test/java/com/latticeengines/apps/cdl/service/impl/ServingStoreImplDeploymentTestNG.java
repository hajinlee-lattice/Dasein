package com.latticeengines.apps.cdl.service.impl;

import java.util.List;
import java.util.function.Predicate;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.ServingStoreService;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;

import reactor.core.publisher.Flux;

public class ServingStoreImplDeploymentTestNG extends ServingStoreDeploymentTestNGBase {

    @Inject
    private ServingStoreProxy servingStoreProxy;

    @Inject
    private ServingStoreService servingStoreService;

    @Test(groups = "deployment-app")
    public void testDecoratedMetadata() {
//        testAccountMetadata();
    }

    @Test(groups = "deployment-app")
    public void testModelAttrs() {
        Flux<ColumnMetadata> newModelingAttrs = servingStoreProxy.getNewModelingAttrs(mainTestTenant.getId());
        Predicate<ColumnMetadata> p = attr -> ApprovedUsage.MODEL_ALLINSIGHTS.equals(attr.getApprovedUsageList().get(0))
                && attr.getTagList() != null;
        Assert.assertTrue(newModelingAttrs.all(p).block());

        Flux<ColumnMetadata> allModelingAttrs = servingStoreProxy.getAllowedModelingAttrs(mainTestTenant.getId());
        p = ColumnMetadata::getCanModel;
        Assert.assertTrue(allModelingAttrs.all(p).block());
    }

    private void testAccountMetadata() {
        List<ColumnMetadata> cms = servingStoreService //
                .getDecoratedMetadataFromCache(mainCustomerSpace, BusinessEntity.Account);
        cms.forEach(cm -> Assert.assertNotNull(cm.getJavaClass()));
    }

}
