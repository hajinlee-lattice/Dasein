package com.latticeengines.datacloud.match.actors.visitor.impl;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.service.EntityMatchInternalService;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityAssociationRequest;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityAssociationResponse;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityRawSeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public class EntityAssociateServiceImplUnitTestNG {

    private static final Tenant TEST_TENANT = getTestTenant();
    private static final String TEST_ENTITY = BusinessEntity.Account.name();
    private static final String TEST_ENTITY_ID = "associate_service_unit_test_entity_id";
    private static final String TEST_ENTITY_ID2 = "associate_service_unit_test_entity_id2";
    private static final String TEST_REQUEST_ID = "associate_service_unit_test_request_id";
    private static final Map<String, String> TEST_EXTRA_ATTRIBUTES = Collections.singletonMap(
            "test_extra_attribute_name", "test_extra_attribute_value");

    @Test(groups = "unit", dataProvider = "entityAssociation", enabled = false)
    private void testAssociate(
            EntityAssociationRequest request, EntityRawSeed currentTargetSnapshot, List<EntityRawSeed> expectedParams,
            List<Triple<EntityRawSeed, List<EntityLookupEntry>, List<EntityLookupEntry>>> results,
            String expectedAssociatedEntityId, boolean hasAssociationError) throws Exception {
        List<EntityRawSeed> params = new ArrayList<>();
        Iterator<Triple<EntityRawSeed, List<EntityLookupEntry>, List<EntityLookupEntry>>> it = results.iterator();
        EntityAssociateServiceImpl service = mock(params, it);

        EntityAssociationResponse response = service.associate(TEST_REQUEST_ID, request, currentTargetSnapshot);
        Assert.assertNotNull(response);
        Assert.assertEquals(response.getAssociatedEntityId(), expectedAssociatedEntityId);
        Assert.assertEquals(response.getEntity(), request.getEntity());
        Assert.assertNotNull(response.getTenant());
        Assert.assertEquals(response.getTenant().getPid(), TEST_TENANT.getPid());

        // should make the expected number of calls (iterate through all the results)
        Assert.assertFalse(it.hasNext());
        // expected associated id matched
        Assert.assertEquals(response.getAssociatedEntityId(), expectedAssociatedEntityId);
        if (hasAssociationError) {
            Assert.assertTrue(response.getAssociationErrors().isEmpty());
        } else {
            Assert.assertFalse(response.getAssociationErrors().isEmpty());
        }

        // verify captured params
        Assert.assertEquals(params.size(), expectedParams.size());
        IntStream.range(0, params.size()).forEach(idx -> {
            EntityRawSeed seed = params.get(idx);
            EntityRawSeed expectedSeed = expectedParams.get(idx);
            Assert.assertNotNull(seed);
            // we can use equals of seed here because we need the lookup entries in the same order
            Assert.assertEquals(seed, expectedSeed);
        });
    }

    @DataProvider(name = "entityAssociation")
    private Object[][] provideAssociationTestData() {
        return new Object[][] {
                // TODO add test cases when we have external system ID in match key tuple
        };
    }

    private EntityAssociateServiceImpl mock(
            @NotNull List<EntityRawSeed> params,
            Iterator<Triple<EntityRawSeed, List<EntityLookupEntry>, List<EntityLookupEntry>>> results) throws Exception {
        EntityAssociateServiceImpl service = new EntityAssociateServiceImpl();
        EntityMatchInternalService internalService = Mockito.mock(EntityMatchInternalService.class);
        Mockito.when(internalService.associate(Mockito.any(), Mockito.any())).thenAnswer(invocation -> {
            Tenant inputTenant = invocation.getArgument(0);
            EntityRawSeed seed = invocation.getArgument(1);
            Assert.assertNotNull(inputTenant);
            Assert.assertNotNull(seed);
            Assert.assertEquals(inputTenant.getPid(), TEST_TENANT.getPid());
            // capture parameter
            params.add(seed);
            return results.next();
        });
        FieldUtils.writeField(service, "entityMatchInternalService", internalService, true);
        return service;
    }

    private static Tenant getTestTenant() {
        Tenant tenant = new Tenant("entity_associate_service_test_tenant_1");
        tenant.setPid(825789853L);
        return tenant;
    }
}
