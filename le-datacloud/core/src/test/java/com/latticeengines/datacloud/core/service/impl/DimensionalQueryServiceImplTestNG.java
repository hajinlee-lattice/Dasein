package com.latticeengines.datacloud.core.service.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.core.entitymgr.CategoricalAttributeEntityMgr;
import com.latticeengines.datacloud.core.entitymgr.impl.CategoricalAttributeEntityMgrImplTestNG;
import com.latticeengines.datacloud.core.service.DimensionalQueryService;
import com.latticeengines.datacloud.core.testframework.DataCloudCoreFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.manage.CategoricalAttribute;
import com.latticeengines.domain.exposed.datacloud.manage.DimensionalQuery;
import com.latticeengines.domain.exposed.exception.LedpException;
public class DimensionalQueryServiceImplTestNG extends DataCloudCoreFunctionalTestNGBase {

    private static final String TEST_SOURCE = CategoricalAttributeEntityMgrImplTestNG.TEST_SOURCE;
    private static final String TEST_DIMENSION = CategoricalAttributeEntityMgrImplTestNG.TEST_DIMENSION;

    @Inject
    private DimensionalQueryService dimensionalQueryService;

    @Inject
    private CategoricalAttributeEntityMgr attributeEntityMgr;

    @Inject
    private CategoricalAttributeEntityMgrImplTestNG categoricalAttributeEntityMgrImplTestNG;

    @Test(groups = "functional")
    public void testFindAttribute() {
        categoricalAttributeEntityMgrImplTestNG.cleanupHierarchy();
        categoricalAttributeEntityMgrImplTestNG.bootstrapHierarchy();

        DimensionalQuery query = new DimensionalQuery();
        query.setDimension(TEST_DIMENSION);
        query.setSource(TEST_SOURCE);
        Map<String, String> qualifiers = new HashMap<>();
        qualifiers.put("Level1", CategoricalAttribute.ALL);
        query.setQualifiers(qualifiers);

        Long attrId = dimensionalQueryService.findAttrId(query);
        verifyAttribute(attrId, "Level1", CategoricalAttribute.ALL);

        boolean error = false;
        try {
            qualifiers.put("Level2", "2.1");
            dimensionalQueryService.findAttrId(query);
        } catch (LedpException e) {
            error = true;
        }
        Assert.assertTrue(error, "Should throw a LedpException.");

        qualifiers = new HashMap<>();
        qualifiers.put("Level1", "1.1");
        query.setQualifiers(qualifiers);
        attrId = dimensionalQueryService.findAttrId(query);
        verifyAttribute(attrId, "Level1", "1.1");

        qualifiers = new HashMap<>();
        qualifiers.put("Level1", "1.1");
        qualifiers.put("Level2", "2.1");
        query.setQualifiers(qualifiers);
        attrId = dimensionalQueryService.findAttrId(query);
        verifyAttribute(attrId, "Level2", "2.1");

        qualifiers = new HashMap<>();
        qualifiers.put("Level1", "1.2");
        qualifiers.put("Level2", "2.3");
        qualifiers.put("Level3", "3.1");
        query.setQualifiers(qualifiers);
        attrId = dimensionalQueryService.findAttrId(query);
        verifyAttribute(attrId, "Level3", "3.1");

        qualifiers = new HashMap<>();
        qualifiers.put("Level1", "1.1");
        qualifiers.put("Level2", "2.1");
        qualifiers.put("Level3", "3.1");
        query.setQualifiers(qualifiers);
        attrId = dimensionalQueryService.findAttrId(query);
        verifyAttribute(attrId, "Level3", "3.1");

        qualifiers = new HashMap<>();
        qualifiers.put("Level1", "1.1");
        qualifiers.put("Level2", "2.3");
        qualifiers.put("Level3", "3.1");
        query.setQualifiers(qualifiers);
        attrId = dimensionalQueryService.findAttrId(query);
        Assert.assertNull(attrId);

        categoricalAttributeEntityMgrImplTestNG.teardownHierarchy();
    }

    @Test(groups = "functional")
    public void testIllegalQuery() {
        DimensionalQuery query = new DimensionalQuery();
        query.setDimension(TEST_DIMENSION);
        query.setSource(TEST_SOURCE);

        boolean error = false;
        try {
            dimensionalQueryService.findAttrId(query);
        } catch (LedpException e) {
            error = true;
        }
        Assert.assertTrue(error, "Should throw a LedpException.");

        error = false;
        try {
            query.setQualifiers(Collections.<String, String>emptyMap());
            dimensionalQueryService.findAttrId(query);
        } catch (LedpException e) {
            error = true;
        }
        Assert.assertTrue(error, "Should throw a LedpException.");
    }

    private void verifyAttribute(Long attrId, String attrName, String attrValue) {
        CategoricalAttribute attribute = attributeEntityMgr.getAttribute(attrId);
        Assert.assertNotNull(attribute);
        Assert.assertEquals(attribute.getAttrName(), attrName);
        Assert.assertEquals(attribute.getAttrValue(), attrValue);
    }

}
