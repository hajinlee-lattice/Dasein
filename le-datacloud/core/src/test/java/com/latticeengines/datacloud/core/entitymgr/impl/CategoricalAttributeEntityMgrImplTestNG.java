package com.latticeengines.datacloud.core.entitymgr.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.core.entitymgr.CategoricalAttributeEntityMgr;
import com.latticeengines.datacloud.core.entitymgr.TestCategoricalAttributeEntityMgr;
import com.latticeengines.datacloud.core.testframework.DataCloudCoreFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.manage.CategoricalAttribute;
import com.latticeengines.domain.exposed.datacloud.manage.CategoricalDimension;

@Component
public class CategoricalAttributeEntityMgrImplTestNG extends DataCloudCoreFunctionalTestNGBase {

    public static final String TEST_SOURCE = "TestTable";
    public static final String TEST_DIMENSION = "TestDimension";
    private static final List<String> TEST_ATTRS = Arrays.asList("Level1", "Level2", "Level3");

    @Autowired
    private CategoricalAttributeEntityMgr attributeEntityMgr;

    @Autowired
    @Qualifier("testCategoricalAttributeEntityMgr")
    private TestCategoricalAttributeEntityMgr testEntityMgr;

    private List<CategoricalAttribute> attributes;
    private CategoricalDimension dimension;
    @Test(groups = "functional")
    public void test() {
        cleanupHierarchy();

        bootstrapHierarchy();

        CategoricalAttribute rootAttr = attributeEntityMgr.getRootAttribute(TEST_SOURCE, TEST_DIMENSION);
        Assert.assertNotNull(rootAttr);
        Assert.assertEquals(rootAttr.getAttrName(), "Level1");
        Assert.assertEquals(rootAttr.getAttrValue(), CategoricalAttribute.ALL);

        List<CategoricalAttribute> attrs = attributeEntityMgr.getChildren(rootAttr.getPid());
        Assert.assertEquals(attrs.size(), 2);

        for (CategoricalAttribute attr: attrs) {
            if ("1.1".equals(attr.getAttrValue())) {
                List<CategoricalAttribute> attrs1 = attributeEntityMgr.getChildren(rootAttr.getPid());
                Assert.assertEquals(attrs1.size(), 2);
            }
        }

        teardownHierarchy();
    }

    public void bootstrapHierarchy() {
        attributes = new ArrayList<>();

        CategoricalAttribute attr0 = testEntityMgr.addAttribute(constructAttribute("Level1", CategoricalAttribute.ALL, null));

        CategoricalAttribute attr11 = testEntityMgr.addAttribute(constructAttribute("Level1", "1.1", attr0.getPid()));
        CategoricalAttribute attr12 = testEntityMgr.addAttribute(constructAttribute("Level1", "1.2", attr0.getPid()));

        CategoricalAttribute attr21 = testEntityMgr.addAttribute(constructAttribute("Level2", "2.1", attr11.getPid()));
        CategoricalAttribute attr22 = testEntityMgr.addAttribute(constructAttribute("Level2", "2.2", attr11.getPid()));
        CategoricalAttribute attr23 = testEntityMgr.addAttribute(constructAttribute("Level2", "2.3", attr12.getPid()));

        CategoricalAttribute attr31 = testEntityMgr.addAttribute(constructAttribute("Level3", "3.1", attr21.getPid()));
        CategoricalAttribute attr32 = testEntityMgr.addAttribute(constructAttribute("Level3", "3.2", attr22.getPid()));
        CategoricalAttribute attr33 = testEntityMgr.addAttribute(constructAttribute("Level3", "3.1", attr23.getPid()));

        attributes.addAll(Arrays.asList(
                attr0, //

                attr11, //
                attr12, //

                attr21, //
                attr22, //
                attr23, //

                attr31, //
                attr32, //
                attr33 //
        ));

        dimension = testEntityMgr.addDimension(constructTestDimension(attr0.getPid()));
    }

    public void teardownHierarchy() {
        testEntityMgr.removeDimension(dimension.getPid());
        for (CategoricalAttribute attr: attributes) {
            testEntityMgr.removeAttribute(attr.getPid());
        }
    }

    public void cleanupHierarchy() {
        for (CategoricalDimension dim : testEntityMgr.allDimensions()) {
            if (TEST_DIMENSION.equals(dim.getDimension()) && TEST_SOURCE.equals(dim.getSource())) {
                testEntityMgr.removeDimension(dim.getPid());
            }
        }
        for (CategoricalAttribute attr : testEntityMgr.allAttributes()) {
            if (TEST_ATTRS.contains(attr.getAttrName())) {
                testEntityMgr.removeAttribute(attr.getPid());
            }
        }
    }

    private CategoricalDimension constructTestDimension(Long rootAttrPid) {
        CategoricalDimension dimension = new CategoricalDimension();
        dimension.setSource(TEST_SOURCE);
        dimension.setDimension(TEST_DIMENSION);
        dimension.setRootAttrId(rootAttrPid);
        return dimension;
    }

    private CategoricalAttribute constructAttribute(String name, String value, Long parentId) {
        CategoricalAttribute attribute = new CategoricalAttribute();
        attribute.setAttrName(name);
        attribute.setAttrValue(value);
        attribute.setParentId(parentId);
        return attribute;
    }

}
