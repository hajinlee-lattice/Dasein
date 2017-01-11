package com.latticeengines.ulysses.entitymgr.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.AssertJUnit.assertNotNull;

import java.util.Collections;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.ulysses.Insight;
import com.latticeengines.domain.exposed.ulysses.InsightAttribute;
import com.latticeengines.domain.exposed.ulysses.InsightSection;
import com.latticeengines.domain.exposed.ulysses.InsightSourceType;
import com.latticeengines.domain.exposed.ulysses.LatticeInsights;
import com.latticeengines.ulysses.entitymgr.LatticeInsightsEntityMgr;
import com.latticeengines.ulysses.testframework.UlyssesTestNGBase;

public class LatticeInsightsEntityMgrImplTestNG extends UlyssesTestNGBase {
    @Autowired
    private LatticeInsightsEntityMgr latticeInsightsEntityMgr;

    private LatticeInsights latticeInsights;

    @BeforeClass(groups = "functional")
    public void setup() {
        super.createTable(latticeInsightsEntityMgr.getRepository(), latticeInsightsEntityMgr.getRecordType());
    }

    @Test(groups = "functional")
    public void create() {
        InsightSection s1 = new InsightSection();
        s1.setHeadline("Headline text");
        s1.setTip("Tip text");
        s1.setDescription("Description text");
        InsightAttribute attribute = new InsightAttribute();
        attribute.setName("foo");
        attribute.setDescription("bar");
        attribute.setId("foo");
        s1.addAttribute(attribute);
        s1.setInsightSourceType(InsightSourceType.BOTH);

        Insight i1 = new Insight();
        i1.setId("Insight1");
        i1.setName("Insight1");
        i1.setInsightSections(Collections.singletonList(s1));

        latticeInsights = new LatticeInsights();
        latticeInsights.setName("Campaign1");
        latticeInsights.setInsights(Collections.singletonList(i1));
        latticeInsights.setInsightModifiers(Collections.singletonList(attribute));

        Tenant tenant = new Tenant();
        tenant.setId("LatticeInsightsEntityMgrImplTestNG.LatticeInsightsEntityMgrImplTestNG.Production");

        latticeInsights.setTenant(tenant);

        latticeInsightsEntityMgr.create(latticeInsights);
    }

    @Test(groups = "functional", dependsOnMethods = {"create"})
    public void findByKey() {
        LatticeInsights returned = latticeInsightsEntityMgr.findByKey(latticeInsights.getId());
        assertNotNull(returned);
        assertEquals(returned.getName(), latticeInsights.getName());
        assertEquals(returned.getInsightModifiers().size(), latticeInsights.getInsightModifiers().size());
        assertNull(returned.getInsights());
    }
}
