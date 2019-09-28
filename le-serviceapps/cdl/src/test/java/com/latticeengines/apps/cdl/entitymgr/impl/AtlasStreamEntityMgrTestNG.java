package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.springframework.dao.DataIntegrityViolationException;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.testframework.service.impl.SimpleRetryListener;

@Listeners({ SimpleRetryListener.class })
public class AtlasStreamEntityMgrTestNG extends ActivityRelatedEntityMgrImplTestNGBase {

    private static final String STREAM_WEBVISIT = "WebVisit";
    private static final String STREAM_MARKETO = "MarketoActivity";
    private static final List<String> STREAM_NAMES = Arrays.asList(STREAM_WEBVISIT, STREAM_MARKETO);

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironmentWithDataCollection();
    }

    @Test(groups = "functional")
    public void testCreate() {
        prepareStream();
        for (String name : STREAM_NAMES) {
            AtlasStream stream = streams.get(name);
            Assert.assertNotNull(stream);
            Assert.assertNotNull(stream.getPid());
        }
    }
    
    // [ Name + Tenant ] need to be unique
    @Test(groups = "functional", dependsOnMethods = "testCreate", expectedExceptions = {
            DataIntegrityViolationException.class })
    public void testCreateConflict() {
        createStream(STREAM_WEBVISIT);
    }

    @Test(groups = "functional", dependsOnMethods = "testCreate")
    public void testFind() {
        for (String name : STREAM_NAMES) {
            AtlasStream stream = streamEntityMgr.findByNameAndTenant(name, mainTestTenant);
            validateStream(stream);
        }
        List<AtlasStream> streams = streamEntityMgr.findByTenant(mainTestTenant);
        Assert.assertEquals(streams.size(), STREAM_NAMES.size());

        Tenant tenant2 = notExistTenant();
        Assert.assertNull(streamEntityMgr.findByNameAndTenant(STREAM_WEBVISIT, tenant2));
        Assert.assertTrue(CollectionUtils.isEmpty(streamEntityMgr.findByTenant(tenant2)));
    }

    private void validateStream(AtlasStream stream) {
        Assert.assertNotNull(stream);
        Assert.assertNotNull(stream.getName());
        Assert.assertNotNull(stream.getTenant());
        Assert.assertNotNull(stream.getMatchEntities());
        Assert.assertEquals(stream.getMatchEntities().size(), MATCH_ENTITIES.size());
        Assert.assertNotNull(stream.getAggrEntities());
        Assert.assertEquals(stream.getAggrEntities().size(), AGGR_ENTITIES.size());
        Assert.assertNotNull(stream.getDateAttribute());
        Assert.assertNotNull(stream.getPeriods());
        Assert.assertEquals(stream.getPeriods().size(), PERIODS.size());
        Assert.assertNotNull(stream.getRetentionDays());
        Assert.assertNotNull(stream.getAttributeDerivers());
        Assert.assertEquals(stream.getAttributeDerivers().size(), attrDerivers.size());
    }

    @Override
    protected List<String> getStreamNames() {
        return STREAM_NAMES;
    }


}
