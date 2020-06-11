package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.dao.DataIntegrityViolationException;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.testframework.service.impl.SimpleRetryAnalyzer;
import com.latticeengines.testframework.service.impl.SimpleRetryListener;

@Listeners({ SimpleRetryListener.class })
public class AtlasStreamEntityMgrTestNG extends ActivityRelatedEntityMgrImplTestNGBase {

    private static final String STREAM_WEBVISIT = "WebVisit";
    private static final String STREAM_MARKETO = "MarketoActivity";
    private static final List<String> STREAM_NAMES = Arrays.asList(STREAM_WEBVISIT, STREAM_MARKETO);

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironmentWithDataCollection();
        prepareDataFeed();
    }

    @Test(groups = "functional")
    public void testCreate() {
        prepareStream();
        for (String name : STREAM_NAMES) {
            AtlasStream stream = streams.get(name);
            stream.setStreamType(name.equals(STREAM_WEBVISIT) ? AtlasStream.StreamType.WebVisit : AtlasStream.StreamType.MarketingActivity);
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

    @Test(groups = "functional", dependsOnMethods = "testCreate", retryAnalyzer = SimpleRetryAnalyzer.class)
    private void testFindAllWithDimensionInflation() {
        // make sure inflation work with null dimensions
        List<AtlasStream> streams = streamEntityMgr.findByTenant(mainTestTenant, true);
        Assert.assertNotNull(streams);
        Assert.assertEquals(streams.size(), STREAM_NAMES.size());
        streams.forEach(stream -> Assert.assertTrue(CollectionUtils.isEmpty(stream.getDimensions()),
                String.format("Should have empty dimensions in stream %s", stream.getName())));
    }

    @Test(groups = "functional", dependsOnMethods = "testCreate", retryAnalyzer = SimpleRetryAnalyzer.class)
    private void testFindByStreamType() {
        List<AtlasStream> streams = streamEntityMgr.findByStreamType(AtlasStream.StreamType.WebVisit);
        streams.forEach(stream -> {
            Assert.assertEquals(stream.getStreamType(), AtlasStream.StreamType.WebVisit);
            Assert.assertEquals(stream.getTenant().getId(), mainTestTenant.getId());
        });
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
