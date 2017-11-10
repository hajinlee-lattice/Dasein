package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.apps.cdl.entitymgr.RatingEngineEntityMgr;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineStatus;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingRule;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.pls.RuleBucketName;
import com.latticeengines.domain.exposed.query.Restriction;

public class RatingEngineEntityMgrImplTestNG extends CDLFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(RatingEngineEntityMgrImplTestNG.class);

    private static final String RATING_ENGINE_NAME = "Rating Engine";
    private static final String RATING_ENGINE_NOTE = "This is a Rating Engine that covers North America market";
    private static final String CREATED_BY = "lattice@lattice-engines.com";
    private static final String LDC_NAME = "LDC_Name";
    private static final String LE_IS_PRIMARY_DOMAIN = "LE_IS_PRIMARY_DOMAIN";

    @Inject
    private RatingEngineEntityMgr ratingEngineEntityMgr;

    private RatingEngine ratingEngine;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        setupTestEnvironmentWithDummySegment();
        ratingEngine = new RatingEngine();
        ratingEngine.setSegment(testSegment);
        ratingEngine.setCreatedBy(CREATED_BY);
        ratingEngine.setType(RatingEngineType.RULE_BASED);
    }

    @Test(groups = "functional")
    public void testBasicOperations() {
        // test creation
        RatingEngine createdRatingEngine = ratingEngineEntityMgr.createOrUpdateRatingEngine(ratingEngine,
                mainTestTenant.getId());
        log.info("Rating Engine is " + createdRatingEngine.toString());
        Assert.assertNotNull(createdRatingEngine);
        Assert.assertNotNull(createdRatingEngine.getActiveModel());
        Assert.assertNotNull(createdRatingEngine.getRatingModels());
        Assert.assertEquals(createdRatingEngine.getRatingModels().size(), 1);
        Assert.assertNotNull(createdRatingEngine.getSegment());
        String ratingEngineId = createdRatingEngine.getId();
        createdRatingEngine = ratingEngineEntityMgr.findById(ratingEngineId);
        Assert.assertNotNull(createdRatingEngine);
        Assert.assertEquals(ratingEngineId, createdRatingEngine.getId());
        Assert.assertNotNull(createdRatingEngine.getCreated());
        Date createdDate = createdRatingEngine.getCreated();
        Assert.assertNotNull(createdRatingEngine.getUpdated());
        Date updatedDate = createdRatingEngine.getUpdated();
        Assert.assertNotNull(createdRatingEngine.getDisplayName());
        Assert.assertNull(createdRatingEngine.getNote());
        Assert.assertEquals(createdRatingEngine.getType(), RatingEngineType.RULE_BASED);
        Assert.assertEquals(createdRatingEngine.getCreatedBy(), CREATED_BY);
        Assert.assertEquals(createdRatingEngine.getStatus(), RatingEngineStatus.INACTIVE);
        Assert.assertTrue(MapUtils.isEmpty(createdRatingEngine.getCountsAsMap()));

        String createdRatingEngineStr = createdRatingEngine.toString();
        log.info("createdRatingEngineStr is " + createdRatingEngineStr);
        createdRatingEngine = JsonUtils.deserialize(createdRatingEngineStr, RatingEngine.class);
        MetadataSegment segment = createdRatingEngine.getSegment();
        Assert.assertNotNull(segment);
        Assert.assertEquals(segment.getDisplayName(), SEGMENT_NAME);
        DataCollection dc = segment.getDataCollection();
        log.info("dc is " + dc);

        Set<RatingModel> ratingModels = createdRatingEngine.getRatingModels();
        Assert.assertNotNull(ratingModels);
        Assert.assertEquals(ratingModels.size(), 1);
        Iterator<RatingModel> it = ratingModels.iterator();
        RatingModel rm = it.next();
        Assert.assertTrue(rm instanceof RuleBasedModel);
        Assert.assertEquals(rm.getIteration(), 1);
        Assert.assertEquals(((RuleBasedModel) rm).getRatingRule().getDefaultBucketName(),
                RatingRule.DEFAULT_BUCKET_NAME);
        log.info("Rating Engine after findById is " + createdRatingEngine.toString());

        // test find all
        List<RatingEngine> ratingEngineList = ratingEngineEntityMgr.findAll();
        Assert.assertNotNull(ratingEngineList);
        Assert.assertEquals(ratingEngineList.size(), 1);
        Assert.assertEquals(ratingEngineId, ratingEngineList.get(0).getId());

        // test find all by type and status
        ratingEngineList = ratingEngineEntityMgr.findAllByTypeAndStatus(null, null);
        Assert.assertEquals(ratingEngineList.size(), 1);
        ratingEngineList = ratingEngineEntityMgr.findAllByTypeAndStatus(RatingEngineType.AI_BASED.name(), null);
        Assert.assertEquals(ratingEngineList.size(), 0);
        ratingEngineList = ratingEngineEntityMgr.findAllByTypeAndStatus(RatingEngineType.RULE_BASED.name(), null);
        Assert.assertEquals(ratingEngineList.size(), 1);
        ratingEngineList = ratingEngineEntityMgr.findAllByTypeAndStatus(null, RatingEngineStatus.INACTIVE.name());
        Assert.assertEquals(ratingEngineList.size(), 1);
        ratingEngineList = ratingEngineEntityMgr.findAllByTypeAndStatus(null, RatingEngineStatus.ACTIVE.name());
        Assert.assertEquals(ratingEngineList.size(), 0);
        ratingEngineList = ratingEngineEntityMgr.findAllByTypeAndStatus(RatingEngineType.RULE_BASED.name(),
                RatingEngineStatus.ACTIVE.name());
        Assert.assertEquals(ratingEngineList.size(), 0);
        ratingEngineList = ratingEngineEntityMgr.findAllByTypeAndStatus(RatingEngineType.RULE_BASED.name(),
                RatingEngineStatus.INACTIVE.name());
        Assert.assertEquals(ratingEngineList.size(), 1);
        ratingEngineList = ratingEngineEntityMgr.findAllByTypeAndStatus(RatingEngineType.AI_BASED.name(),
                RatingEngineStatus.INACTIVE.name());
        Assert.assertEquals(ratingEngineList.size(), 0);
        ratingEngineList = ratingEngineEntityMgr.findAllByTypeAndStatus(RatingEngineType.AI_BASED.name(),
                RatingEngineStatus.ACTIVE.name());
        Assert.assertEquals(ratingEngineList.size(), 0);

        // test update
        RatingEngine re = new RatingEngine();
        re.setDisplayName(RATING_ENGINE_NAME);
        re.setNote(RATING_ENGINE_NOTE);
        re.setStatus(RatingEngineStatus.ACTIVE);
        re.setId(ratingEngine.getId());
        re.setCountsByMap(ImmutableMap.of( //
                RuleBucketName.A.getName(), 1L, //
                RuleBucketName.B.getName(), 2L, //
                RuleBucketName.C.getName(), 3L));
        createdRatingEngine = ratingEngineEntityMgr.createOrUpdateRatingEngine(re, mainTestTenant.getId());
        log.info("Rating Engine after update is " + createdRatingEngine.toString());
        Assert.assertEquals(createdRatingEngine.getStatus(), RatingEngineStatus.ACTIVE);
        Assert.assertNotNull(createdRatingEngine.getActiveModel());
        Assert.assertNotNull(createdRatingEngine.getRatingModels());
        Assert.assertEquals(createdRatingEngine.getRatingModels().size(), 1);
        Assert.assertNotNull(createdRatingEngine.getSegment());
        Assert.assertEquals(RATING_ENGINE_NAME, createdRatingEngine.getDisplayName());
        Assert.assertEquals(RATING_ENGINE_NOTE, createdRatingEngine.getNote());
        System.out.println("update date is " + updatedDate);
        System.out.println("The update date for the newly updated one is "
                + ratingEngineEntityMgr.findById(ratingEngine.getId()).getUpdated());
        System.out.println("Created date is " + createdDate);
        System.out.println("The create date for the newly updated one is " + createdRatingEngine.getCreated());
        ratingEngineList = ratingEngineEntityMgr.findAll();
        Assert.assertNotNull(ratingEngineList);
        Assert.assertEquals(ratingEngineList.size(), 1);
        Assert.assertEquals(ratingEngineId, ratingEngineList.get(0).getId());
        RatingEngine retrievedRatingEngine = ratingEngineEntityMgr.findById(ratingEngineId);
        log.info("Rating Engine after update is " + retrievedRatingEngine.toString());
        Map<String, Long> counts = re.getCountsAsMap();
        Assert.assertTrue(MapUtils.isNotEmpty(counts));
        Assert.assertEquals(counts.get(RuleBucketName.A.getName()), new Long(1));
        Assert.assertEquals(counts.get(RuleBucketName.B.getName()), new Long(2));
        Assert.assertEquals(counts.get(RuleBucketName.C.getName()), new Long(3));


        ratingEngineList = ratingEngineEntityMgr.findAllByTypeAndStatus(RatingEngineType.RULE_BASED.name(),
                RatingEngineStatus.ACTIVE.name());
        Assert.assertEquals(ratingEngineList.size(), 1);
        ratingEngineList = ratingEngineEntityMgr.findAllByTypeAndStatus(null, RatingEngineStatus.ACTIVE.name());
        Assert.assertEquals(ratingEngineList.size(), 1);
        ratingEngineList = ratingEngineEntityMgr.findAllByTypeAndStatus(RatingEngineType.RULE_BASED.name(),
                RatingEngineStatus.INACTIVE.name());
        Assert.assertEquals(ratingEngineList.size(), 0);
        ratingEngineList = ratingEngineEntityMgr.findAllByTypeAndStatus(null, RatingEngineStatus.INACTIVE.name());
        Assert.assertEquals(ratingEngineList.size(), 0);

        // test deletion
        ratingEngineEntityMgr.deleteById(ratingEngineId);
        ratingEngineList = ratingEngineEntityMgr.findAll();
        Assert.assertNotNull(ratingEngineList);
        Assert.assertEquals(ratingEngineList.size(), 0);

        createdRatingEngine = ratingEngineEntityMgr.findById(ratingEngineId);
        Assert.assertNull(createdRatingEngine);

    }

    @Test(groups = "functional")
    public void testFindUsedAttributes() {
        RatingEngineEntityMgrImpl r = new RatingEngineEntityMgrImpl();
        Restriction restr = getTestRestriction();
        MetadataSegment segment = new MetadataSegment();
        segment.setAccountRestriction(restr);
        segment.setContactRestriction(restr);
        List<String> usedAttributesInSegment = r.findUsedAttributes(segment);
        Assert.assertNotNull(usedAttributesInSegment);
        Set<String> expectedResult = new HashSet<>();
        expectedResult.add(LE_IS_PRIMARY_DOMAIN);
        expectedResult.add(LDC_NAME);
        Assert.assertEquals(usedAttributesInSegment.size(), expectedResult.size());

        for (String attr : usedAttributesInSegment) {
            if (!expectedResult.contains(attr)) {
                log.info("Selected attribute not expected: " + attr);
            }
            Assert.assertTrue(expectedResult.contains(attr));
        }
    }

    public static Restriction getTestRestriction() {
        return JsonUtils.deserialize(TEST_RESTRICTION, Restriction.class);
    }

    private static String TEST_RESTRICTION = "{ " //
            + "  \"logicalRestriction\": { " //
            + "    \"operator\": \"AND\", " //
            + "    \"restrictions\": [ " //
            + "      { " //
            + "        \"logicalRestriction\": { " //
            + "          \"operator\": \"AND\", " //
            + "          \"restrictions\": [ " //
            + "            { " //
            + "              \"logicalRestriction\": { " //
            + "                \"operator\": \"AND\", " //
            + "                \"restrictions\": [ " //
            + "                  { " //
            + "                    \"bucketRestriction\": { " //
            + "                      \"bkt\": { " //
            + "                        \"Lbl\": \"Yes\", " //
            + "                        \"Cnt\": 2006, " //
            + "                        \"Id\": 1 " //
            + "                      }, " //
            + "                      \"attr\": \"Account." + LE_IS_PRIMARY_DOMAIN + "\" " //
            + "                    } " //
            + "                  } " //
            + "                ] " //
            + "              } " //
            + "            }, " //
            + "            { " //
            + "              \"bucketRestriction\": { " //
            + "                \"bkt\": { " //
            + "                  \"Lbl\": \"Yes\", " //
            + "                  \"Cnt\": 2006, " //
            + "                  \"Id\": 1 " //
            + "                }, " //
            + "                \"attr\": \"Account." + LE_IS_PRIMARY_DOMAIN + "\" " //
            + "              } " //
            + "            } " //
            + "          ] " //
            + "        } " //
            + "      }, " //
            + "      { " //
            + "        \"concreteRestriction\": { " //
            + "          \"negate\": false, " //
            + "          \"lhs\": { " //
            + "            \"attribute\": { " //
            + "              \"entity\": \"Account\", " //
            + "              \"attribute\": \"" + LDC_NAME + "\" " //
            + "            } " //
            + "          }, " //
            + "          \"relation\": \"IN_RANGE\", " //
            + "          \"rhs\": { " //
            + "            \"range\": { " //
            + "              \"min\": \"A\", " //
            + "              \"max\": \"O\" " //
            + "            } " //
            + "          } " //
            + "        } " //
            + "      } " //
            + "    ] " //
            + "  } " //
            + "} ";
}
