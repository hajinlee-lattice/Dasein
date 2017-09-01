package com.latticeengines.leadprioritization.workflow.steps;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.playmakercore.Recommendation;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingRule;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.PlayLaunchInitStepConfiguration;
import com.latticeengines.playmakercore.service.RecommendationService;
import com.latticeengines.proxy.exposed.dante.DanteLeadProxy;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;

@ContextConfiguration(locations = { "classpath:test-playlaunch-properties-context.xml" })
public class PlayLaunchInitStepDeploymentTestNG extends AbstractTestNGSpringContextTests {

    private PlayLaunchInitStep playLaunchInitStep;

    @Mock
    PlayLaunchInitStepConfiguration configuration;

    // no mocking needed
    EntityProxy entityProxy;

    @Mock
    DataCollectionProxy dataCollectionProxy;

    @Mock
    InternalResourceRestApiProxy internalResourceRestApiProxy;

    @Mock
    RecommendationService recommendationService;

    @Mock
    DanteLeadProxy danteLeadProxy;

    @Mock
    TenantEntityMgr tenantEntityMgr;

    String randId = UUID.randomUUID().toString();

    @BeforeClass(groups = "workflow")
    public void setup()
            throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
        // this test tenant has account data loaded in CDL
        String tenantIdentifier = "Segment_M14.Segment_M14.Production";
        String playId = "play__" + randId;
        String playLaunchId = "launch__" + randId;
        long pageSize = 2L;

        MockitoAnnotations.initMocks(this);

        initEntityProxy();

        mockInternalResource(playId, playLaunchId);

        mockTenantMgr(new Tenant(tenantIdentifier));

        mockRecommendationService();

        mockTalkingPointProxy();

        mockDanteLeadProxy();

        playLaunchInitStep = new PlayLaunchInitStep();

        playLaunchInitStep.setEntityProxy(entityProxy);
        playLaunchInitStep.setDataCollectionProxy(dataCollectionProxy);
        playLaunchInitStep.setInternalResourceRestApiProxy(internalResourceRestApiProxy);
        playLaunchInitStep.setPageSize(pageSize);
        playLaunchInitStep.setRecommendationService(recommendationService);
        playLaunchInitStep.setDanteLeadProxy(danteLeadProxy);
        playLaunchInitStep.setTenantEntityMgr(tenantEntityMgr);

        playLaunchInitStep.setConfiguration(createConf(CustomerSpace.parse(tenantIdentifier), playId, playLaunchId));
    }

    @SuppressWarnings("unchecked")
    private void initEntityProxy() throws NoSuchFieldException, IllegalAccessException {
        Field propMapField = PropertyUtils.class.getDeclaredField("propertiesMap");
        propMapField.setAccessible(true);
        Map<String, String> propertiesMap = (Map<String, String>) propMapField.get(null);

        // temporary
        propertiesMap.put("common.microservice.url",
                "https://internal-private-lpi-b-282775961.us-east-1.elb.amazonaws.com");

        entityProxy = new EntityProxy();

        Field f1 = entityProxy.getClass().getSuperclass().getSuperclass().getDeclaredField("initialWaitMsec");
        f1.setAccessible(true);
        f1.set(entityProxy, 1000L);

        f1 = entityProxy.getClass().getSuperclass().getSuperclass().getDeclaredField("multiplier");
        f1.setAccessible(true);
        f1.set(entityProxy, 2D);

        f1 = entityProxy.getClass().getSuperclass().getSuperclass().getDeclaredField("maxAttempts");
        f1.setAccessible(true);
        f1.set(entityProxy, 10);
    }

    @Test(groups = "workflow")
    public void testExecute() {
        playLaunchInitStep.execute();
    }

    private PlayLaunchInitStepConfiguration createConf(CustomerSpace customerSpace, String playName,
            String playLaunchId) {
        PlayLaunchInitStepConfiguration config = new PlayLaunchInitStepConfiguration();
        config.setCustomerSpace(customerSpace);
        config.setPlayLaunchId(playLaunchId);
        config.setPlayName(playName);
        return config;
    }

    private void mockTalkingPointProxy() {
        doNothing() //
                .when(internalResourceRestApiProxy) //
                .publishTalkingPoints(any(CustomerSpace.class), anyString());
    }

    private void mockDanteLeadProxy() {
        doNothing() //
                .when(danteLeadProxy) //
                .create(any(Recommendation.class), anyString());
    }

    private void mockRecommendationService() {
        doNothing() //
                .when(recommendationService) //
                .create(any(Recommendation.class));
    }

    private void mockTenantMgr(Tenant tenant) {
        when(tenantEntityMgr.findByTenantId( //
                anyString())) //
                        .thenReturn(tenant);
    }

    private void mockInternalResource(String playId, String playLaunchId) {

        when(internalResourceRestApiProxy.findPlayByName( //
                any(CustomerSpace.class), //
                anyString())) //
                        .thenReturn(createPlay(playId));

        when(internalResourceRestApiProxy.getPlayLaunch( //
                any(CustomerSpace.class), //
                anyString(), //
                anyString())) //
                        .thenReturn(createPlayLaunch(playId, playLaunchId));

        when(internalResourceRestApiProxy.getSegmentRestrictionQuery( //
                any(CustomerSpace.class), //
                anyString())) //
                        .thenReturn(createSegmentRestrictionQuery());

        doNothing() //
                .when(internalResourceRestApiProxy) //
                .updatePlayLaunch( //
                        any(CustomerSpace.class), //
                        anyString(), //
                        anyString(), //
                        any(LaunchState.class));
    }

    private void mockAccountProxy(long pageSize) {
        when(entityProxy.getCount( //
                anyString(), //
                any(FrontEndQuery.class))) //
                        .thenReturn(2L);

        when(entityProxy.getData( //
                anyString(), //
                any(FrontEndQuery.class))) //
                        .thenReturn(generateDataPage(pageSize));
    }

    private Restriction createSegmentRestrictionQuery() {
        return null;// new RestrictionBuilder().build();
    }

    private Play createPlay(String playId) {
        Play play = new Play();
        play.setName(playId);
        play.setSegmentName("SegmentName");
        RatingEngine ratingEngine = new RatingEngine();
        MetadataSegment segment = new MetadataSegment();
        Restriction restriction = JsonUtils.deserialize(restrictionJson, Restriction.class);
        segment.setAccountRestriction(restriction);

        ratingEngine.setSegment(segment);
        Set<RatingModel> ratingModels = new HashSet<>();
        RuleBasedModel ruleBasedModel = new RuleBasedModel();
        RatingRule ratingRule = JsonUtils.deserialize(ratingRuleJson, RatingRule.class);
        ruleBasedModel.setRatingRule(ratingRule);
        ruleBasedModel.setId(randId);
        ratingModels.add(ruleBasedModel);
        ratingEngine.setRatingModels(ratingModels);
        play.setRatingEngine(ratingEngine);
        return play;
    }

    private PlayLaunch createPlayLaunch(String playId, String playLaunchId) {
        PlayLaunch launch = new PlayLaunch();
        launch.setPlay(createPlay(playId));
        launch.setLaunchId(playLaunchId);
        return launch;
    }

    private DataPage generateDataPage(long pageSize) {
        List<Map<String, Object>> dataList = new ArrayList<>();
        for (long i = 0; i < pageSize; i++) {
            Map<String, Object> data = new HashMap<>();
            dataList.add(data);
        }
        return new DataPage(dataList);
    }

    String restrictionJson = //
            "{ " //
                    + "      \"logicalRestriction\": { " //
                    + "        \"operator\": \"AND\", " //
                    + "        \"restrictions\": [ " //
                    + "          { " //
                    + "            \"logicalRestriction\": { " //
                    + "              \"operator\": \"AND\", " //
                    + "              \"restrictions\": [ " //
                    + "                { " //
                    + "                  \"bucketRestriction\": { " //
                    + "                    \"bkt\": { " //
                    + "                      \"Lbl\": \"6\", " //
                    + "                      \"Cnt\": 4884, " //
                    + "                      \"Id\": 4 " //
                    + "                    }, " //
                    + "                    \"attr\": \"Account.COMPOSITE_RISK_SCORE\" " //
                    + "                  } " //
                    + "                }, " //
                    + "                { " //
                    + "                  \"bucketRestriction\": { " //
                    + "                    \"bkt\": { " //
                    + "                      \"Lbl\": \"1\", " //
                    + "                      \"Cnt\": 7521, " //
                    + "                      \"Id\": 1 " //
                    + "                    }, " //
                    + "                    \"attr\": \"Account.PREMIUM_MARKETING_PRESCREEN\" " //
                    + "                  } " //
                    + "                }, " //
                    + "                { " //
                    + "                  \"bucketRestriction\": { " //
                    + "                    \"bkt\": { " //
                    + "                      \"Lbl\": \"CALIFORNIA\", " //
                    + "                      \"Cnt\": 5463, " //
                    + "                      \"Id\": 562 " //
                    + "                    }, " //
                    + "                    \"attr\": \"Account.LDC_State\" " //
                    + "                  } " //
                    + "                }, " //
                    + "                { " //
                    + "                  \"bucketRestriction\": { " //
                    + "                    \"bkt\": { " //
                    + "                      \"Lbl\": \"0 - 1\", " //
                    + "                      \"Cnt\": 99983, " //
                    + "                      \"Id\": 2, " //
                    + "                      \"Rng\": [ " //
                    + "                        0, " //
                    + "                        1 " //
                    + "                      ] " //
                    + "                    }, " //
                    + "                    \"attr\": \"Account.PD_DC_FEATURETERMSELLTICKETS_0E0936B53D\" " //
                    + "                  } " //
                    + "                }, " //
                    + "                { " //
                    + "                  \"bucketRestriction\": { " //
                    + "                    \"bkt\": { " //
                    + "                      \"Lbl\": \"0 - 10\", " //
                    + "                      \"Cnt\": 98353, " //
                    + "                      \"Id\": 2, " //
                    + "                      \"Rng\": [ " //
                    + "                        0, " //
                    + "                        10 " //
                    + "                      ] " //
                    + "                    }, " //
                    + "                    \"attr\": \"Account.ACCT_I_RANK_PCTCHANGE_6MONTH_470ECBCC2A\" " //
                    + "                  } " //
                    + "                }, " //
                    + "                { " //
                    + "                  \"bucketRestriction\": { " //
                    + "                    \"bkt\": { " //
                    + "                      \"Lbl\": \"< 2\", " //
                    + "                      \"Cnt\": 5583, " //
                    + "                      \"Id\": 1, " //
                    + "                      \"Rng\": [ " //
                    + "                        null, " //
                    + "                        2 " //
                    + "                      ] " //
                    + "                    }, " //
                    + "                    \"attr\": \"Account.CloudTechnologies_ContactCenterManagement\" " //
                    + "                  } " //
                    + "                }, " //
                    + "                { " //
                    + "                  \"bucketRestriction\": { " //
                    + "                    \"bkt\": { " //
                    + "                      \"Lbl\": \"< 4\", " //
                    + "                      \"Cnt\": 35291, " //
                    + "                      \"Id\": 1, " //
                    + "                      \"Rng\": [ " //
                    + "                        null, " //
                    + "                        4 " //
                    + "                      ] " //
                    + "                    }, " //
                    + "                    \"attr\": \"Account.BusinessTechnologiesSsl\" " //
                    + "                  } " //
                    + "                }, " //
                    + "                { " //
                    + "                  \"bucketRestriction\": { " //
                    + "                    \"bkt\": { " //
                    + "                      \"Lbl\": \"< 3\", " //
                    + "                      \"Cnt\": 23987, " //
                    + "                      \"Id\": 1, " //
                    + "                      \"Rng\": [ " //
                    + "                        null, " //
                    + "                        3 " //
                    + "                      ] " //
                    + "                    }, " //
                    + "                    \"attr\": \"Account.BusinessTechnologiesAnalytics\" " //
                    + "                  } " //
                    + "                } " //
                    + "              ] " //
                    + "            } " //
                    + "          }, " //
                    + "          { " //
                    + "            \"logicalRestriction\": { " //
                    + "              \"operator\": \"OR\", " //
                    + "              \"restrictions\": [] " //
                    + "            } " //
                    + "          } " //
                    + "        ] " //
                    + "      } " //
                    + "    } " //
                    + "} ";

    String ratingRuleJson = //
            " { " //
                    + "   \"bucketToRuleMap\": { " //
                    + "     \"A\": { " //
                    + "       \"accountRule\": { " //
                    + "         \"concreteRestriction\": { " //
                    + "           \"negate\": false, " //
                    + "           \"lhs\": { " //
                    + "             \"attribute\": { " //
                    + "               \"entity\": \"Account\", " //
                    + "               \"attribute\": \"LDC_Name\" " //
                    + "             } " //
                    + "           }, " //
                    + "           \"relation\": \"IN_RANGE\", " //
                    + "           \"rhs\": { " //
                    + "             \"range\": { " //
                    + "               \"min\": \"A\", " //
                    + "               \"max\": \"G\" " //
                    + "             } " //
                    + "           } " //
                    + "         } " //
                    + "       } " //
                    + "     }, " //
                    + "     \"A-\": { " //
                    + "       \"contactRule\": { " //
                    + "         \"logicalRestriction\": { " //
                    + "           \"operator\": \"AND\", " //
                    + "           \"restrictions\": [] " //
                    + "         } " //
                    + "       }, " //
                    + "       \"accountRule\": { " //
                    + "         \"logicalRestriction\": { " //
                    + "           \"operator\": \"AND\", " //
                    + "           \"restrictions\": [] " //
                    + "         } " //
                    + "       } " //
                    + "     }, " //
                    + "     \"B\": { " //
                    + "       \"contactRule\": { " //
                    + "         \"logicalRestriction\": { " //
                    + "           \"operator\": \"AND\", " //
                    + "           \"restrictions\": [] " //
                    + "         } " //
                    + "       }, " //
                    + "       \"accountRule\": { " //
                    + "         \"logicalRestriction\": { " //
                    + "           \"operator\": \"AND\", " //
                    + "           \"restrictions\": [] " //
                    + "         } " //
                    + "       } " //
                    + "     }, " //
                    + "     \"C\": { " //
                    + "       \"accountRule\": { " //
                    + "         \"concreteRestriction\": { " //
                    + "           \"negate\": false, " //
                    + "           \"lhs\": { " //
                    + "             \"attribute\": { " //
                    + "               \"entity\": \"Account\", " //
                    + "               \"attribute\": \"LDC_Name\" " //
                    + "             } " //
                    + "           }, " //
                    + "           \"relation\": \"IN_RANGE\", " //
                    + "           \"rhs\": { " //
                    + "             \"range\": { " //
                    + "               \"min\": \"h\", " //
                    + "               \"max\": \"n\" " //
                    + "             } " //
                    + "           } " //
                    + "         } " //
                    + "       } " //
                    + "     }, " //
                    + "     \"D\": { " //
                    + "       \"contactRule\": { " //
                    + "         \"logicalRestriction\": { " //
                    + "           \"operator\": \"AND\", " //
                    + "           \"restrictions\": [] " //
                    + "         } " //
                    + "       }, " //
                    + "       \"accountRule\": { " //
                    + "         \"concreteRestriction\": { " //
                    + "           \"negate\": false, " //
                    + "           \"lhs\": { " //
                    + "             \"attribute\": { " //
                    + "               \"entity\": \"Account\", " //
                    + "               \"attribute\": \"LDC_Name\" " //
                    + "             } " //
                    + "           }, " //
                    + "           \"relation\": \"IN_RANGE\", " //
                    + "           \"rhs\": { " //
                    + "             \"range\": { " //
                    + "               \"min\": \"A\", " //
                    + "               \"max\": \"O\" " //
                    + "             } " //
                    + "           } " //
                    + "         } " //
                    + "       } " //
                    + "     }, " //
                    + "     \"F\": { " //
                    + "       \"contactRule\": { " //
                    + "         \"logicalRestriction\": { " //
                    + "           \"operator\": \"AND\", " //
                    + "           \"restrictions\": [] " //
                    + "         } " //
                    + "       }, " //
                    + "       \"accountRule\": { " //
                    + "         \"logicalRestriction\": { " //
                    + "           \"operator\": \"AND\", " //
                    + "           \"restrictions\": [] " //
                    + "         } " //
                    + "       } " //
                    + "     } " //
                    + "   }, " //
                    + "   \"defaultBucketName\": \"C\" " //
                    + " } ";
}
