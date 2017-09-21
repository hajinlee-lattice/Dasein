package com.latticeengines.leadprioritization.workflow.steps;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.UUID;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.playmakercore.Recommendation;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQueryConstants;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.PlayLaunchInitStepConfiguration;
import com.latticeengines.leadprioritization.workflow.steps.play.PlayLaunchInitStepTestHelper;
import com.latticeengines.playmakercore.service.RecommendationService;
import com.latticeengines.pls.controller.PlayResourceDeploymentTestNG;
import com.latticeengines.proxy.exposed.dante.DanteLeadProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.testframework.service.impl.GlobalAuthDeploymentTestBed;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-pls-context.xml", "classpath:playmakercore-context.xml",
        "classpath:test-playlaunch-properties-context.xml" })
public class PlayLaunchInitStepDeploymentTestNG extends AbstractTestNGSpringContextTests {

    private PlayLaunchInitStep playLaunchInitStep;

    private PlayLaunchInitStepTestHelper helper;

    @Autowired
    @Qualifier(value = "deploymentTestBed")
    protected GlobalAuthDeploymentTestBed deploymentTestBed;

    @Autowired
    private PlayResourceDeploymentTestNG playResourceDeploymentTestNG;

    @Mock
    PlayLaunchInitStepConfiguration configuration;

    // no mocking needed
    EntityProxy entityProxy;

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    private InternalResourceRestApiProxy internalResourceRestApiProxy;

    @Autowired
    RecommendationService recommendationService;

    @Mock
    DanteLeadProxy danteLeadProxy;

    @Autowired
    TenantEntityMgr tenantEntityMgr;

    String randId = UUID.randomUUID().toString();

    @BeforeClass(groups = "workflow")
    public void setup() throws Exception {
        // this test tenant has account data loaded in CDL
        String tenantIdentifier = "hliu_09_07_fisher.hliu_09_07_fisher.Production";
        Tenant tenant = null;
        tenant = tenantEntityMgr.findByTenantId(tenantIdentifier);
        if (tenant == null) {
            System.out.println("Creating new tenant: " + tenantIdentifier);
            tenant = deploymentTestBed.bootstrapForProduct(tenantIdentifier, LatticeProduct.LPA3);
        } else {
            deploymentTestBed.loginAD();
            deploymentTestBed.getTestTenants().add(tenant);
        }
        
        playResourceDeploymentTestNG.setShouldSkipAutoTenantCreation(true);
        playResourceDeploymentTestNG.setMainTestTenant(tenant);
        playResourceDeploymentTestNG.setup();

        Restriction accountRestriction = JsonUtils.deserialize(accountRestrictionJson, Restriction.class);
        Restriction contactRestriction = JsonUtils.deserialize(contactRestrictionJson, Restriction.class);

        MetadataSegment segment = playResourceDeploymentTestNG.createSegment(
                accountRestriction, contactRestriction);
        playResourceDeploymentTestNG.createRatingEngine(segment);

        playResourceDeploymentTestNG.getCrud();
        playResourceDeploymentTestNG.createPlayLaunch();

        Play play = playResourceDeploymentTestNG.getPlay();
        PlayLaunch playLaunch = playResourceDeploymentTestNG.getPlayLaunch();

        String playId = play.getName();
        String playLaunchId = playLaunch.getId();
        long pageSize = 2L;

        internalResourceRestApiProxy = new InternalResourceRestApiProxy(internalResourceHostPort);

        MockitoAnnotations.initMocks(this);

        initEntityProxy();

        mockDanteLeadProxy();

        helper = new PlayLaunchInitStepTestHelper(internalResourceRestApiProxy, entityProxy, recommendationService,
                danteLeadProxy, pageSize);

        playLaunchInitStep = new PlayLaunchInitStep();
        playLaunchInitStep.setPlayLaunchProcessor(helper.getPlayLaunchProcessor());
        playLaunchInitStep.setInternalResourceRestApiProxy(internalResourceRestApiProxy);
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

    private void mockDanteLeadProxy() {
        doNothing() //
                .when(danteLeadProxy) //
                .create(any(Recommendation.class), anyString());
    }

    String accountRestrictionJson = //
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

    String contactRestrictionJson = //
            "{ " //
                    + "      \"logicalRestriction\": {}} ";

    String ratingRuleJson = //
            " { " //
                    + "   \"bucketToRuleMap\": { " //
                    + "     \"A-\": { " //
                    + "       \"" + FrontEndQueryConstants.ACCOUNT_RESTRICTION + "\": { " //
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
                    + "     \"A\": { " //
                    + "       \"" + FrontEndQueryConstants.CONTACT_RESTRICTION + "\": { " //
                    + "         \"logicalRestriction\": { " //
                    + "           \"operator\": \"AND\", " //
                    + "           \"restrictions\": [] " //
                    + "         } " //
                    + "       }, " //
                    + "       \"" + FrontEndQueryConstants.ACCOUNT_RESTRICTION + "\": { " //
                    + "         \"logicalRestriction\": { " //
                    + "           \"operator\": \"AND\", " //
                    + "           \"restrictions\": [] " //
                    + "         } " //
                    + "       } " //
                    + "     }, " //
                    + "     \"B\": { " //
                    + "       \"" + FrontEndQueryConstants.CONTACT_RESTRICTION + "\": { " //
                    + "         \"logicalRestriction\": { " //
                    + "           \"operator\": \"AND\", " //
                    + "           \"restrictions\": [] " //
                    + "         } " //
                    + "       }, " //
                    + "       \"" + FrontEndQueryConstants.ACCOUNT_RESTRICTION + "\": { " //
                    + "         \"logicalRestriction\": { " //
                    + "           \"operator\": \"AND\", " //
                    + "           \"restrictions\": [] " //
                    + "         } " //
                    + "       } " //
                    + "     }, " //
                    + "     \"C\": { " //
                    + "       \"" + FrontEndQueryConstants.ACCOUNT_RESTRICTION + "\": { " //
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
                    + "       \"" + FrontEndQueryConstants.CONTACT_RESTRICTION + "\": { " //
                    + "         \"logicalRestriction\": { " //
                    + "           \"operator\": \"AND\", " //
                    + "           \"restrictions\": [] " //
                    + "         } " //
                    + "       }, " //
                    + "       \"" + FrontEndQueryConstants.ACCOUNT_RESTRICTION + "\": { " //
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
                    + "       \"" + FrontEndQueryConstants.CONTACT_RESTRICTION + "\": { " //
                    + "         \"logicalRestriction\": { " //
                    + "           \"operator\": \"AND\", " //
                    + "           \"restrictions\": [] " //
                    + "         } " //
                    + "       }, " //
                    + "       \"" + FrontEndQueryConstants.ACCOUNT_RESTRICTION + "\": { " //
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