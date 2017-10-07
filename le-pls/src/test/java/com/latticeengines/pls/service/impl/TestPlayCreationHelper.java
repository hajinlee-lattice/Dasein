package com.latticeengines.pls.service.impl;

import java.lang.reflect.Field;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.testng.Assert;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.RatingRule;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQueryConstants;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.service.SegmentService;
import com.latticeengines.pls.controller.PlayResourceDeploymentTestNG;
import com.latticeengines.pls.entitymanager.RatingEngineEntityMgr;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;
import com.latticeengines.testframework.service.impl.GlobalAuthDeploymentTestBed;

@Component
public class TestPlayCreationHelper {

    private static final Logger log = LoggerFactory.getLogger(TestPlayCreationHelper.class);

    @Autowired
    @Qualifier(value = "deploymentTestBed")
    protected GlobalAuthDeploymentTestBed deploymentTestBed;

    @Autowired
    private PlayResourceDeploymentTestNG playResourceDeploymentTestNG;

    @Autowired
    private RatingEngineEntityMgr ratingEngineEntityMgr;
    @Autowired
    private SegmentService segmentService;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    // this test tenant has account data loaded in CDL
    private String tenantIdentifier = "CDLTest_Lynn_0920.CDLTest_Lynn_0920.Production";

    private boolean tenantCleanupAllowed = false;

    private Tenant tenant;

    private Play play;

    private PlayLaunch playLaunch;

    private MetadataSegment segment;

    public void setupTenant() {
        setupTenant(tenantIdentifier);
    }

    public void setupTenant(String customTenantIdentifier) {
        if (StringUtils.isNoneBlank(customTenantIdentifier)) {
            tenantIdentifier = customTenantIdentifier;
        }

        tenant = tenantEntityMgr.findByTenantId(tenantIdentifier);
        if (tenant == null) {
            System.out.println("Creating new tenant: " + tenantIdentifier);
            tenantCleanupAllowed = true;
            tenant = deploymentTestBed.bootstrapForProduct(tenantIdentifier, LatticeProduct.LPA3);
        } else {
            deploymentTestBed.loginAD();
            deploymentTestBed.getTestTenants().add(tenant);
        }

        tenant = tenantEntityMgr.findByTenantId(tenantIdentifier);
        MultiTenantContext.setTenant(tenant);
    }

    public void setupTenantAndCreatePlay() throws Exception {
        setupTenantAndCreatePlay(tenantIdentifier);
    }

    public void setupTenantAndCreatePlay(String customTenantIdentifier) throws Exception {
        if (StringUtils.isNoneBlank(customTenantIdentifier)) {
            tenantIdentifier = customTenantIdentifier;
        }
        setupTenant(tenantIdentifier);

        playResourceDeploymentTestNG.setShouldSkipAutoTenantCreation(true);
        playResourceDeploymentTestNG.setMainTestTenant(tenant);
        playResourceDeploymentTestNG.setup();

        Restriction accountRestriction = JsonUtils.deserialize(accountRestrictionJson, Restriction.class);
        Restriction contactRestriction = JsonUtils.deserialize(contactRestrictionJson, Restriction.class);
        RatingRule ratingRule = JsonUtils.deserialize(ratingRuleJson, RatingRule.class);

        segment = playResourceDeploymentTestNG.createSegment(accountRestriction, contactRestriction);
        playResourceDeploymentTestNG.createRatingEngine(segment, ratingRule);

        playResourceDeploymentTestNG.getCrud();
        playResourceDeploymentTestNG.createPlayLaunch();

        play = playResourceDeploymentTestNG.getPlay();
        playLaunch = playResourceDeploymentTestNG.getPlayLaunch();

        Assert.assertNotNull(play);
        Assert.assertNotNull(playLaunch);
    }

    public Tenant getTenant() {
        return tenant;
    }

    public Play getPlay() {
        return play;
    }

    public PlayLaunch getPlayLaunch() {
        return playLaunch;
    }

    @SuppressWarnings("unchecked")
    public EntityProxy initEntityProxy() throws NoSuchFieldException, IllegalAccessException {
        Field propMapField = PropertyUtils.class.getDeclaredField("propertiesMap");
        propMapField.setAccessible(true);
        Map<String, String> propertiesMap = (Map<String, String>) propMapField.get(null);

        // temporary
        propertiesMap.put("common.microservice.url",
                "https://internal-private-lpi-b-282775961.us-east-1.elb.amazonaws.com");

        EntityProxy entityProxy = new EntityProxy();

        Field f1 = entityProxy.getClass().getSuperclass().getSuperclass().getDeclaredField("initialWaitMsec");
        f1.setAccessible(true);
        f1.set(entityProxy, 1000L);

        f1 = entityProxy.getClass().getSuperclass().getSuperclass().getDeclaredField("multiplier");
        f1.setAccessible(true);
        f1.set(entityProxy, 2D);

        f1 = entityProxy.getClass().getSuperclass().getSuperclass().getDeclaredField("maxAttempts");
        f1.setAccessible(true);
        f1.set(entityProxy, 10);

        return entityProxy;
    }

    public void cleanupArtifacts() {
        try {
            log.info("Cleaning up play launch: " + playLaunch.getId());
            playResourceDeploymentTestNG.deletePlayLaunch(play.getName(), playLaunch.getId());
        } catch (Exception ex) {
            ignoreException(ex);
        }

        try {
            log.info("Cleaning up play: " + play.getName());
            playResourceDeploymentTestNG.deletePlay(play.getName());
        } catch (Exception ex) {
            ignoreException(ex);
        }

        try {
            log.info("Cleaning up rating engine: " + play.getRatingEngine().getId());
            ratingEngineEntityMgr.deleteById(play.getRatingEngine().getId());
        } catch (Exception ex) {
            ignoreException(ex);
        }

        try {
            log.info("Cleaning up segment: " + segment.getName());
            segmentService.deleteSegmentByName(tenantIdentifier, segment.getName());
        } catch (Exception ex) {
            ignoreException(ex);
        }
    }

    private void ignoreException(Exception ex) {
        log.info("Could not cleanup artifact. Ignoring exception: ", ex);
    }

    public void cleanupTenant() {
        if (tenantCleanupAllowed) {
            deploymentTestBed.deleteTenant(tenant);
        }
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
