package com.latticeengines.apps.cdl.service.impl;

import static com.latticeengines.domain.exposed.serviceapps.core.AttrState.Active;
import static com.latticeengines.domain.exposed.serviceapps.core.AttrState.Inactive;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.mds.RawSystemMetadataStore;
import com.latticeengines.apps.cdl.mds.SystemMetadataStore;
import com.latticeengines.apps.cdl.service.CDLExternalSystemService;
import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.apps.core.service.AttrConfigService;
import com.latticeengines.apps.core.service.ZKConfigService;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystem;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
import com.latticeengines.domain.exposed.util.ActivityMetricsUtils;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.testframework.exposed.service.CDLTestDataService;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * $ dpltc deploy -a admin,matchapi,pls,metadata,cdl,lp
 */
public class AttrConfigServiceImplDeploymentTestNG extends CDLDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(AttrConfigServiceImplDeploymentTestNG.class);

    private static final String CRM_ID = "CrmAccount_External_ID";

    @Inject
    private CDLTestDataService cdlTestDataService;

    @Inject
    private AttrConfigService attrConfigService;

    @Inject
    private ColumnMetadataProxy columnMetadataProxy;

    @Inject
    private BatonService batonService;

    @Inject
    private ZKConfigService zkConfigService;

    @Inject
    private ServingStoreProxy servingStoreProxy;

    @Inject
    private CDLExternalSystemService externalSystemService;

    private Set<String> accountStandardAttrs = SchemaRepository.getStandardAttributes(BusinessEntity.Account).stream() //
            .map(InterfaceName::name).collect(Collectors.toSet());
    private Set<String> accountSystemAttrs = SchemaRepository.getSystemAttributes(BusinessEntity.Account).stream() //
            .map(InterfaceName::name).collect(Collectors.toSet());
    private Set<String> accountExportAttrs = SchemaRepository.getDefaultExportAttributes(BusinessEntity.Account)
            .stream() //
            .map(InterfaceName::name).collect(Collectors.toSet());

    private Set<String> contactStandardAttrs = SchemaRepository.getStandardAttributes(BusinessEntity.Contact).stream() //
            .map(InterfaceName::name).collect(Collectors.toSet());
    private Set<String> contactSystemAttrs = SchemaRepository.getSystemAttributes(BusinessEntity.Contact).stream() //
            .map(InterfaceName::name).collect(Collectors.toSet());
    private Set<String> contactExportAttrs = SchemaRepository.getDefaultExportAttributes(BusinessEntity.Contact) //
            .stream().map(InterfaceName::name).collect(Collectors.toSet());

    private final Set<String> internalEnrichAttrs = new HashSet<>();
    private final Set<String> cannotSegmentAttrs = new HashSet<>();
    private final Set<String> cannotEnrichmentAttrs = new HashSet<>();
    private final Set<String> cannotModelAttrs = new HashSet<>();
    private final Set<String> deprecatedAttrs = new HashSet<>();
    private Scheduler scheduler = Schedulers.newParallel("verification");

    @BeforeClass(groups = "deployment-app")
    public void setup() throws Exception {
        List<Runnable> runnables = new ArrayList<>();
        runnables.add(() -> {
            setupTestEnvironment();
            cdlTestDataService.populateMetadata(mainTestTenant.getId(), 5);
            batonService.setFeatureFlag(CustomerSpace.parse(mainTestTenant.getId()), //
                    LatticeFeatureFlag.ENABLE_INTERNAL_ENRICHMENT_ATTRIBUTES, false);
        });
        runnables.add(() -> {
            List<ColumnMetadata> amCols = columnMetadataProxy.getAllColumns();
            amCols.forEach(cm -> {
                if (Boolean.TRUE.equals(cm.getCanInternalEnrich())) {
                    internalEnrichAttrs.add(cm.getAttrName());
                }
                if (!cm.isEnabledFor(ColumnSelection.Predefined.Segment)) {
                    cannotSegmentAttrs.add(cm.getAttrName());
                }
                if (!cm.isEnabledFor(ColumnSelection.Predefined.Enrichment)) {
                    cannotEnrichmentAttrs.add(cm.getAttrName());
                }
                if (!cm.isEnabledFor(ColumnSelection.Predefined.Model)) {
                    cannotModelAttrs.add(cm.getAttrName());
                }
                if (Boolean.TRUE.equals(cm.getShouldDeprecate())) {
                    deprecatedAttrs.add(cm.getAttrName());
                }
            });
        });
        ExecutorService tp = ThreadPoolUtils.getFixedSizeThreadPool("test-setup", 2);
        ThreadPoolUtils.runRunnablesInParallel(tp, runnables, 30, 1);
        tp.shutdown();
        MultiTenantContext.setTenant(mainTestTenant);
        Assert.assertFalse(zkConfigService.isInternalEnrichmentEnabled(CustomerSpace.parse(mainCustomerSpace)));

        // setup external id attrs
        createExternalSystem();

        // TODO: setup rating engines and rating attrs

    }

    @Test(groups = "deployment-app")
    public void test() {
        testLDCAttrs();
        testContactAttributes();
        testMyAttributes();
        testProductSpendAttributes();
        testCuratedAccountAttributes();
    }

    @Test(groups = "deployment-app")
    public void testServingStoreProxy() {
        Flux<ColumnMetadata> newModelingAttrs = servingStoreProxy.getNewModelingAttrs(mainTestTenant.getId());
        Predicate<ColumnMetadata> p = attr -> ApprovedUsage.MODEL_ALLINSIGHTS.equals(attr.getApprovedUsageList().get(0))
                && attr.getTagList() != null;
        Assert.assertTrue(newModelingAttrs.all(p).block());

        Flux<ColumnMetadata> allModelingAttrs = servingStoreProxy.getAllowedModelingAttrs(mainTestTenant.getId());
        p = ColumnMetadata::getCanModel;
        Assert.assertTrue(allModelingAttrs.all(p).block());
    }

    private void testMyAttributes() {
        final Category cat = Category.ACCOUNT_ATTRIBUTES;
        checkAndVerifyCategory(cat, config -> {
            String attrName = config.getAttrName();
            Assert.assertNotNull(attrName, JsonUtils.pprint(config));
            String partition = getMyAttributesPartition(attrName);
            switch (partition) {
            case Partition.SYSTEM:
                verifySystemAttr(config, cat);
                break;
            case Partition.STD_ATTRS:
                boolean exportByDefault = accountExportAttrs.contains(attrName);
                verifyFlags(config, cat, partition, //
                        Active, false, //
                        true, true, //
                        exportByDefault, true, //
                        true, true, //
                        false, true, //
                        false, true);
                break;
            case Partition.EXTERNAL_ID:
                verifyFlags(config, cat, partition, //
                        Active, false, //
                        true, true, //
                        false, true, //
                        true, true, //
                        false, true, //
                        false, false);
                break;
            case Partition.OTHERS:
                verifyFlags(config, cat, partition, //
                        Active, true, //
                        true, true, //
                        false, true, //
                        true, true, //
                        false, true, //
                        false, true);
            }
            return true;
        });
    }

    private String getMyAttributesPartition(String attrName) {
        String partiion;
        if (accountSystemAttrs.contains(attrName)) {
            partiion = Partition.SYSTEM;
        } else if (accountStandardAttrs.contains(attrName)) {
            partiion = Partition.STD_ATTRS;
        } else if (CRM_ID.equals(attrName)) {
            partiion = Partition.EXTERNAL_ID;
        } else {
            partiion = Partition.OTHERS;
        }
        return partiion;
    }

    private void testContactAttributes() {
        final Category cat = Category.CONTACT_ATTRIBUTES;
        checkAndVerifyCategory(cat, (config) -> {
            String attrName = config.getAttrName();
            Assert.assertNotNull(attrName, JsonUtils.pprint(config));
            String partition = getContactAttributesPartition(attrName);
            switch (partition) {
            case Partition.SYSTEM:
                verifySystemAttr(config, cat);
                break;
            case Partition.STD_ATTRS:
                boolean exportByDefault = contactExportAttrs.contains(attrName);
                verifyFlags(config, cat, partition, //
                        Active, false, //
                        true, true, //
                        exportByDefault, true, //
                        true, true, //
                        false, true, //
                        false, false);
                break;
            case Partition.OTHERS:
                verifyFlags(config, cat, partition, //
                        Active, true, //
                        true, true, //
                        false, true, //
                        true, true, //
                        false, true, //
                        false, false);
            }
            return true;
        });
    }

    private String getContactAttributesPartition(String attrName) {
        String partiion;
        if (contactSystemAttrs.contains(attrName)) {
            partiion = Partition.SYSTEM;
        } else if (contactStandardAttrs.contains(attrName)) {
            partiion = Partition.STD_ATTRS;
        } else {
            partiion = Partition.OTHERS;
        }
        return partiion;
    }

    private void testProductSpendAttributes() {
        final Category cat = Category.PRODUCT_SPEND;
        checkAndVerifyCategory(cat, (config) -> {
            String attrName = config.getAttrName();
            Assert.assertNotNull(attrName, JsonUtils.pprint(config));
            String partition = getProductSpentPartition(attrName);
            switch (partition) {
                case Partition.HAS_PURCHASED:
                    verifyFlags(config, cat, partition, //
                            Active, false, //
                            true, true, //
                            false, false, //
                            false, false, //
                            false, false, //
                            false, false);
                    break;
                case Partition.OTHERS:
                    verifyFlags(config, cat, partition, //
                            Active, false, //
                            true, true, //
                            false, true, //
                            true, true, //
                            false, true, //
                            false, false);
            }
            return true;
        });
    }

    private String getProductSpentPartition(String attrName) {
        String partiion;
        if (ActivityMetricsUtils.isHasPurchasedAttr(attrName)) {
            partiion = Partition.HAS_PURCHASED;
        } else {
            partiion = Partition.OTHERS;
        }
        return partiion;
    }

    private void testCuratedAccountAttributes() {
        final Category cat = Category.CURATED_ACCOUNT_ATTRIBUTES;
        checkAndVerifyCategory(cat, (config) -> {
            String attrName = config.getAttrName();
            Assert.assertNotNull(attrName, JsonUtils.pprint(config));
            verifyFlags(config, cat, null, //
                    Active, false, //
                    false, true, //
                    false, true, //
                    true, true, //
                    false, true, //
                    false, false);
            return true;
        });
    }

    private void testLDCAttrs() {
        checkAndVerifyCategory(Category.FIRMOGRAPHICS, (config) -> {
            AttrState initialState = AttrState.Active;
            boolean[] flags = new boolean[] { true, // life cycle change
                    true, true, // segment
                    true, true, // export
                    true, true, // tp
                    false, true, // cp
                    true, true // model
            };
            initialState = overwrite11Flags(flags, initialState, config.getAttrName());
            String partition = getLDCPartition(config.getAttrName());
            verifyFlags(config, Category.FIRMOGRAPHICS, partition, initialState, flags);
            return true;
        });

        checkAndVerifyCategory(Category.GROWTH_TRENDS, (config) -> {
            AttrState initialState = AttrState.Active;
            boolean[] flags = new boolean[] { true, // life cycle change
                    true, true, // segment
                    false, true, // export
                    true, true, // tp
                    false, true, // cp
                    true, true // model
            };
            initialState = overwrite11Flags(flags, initialState, config.getAttrName());
            String partition = getLDCPartition(config.getAttrName());
            verifyFlags(config, Category.GROWTH_TRENDS, partition, initialState, flags);
            return true;
        });

        checkAndVerifyCategory(Category.ONLINE_PRESENCE, (config) -> {
            AttrState initialState = AttrState.Active;
            boolean[] flags = new boolean[] { true, // life cycle change
                    true, true, // segment
                    false, true, // export
                    true, true, // tp
                    false, true, // cp
                    true, true // model
            };
            initialState = overwrite11Flags(flags, initialState, config.getAttrName());
            String partition = getLDCPartition(config.getAttrName());
            verifyFlags(config, Category.ONLINE_PRESENCE, partition, initialState, flags);
            return true;
        });

        checkAndVerifyCategory(Category.WEBSITE_PROFILE, (config) -> {
            AttrState initialState = AttrState.Active;
            boolean[] flags = new boolean[] { true, // life cycle change
                    true, true, // segment
                    false, true, // export
                    true, true, // tp
                    false, true, // cp
                    true, true // model
            };
            initialState = overwrite11Flags(flags, initialState, config.getAttrName());
            String partition = getLDCPartition(config.getAttrName());
            verifyFlags(config, Category.WEBSITE_PROFILE, partition, initialState, flags);
            return true;
        });

        checkAndVerifyCategory(Category.INTENT, (config) -> {
            AttrState initialState = AttrState.Inactive;
            boolean[] flags = new boolean[] { true, // life cycle change
                    true, true, // segment
                    true, true, // export
                    true, true, // tp
                    false, true, // cp
                    true, true // model
            };
            initialState = overwrite11Flags(flags, initialState, config.getAttrName());
            String partition = getLDCPartition(config.getAttrName());
            verifyFlags(config, Category.INTENT, partition, initialState, flags);
            return true;
        });

        checkAndVerifyCategory(Category.TECHNOLOGY_PROFILE, (config) -> {
            AttrState initialState = AttrState.Inactive;
            boolean[] flags = new boolean[] { true, // life cycle change
                    true, true, // segment
                    true, true, // export
                    true, true, // tp
                    false, true, // cp
                    true, true // model
            };
            initialState = overwrite11Flags(flags, initialState, config.getAttrName());
            String partition = getLDCPartition(config.getAttrName());
            verifyFlags(config, Category.TECHNOLOGY_PROFILE, partition, initialState, flags);
            return true;
        });

        checkAndVerifyCategory(Category.WEBSITE_KEYWORDS, (config) -> {
            AttrState initialState = AttrState.Inactive;
            boolean[] flags = new boolean[] { true, // life cycle change
                    true, true, // segment
                    true, true, // export
                    true, true, // tp
                    false, true, // cp
                    true, true // model
            };
            initialState = overwrite11Flags(flags, initialState, config.getAttrName());
            String partition = getLDCPartition(config.getAttrName());
            verifyFlags(config, Category.WEBSITE_KEYWORDS, partition, initialState, flags);
            return true;
        });
    }

    private AttrState overwrite11Flags(boolean[] flags, AttrState initialState, String attrName) {
        AttrState state = initialState;
        if (cannotSegmentAttrs.contains(attrName)) {
            flags[1] = false;
            flags[2] = false;
        }
        if (cannotEnrichmentAttrs.contains(attrName) || internalEnrichAttrs.contains(attrName)) {
            flags[3] = false;
            flags[4] = false;
            flags[5] = false;
            flags[6] = false;
            flags[7] = false;
            flags[8] = false;
        }
        if (cannotModelAttrs.contains(attrName)) {
            flags[9] = false;
            flags[10] = false;
        }
        if (!flags[2] && !flags[4] && !flags[10]) {
            // if cannot enable for segment, enrichment or model
            // the cannot be activated
            flags[0] = false;
        }
        if (deprecatedAttrs.contains(attrName)) {
            // deprecated attrs are inactive and cannot change
            state = AttrState.Inactive;
            flags[0] = false;
        }
        if (AttrState.Inactive.equals(state)) {
            // cannot change usage for inactive attributes
            flags[2] = false;
            flags[4] = false;
            flags[6] = false;
            flags[8] = false;
            flags[10] = false;
        }
        return state;
    }

    private String getLDCPartition(String attrName) {
        List<String> partitions = new ArrayList<>();
        if (cannotSegmentAttrs.contains(attrName)) {
            partitions.add("CannotSegment");
        }
        if (cannotEnrichmentAttrs.contains(attrName)) {
            partitions.add("CannotEnrich");
        }
        if (internalEnrichAttrs.contains(attrName)) {
            partitions.add("InternalEnrich");
        }
        if (cannotModelAttrs.contains(attrName)) {
            partitions.add("CannotModel");
        }
        if (CollectionUtils.isNotEmpty(partitions)) {
            return StringUtils.join(partitions, " & ");
        } else {
            return Partition.OTHERS;
        }
    }

    private void checkAndVerifyCategory(Category category, Function<AttrConfig, Boolean> verifier) {
        List<AttrConfig> attrConfigs = attrConfigService.getRenderedList(category);
//        System.out.println(attrConfigs);
        Assert.assertTrue(CollectionUtils.isNotEmpty(attrConfigs));
        Long count = Flux.fromIterable(attrConfigs).parallel().runOn(scheduler) //
                .map(verifier).sequential().count().block();
        log.info("Verified " + count + " attr configs in the category " + category);
    }

    private void verifySystemAttr(AttrConfig attrConfig, Category category) {
        verifyFlags(attrConfig, category, Partition.SYSTEM, //
                Inactive, false, //
                false, false, //
                false, false, //
                false, false, //
                false, false, //
                false, false);
    }

    private void verifyFlags(AttrConfig attrConfig, Category category, String partition, AttrState initState,
            boolean[] flags) {
        verifyFlags(attrConfig, category, partition, initState, flags[0], flags[1], flags[2], flags[3], flags[4],
                flags[5], flags[6], flags[7], flags[8], flags[9], flags[10]);
    }

    private void verifyFlags(AttrConfig attrConfig, Category category, String partition, //
            AttrState initState, boolean lcChg, //
            boolean segment, boolean segChg, //
            boolean export, boolean exportChg, //
            boolean talkingPoint, boolean tpChg, //
            boolean companyProfile, boolean cpChg, //
            boolean model, boolean modelChg //
    ) {
        String attrName = attrConfig.getAttrName();
        String displayName = attrConfig.getPropertyFinalValue(ColumnMetadataKey.DisplayName, String.class);
        String logPrefix;
        if (StringUtils.isNotBlank(partition)) {
            logPrefix = String.format("%s (%s) [%s - %s]", attrName, displayName, category.getName(), partition);
        } else {
            logPrefix = String.format("%s (%s) [%s]", attrName, displayName, category.getName());
        }
        String property = ColumnMetadataKey.State;
        AttrState state = attrConfig.getPropertyFinalValue(property, AttrState.class);
        Assert.assertEquals(state, initState, //
                String.format("%s should be in the state of %s but found to be %s", logPrefix, initState, state));
        boolean chg = attrConfig.getProperty(property).isAllowCustomization();
        Assert.assertEquals(chg, lcChg, String.format("%s allow change life-cycle state", logPrefix));

        property = ColumnSelection.Predefined.Segment.name();
        verifyUsage(logPrefix, attrConfig, property, segment, segChg);
        property = ColumnSelection.Predefined.Enrichment.name();
        verifyUsage(logPrefix, attrConfig, property, export, exportChg);
        property = ColumnSelection.Predefined.TalkingPoint.name();
        verifyUsage(logPrefix, attrConfig, property, talkingPoint, tpChg);
        property = ColumnSelection.Predefined.CompanyProfile.name();
        verifyUsage(logPrefix, attrConfig, property, companyProfile, cpChg);
        property = ColumnSelection.Predefined.Model.name();
        verifyUsage(logPrefix, attrConfig, property, model, modelChg);
    }

    private void verifyUsage(String logPrefix, AttrConfig attrConfig, String property, boolean expectedValue,
            boolean expectedChg) {
        boolean enabled = Boolean.TRUE.equals(attrConfig.getPropertyFinalValue(property, Boolean.class));
        Assert.assertEquals(enabled, expectedValue,
                String.format("%s should be enabled for %s usage", logPrefix, property));
        boolean chg = attrConfig.getProperty(property).isAllowCustomization();
        Assert.assertEquals(chg, expectedChg, String.format("%s allow change %s usage", logPrefix, property));
    }

    private static final class Partition {
        static final String SYSTEM = "System";
        static final String STD_ATTRS = "StdAttrs";
        static final String EXTERNAL_ID = "ExternalID";
        static final String HAS_PURCHASED = "HasPurchased";
        static final String OTHERS = "Others";

        // skip verification on these attributes
        // may because cannot tell the partition
        // or just want to sample fewer attrs
        static final String SKIP = "Skip";
    }

    private void createExternalSystem() {
        CDLExternalSystem cdlExternalSystem = new CDLExternalSystem();
        List<String> crmIds = new ArrayList<>();
        crmIds.add(CRM_ID);
        cdlExternalSystem.setCRMIdList(crmIds);
        cdlExternalSystem.setEntity(BusinessEntity.Account);
        externalSystemService.createOrUpdateExternalSystem(mainCustomerSpace, cdlExternalSystem,
                BusinessEntity.Account);
    }

}
