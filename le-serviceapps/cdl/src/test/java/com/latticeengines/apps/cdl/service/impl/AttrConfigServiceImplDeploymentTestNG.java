package com.latticeengines.apps.cdl.service.impl;

import static com.latticeengines.domain.exposed.serviceapps.core.AttrState.Active;
import static com.latticeengines.domain.exposed.serviceapps.core.AttrState.Inactive;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.support.RetryTemplate;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.apps.core.service.AttrConfigService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.metadata.AttributeSet;
import com.latticeengines.domain.exposed.metadata.AttributeSetResponse;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigProp;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
import com.latticeengines.domain.exposed.util.ActivityMetricsUtils;
import com.latticeengines.domain.exposed.util.ApsGeneratorUtils;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * $ dpltc deploy -a admin,matchapi,pls,metadata,cdl,lp
 */
public class AttrConfigServiceImplDeploymentTestNG extends ServingStoreDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(AttrConfigServiceImplDeploymentTestNG.class);

    @Inject
    private AttrConfigService attrConfigService;

    private Set<String> accountAttributes = SchemaRepository.getDefaultExportAttributes(BusinessEntity.Account, true)
            .stream().map(InterfaceName::name).collect(Collectors.toSet());
    private Set<String> contactAttributes = SchemaRepository.getDefaultExportAttributes(BusinessEntity.Contact, true)
            .stream().map(InterfaceName::name).collect(Collectors.toSet());

    private Set<String> accountStandardAttrs = SchemaRepository.getStandardAttributes(BusinessEntity.Account, false)
            .stream() //
            .map(InterfaceName::name).collect(Collectors.toSet());
    private Set<String> accountStandardAttrsEntityMatchEnabled = SchemaRepository
            .getStandardAttributes(BusinessEntity.Account, true).stream() //
            .map(InterfaceName::name).collect(Collectors.toSet());
    private Set<String> accountSystemAttrs = SchemaRepository.getSystemAttributes(BusinessEntity.Account, false)
            .stream() //
            .map(InterfaceName::name).collect(Collectors.toSet());
    private Set<String> accountSystemAttrsEntityMatchEnabled = SchemaRepository
            .getSystemAttributes(BusinessEntity.Account, true).stream() //
            .map(InterfaceName::name).collect(Collectors.toSet());
    private Set<String> accountExportAttrs = SchemaRepository.getDefaultExportAttributes(BusinessEntity.Account, false)
            .stream() //
            .map(InterfaceName::name).collect(Collectors.toSet());
    private Set<String> accountExportAttrsEntityMatchEnabled = SchemaRepository
            .getDefaultExportAttributes(BusinessEntity.Account, true)
            .stream() //
            .map(InterfaceName::name).collect(Collectors.toSet());

    private Set<String> contactStandardAttrs = SchemaRepository.getStandardAttributes(BusinessEntity.Contact, false)
            .stream() //
            .map(InterfaceName::name).collect(Collectors.toSet());
    private Set<String> contactStandardAttrsEntityMatchEnabled = SchemaRepository
            .getStandardAttributes(BusinessEntity.Contact, true).stream() //
            .map(InterfaceName::name).collect(Collectors.toSet());
    private Set<String> contactSystemAttrs = SchemaRepository.getSystemAttributes(BusinessEntity.Contact, false)
            .stream() //
            .map(InterfaceName::name).collect(Collectors.toSet());
    private Set<String> contactSystemAttrsEntityMatchEnabled = SchemaRepository
            .getSystemAttributes(BusinessEntity.Contact, true).stream() //
            .map(InterfaceName::name).collect(Collectors.toSet());
    private Set<String> contactExportAttrs = SchemaRepository.getDefaultExportAttributes(BusinessEntity.Contact, false) //
            .stream().map(InterfaceName::name).collect(Collectors.toSet());
    private Set<String> contactExportAttrsEntityMatchEnabled = SchemaRepository
            .getDefaultExportAttributes(BusinessEntity.Contact, true) //
            .stream().map(InterfaceName::name).collect(Collectors.toSet());

    private Set<String> psSystemAttrs = SchemaRepository
            .getSystemAttributes(BusinessEntity.DepivotedPurchaseHistory, false) //
            .stream().map(InterfaceName::name).collect(Collectors.toSet());

    private Set<String> apsSystemAttrs = SchemaRepository
            .getSystemAttributes(BusinessEntity.AnalyticPurchaseState, false) //
            .stream().map(InterfaceName::name).collect(Collectors.toSet());

    private Set<String> caSystemAttrs = SchemaRepository.getSystemAttributes(BusinessEntity.CuratedAccount, false) //
            .stream().map(InterfaceName::name).collect(Collectors.toSet());

    private Scheduler scheduler = Schedulers.newParallel("verification");

    @Test(groups = "deployment-app", priority = 1)
    public void test() {
        testMyAttributes(false, false);
        testContactAttributes(false, false);
        testLDCAttrs();
        testProductSpendAttributes();
        testCuratedAccountAttributes();
    }

    @Test(groups = "deployment-app", priority = 2)
    public void testEntityMatchEnabledGA() {
        // Enable EntityMatch GA feature flag
        testBed.overwriteFeatureFlag(mainTestTenant, LatticeFeatureFlag.ENABLE_ENTITY_MATCH_GA.getName(), true);

        testMyAttributes(true, true);
        testContactAttributes(true, true);
    }

    @Test(groups = "deployment-app", priority = 3)
    public void testEntityMatchEnabled() {
        // Enable EntityMatch feature flag
        testBed.overwriteFeatureFlag(mainTestTenant, LatticeFeatureFlag.ENABLE_ENTITY_MATCH_GA.getName(), false);
        testBed.overwriteFeatureFlag(mainTestTenant, LatticeFeatureFlag.ENABLE_ENTITY_MATCH.getName(), true);

        testMyAttributes(true, false);
        testContactAttributes(true, false);
    }

    @Test(groups = "deployment-app", priority = 4)
    public void testCrudAttributeSet() {
        String displayName = "TestAttributeSet";
        AttributeSet attributeSet = createAttributeSet(displayName);
        AttributeSetResponse attributeSetResponse = attrConfigService.createAttributeSet(attributeSet);
        attributeSet = attributeSetResponse.getAttributeSet();
        String generatedName = attributeSet.getName();
        log.info("Attribute set name {}", generatedName);
        AtomicReference<AttributeSet> attributeSetAtom = new AtomicReference<>();
        RetryTemplate retry = RetryUtils.getRetryTemplate(10, //
                Collections.singleton(AssertionError.class), null);
        retry.execute(context -> {
            attributeSetAtom.set(attrConfigService.getAttributeSetByName(generatedName));
            Assert.assertNotNull(attributeSetAtom.get());
            return true;
        });
        attributeSet = attributeSetAtom.get();
        Assert.assertEquals(attributeSet.getAttributesMap().get(Category.ACCOUNT_ATTRIBUTES.name()).size(), accountAttributes.size());
        Assert.assertEquals(attributeSet.getDisplayName(), displayName);
        List<AttrConfig> attrConfigs = attrConfigService.getRenderedList(Category.CONTACT_ATTRIBUTES, attributeSet.getName());
        for (AttrConfig attrConfig : attrConfigs) {
            if (contactAttributes.contains(attrConfig.getAttrName())) {
                Assert.assertTrue(((AttrConfigProp<Boolean>) attrConfig.getAttrProps().get(ColumnSelection.Predefined.Enrichment.name())).getCustomValue());
            }
        }
    }

    private AttributeSet createAttributeSet(String displayName) {
        AttributeSet attributeSet = new AttributeSet();
        attributeSet.setDisplayName(displayName);
        Map<String, Set<String>> attributesMap = new HashMap<>();
        attributesMap.put(Category.ACCOUNT_ATTRIBUTES.name(), accountAttributes);
        attributesMap.put(Category.CONTACT_ATTRIBUTES.name(), contactAttributes);
        attributeSet.setAttributesMap(attributesMap);
        return attributeSet;
    }

    private void testMyAttributes(boolean entityMatchEnabled, boolean onlyEntityMatchGAEnabled) {
        final Category cat = Category.ACCOUNT_ATTRIBUTES;
        checkAndVerifyCategory(cat, config -> {
            String attrName = config.getAttrName();
            Assert.assertNotNull(attrName, JsonUtils.pprint(config));
            String partition = getMyAttributesPartition(attrName, entityMatchEnabled);
            switch (partition) {
            case Partition.SYSTEM:
                verifySystemAttr(config, cat);
                break;
            case Partition.STD_ATTRS:
                boolean exportByDefault = getAccountExportAttrs(entityMatchEnabled).contains(attrName);
                verifyFlags(config, cat, partition, //
                        Active, false, //
                        true, true, //
                        exportByDefault, true, //
                        true, true, //
                        false, true, //
                        false, true);
                break;
            case Partition.EXTERNAL_ID:
                boolean flag = entityMatchEnabled && !onlyEntityMatchGAEnabled;
                verifyFlags(config, cat, partition, //
                        Active, false, //
                        !flag, true, //
                        flag, true, //
                        !flag, true, //
                        false, true, //
                        false, false);
                break;
            case Partition.ACCOUNT_ID:
                boolean canChangeExport = !onlyEntityMatchGAEnabled;
                AttrState state = onlyEntityMatchGAEnabled ? Inactive : Active;
                verifyFlags(config, cat, partition, //
                        state, false, //
                        false, false, //
                        true, canChangeExport, //
                        false, canChangeExport, //
                        false, canChangeExport, //
                        false, false);
                break;
            case Partition.SYSTEM_ID:
                verifySystemID(config, entityMatchEnabled);
                break;
            case Partition.OTHERS:
                verifyFlags(config, cat, partition, //
                        Active, true, //
                        true, true, //
                        false, true, //
                        true, true, //
                        false, true, //
                        false, true);
                break;
            default:
            }
            return true;
        });
    }

    private void verifySystemID(AttrConfig attrConfig, boolean entityMatchEnabled) {
        if (!entityMatchEnabled) {
            return ;
        }
        Assert.assertEquals(attrConfig.getAttrProps().get(ColumnMetadataKey.Subcategory).getSystemValue().toString(),
                Category.SUB_CAT_ACCOUNT_IDS, attrConfig.getAttrName());
        Assert.assertEquals(attrConfig.getAttrProps().get(ColumnMetadataKey.DisplayName).getSystemValue().toString(),
                "DefaultSystem Account ID", attrConfig.getAttrName());
    }

    private void verifyContactSystemID(AttrConfig attrConfig, boolean entityMatchEnabled) {
        if (!entityMatchEnabled) {
            return ;
        }
        Assert.assertEquals(attrConfig.getAttrProps().get(ColumnMetadataKey.Subcategory).getSystemValue().toString(),
                Category.SUB_CAT_ACCOUNT_IDS, attrConfig.getAttrName());
        Assert.assertEquals(attrConfig.getAttrProps().get(ColumnMetadataKey.DisplayName).getSystemValue().toString(),
                "DefaultSystem_2 Account ID", attrConfig.getAttrName());
    }

    private String getMyAttributesPartition(String attrName, boolean entityMatchEnabled) {
        String partiion;
        if (entityMatchEnabled && InterfaceName.AccountId.name().equals(attrName)) {
            partiion = Partition.ACCOUNT_ID;
        } else if (getAccountSystemAttrs(entityMatchEnabled).contains(attrName)) {
            partiion = Partition.SYSTEM;
        } else if (getAccountStandardAttrs(entityMatchEnabled).contains(attrName)) {
            partiion = Partition.STD_ATTRS;
        } else if (CRM_ID.equals(attrName)) {
            partiion = Partition.EXTERNAL_ID;
        } else if (ACCOUNT_SYSTEM_ID.equals(attrName)) {
            partiion = Partition.SYSTEM_ID;
        } else {
            partiion = Partition.OTHERS;
        }
        return partiion;
    }

    private void testContactAttributes(boolean entityMatchEnabled, boolean onlyEntityMatchGAEnabled) {
        final Category cat = Category.CONTACT_ATTRIBUTES;
        checkAndVerifyCategory(cat, (config) -> {
            String attrName = config.getAttrName();
            Assert.assertNotNull(attrName, JsonUtils.pprint(config));
            String partition = getContactAttributesPartition(attrName, entityMatchEnabled);
            switch (partition) {
            case Partition.SYSTEM:
                verifySystemAttr(config, cat);
                break;
            case Partition.STD_ATTRS:
                boolean exportByDefault = getContactExportAttrs(entityMatchEnabled).contains(attrName);
                verifyFlags(config, cat, partition, //
                        Active, false, //
                        true, true, //
                        exportByDefault, true, //
                        true, true, //
                        false, true, //
                        false, false);
                break;
            case Partition.CONTACT_ID:
                boolean canChangeExport = !onlyEntityMatchGAEnabled;
                AttrState state = onlyEntityMatchGAEnabled ? Inactive : Active;
                verifyFlags(config, cat, partition, //
                        state, false, //
                        false, false, //
                        true, canChangeExport, //
                        false, canChangeExport, //
                        false, canChangeExport, //
                        false, false);
                break;
            case Partition.SYSTEM_ID:
                verifyContactSystemID(config, entityMatchEnabled);
                break;
            case Partition.OTHERS:
                verifyFlags(config, cat, partition, //
                        Active, true, //
                        true, true, //
                        false, true, //
                        true, true, //
                        false, true, //
                        false, false);
                break;
            default:
            }
            return true;
        });
    }

    private String getContactAttributesPartition(String attrName, boolean entityMatchEnabled) {
        String partiion;
        if (entityMatchEnabled && InterfaceName.ContactId.name().equals(attrName)) {
            partiion = Partition.CONTACT_ID;
        } else if (getContactSystemAttrs(entityMatchEnabled).contains(attrName)) {
            partiion = Partition.SYSTEM;
        } else if (getContactStandardAttrs(entityMatchEnabled).contains(attrName)) {
            partiion = Partition.STD_ATTRS;
        } else if (OTHERSYSTEM_ACCOUNT_SYSTEM_ID.equals(attrName)) {
            partiion = Partition.SYSTEM_ID;
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
                break;
            case Partition.APS:
                verifyFlags(config, cat, partition, //
                        Active, false, //
                        false, false, //
                        false, false, //
                        false, false, //
                        false, false, //
                        true, true);
                break;
            default:
            }
            return true;
        });
    }

    private String getProductSpentPartition(String attrName) {
        String partition;
        if (psSystemAttrs.contains(attrName) || apsSystemAttrs.contains(attrName)) {
            partition = Partition.SYSTEM;
        } else if (ActivityMetricsUtils.isHasPurchasedAttr(attrName)) {
            partition = Partition.HAS_PURCHASED;
        } else if (ApsGeneratorUtils.isApsAttr(attrName)) {
            partition = Partition.APS;
        } else {
            partition = Partition.OTHERS;
        }
        return partition;
    }

    private void testCuratedAccountAttributes() {
        final Category cat = Category.CURATED_ACCOUNT_ATTRIBUTES;
        checkAndVerifyCategory(cat, (config) -> {
            String attrName = config.getAttrName();
            Assert.assertNotNull(attrName, JsonUtils.pprint(config));
            if (caSystemAttrs.contains(attrName)) {
                verifySystemAttr(config, cat);
            } else {
                verifyFlags(config, cat, null, //
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

    private void testLDCAttrs() {
        testLDCFirmographics();
        testLDCCovid19();
        testLDCOnlinePresence();
        testLDCWebsiteProfile();
        testLDCIntent();
        testLDCTechProfile();
        // testDNBTechProfile();
        testLDCWebsiteKeywords();
    }

    private boolean skipVerifyDeprecatedLDCAttrs(AttrConfig config) {
        AttrState state = config.getPropertyFinalValue(ColumnMetadataKey.State, AttrState.class);
        if (AttrState.Deprecated.equals(state)) {
            // temp fix for data cloud upgrade changes, all new deprecated LDC attributes can't pass the test, so skip it.
            // maybe need to set DataLicense and let the attribute Inactive? see line 424 in AccountMasterColumn
            return true;
        } else {
            return false;
        }
    }

    private void testLDCFirmographics() {
        checkAndVerifyCategory(Category.FIRMOGRAPHICS, (config) -> {
            if (skipVerifyDeprecatedLDCAttrs(config)) {
                return true;
            }
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
    }

    private void testLDCCovid19() {
        checkAndVerifyEmptyCategory(Category.COVID_19, (config) -> {
            if (skipVerifyDeprecatedLDCAttrs(config)) {
                return true;
            }
            AttrState initialState = AttrState.Inactive;
            boolean[] flags = new boolean[] { true, // life cycle change
                    true, true, // segment
                    false, true, // export
                    true, true, // tp
                    false, true, // cp
                    false, false // model
            };
            initialState = overwrite11Flags(flags, initialState, config.getAttrName());
            String partition = getLDCPartition(config.getAttrName());
            verifyFlags(config, Category.COVID_19, partition, initialState, flags);
            return true;
        });
    }

    private void testLDCOnlinePresence() {
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
    }

    private void testLDCWebsiteProfile() {
        checkAndVerifyCategory(Category.WEBSITE_PROFILE, (config) -> {
            if (skipVerifyDeprecatedLDCAttrs(config)) {
                return true;
            }
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
    }

    private void testLDCIntent() {
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
    }

    private void testDNBTechProfile() {
        checkAndVerifyCategory(Category.DNB_TECHNOLOGY_PROFILE, (config) -> {
            AttrState initialState = AttrState.Inactive;
            boolean[] flags = new boolean[] { true, // life cycle change
                    true, true, // segment
                    true, true, // export
                    true, true, // tp
                    false, true, // cp
                    false, false // model
            };
            initialState = overwrite11Flags(flags, initialState, config.getAttrName());
            String partition = getLDCPartition(config.getAttrName());
            verifyFlags(config, Category.DNB_TECHNOLOGY_PROFILE, partition, initialState, flags);
            return true;
        });
    }

    private void testLDCTechProfile() {
        checkAndVerifyEmptyCategory(Category.TECHNOLOGY_PROFILE, (config) -> {
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
    }

    private void testLDCWebsiteKeywords() {
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
            // deprecated attrs are not enabled for Export
            flags[3] = false;
            // deprecated attrs are not enabled for Model
            flags[9] = false;
        }
        if (AttrState.Inactive.equals(state)) {
            // cannot change usage for inactive attributes
            flags[2] = false;
            flags[4] = false;
            flags[6] = false;
            flags[8] = false;
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
        Assert.assertTrue(CollectionUtils.isNotEmpty(attrConfigs));
        Long count = Flux.fromIterable(attrConfigs).parallel().runOn(scheduler) //
                .map(verifier).sequential().count().block();
        log.info("Verified " + count + " attr configs in the category " + category);
    }

    private void checkAndVerifyEmptyCategory(Category category, Function<AttrConfig, Boolean> verifier) {
        List<AttrConfig> attrConfigs = attrConfigService.getRenderedList(category);
        Assert.assertFalse(CollectionUtils.isNotEmpty(attrConfigs));
    }

    private void verifySystemAttr(AttrConfig attrConfig, Category category) {
        log.info("Verifying system attr " + attrConfig.getAttrName() + " in " + category);
        verifyFlags(attrConfig, category, Partition.SYSTEM, //
                Active, false, //
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
        Assert.assertEquals(enabled, expectedValue, String.format("%s enabled for %s usage", logPrefix, property));
        boolean chg = attrConfig.getProperty(property).isAllowCustomization();
        Assert.assertEquals(chg, expectedChg, String.format("%s allow change %s usage", logPrefix, property));
    }

    private Set<String> getAccountStandardAttrs(boolean entityMatchEnabled) {
        if (entityMatchEnabled) {
            return accountStandardAttrsEntityMatchEnabled;
        } else {
            return accountStandardAttrs;
        }
    }

    private Set<String> getAccountSystemAttrs(boolean entityMatchEnabled) {
        if (entityMatchEnabled) {
            return accountSystemAttrsEntityMatchEnabled;
        } else {
            return accountSystemAttrs;
        }
    }

    private Set<String> getAccountExportAttrs(boolean entityMatchEnabled) {
        if (entityMatchEnabled) {
            return accountExportAttrsEntityMatchEnabled;
        } else {
            return accountExportAttrs;
        }
    }

    private Set<String> getContactStandardAttrs(boolean entityMatchEnabled) {
        if (entityMatchEnabled) {
            return contactStandardAttrsEntityMatchEnabled;
        } else {
            return contactStandardAttrs;
        }
    }

    private Set<String> getContactSystemAttrs(boolean entityMatchEnabled) {
        if (entityMatchEnabled) {
            return contactSystemAttrsEntityMatchEnabled;
        } else {
            return contactSystemAttrs;
        }
    }

    private Set<String> getContactExportAttrs(boolean entityMatchEnabled) {
        if (entityMatchEnabled) {
            return contactExportAttrsEntityMatchEnabled;
        } else {
            return contactExportAttrs;
        }
    }

    private static final class Partition {
        static final String SYSTEM = "System";
        static final String STD_ATTRS = "StdAttrs";
        static final String EXTERNAL_ID = "ExternalID";
        static final String SYSTEM_ID = "SystemID";
        static final String HAS_PURCHASED = "HasPurchased";
        static final String APS = "APS";
        static final String ACCOUNT_ID = "AccountID";
        static final String CONTACT_ID = "ContactID";
        static final String OTHERS = "Others";

        // skip verification on these attributes
        // may because cannot tell the partition
        // or just want to sample fewer attrs
        static final String SKIP = "Skip";
    }

}
