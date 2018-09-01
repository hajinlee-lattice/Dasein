package com.latticeengines.apps.cdl.controller;

import static com.latticeengines.domain.exposed.serviceapps.core.AttrState.Active;
import static com.latticeengines.domain.exposed.serviceapps.core.AttrState.Inactive;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.apps.core.service.AttrConfigService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.testframework.exposed.service.CDLTestDataService;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * $ dpltc deploy -a admin,matchapi,microservice,pls -m metadata,cdl,lp
 */
public class AttrConfigResourceDeploymentTestNG extends CDLDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(AttrConfigResourceDeploymentTestNG.class);

    @Inject
    private CDLTestDataService cdlTestDataService;

    @Inject
    private AttrConfigService attrConfigService;

    @Inject
    private ColumnMetadataProxy columnMetadataProxy;

    private Set<String> accountStandardAttrs = SchemaRepository.getStandardAttributes(BusinessEntity.Account).stream() //
            .map(InterfaceName::name).collect(Collectors.toSet());
    private Set<String> accountSystemAttrs = SchemaRepository.getSystemAttributes(BusinessEntity.Account).stream() //
            .map(InterfaceName::name).collect(Collectors.toSet());
    private Set<String> accountExportAttrs = SchemaRepository.getDefaultExportAttributes(BusinessEntity.Account)
            .stream() //
            .map(InterfaceName::name).collect(Collectors.toSet());

    private Set<String> contactStandardAttrs = SchemaRepository.getStandardAttributes(BusinessEntity.Contact).stream() //
            .map(InterfaceName::name).collect(Collectors.toSet());
    private Set<String> conatactSystemAttrs = SchemaRepository.getSystemAttributes(BusinessEntity.Contact).stream() //
            .map(InterfaceName::name).collect(Collectors.toSet());
    private Set<String> contactExportAttrs = SchemaRepository.getDefaultExportAttributes(BusinessEntity.Contact) //
            .stream().map(InterfaceName::name).collect(Collectors.toSet());

    private List<ColumnMetadata> amCols;
    private Set<String> internalEnrichAttrs;
    private Scheduler scheduler = Schedulers.newParallel("verification");

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        List<Runnable> runnables = new ArrayList<>();
        runnables.add(() -> {
            setupTestEnvironment();
            cdlTestDataService.populateMetadata(mainTestTenant.getId(), 3);
        });
        runnables.add(() -> {
            amCols = columnMetadataProxy.getAllColumns();
            internalEnrichAttrs = new HashSet<>();
            amCols.forEach(cm -> {
                if (Boolean.TRUE.equals(cm.getCanInternalEnrich())) {
                    internalEnrichAttrs.add(cm.getAttrName());
                }
            });
        });
        ExecutorService tp = ThreadPoolUtils.getFixedSizeThreadPool("test-setup", 2);
        ThreadPoolUtils.runRunnablesInParallel(tp, runnables, 30, 1);
        tp.shutdown();
        MultiTenantContext.setTenant(mainTestTenant);

        // TODO: setup some customer account attributes
        // TODO: to be external system ids

        // TODO: setup rating engines and rating attrs

        // TODO: setup curated attrs
    }

    @Test(groups = "deployment")
    public void test() {
        testContactAttributes();
        testMyAttributes();
    }

    private void testMyAttributes() {
        Category cat = Category.ACCOUNT_ATTRIBUTES;
        List<AttrConfig> attrConfigs = attrConfigService.getRenderedList(cat);
        Assert.assertTrue(CollectionUtils.isNotEmpty(attrConfigs));
        Long count = Flux.fromIterable(attrConfigs).parallel().runOn(scheduler) //
                .map(config -> {
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
                }) //
                .sequential().count().block();
        log.info("Verified " + count + " attr configs.");
    }

    private String getMyAttributesPartition(String attrName) {
        String partiion;
        if (accountSystemAttrs.contains(attrName)) {
            partiion = Partition.SYSTEM;
        } else if (accountStandardAttrs.contains(attrName)) {
            partiion = Partition.STD_ATTRS;
        } else {
            partiion = Partition.OTHERS;
        }
        return partiion;
    }

    private void testContactAttributes() {
        Category cat = Category.CONTACT_ATTRIBUTES;
        List<AttrConfig> attrConfigs = attrConfigService.getRenderedList(cat);
        Assert.assertTrue(CollectionUtils.isNotEmpty(attrConfigs));

        Long count = Flux.fromIterable(attrConfigs).parallel().runOn(scheduler) //
                .map(config -> {
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
                                false, true);
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
                }) //
                .sequential().count().block();
        log.info("Verified " + count + " attr configs.");
    }

    private String getContactAttributesPartition(String attrName) {
        String partiion;
        if (conatactSystemAttrs.contains(attrName)) {
            partiion = Partition.SYSTEM;
        } else if (contactStandardAttrs.contains(attrName)) {
            partiion = Partition.STD_ATTRS;
        } else {
            partiion = Partition.OTHERS;
        }
        return partiion;
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

    private void verifyFlags(AttrConfig attrConfig, Category category, String partition,//
            AttrState initState, boolean lcChg, //
            boolean segment, boolean segChg, //
            boolean export, boolean exportChg, //
            boolean talkingPoint, boolean tpChg, //
            boolean companyProfile, boolean cpChg, //
            boolean model, boolean modelChg //
    ) {
        String attrName = attrConfig.getAttrName();
        String displayName = attrConfig.getPropertyFinalValue(ColumnMetadataKey.DisplayName, String.class);
        String logPrefix = String.format("%s (%s) [%s - %s]", attrName, displayName, category.getName(), partition);

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

        //TODO: Cannot make model part pass for Contact and Account attrs.
//        property = ColumnSelection.Predefined.Model.name();
//        verifyUsage(logPrefix, attrConfig, property, model, modelChg);
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
        static final String OTHERS = "Others";

        // skip verification on these attributes
        // may because cannot tell the partition
        // or just want to sample fewer attrs
        static final String SKIP = "Skip";
    }

}
