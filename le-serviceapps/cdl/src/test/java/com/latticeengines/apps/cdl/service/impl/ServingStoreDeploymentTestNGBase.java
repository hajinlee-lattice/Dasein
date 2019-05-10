package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;

import com.latticeengines.apps.cdl.service.CDLExternalSystemService;
import com.latticeengines.apps.cdl.service.ServingStoreService;
import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.apps.core.service.ZKConfigService;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystem;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.testframework.exposed.service.CDLTestDataService;

public abstract class ServingStoreDeploymentTestNGBase extends CDLDeploymentTestNGBase {

    static final String CRM_ID = "CrmAccount_External_ID";

    @Inject
    private CDLTestDataService cdlTestDataService;

    @Inject
    private ColumnMetadataProxy columnMetadataProxy;

    @Inject
    private BatonService batonService;

    @Inject
    private ZKConfigService zkConfigService;

    @Inject
    private CDLExternalSystemService externalSystemService;

    @Inject
    protected ServingStoreService servingStoreService;

    final Set<String> internalEnrichAttrs = new HashSet<>();
    final Set<String> cannotSegmentAttrs = new HashSet<>();
    final Set<String> cannotEnrichmentAttrs = new HashSet<>();
    final Set<String> cannotModelAttrs = new HashSet<>();
    final Set<String> deprecatedAttrs = new HashSet<>();

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

    private void createExternalSystem() {
        CDLExternalSystem cdlExternalSystem = new CDLExternalSystem();
        List<String> crmIds = new ArrayList<>();
        crmIds.add(CRM_ID);
        cdlExternalSystem.setCRMIdList(crmIds);
        cdlExternalSystem.setEntity(BusinessEntity.Account);
        externalSystemService.createOrUpdateExternalSystem(mainCustomerSpace, cdlExternalSystem,
                BusinessEntity.Account);
    }

    protected void testAccountMetadata() {
        List<ColumnMetadata> cms = servingStoreService //
                .getDecoratedMetadataFromCache(mainCustomerSpace, BusinessEntity.Account);
        cms.forEach(cm -> Assert.assertNotNull(cm.getJavaClass(), //
                String.format("[%s] does not have a java class: %s", cm.getAttrName(), JsonUtils.serialize(cm))));

        Map<String, ColumnMetadata> cmsToVerify = getAccountMetadataToVerify();
        verifyMetadata(cms, cmsToVerify);
    }

    protected void testContactMetadata() {
        List<ColumnMetadata> cms = servingStoreService //
                .getDecoratedMetadataFromCache(mainCustomerSpace, BusinessEntity.Contact);
        cms.forEach(cm -> Assert.assertNotNull(cm.getJavaClass(), //
                String.format("[%s] does not have a java class: %s", cm.getAttrName(), JsonUtils.serialize(cm))));

        Map<String, ColumnMetadata> cmsToVerify = getContactMetadataToVerify();
        verifyMetadata(cms, cmsToVerify);
    }

    private void verifyMetadata(List<ColumnMetadata> cms, Map<String, ColumnMetadata> cmsToVerify) {
        cms.forEach(cm -> {
            if (cmsToVerify.containsKey(cm.getAttrName())) {
                verifyColumnMetadata(cm, cmsToVerify.get(cm.getAttrName()));
                cmsToVerify.remove(cm.getAttrName());
            }
        });
        Assert.assertTrue(cmsToVerify.isEmpty());
    }

    // Currently only verify Category, Subcategory, Groups
    // FIXME Why is getEnabledGroups is deprecated?
    @SuppressWarnings("deprecation")
    private void verifyColumnMetadata(ColumnMetadata cm, ColumnMetadata cmExpected) {
        Assert.assertEquals(cm.getCategory(), cmExpected.getCategory());
        Assert.assertEquals(cm.getSubcategory(), cmExpected.getSubcategory());
        List<ColumnSelection.Predefined> enabledGroups = cm.getEnabledGroups();
        List<ColumnSelection.Predefined> enabledGroupsExpected = cmExpected.getEnabledGroups();
        if (enabledGroupsExpected == null) {
            Assert.assertNull(enabledGroups);
        } else {
            Assert.assertNotNull(enabledGroups);
            Collections.sort(enabledGroups);
            Collections.sort(enabledGroupsExpected);
            Assert.assertEquals(enabledGroups, enabledGroupsExpected);
        }
    }

    @SuppressWarnings("unchecked")
    protected Map<String, ColumnMetadata> getAccountMetadataToVerify() {
        return Collections.EMPTY_MAP;
    }

    @SuppressWarnings("unchecked")
    protected Map<String, ColumnMetadata> getContactMetadataToVerify() {
        return Collections.EMPTY_MAP;
    }

    class ColumnMetadataBuilder {
        private ColumnMetadata cm;

        ColumnMetadataBuilder() {
            cm = new ColumnMetadata();
        }

        ColumnMetadataBuilder withAttrName(String attrName) {
            cm.setAttrName(attrName);
            return this;
        }

        ColumnMetadataBuilder withCategory(Category category) {
            cm.setCategory(category);
            return this;
        }

        ColumnMetadataBuilder withSubcategory(String subcategory) {
            cm.setSubcategory(subcategory);
            return this;
        }

        ColumnMetadataBuilder withGroups(ColumnSelection.Predefined... groups) {
            for (ColumnSelection.Predefined group : groups) {
                cm.enableGroup(group);
            }
            return this;
        }

        ColumnMetadata build() {
            return cm;
        }
    }

}
