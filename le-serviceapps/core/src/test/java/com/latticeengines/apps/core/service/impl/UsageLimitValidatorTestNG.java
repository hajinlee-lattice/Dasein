package com.latticeengines.apps.core.service.impl;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.apache.hadoop.util.StringUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.core.testframework.ServiceAppsFunctionalTestNGBase;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.pls.AttrConfigUsageOverview;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigProp;
import com.latticeengines.domain.exposed.serviceapps.core.ValidationDetails.AttrValidation;

public class UsageLimitValidatorTestNG extends ServiceAppsFunctionalTestNGBase {

    @Inject
    private UsageLimitValidator usageLimitValidator;

    private static final long exportLimit = AttrConfigUsageOverview.defaultExportLimit;
    private static final long companyProfileLimit = AttrConfigUsageOverview.defaultCompanyProfileLimit;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironment();
    }

    @Test(groups = "functional")
    public void testGetLimit() {
        Assert.assertEquals(usageLimitValidator.getLimit("Enrichment"), (int) exportLimit);
        Assert.assertEquals(usageLimitValidator.getLimit("CompanyProfile"), (int) companyProfileLimit);
    }

    @Test(groups = "functional")
    public void testUsageLimit() throws Exception {
        List<AttrConfig> attrConfigs = new ArrayList<>();
        AttrConfigProp<Boolean> enrichProp = new AttrConfigProp<>();
        enrichProp.setAllowCustomization(Boolean.TRUE);
        enrichProp.setCustomValue(Boolean.TRUE);
        for (int i = 0; i < exportLimit; i++) {
            AttrConfig config = new AttrConfig();
            config.setAttrName(StringUtils.format("Attr%d", i));
            config.setEntity(BusinessEntity.Account);
            config.putProperty(ColumnSelection.Predefined.Enrichment.name(), enrichProp);
            attrConfigs.add(config);
        }
        usageLimitValidator.validate(new ArrayList<>(), attrConfigs, new AttrValidation());
        assertEquals(AttrConfigTestUtils.getErrorNumber(attrConfigs), 0);

        AttrConfig activeConfig = new AttrConfig();
        activeConfig.setAttrName("Attr_enrich1");
        activeConfig.setEntity(BusinessEntity.Account);
        activeConfig.putProperty(ColumnSelection.Predefined.Enrichment.name(), enrichProp);
        attrConfigs.add(activeConfig);
        usageLimitValidator.validate(new ArrayList<>(), attrConfigs, new AttrValidation());
        assertEquals(AttrConfigTestUtils.getErrorNumber(attrConfigs), attrConfigs.size());

        attrConfigs.forEach(e -> e.setValidationErrors(null));
        AttrConfig inactiveConfig = new AttrConfig();
        inactiveConfig.setAttrName("Attr_enrich_disable1");
        inactiveConfig.setEntity(BusinessEntity.Account);

        AttrConfigProp<Boolean> inactiveProp = new AttrConfigProp<>();
        inactiveProp.setAllowCustomization(Boolean.TRUE);
        inactiveProp.setCustomValue(Boolean.FALSE);
        inactiveConfig.putProperty(ColumnSelection.Predefined.Enrichment.name(), inactiveProp);
        attrConfigs.add(inactiveConfig);
        usageLimitValidator.validate(new ArrayList<>(), attrConfigs, new AttrValidation());
        // the new config should not have error message
        assertEquals(AttrConfigTestUtils.getErrorNumber(attrConfigs), attrConfigs.size() - 1);

        attrConfigs.forEach(e -> e.setValidationErrors(null));
        usageLimitValidator.validate(
                AttrConfigTestUtils.generatePropertyList(Category.FIRMOGRAPHICS, true, false, false, false, false),
                attrConfigs, new AttrValidation());
        // expect have error message in user provide list
        assertEquals(AttrConfigTestUtils.getErrorNumber(attrConfigs), attrConfigs.size() - 1);

        attrConfigs.forEach(e -> e.setValidationErrors(null));
        // the static list returned by AttrConfigTestUtils.generatePropertyList
        // have 5 config which are activated and enabled for enrichment
        List<AttrConfig> subList = attrConfigs.subList(0, (int) (exportLimit - 5));
        usageLimitValidator.validate(
                AttrConfigTestUtils.generatePropertyList(Category.FIRMOGRAPHICS, true, false, false, false, false),
                subList, new AttrValidation());
        assertEquals(AttrConfigTestUtils.getErrorNumber(subList), 0);

        subList.add(activeConfig);
        usageLimitValidator.validate(
                AttrConfigTestUtils.generatePropertyList(Category.FIRMOGRAPHICS, true, false, false, false, false),
                subList, new AttrValidation());
        assertEquals(AttrConfigTestUtils.getErrorNumber(subList), subList.size());
    }

}
