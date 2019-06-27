package com.latticeengines.pls.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.junit.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.pls.frontend.FieldCategory;
import com.latticeengines.domain.exposed.pls.frontend.TemplateFieldPreview;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.service.CDLService;

public class CDLServiceImplTestNG extends PlsFunctionalTestNGBase {

    @Inject
    private CDLService cdlService;

    @BeforeClass(groups = { "functional" })
    public void setup() throws Exception {
        super.setup();
    }

    @Test(groups = { "functional" })
    public void testTemplatePreview() {
        Table standardTable = SchemaRepository.instance().getSchema(BusinessEntity.Contact);
        Table templateTable = TableUtils.clone(standardTable, "tempalte");
        Attribute attr = new Attribute("TestAttr");
        attr.setDisplayName("TestDisplay");
        attr.setPhysicalDataType("Long");
        attr.setDateFormatString("MM/DD/YY");
        attr.setTimeFormatString("00-00-00 12H");
        templateTable.addAttribute(attr);
        templateTable.removeAttribute("Website");
        templateTable.removeAttribute("Email");
        List<TemplateFieldPreview> templatePreview = cdlService.getTemplatePreview(mainTestTenant.getId(), templateTable,
                standardTable);
        Assert.assertNotNull(templatePreview);
        Assert.assertTrue(templatePreview.size() > 1);
        TemplateFieldPreview fieldPreview =
                templatePreview.stream().filter(preview -> preview.getNameInTemplate().equals("TestAttr")).findFirst().get();
        Assert.assertEquals(fieldPreview.getFieldCategory(), FieldCategory.CustomField);
        TemplateFieldPreview website =
                templatePreview.stream().filter(preview -> preview.getNameInTemplate().equals("Website")).findFirst().get();
        TemplateFieldPreview email =
                templatePreview.stream().filter(preview -> preview.getNameInTemplate().equals("Email")).findFirst().get();
        Assert.assertTrue(website.isUnmapped());
        Assert.assertTrue(email.isUnmapped());
        Assert.assertEquals(website.getFieldCategory(), FieldCategory.LatticeField);
        Assert.assertEquals(email.getFieldCategory(), FieldCategory.LatticeField);
    }
}
