package com.latticeengines.apps.cdl.service.impl;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.standardschemas.SchemaRepository;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class CSVDataFeedMetadataServiceImplTestNG extends CDLFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(CSVDataFeedMetadataServiceImplTestNG.class);

    @Autowired
    private CSVDataFeedMetadataServiceImpl csvDataFeedMetadataService;

    @Test(groups = "functional")
    public void testCompareMetadata() {
        Table sourceTable = SchemaRepository.instance().getSchema(BusinessEntity.Account, true, false);
        Table targetTable = SchemaRepository.instance().getSchema(BusinessEntity.Account, true, false);
        Assert.assertTrue(csvDataFeedMetadataService.compareMetadata(sourceTable, targetTable, false));
        sourceTable.removeAttribute(InterfaceName.Website.name());
        Assert.assertFalse(csvDataFeedMetadataService.compareMetadata(sourceTable, targetTable, false));
        targetTable.removeAttribute(InterfaceName.Website.name());
        Attribute sourceAttr =sourceTable.getAttribute(InterfaceName.PhoneNumber.name());
        sourceTable.removeAttribute(InterfaceName.PhoneNumber.name());
        sourceAttr.setPhysicalDataType("double");
        sourceTable.addAttribute(sourceAttr);
        Assert.assertTrue(csvDataFeedMetadataService.compareMetadata(sourceTable, targetTable, false));
        Exception ex = null;
        try {
            csvDataFeedMetadataService.compareMetadata(sourceTable, targetTable, true);
        } catch (RuntimeException e) {
            ex = e;
        }
        Assert.assertNotNull(ex);

    }

}
