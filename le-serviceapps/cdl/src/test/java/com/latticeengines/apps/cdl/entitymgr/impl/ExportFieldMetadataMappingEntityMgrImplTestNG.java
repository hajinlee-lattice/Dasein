package com.latticeengines.apps.cdl.entitymgr.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.dao.ExportFieldMetadataMappingDao;
import com.latticeengines.apps.cdl.entitymgr.ExportFieldMetadataMappingEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.LookupIdMappingEntityMgr;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.ExportFieldMetadataMapping;
import com.latticeengines.domain.exposed.pls.LookupIdMap;

public class ExportFieldMetadataMappingEntityMgrImplTestNG extends CDLFunctionalTestNGBase {

    private Logger log = LoggerFactory.getLogger(getClass());

    @Inject
    private LookupIdMappingEntityMgr lookupIdMappingEntityMgr;

    @Inject
    ExportFieldMetadataMappingEntityMgr exportFieldMetadataMappingEntityMgr;

    @Inject
    ExportFieldMetadataMappingDao exportFieldMetadataMappingDao;

    private LookupIdMap lookupIdMap;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        setupTestEnvironment();
    }

    @Test(groups = "functional")
    public void testCreate() {
        lookupIdMap = new LookupIdMap();
        lookupIdMap.setExternalSystemType(CDLExternalSystemType.MAP);
        lookupIdMap.setExternalSystemName(CDLExternalSystemName.Marketo);
        lookupIdMap.setOrgId(lookupIdMap.getExternalSystemName() + "_" + System.currentTimeMillis());
        lookupIdMap.setOrgName(lookupIdMap.getExternalSystemName() + "_Test");
        lookupIdMap = lookupIdMappingEntityMgr.createExternalSystem(lookupIdMap);
        assertNotNull(lookupIdMap);
        assertNotNull(lookupIdMap.getId());

        List<ExportFieldMetadataMapping> exportFieldMappings = new ArrayList<ExportFieldMetadataMapping>();

        ExportFieldMetadataMapping fieldMapping_1 = new ExportFieldMetadataMapping();
        fieldMapping_1.setSourceField(InterfaceName.AccountId.toString());
        fieldMapping_1.setDestinationField("CompanyId");
        fieldMapping_1.setOverwriteValue(false);
        fieldMapping_1.setLookupIdMap(lookupIdMap);
        fieldMapping_1.setTenant(mainTestTenant);
        exportFieldMappings.add(fieldMapping_1);

        ExportFieldMetadataMapping fieldMapping_2 = new ExportFieldMetadataMapping();
        fieldMapping_2.setSourceField(InterfaceName.CompanyName.toString());
        fieldMapping_2.setDestinationField("CompanyName");
        fieldMapping_2.setOverwriteValue(false);
        fieldMapping_2.setLookupIdMap(lookupIdMap);
        fieldMapping_2.setTenant(mainTestTenant);
        exportFieldMappings.add(fieldMapping_2);

        ExportFieldMetadataMapping fieldMapping_3 = new ExportFieldMetadataMapping();
        fieldMapping_3.setSourceField(InterfaceName.Email.toString());
        fieldMapping_3.setDestinationField("Email");
        fieldMapping_3.setOverwriteValue(false);
        fieldMapping_3.setLookupIdMap(lookupIdMap);
        fieldMapping_3.setTenant(mainTestTenant);
        exportFieldMappings.add(fieldMapping_3);

        exportFieldMappings = exportFieldMetadataMappingEntityMgr.createAll(exportFieldMappings);

        assertNotNull(exportFieldMappings);

    }

    @Test(groups = "functional", dependsOnMethods = "testCreate")
    public void testFind() {
        List<ExportFieldMetadataMapping> fieldMappings = exportFieldMetadataMappingEntityMgr
                .findByOrgId(lookupIdMap.getOrgId());
        assertNotNull(fieldMappings);

        assertEquals(fieldMappings.size(), 3);

        fieldMappings.forEach(fm -> {
            assertNotNull(fm.getSourceField());
            assertNotNull(fm.getDestinationField());
        });
    }

    @Test(groups = "functional", dependsOnMethods = "testCreate")
    public void testUpdate() {

        List<ExportFieldMetadataMapping> exportFieldMappings = new ArrayList<ExportFieldMetadataMapping>();

        ExportFieldMetadataMapping fieldMapping_1 = new ExportFieldMetadataMapping();
        fieldMapping_1.setLookupIdMap(lookupIdMap);
        fieldMapping_1.setSourceField(InterfaceName.PhoneNumber.toString());
        fieldMapping_1.setDestinationField("PhoneNumber");
        fieldMapping_1.setOverwriteValue(false);
        exportFieldMappings.add(fieldMapping_1);

        ExportFieldMetadataMapping fieldMapping_2 = new ExportFieldMetadataMapping();
        fieldMapping_2.setLookupIdMap(lookupIdMap);
        fieldMapping_2.setSourceField(InterfaceName.ContactName.toString());
        fieldMapping_2.setDestinationField("Name");
        fieldMapping_2.setOverwriteValue(false);
        exportFieldMappings.add(fieldMapping_2);

        exportFieldMetadataMappingEntityMgr.update(lookupIdMap, exportFieldMappings);

        List<ExportFieldMetadataMapping> updatedExportFieldMappings = exportFieldMetadataMappingEntityMgr
                .findByOrgId(lookupIdMap.getOrgId());

        assertNotNull(updatedExportFieldMappings);
        assertNotEquals(updatedExportFieldMappings, exportFieldMappings);

        assertEquals(updatedExportFieldMappings.size(), 2);

        updatedExportFieldMappings.forEach(fm -> {
            assertNotNull(fm.getSourceField());
            assertNotNull(fm.getDestinationField());
        });
    }

}
