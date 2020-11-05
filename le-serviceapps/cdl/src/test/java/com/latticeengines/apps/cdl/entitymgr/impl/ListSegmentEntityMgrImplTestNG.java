package com.latticeengines.apps.cdl.entitymgr.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.ListSegmentEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.SegmentEntityMgr;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.ListSegment;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.metadata.template.CSVAdaptor;
import com.latticeengines.domain.exposed.metadata.template.ImportFieldMapping;

public class ListSegmentEntityMgrImplTestNG extends CDLFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ListSegmentEntityMgrImplTestNG.class);

    private String listSegmentName;

    @Inject
    private SegmentEntityMgr segmentEntityMgr;

    @Inject
    private ListSegmentEntityMgr listSegmentEntityMgr;

    @BeforeClass(groups = "functional")
    public void setup() {
        listSegmentName = NamingUtils.uuid("LIST_SEGMENT_NAME");
        setupTestEnvironmentWithDataCollection();
        MultiTenantContext.setTenant(mainTestTenant);
    }

    @Test(groups = "functional")
    public void createOrUpdateListSegment() {
        MetadataSegment metadataSegment = new MetadataSegment();
        String segmentDisplayName = "list-segment-display-name";
        String segmentDescription = "list-segment-description";
        String externalSystem = "dataVision";
        String externalSegmentId = "dataVisionSegment";
        String s3DropFolder = "/latticeengines-qa-data-stage/datavision_segment/" + listSegmentName + "/input";
        ListSegment listSegment = createListSegment(externalSystem, externalSegmentId, s3DropFolder);
        metadataSegment.setName(listSegmentName);
        metadataSegment.setDisplayName(segmentDisplayName);
        metadataSegment.setDescription(segmentDescription);
        metadataSegment.setDataCollection(dataCollection);
        metadataSegment.setType(MetadataSegment.SegmentType.List);
        metadataSegment.setTenant(MultiTenantContext.getTenant());
        metadataSegment.setListSegment(listSegment);
        segmentEntityMgr.createListSegment(metadataSegment);

        listSegment = listSegmentEntityMgr.findByExternalInfo(listSegment.getExternalSystem(), listSegment.getExternalSegmentId());
        assertNotNull(listSegment);
        listSegment.setCsvAdaptor(generateCSVAdaptor());
        listSegmentEntityMgr.updateListSegment(listSegment);
        listSegment = listSegmentEntityMgr.findByExternalInfo(listSegment.getExternalSystem(), listSegment.getExternalSegmentId());
        verifyListSegment(listSegment);
    }

    private void verifyListSegment(ListSegment listSegment) {
        assertNotNull(listSegment);
        CSVAdaptor csvAdaptor = listSegment.getCsvAdaptor();
        assertNotNull(csvAdaptor);
        assertNotNull(csvAdaptor.getImportFieldMappings());
        assertEquals(csvAdaptor.getImportFieldMappings().size(), 1);
        ImportFieldMapping importFieldMapping = csvAdaptor.getImportFieldMappings().get(0);
        assertEquals(importFieldMapping.getFieldName(), InterfaceName.CompanyName.name());
        assertEquals(importFieldMapping.getFieldType(), UserDefinedType.TEXT);
        assertEquals(importFieldMapping.getUserFieldName(), "Company Name");
    }

    private CSVAdaptor generateCSVAdaptor() {
        CSVAdaptor csvAdaptor = new CSVAdaptor();
        List<ImportFieldMapping> importFieldMappings = new ArrayList<>();
        ImportFieldMapping importFieldMapping = new ImportFieldMapping();
        importFieldMapping.setFieldName(InterfaceName.CompanyName.name());
        importFieldMapping.setFieldType(UserDefinedType.TEXT);
        importFieldMapping.setUserFieldName("Company Name");
        importFieldMappings.add(importFieldMapping);
        csvAdaptor.setImportFieldMappings(importFieldMappings);
        return csvAdaptor;
    }

    private ListSegment createListSegment(String externalSystem, String externalSegmentId, String s3DropFolder) {
        ListSegment listSegment = new ListSegment();
        listSegment.setExternalSystem(externalSystem);
        listSegment.setExternalSegmentId(externalSegmentId);
        listSegment.setS3DropFolder(s3DropFolder);
        return listSegment;
    }
}
