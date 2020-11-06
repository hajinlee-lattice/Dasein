package com.latticeengines.apps.cdl.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.ListSegment;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.metadata.template.CSVAdaptor;
import com.latticeengines.domain.exposed.metadata.template.ImportFieldMapping;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;

public class SegmentResourceDeploymentTestNG extends CDLDeploymentTestNGBase {

    @Inject
    private SegmentProxy segmentProxy;

    private String listSegmentName;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironment();
        MultiTenantContext.setTenant(mainTestTenant);
    }

    @Test(groups = "deployment")
    public void testCrudListSegment() {
        String tenantId = mainTestTenant.getId();
        MetadataSegment metadataSegment = new MetadataSegment();
        String segmentDisplayName = "list-segment-display-name";
        String segmentDescription = "list-segment-description";
        String externalSystem = "dataVision";
        String externalSegmentId = "dataVisionSegment";
        listSegmentName = NamingUtils.uuid("listSegment");
        ListSegment listSegment = createListSegment(externalSystem, externalSegmentId);
        metadataSegment.setName(listSegmentName);
        metadataSegment.setDisplayName(segmentDisplayName);
        metadataSegment.setDescription(segmentDescription);
        metadataSegment.setType(MetadataSegment.SegmentType.List);
        metadataSegment.setTenant(MultiTenantContext.getTenant());
        metadataSegment.setListSegment(listSegment);

        segmentProxy.createOrUpdateListSegment(tenantId, metadataSegment);
        metadataSegment = segmentProxy.getMetadataSegmentByName(tenantId, listSegmentName);
        verifyMetadataSegment(metadataSegment, segmentDisplayName, segmentDescription, externalSystem, externalSegmentId);
        segmentDisplayName = "list-segment-display-name2";
        segmentDescription = "list-segment-description2";
        metadataSegment.setDisplayName(segmentDisplayName);
        metadataSegment.setDescription(segmentDescription);

        segmentProxy.createOrUpdateSegment(tenantId, metadataSegment);
        metadataSegment = segmentProxy.getMetadataSegmentByName(tenantId, listSegmentName);
        verifyMetadataSegment(metadataSegment, segmentDisplayName, segmentDescription, externalSystem, externalSegmentId);
        metadataSegment = segmentProxy.getListSegmentByExternalInfo(tenantId, externalSystem, externalSegmentId);
        verifyMetadataSegment(metadataSegment, segmentDisplayName, segmentDescription, externalSystem, externalSegmentId);

        listSegment.setCsvAdaptor(generateCSVAdaptor());
        segmentProxy.updateListSegment(tenantId, listSegment);
        metadataSegment = segmentProxy.getMetadataSegmentByName(tenantId, listSegmentName);
        assertNotNull(metadataSegment);
        verifyListSegment(metadataSegment.getListSegment());

        segmentProxy.deleteSegmentByExternalInfo(tenantId, metadataSegment, false);
        assertEquals(segmentProxy.getMetadataSegments(tenantId).size(), 0);
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

    private void verifyMetadataSegment(MetadataSegment segment, String displayName, String description, String externalSystemName, String externalSegmentId) {
        assertNotNull(segment);
        assertEquals(segment.getDescription(), description);
        assertEquals(segment.getDisplayName(), displayName);
        assertEquals(segment.getName(), listSegmentName);
        ListSegment listSegment = segment.getListSegment();
        assertNotNull(listSegment);
        assertEquals(listSegment.getExternalSystem(), externalSystemName);
        assertEquals(listSegment.getExternalSegmentId(), externalSegmentId);
        assertNotNull(listSegment.getS3DropFolder());
        assertNotNull(listSegment.getCsvAdaptor());
    }

    private ListSegment createListSegment(String externalSystem, String externalSegmentId) {
        ListSegment listSegment = new ListSegment();
        listSegment.setExternalSystem(externalSystem);
        listSegment.setExternalSegmentId(externalSegmentId);
        return listSegment;
    }

}
