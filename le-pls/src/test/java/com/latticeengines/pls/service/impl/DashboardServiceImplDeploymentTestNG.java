package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.support.RetryTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.ListSegmentSummary;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.metadata.template.CSVAdaptor;
import com.latticeengines.domain.exposed.metadata.template.ImportFieldMapping;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.pls.service.vidashboard.DashboardService;

public class DashboardServiceImplDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(DashboardServiceImplDeploymentTestNG.class);

    private static final String DISPLAY_NAME = "segmentDisplayName1";

    @Inject
    private DashboardService dashboardService;

    private RetryTemplate retry;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenant();
    }

    @Test(groups = "deployment")
    public void testCrud() {
        MetadataSegment segment = dashboardService.createListSegment(mainTestTenant.getId(), DISPLAY_NAME);
        String segmentName = segment.getName();
        dashboardService.updateSegmentFieldMapping(mainTestTenant.getId(), segmentName, generateCSVAdaptor());
        AtomicReference<ListSegmentSummary> createdAtom = new AtomicReference<>();
        retry.execute(context -> {
            createdAtom.set(dashboardService.getListSegmentMappings(mainTestTenant.getId(), segmentName));
            Assert.assertEquals(generateCSVAdaptor(), createdAtom.get().getCsvAdaptor());
            return true;
        });
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
}
