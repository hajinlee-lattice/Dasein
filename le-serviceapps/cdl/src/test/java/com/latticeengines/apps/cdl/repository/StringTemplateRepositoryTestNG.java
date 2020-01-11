package com.latticeengines.apps.cdl.repository;

import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.repository.reader.StringTemplateReaderRepository;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.domain.exposed.StringTemplateConstants;
import com.latticeengines.domain.exposed.metadata.StringTemplate;

public class StringTemplateRepositoryTestNG extends CDLFunctionalTestNGBase {

    private static final List<String> NAMES = Arrays.asList(
            StringTemplateConstants.ACTIVITY_METRICS_GROUP_TOTAL_VISIT_DISPLAYNAME,
            StringTemplateConstants.ACTIVITY_METRICS_GROUP_TOTAL_VISIT_DESCRIPTION,
            StringTemplateConstants.ACTIVITY_METRICS_GROUP_SUBCATEGORY,
            StringTemplateConstants.ACTIVITY_METRICS_GROUP_SOURCEMEDIUM_DISPLAYNAME,
            StringTemplateConstants.ACTIVITY_METRICS_GROUP_SOURCEMEDIUM_DESCRIPTION
    );

    @Inject
    private StringTemplateReaderRepository stringTemplateReaderRepository;

    @Test(groups = "functional")
    private void testFind() { // ensure templates that should have been added to db manually
        NAMES.forEach(name -> {
            StringTemplate found = stringTemplateReaderRepository.findByName(name);
            Assert.assertNotNull(found);
            Assert.assertEquals(found.getName(), name);
        });
    }
}
