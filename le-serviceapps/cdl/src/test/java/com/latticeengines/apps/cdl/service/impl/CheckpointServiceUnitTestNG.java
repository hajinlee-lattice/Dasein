package com.latticeengines.apps.cdl.service.impl;

import static org.mockito.Mockito.doReturn;

import java.util.Arrays;

import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.security.Tenant;

public class CheckpointServiceUnitTestNG {

    /**
     * No automated verification. Only for manual spot check whether printed
     * PublishEntityRequest info is reasonable or not
     */
    @Test(groups = { "unit" })
    public void testPrintPublishEntityRequest() {
        CheckpointService checkpointService = Mockito.spy(CheckpointService.class);
        Tenant mainTestTenant = new Tenant(
                CustomerSpace.parse(CheckpointServiceUnitTestNG.class.getSimpleName()).toString());
        checkpointService.setMainTestTenant(mainTestTenant);
        ReflectionTestUtils.setField(checkpointService, "matchapiHostPort", "https://localhost:9076");
        doReturn(true).when(checkpointService).isEntityMatchEnabled();

        checkpointService.printPublishEntityRequest("process1", "1");
        checkpointService.setPrecedingCheckpoints(Arrays.asList("process1"));
        checkpointService.printPublishEntityRequest("process2", "1");
    }
}
