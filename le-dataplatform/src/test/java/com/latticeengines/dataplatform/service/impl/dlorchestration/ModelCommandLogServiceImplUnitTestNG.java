package com.latticeengines.dataplatform.service.impl.dlorchestration;

import static org.mockito.Mockito.mock;
import static org.mockito.MockitoAnnotations.initMocks;

import java.util.ArrayList;

import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.entitymanager.ModelCommandLogEntityMgr;
import com.latticeengines.dataplatform.entitymanager.impl.ModelCommandLogEntityMgrImpl;
import com.latticeengines.dataplatform.service.impl.ModelingServiceTestUtils;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandParameter;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStatus;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStep;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

public class ModelCommandLogServiceImplUnitTestNG {

    ModelCommandLogServiceImpl modelCommandLogServiceImpl = new ModelCommandLogServiceImpl();

    @BeforeClass(groups = "unit")
    public void beforeClass() throws Exception {
        initMocks(this);

        ModelCommandLogEntityMgr modelCommandLogEntityMgr = mock(ModelCommandLogEntityMgrImpl.class);
        ReflectionTestUtils.setField(modelCommandLogServiceImpl, "modelCommandLogEntityMgr", modelCommandLogEntityMgr);
    }

    @Test(groups = "unit")
    public void testLogBeginStep() { // This test just confirms execution with
                                     // no exceptions raised
        ModelCommand command = new ModelCommand(1L, "Nutanix", "Nutanix",
                ModelCommandStatus.NEW, new ArrayList<ModelCommandParameter>(), ModelCommand.TAHOE, ModelingServiceTestUtils.EVENT_TABLE);
        modelCommandLogServiceImpl.logBeginStep(command, ModelCommandStep.LOAD_DATA);
        modelCommandLogServiceImpl.logCompleteStep(command, ModelCommandStep.LOAD_DATA, ModelCommandStatus.SUCCESS);
        modelCommandLogServiceImpl.logLedpException(command, new LedpException(LedpCode.LEDP_16000,
                new IllegalArgumentException("Some test exception message"), new String[] { "sometext" }));

        modelCommandLogServiceImpl.logException(command, new IllegalArgumentException("Some test exception message"));
        modelCommandLogServiceImpl.logException(command, "Some message", new IllegalArgumentException(
                "Some test exception message"));
    }
}
