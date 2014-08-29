package com.latticeengines.dataplatform.infrastructure;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.PumpStreamHandler;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-dataplatform-context.xml" })
public class DataPlatformInfrastructureTestNGBase extends AbstractTestNGSpringContextTests {

    public String executeCommand(String command) throws ExecuteException, IOException {
        CommandLine cmdl = CommandLine.parse(command);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PumpStreamHandler stream = new PumpStreamHandler(outputStream);
        Executor executor = new DefaultExecutor();
        executor.setStreamHandler(stream);
        executor.execute(cmdl);

        return outputStream.toString();
    }

}
