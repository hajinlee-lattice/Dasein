package com.latticeengines.pls.service.impl.fileprocessor.state;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.io.File;
import java.net.URL;
import java.util.Collection;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.domain.exposed.pls.FilePayload;
import com.latticeengines.pls.entitymanager.impl.microservice.RestApiProxy;
import com.latticeengines.pls.service.impl.fileprocessor.FileProcessingState;
import com.latticeengines.workflow.exposed.build.WorkflowConfiguration;

public class ProcessingStateProcessorUnitTestNG {
    
    private File queuedDir = new File("/tmp/queued");
    private final ProcessingStateProcessor processor = new ProcessingStateProcessor();
    private Properties props = new Properties();
    
    @BeforeMethod(groups = "unit")
    public void setup() throws Exception {
        URL url = ClassLoader.getSystemResource("com/latticeengines/pls/service/impl/fileprocessor/processingstate");
        File dir = new File(url.getPath());
        Collection<File> files = FileUtils.listFiles(dir, new String[] { "json" }, false);
        for (File file : files) {
            FileUtils.copyFileToDirectory(file, queuedDir);
        }
        RestApiProxy apiProxy = mock(RestApiProxy.class);
        when(apiProxy.submitWorkflow(any(WorkflowConfiguration.class))).thenReturn( //
                YarnUtils.appIdFromString("application_1448430180764_0001"));
        props.put("restApiProxy", apiProxy);
    }
    
    @AfterMethod(groups = "unit")
    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(new File("/tmp/processing"));
        FileUtils.deleteDirectory(queuedDir);
    }
    
    @Test(groups = "unit")
    public void processDir() {
        processor.processDir(new File("/tmp"), FileProcessingState.PROCESSING, FileProcessingState.QUEUED, props);
        assertEquals(FileUtils.listFiles(queuedDir, new String[] { "json" }, false).size(), 0);
        assertEquals(FileUtils.listFiles(new File("/tmp/processing"), new String[] { "json" }, false).size(), 10);
    }

    @Test(groups = "unit")
    public void processDirFileUndeleted() throws Exception {
        FileDeleter fileDeleter = mock(FileDeleter.class);
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                File file = (File) invocation.getArguments()[0];
                // Just successfully delete 1 file
                if (file.getName().startsWith("8b")) {
                    FileUtils.deleteQuietly(file);
                }
                return null;
            }
        }).when(fileDeleter).deleteFile(any(File.class));
        ReflectionTestUtils.setField(processor, "fileDeleter", fileDeleter);
        processor.processDir(new File("/tmp"), FileProcessingState.PROCESSING, FileProcessingState.QUEUED, props);
        // Since 9 files weren't deleted (mocked), then 9 files are kept in the queue directory
        // and 1 file moved to processing directory 
        assertEquals(FileUtils.listFiles(queuedDir, new String[] { "json" }, false).size(), 9);
        Collection<File> processingFiles = FileUtils.listFiles(new File("/tmp/processing"), new String[] { "json" }, false); 
        assertEquals(processingFiles.size(), 1);
        FilePayload payload = JsonUtils.deserialize(FileUtils.readFileToString(processingFiles.iterator().next()), //
                FilePayload.class);
        assertEquals(payload.applicationId, "application_1448430180764_0001");
    }
}
