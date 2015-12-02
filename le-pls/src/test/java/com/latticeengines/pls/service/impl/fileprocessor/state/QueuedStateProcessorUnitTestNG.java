package com.latticeengines.pls.service.impl.fileprocessor.state;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.pls.FilePayload;
import com.latticeengines.pls.service.impl.fileprocessor.FileProcessingState;

public class QueuedStateProcessorUnitTestNG  {
    
    private File dataDir = new File("/tmp/data");
    private File queuedDir = new File("/tmp/queued");
    private File processingDir = new File("/tmp/processing");
    private QueuedStateProcessor processor = new QueuedStateProcessor();
    private final static String TENANT = "DemoContract.DemoTenant.Production";

    private void createFiles(int numFiles, String tenant, File parent) throws Exception {
        for (int i = 0; i < numFiles; i++) {
            String filename = String.format("%s~%s.csv", tenant, UUID.randomUUID().toString());
            // empty data should be fine since we're only testing how payloads are created
            FileUtils.write(new File(parent.getAbsolutePath() + "/" + filename), "");
        }
    }
    
    @BeforeClass(groups = "unit")
    public void setup() throws Exception {
        dataDir.mkdir();
        createFiles(5, TENANT, dataDir);
    }
    
    @AfterClass(groups = "unit")
    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(dataDir);
        FileUtils.deleteDirectory(queuedDir);
        FileUtils.deleteDirectory(processingDir);
    }
    
    @Test(groups = "unit")
    public void processDir() throws Exception {
        processor.processDir(new File("/tmp"), FileProcessingState.QUEUED, null, new Properties());
        
        assertTrue(queuedDir.exists());
        Collection<File> files = FileUtils.listFiles(queuedDir, new String[] { "json" }, false); 
        assertEquals(files.size(), 5);
        Map<String, Long> fileToTimestampMap = new HashMap<>();
        for (File f : files) {
            FilePayload payload = JsonUtils.deserialize(FileUtils.readFileToString(f), FilePayload.class);
            assertEquals(payload.customerSpace, TENANT);
            File dataFile = new File("/tmp/data/" + processor.stripExtension(f)[1] + ".csv");
            assertEquals(payload.filePath, dataFile.getAbsolutePath());
            fileToTimestampMap.put(dataFile.getName(), dataFile.lastModified());
        }
        // Create 5 more files in the data directory then process
        createFiles(5, TENANT, dataDir);
        processor.processDir(new File("/tmp"), FileProcessingState.QUEUED, null, new Properties());
        files = FileUtils.listFiles(queuedDir, new String[] { "json" }, false);
        assertEquals(files.size(), 10);
        
        for (File f : files) {
            FilePayload payload = JsonUtils.deserialize(FileUtils.readFileToString(f), FilePayload.class);
            File dataFile = new File(payload.filePath);
            Long ts = fileToTimestampMap.get(dataFile.getName());
            if (ts != null) {
                assertEquals(dataFile.lastModified(), ts.longValue());
            }
        }
    }

    @Test(groups = "unit", dependsOnMethods = { "processDir" })
    public void processDirWithFilesMovedToProcessingState() throws Exception {
        Collection<File> files = FileUtils.listFiles(queuedDir, new String[] { "json" }, false);
        
        int i = 0;
        for (File file : files) {
            if (i == 5) {
                break;
            }
            // Copy to processing directory
            FileUtils.moveFileToDirectory(file, processingDir, false);
            i++;
        }
        // Out of the 10 data files, 5 are in QUEUED state and 5 are in PROCESSING state.
        processor.processDir(new File("/tmp"), FileProcessingState.QUEUED, null, new Properties());
        files = FileUtils.listFiles(queuedDir, new String[] { "json" }, false);
        // No new payload files should be been created for the QUEUED state
        assertEquals(files.size(), 5);
    }
}
