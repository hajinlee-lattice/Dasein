package com.latticeengines.serviceflows.functionalframework;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataflow.exposed.service.DataTransformationService;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-serviceflows-context.xml" })
public class ServiceFlowsFunctionalTestNGBase extends AbstractTestNGSpringContextTests {

    protected ServiceFlowsFunctionalTestNGBase() {
        yarnConfiguration.set("fs.defaultFS", "file:///");
    }

    @Autowired
    private DataTransformationService dataTransformationService;
    
    @Autowired
    private ApplicationContext appContext;
    
    protected Configuration yarnConfiguration = new Configuration();

    protected Table executeDataFlow(DataFlowContext dataFlowContext, String beanName) throws Exception {
        return dataTransformationService.executeNamedTransformation(dataFlowContext, beanName);
    }
    
    protected DataFlowContext createDataFlowContext() {
        DataFlowContext ctx = new DataFlowContext();
        ctx.setProperty("QUEUE", LedpQueueAssigner.getModelingQueueNameForSubmission());
        ctx.setProperty("CHECKPOINT", false);
        ctx.setProperty("HADOOPCONF", yarnConfiguration);
        ctx.setProperty("ENGINE", "MR");
        return ctx;
    }

    protected List<GenericRecord> readTable(String avroFile) {
        try {
            List<String> matches = HdfsUtils.getFilesByGlob(yarnConfiguration, avroFile);
            List<GenericRecord> output = new ArrayList<>();
            for (String match : matches) {
                output.addAll(AvroUtils.getData(yarnConfiguration, new Path(match)));
            }
            return output;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns whether everything in test with testId exists in master using masterId.
     * Duplicates are ignored.
     * Can't we just upgrade to Java 8 already?
     */
    protected boolean allEntriesExist(List<GenericRecord> test, String testId, List<GenericRecord> master, String masterId) {

        for (GenericRecord record : test) {
            Object value = record.get(testId);
            boolean found = false;
            for (GenericRecord inner : master) {
                Object innerValue = inner.get(masterId);
                if (value.equals(innerValue)) {
                    found = true;
                    break;
                }
            }
            if (found == false) {
                return false;
            }
        }

        return true;
    }

    protected boolean identicalSets(List<GenericRecord> left, String leftId, List<GenericRecord> right, String rightId) {
        return allEntriesExist(left, leftId, right, rightId) && allEntriesExist(right, rightId, left, leftId);
    }
}
