package com.latticeengines.serviceflows.functionalframework;

import java.nio.file.PathMatcher;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

import com.latticeengines.dataflow.exposed.builder.CascadingDataFlowBuilder;
import com.latticeengines.dataflow.exposed.service.DataTransformationService;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.LastModifiedKey;
import com.latticeengines.domain.exposed.metadata.PrimaryKey;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-serviceflows-context.xml" })
public class ServiceFlowsFunctionalTestNGBase extends AbstractTestNGSpringContextTests {
    
    @Autowired
    private DataTransformationService dataTransformationService;
    
    @Autowired
    private ApplicationContext appContext;
    
    protected Configuration yarnConfiguration = new Configuration();
    
    protected void executeDataFlow(DataFlowContext dataFlowContext, String beanName) throws Exception {
        dataTransformationService.executeNamedTransformation(dataFlowContext, beanName);
    }
    
    protected DataFlowContext createDataFlowContext() {
        DataFlowContext ctx = new DataFlowContext();
        ctx.setProperty("QUEUE", LedpQueueAssigner.getModelingQueueNameForSubmission());
        ctx.setProperty("CHECKPOINT", false);
        ctx.setProperty("HADOOPCONF", yarnConfiguration);
        ctx.setProperty("ENGINE", "MR");
        return ctx;
    }
    
    protected Table createTableFromDir(String tableName, String path, String lastModifiedColName) {
        Table table = new Table();
        table.setName(tableName);
        Extract extract = new Extract();
        extract.setName("e1");
        extract.setPath(path);
        table.addExtract(extract);
        PrimaryKey pk = new PrimaryKey();
        Attribute pkAttr = new Attribute();
        pkAttr.setName("Id");
        pk.addAttribute("Id");
        LastModifiedKey lmk = new LastModifiedKey();
        Attribute lastModifiedColumn = new Attribute();
        lastModifiedColumn.setName(lastModifiedColName);
        lmk.addAttribute(lastModifiedColName);
        table.setPrimaryKey(pk);
        table.setLastModifiedKey(lmk);
        return table;
    }

    protected List<GenericRecord> readTable(String path) {
        try {
            List<String> matches = HdfsUtils.getFilesByGlob(yarnConfiguration, path);
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
