package com.latticeengines.dataflow.functionalframework;

import static org.testng.Assert.assertEquals;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;

import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.util.FileCopyUtils;
import org.springframework.util.StringUtils;
import org.springframework.yarn.client.YarnClient;
import org.testng.annotations.BeforeClass;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.LastModifiedKey;
import com.latticeengines.domain.exposed.metadata.PrimaryKey;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.security.Tenant;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-dataflow-context.xml" })
public class DataFlowFunctionalTestNGBase extends AbstractTestNGSpringContextTests {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(DataFlowFunctionalTestNGBase.class);
    
    @Autowired
    private Configuration yarnConfiguration;
    
    @Autowired
    private YarnClient defaultYarnClient;
    
    protected DataPlatformFunctionalTestNGBase platformTestBase;

    @BeforeClass(groups = { "functional", "deployment" })
    public void setupRunEnvironment() throws Exception {
        platformTestBase = new DataPlatformFunctionalTestNGBase(yarnConfiguration);
        platformTestBase.setYarnClient(defaultYarnClient);
    }


    public void doCopy(FileSystem fs, List<AbstractMap.SimpleEntry<String, String>> copyEntries) throws Exception {
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();

        for (AbstractMap.SimpleEntry<String, String> e : copyEntries) {
            for (String pattern : StringUtils.commaDelimitedListToStringArray(e.getKey())) {
                for (Resource res : resolver.getResources(pattern)) {
                    Path destinationPath = getDestinationPath(e.getValue(), res);
                    FSDataOutputStream os = fs.create(destinationPath);
                    FileCopyUtils.copy(res.getInputStream(), os);
                }
            }
        }

    }

    protected Path getDestinationPath(String destPath, Resource res) throws IOException {
        Path dest = new Path(destPath, res.getFilename());
        return dest;
    }

    protected void verifyNumRows(Configuration config, String targetDir, int expectedNumRows) throws Exception {
        List<String> avroFiles = HdfsUtils.getFilesByGlob(config, String.format("%s/*.avro", targetDir));

        int numRows = 0;
        for (String avroFile : avroFiles) {
            try (FileReader<GenericRecord> reader = AvroUtils.getAvroFileReader(config, new Path(avroFile))) {
                while (reader.hasNext()) {
                    reader.next();
                    numRows++;
                }
            }
        }
        assertEquals(numRows, expectedNumRows);
    }

    protected Table createTableFromDir(String tableName, String path, String lastModifiedColName) {
        Tenant tenant = new Tenant();
        tenant.setId("T1");
        tenant.setName("T1");
        Table table = new Table();
        table.setTenant(tenant);
        table.setName(tableName);
        Extract extract = new Extract();
        extract.setName("e1");
        extract.setPath(path);
        table.addExtract(extract);
        PrimaryKey pk = new PrimaryKey();
        pk.setTenant(tenant);
        Attribute pkAttr = new Attribute();
        pkAttr.setName("Id");
        pk.setAttributes(Arrays.<Attribute>asList(new Attribute[] { pkAttr }));
        LastModifiedKey lmk = new LastModifiedKey();
        lmk.setTenant(tenant);
        Attribute lastModifiedColumn = new Attribute();
        lastModifiedColumn.setName(lastModifiedColName);
        lmk.setAttributes(Arrays.<Attribute>asList(new Attribute[] { lastModifiedColumn }));
        table.setPrimaryKey(pk);
        table.setLastModifiedKey(lmk);
        return table;
    }
}
