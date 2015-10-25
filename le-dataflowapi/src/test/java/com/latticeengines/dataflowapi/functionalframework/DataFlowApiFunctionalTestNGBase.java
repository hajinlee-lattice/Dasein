package com.latticeengines.dataflowapi.functionalframework;

import static org.testng.Assert.assertEquals;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.List;

import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
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

import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataflowapi.util.MetadataProxy;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.LastModifiedKey;
import com.latticeengines.domain.exposed.metadata.PrimaryKey;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.entitymanager.impl.TenantEntityMgrImpl;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-dataflowapi-context.xml" })
public class DataFlowApiFunctionalTestNGBase extends AbstractTestNGSpringContextTests {

    protected static final CustomerSpace CUSTOMERSPACE = CustomerSpace.parse("DFAPITests.DFAPITests.DFAPITests");

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(DataFlowApiFunctionalTestNGBase.class);

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private YarnClient defaultYarnClient;

    @Autowired
    private TenantEntityMgr tenantEntityMgr = new TenantEntityMgrImpl();

    @Autowired
    private MetadataProxy proxy;

    protected DataPlatformFunctionalTestNGBase platformTestBase;

    @BeforeClass(groups = { "functional", "deployment" })
    public void setupRunEnvironment() throws Exception {
        platformTestBase = new DataPlatformFunctionalTestNGBase(yarnConfiguration);
        platformTestBase.setYarnClient(defaultYarnClient);
        Tenant t = tenantEntityMgr.findByTenantId(CUSTOMERSPACE.toString());
        if (t != null) {
            tenantEntityMgr.delete(t);
        }
        t = createTenant(CUSTOMERSPACE);
        tenantEntityMgr.create(t);

        com.latticeengines.domain.exposed.camille.Path path = //
        PathBuilder.buildCustomerSpacePath("Production", CUSTOMERSPACE);
        HdfsUtils.rmdir(yarnConfiguration, path.toString());
        HdfsUtils.mkdir(yarnConfiguration, path.toString());
    }

    private Tenant createTenant(CustomerSpace customerSpace) {
        Tenant tenant = new Tenant();
        tenant.setId(customerSpace.toString());
        tenant.setName(customerSpace.toString());
        return tenant;
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

    protected void createAndRegisterMetadata(String tableName, String path, String lastModifiedColName) {
        Table table = new Table();
        table.setName(tableName);
        table.setDisplayName(tableName);

        Extract extract = new Extract();
        extract.setName("extract");
        extract.setPath(path);
        extract.setExtractionTimestamp(DateTime.now().getMillis());
        table.addExtract(extract);

        PrimaryKey pk = new PrimaryKey();
        Attribute pkAttr = new Attribute();
        pkAttr.setName("Id");
        pkAttr.setDisplayName("Id");
        pk.setName("Id");
        pk.setDisplayName("Id");
        pk.addAttribute("Id");

        LastModifiedKey lmk = new LastModifiedKey();
        Attribute lastModifiedColumn = new Attribute();
        lastModifiedColumn.setName(lastModifiedColName);
        lastModifiedColumn.setDisplayName(lastModifiedColName);
        lmk.setName(lastModifiedColName);
        lmk.setDisplayName(lastModifiedColName);
        lmk.addAttribute(lastModifiedColName);
        lmk.setLastModifiedTimestamp(DateTime.now().getMillis());

        table.setPrimaryKey(pk);
        table.setLastModifiedKey(lmk);
        try {
            proxy.deleteMetadata(CUSTOMERSPACE, table);
        } catch (Exception e) {
            // ignore if table doesn't exist yet
        }
        
        proxy.setMetadata(CUSTOMERSPACE, table);
    }
}
