package com.latticeengines.dataflowapi.functionalframework;

import static org.testng.Assert.assertEquals;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.util.FileCopyUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestTemplate;
import org.springframework.yarn.client.YarnClient;
import org.testng.annotations.BeforeClass;

import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dataflow.DataFlowConfiguration;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.LastModifiedKey;
import com.latticeengines.domain.exposed.metadata.PrimaryKey;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.MagicAuthenticationHeaderHttpRequestInterceptor;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.entitymanager.impl.TenantEntityMgrImpl;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-dataflowapi-context.xml" })
public class DataFlowApiDeploymentTestNGBase extends AbstractTestNGSpringContextTests {

    protected RestTemplate restTemplate = new RestTemplate();

    @Value("${common.test.microservice.url}")
    private String hostPort;

    protected String getRestAPIHostPort() {
        return hostPort.endsWith("/") ? hostPort.substring(0, hostPort.length() - 1) : hostPort;
    }

    protected static final CustomerSpace CUSTOMERSPACE = CustomerSpace.parse("DFAPITests.DFAPITests.DFAPITests");

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(DataFlowApiDeploymentTestNGBase.class);

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private YarnClient defaultYarnClient;

    @Autowired
    private TenantEntityMgr tenantEntityMgr = new TenantEntityMgrImpl();

    @Autowired
    protected MetadataProxy metadataProxy;

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

        List<ClientHttpRequestInterceptor> interceptors = new ArrayList<>();
        interceptors.add(new MagicAuthenticationHeaderHttpRequestInterceptor());
        restTemplate.setInterceptors(interceptors);
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

    protected void createAndRegisterMetadata(String tableName, String path, String primaryKeyName,
            String lastModifiedColName) {
        Table table = new Table();
        table.setName(tableName);
        table.setDisplayName(tableName);

        Extract extract = new Extract();
        extract.setName("extract");
        extract.setPath(path);
        extract.setExtractionTimestamp(DateTime.now().getMillis());
        table.addExtract(extract);

        if (primaryKeyName != null) {
            PrimaryKey pk = new PrimaryKey();
            Attribute pkAttr = new Attribute();
            pkAttr.setName(primaryKeyName);
            pkAttr.setDisplayName(primaryKeyName);
            pk.setName(primaryKeyName);
            pk.setDisplayName(primaryKeyName);
            pk.addAttribute(primaryKeyName);
            table.setPrimaryKey(pk);
        }

        if (lastModifiedColName != null) {
            LastModifiedKey lmk = new LastModifiedKey();
            Attribute lastModifiedColumn = new Attribute();
            lastModifiedColumn.setName(lastModifiedColName);
            lastModifiedColumn.setDisplayName(lastModifiedColName);
            lmk.setName(lastModifiedColName);
            lmk.setDisplayName(lastModifiedColName);
            lmk.addAttribute(lastModifiedColName);
            lmk.setLastModifiedTimestamp(DateTime.now().getMillis());
            table.setLastModifiedKey(lmk);
        }

        try {
            metadataProxy.deleteTable(CUSTOMERSPACE.toString(), table.getName());
        } catch (Exception e) {
            // ignore if table doesn't exist yet
        }

        metadataProxy.updateTable(CUSTOMERSPACE.toString(), table.getName(), table);
    }

    protected AppSubmission submitDataFlow(DataFlowConfiguration configuration) {
        String url = String.format("%s/dataflowapi/dataflows/", getRestAPIHostPort());
        try {
            AppSubmission submission = restTemplate.postForObject(url, configuration, AppSubmission.class);
            return submission;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
