package com.latticeengines.eai.functionalframework;

import static org.testng.Assert.assertEquals;

import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.apache.camel.testng.AbstractCamelTestNGSpringContextTests;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.yarn.client.YarnClient;
import org.testng.annotations.BeforeClass;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFileFilter;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.yarn.functionalframework.YarnFunctionalTestNGBase;

@DirtiesContext
@ContextConfiguration(locations = { "classpath:test-eai-context.xml" })
public class EaiFunctionalTestNGBase extends AbstractCamelTestNGSpringContextTests
        implements EaiFunctionalTestNGInterface {

    protected static final Logger log = LoggerFactory.getLogger(EaiFunctionalTestNGBase.class);
    protected static final long WORKFLOW_WAIT_TIME_IN_MILLIS = TimeUnit.MINUTES.toMillis(90);

    @Inject
    protected Configuration yarnConfiguration;

    @Inject
    private YarnClient defaultYarnClient;

    @Inject
    protected TenantService tenantService;

    protected YarnFunctionalTestNGBase platformTestBase;

    @Value("${eai.test.metadata.url}")
    protected String mockMetadataUrl;

    @Value("${eai.test.metadata.port}")
    protected int mockPort;

    @BeforeClass(groups = { "functional", "deployment", "aws", "aws-deployment", "deployment.vdb" })
    public void setupRunEnvironment() throws Exception {
        platformTestBase = new YarnFunctionalTestNGBase(yarnConfiguration);
        platformTestBase.setYarnClient(defaultYarnClient);
    }

    protected void initZK(String customer) throws Exception {
        Camille camille = CamilleEnvironment.getCamille();
        Path docPath = PathBuilder.buildCustomerSpaceServicePath(CamilleEnvironment.getPodId(),
                CustomerSpace.parse(customer), "Eai");
        try {
            camille.delete(docPath);
        } catch (Exception e) {
        }

        Path connectTimeoutDocPath = docPath.append("SalesforceEndpointConfig").append("HttpClient")
                .append("ConnectTimeout");
        camille.create(connectTimeoutDocPath, new Document("60000"), ZooDefs.Ids.OPEN_ACL_UNSAFE);

        Path importTimeoutDocPath = docPath.append("SalesforceEndpointConfig").append("HttpClient")
                .append("ImportTimeout");
        camille.create(importTimeoutDocPath, new Document("3600000"), ZooDefs.Ids.OPEN_ACL_UNSAFE);
    }

    protected List<String> getFilesFromHdfs(String targetPath, String table) throws Exception {
        return HdfsUtils.getFilesForDirRecursive(yarnConfiguration, targetPath + "/" + table, new HdfsFileFilter() {

            @Override
            public boolean accept(FileStatus file) {
                return file.getPath().getName().endsWith(".avro");
            }

        });
    }

    protected void checkDataExists(String targetPath, List<String> tables, int number) throws Exception {
        for (String table : tables) {
            List<String> filesForTable = getFilesFromHdfs(targetPath, table);
            assertEquals(filesForTable.size(), number);
        }
    }

    protected Tenant createTenant(String customerSpace) {
        Tenant tenant = new Tenant();
        tenant.setId(customerSpace);
        tenant.setName(customerSpace);
        tenant.setRegisteredTime(System.currentTimeMillis());
        return tenant;
    }

}
