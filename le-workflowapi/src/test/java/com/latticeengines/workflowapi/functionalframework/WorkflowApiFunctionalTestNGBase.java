package com.latticeengines.workflowapi.functionalframework;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.util.FileCopyUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestTemplate;
import org.springframework.yarn.client.YarnClient;
import org.testng.annotations.BeforeClass;

import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.rest.URLUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.workflow.exposed.build.WorkflowConfiguration;
import com.latticeengines.workflow.functionalframework.WorkflowFunctionalTestNGBase;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-workflowapi-context.xml" })
public class WorkflowApiFunctionalTestNGBase extends WorkflowFunctionalTestNGBase {

    protected static final CustomerSpace CUSTOMERSPACE = CustomerSpace.parse("WFAPITests.WFAPITests.WFAPITests");

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(WorkflowApiFunctionalTestNGBase.class);

    protected RestTemplate restTemplate = new RestTemplate();

    @Value("${workflowapi.test.api.hostport}")
    protected String hostPort;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private YarnClient defaultYarnClient;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

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

    protected AppSubmission submitWorkflow(WorkflowConfiguration configuration) {
        String url = String.format("%s/workflowapi/workflows/", URLUtils.getRestAPIHostPort(hostPort));
        try {
            AppSubmission submission = restTemplate.postForObject(url, configuration, AppSubmission.class);
            return submission;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
