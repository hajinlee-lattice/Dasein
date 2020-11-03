package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.BeforeClass;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;
import com.latticeengines.testframework.exposed.service.TestArtifactService;

@ContextConfiguration(locations = { "classpath:serviceflows-cdl-workflow-context.xml", "classpath:test-serviceflows-cdl-context.xml" })
public abstract class CDLWorkflowFunctionalTestNGBase extends ServiceFlowsDataFlowFunctionalTestNGBase {

    protected static final String TEST_AVRO_DIR = "le-serviceflows/cdl/functional/avro";
    protected static final String TEST_AVRO_VERSION = "1";
    protected static final CustomerSpace CDL_WF_FUNCTION_TEST_CUSTOMERSPACE = CustomerSpace
            .parse("CDLWFFunctionTests.CDLWFFunctionTests.CDLWFFunctionTests");
    protected String fileDestination;

    protected String fileName;

    @Inject
    protected TestArtifactService testArtifactService;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    protected Tenant tenant;

    @BeforeClass(groups = { "functional" })
    public void setup() throws Exception {
        tenant = tenantEntityMgr.findByTenantId(CDL_WF_FUNCTION_TEST_CUSTOMERSPACE.toString());
        if (tenant != null) {
            tenantEntityMgr.delete(tenant);
        }
        tenant = new Tenant();
        tenant.setId(CDL_WF_FUNCTION_TEST_CUSTOMERSPACE.toString());
        tenant.setName(CDL_WF_FUNCTION_TEST_CUSTOMERSPACE.toString());
        tenantEntityMgr.create(tenant);
        MultiTenantContext.setTenant(tenant);

        com.latticeengines.domain.exposed.camille.Path path = //
                PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(), CDL_WF_FUNCTION_TEST_CUSTOMERSPACE);
        HdfsUtils.rmdir(yarnConfiguration, path.toString());
        HdfsUtils.mkdir(yarnConfiguration, path.toString());
    }
}
