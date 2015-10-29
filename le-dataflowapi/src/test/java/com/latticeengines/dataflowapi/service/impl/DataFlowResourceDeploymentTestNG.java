package com.latticeengines.dataflowapi.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;

import java.io.File;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataflowapi.functionalframework.DataFlowApiFunctionalTestNGBase;
import com.latticeengines.dataflowapi.service.DataFlowService;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.dataflow.DataFlowConfiguration;
import com.latticeengines.domain.exposed.dataflow.DataFlowSource;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.swlib.SoftwarePackage;
import com.latticeengines.swlib.exposed.service.SoftwareLibraryService;

public class DataFlowResourceDeploymentTestNG extends DataFlowApiFunctionalTestNGBase {

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private DataFlowService dataFlowService;

    @Autowired
    private SoftwareLibraryService softwareLibraryService;

    private String account;

    private String contact;

    private String opportunity;

    public DataFlowResourceDeploymentTestNG() {
    }

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        String jarFile = ClassLoader.getSystemResource(
                "com/latticeengines/dataflowapi/service/impl/le-serviceflows-prospectdiscovery.jar").getPath();
        HdfsUtils.rmdir(yarnConfiguration, //
                String.format("%s/%s", SoftwareLibraryService.TOPLEVELPATH, "dataflowapi"));
        SoftwarePackage pkg = new SoftwarePackage();
        pkg.setModule("dataflowapi");
        pkg.setGroupId("com.latticeengines");
        pkg.setArtifactId("le-serviceflows-prospectdiscovery");
        pkg.setVersion("2.0.12-SNAPSHOT");
        pkg.setInitializerClass("com.latticeengines.prospectdiscovery.Initializer");
        softwareLibraryService.installPackage(pkg, new File(jarFile));

        HdfsUtils.rmdir(yarnConfiguration, "/tmp/avro");
        HdfsUtils.rmdir(yarnConfiguration, "/tmp/PDTable");
        HdfsUtils.rmdir(yarnConfiguration, "/tmp/EventTable");
        HdfsUtils.rmdir(yarnConfiguration, "/tmp/TmpEventTable");
        HdfsUtils.rmdir(yarnConfiguration, "/tmp/checkpoints");

        account = ClassLoader.getSystemResource("Account/Account.avro").getPath();
        opportunity = ClassLoader.getSystemResource("Opportunity/Opportunity.avro").getPath();
        contact = ClassLoader.getSystemResource("Contact/Contact.avro").getPath();

        List<AbstractMap.SimpleEntry<String, String>> entries = new ArrayList<>();

        entries.add(new AbstractMap.SimpleEntry<>("file://" + account, "/tmp/avro/Account"));
        entries.add(new AbstractMap.SimpleEntry<>("file://" + opportunity, "/tmp/avro/Opportunity"));
        entries.add(new AbstractMap.SimpleEntry<>("file://" + contact, "/tmp/avro/Contact"));

        account = "/tmp/avro/Account/*.avro";
        contact = "/tmp/avro/Contact/*.avro";
        opportunity = "/tmp/avro/Opportunity/*.avro";
        createAndRegisterMetadata("Account", account, "CreatedDate");
        createAndRegisterMetadata("Contact", contact, "LastModifiedDate");
        createAndRegisterMetadata("Opportunity", opportunity, "LastModifiedDate");

        FileSystem fs = FileSystem.get(yarnConfiguration);
        doCopy(fs, entries);

    }

    @Test(groups = "deployment")
    public void submitDataFlow() throws Exception {
        DataFlowConfiguration config = new DataFlowConfiguration();
        config.setName("DataFlowServiceImpl_submitDataFlow");
        config.setCustomerSpace(CUSTOMERSPACE);
        config.setDataFlowBeanName("createEventTable");
        List<DataFlowSource> sources = new ArrayList<>();
        sources.add(createDataFlowSource("Account"));
        sources.add(createDataFlowSource("Contact"));
        sources.add(createDataFlowSource("Opportunity"));

        config.setDataSources(sources);
        config.setTargetPath("/TmpEventTable");

        AppSubmission submission = submitDataFlow(config);
        assertNotNull(submission);
        assertNotEquals(submission.getApplicationIds().size(), 0);
        String appId = submission.getApplicationIds().get(0);
        assertNotNull(appId);
        FinalApplicationStatus status = platformTestBase.waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
        Table metadata = proxy.getMetadata(config.getCustomerSpace(), config.getName());
        assertNotNull(metadata);
        assertEquals(metadata.getExtracts().size(), 1);
        Path expectedLocation = PathBuilder.buildDataTablePath( //
                CamilleEnvironment.getPodId(), config.getCustomerSpace());
        assertEquals(metadata.getExtracts().get(0).getPath(), //
                expectedLocation.append(config.getTargetPath()).toString());
    }

    private DataFlowSource createDataFlowSource(String name) {
        DataFlowSource s = new DataFlowSource();
        s.setName(name);
        return s;
    }

}
