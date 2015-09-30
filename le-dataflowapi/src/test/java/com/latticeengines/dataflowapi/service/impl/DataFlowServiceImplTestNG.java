package com.latticeengines.dataflowapi.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.File;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataflowapi.functionalframework.DataFlowApiFunctionalTestNGBase;
import com.latticeengines.dataflowapi.service.DataFlowService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dataflow.DataFlowConfiguration;
import com.latticeengines.domain.exposed.dataflow.DataFlowSource;
import com.latticeengines.domain.exposed.swlib.SoftwarePackage;
import com.latticeengines.swlib.exposed.service.SoftwareLibraryService;

public class DataFlowServiceImplTestNG extends DataFlowApiFunctionalTestNGBase {

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private DataFlowService dataFlowService;

    @Autowired
    private SoftwareLibraryService softwareLibraryService;
    
    private String account;
    
    private String contact;
    
    private String opportunity;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        String jarFile = ClassLoader.getSystemResource(
                "com/latticeengines/dataflowapi/service/impl/le-serviceflows-prospectdiscovery.jar").getPath();
        HdfsUtils.rmdir(yarnConfiguration, //
                String.format("%s/%s", SoftwareLibraryService.TOPLEVELPATH, "dataflowapi"));
        SoftwarePackage pkg = new SoftwarePackage();
        pkg.setModule("dataflowapi");
        pkg.setGroupId("com.latticeengines");
        pkg.setArtifactId("le-serviceflows-prospectdiscovery");
        pkg.setVersion("2.0.9-SNAPSHOT");
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

        FileSystem fs = FileSystem.get(yarnConfiguration);
        doCopy(fs, entries);

    }

    @Test(groups = "functional")
    public void submitDataFlow() throws Exception {
        DataFlowConfiguration config = new DataFlowConfiguration();
        config.setName("DataFlowConfig1");
        config.setCustomerSpace(CustomerSpace.parse("C1.T1.QA"));
        config.setDataFlowBeanName("createEventTable");
        List<DataFlowSource> sources = new ArrayList<>();
        
        sources.add(createDataFlowSource("Account", account, "CreatedDate"));
        sources.add(createDataFlowSource("Contact", contact, "LastModifiedDate"));
        sources.add(createDataFlowSource("Opportunity", opportunity, "LastModifiedDate"));
        
        config.setDataSources(sources);
        config.setTargetPath("/tmp/TmpEventTable");

        ApplicationId appId = dataFlowService.submitDataFlow(config);
        assertNotNull(appId);
        FinalApplicationStatus status = platformTestBase.waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
        
    }
    
    private DataFlowSource createDataFlowSource(String name, String path, String lastModifiedColName) {
        DataFlowSource s = new DataFlowSource();
        s.setName(name);
        s.setTable(createTableFromDir(name, path, lastModifiedColName));
        return s;
    }
}

