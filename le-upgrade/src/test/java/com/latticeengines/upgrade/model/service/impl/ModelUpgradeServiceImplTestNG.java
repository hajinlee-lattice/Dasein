package com.latticeengines.upgrade.model.service.impl;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import static org.testng.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFileFilter;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.upgrade.functionalframework.UpgradeFunctionalTestNGBase;
import com.latticeengines.upgrade.jdbc.BardJdbcManager;
import com.latticeengines.upgrade.model.service.ModelUpgradeService;

public class ModelUpgradeServiceImplTestNG extends UpgradeFunctionalTestNGBase {

    @Autowired
    private ModelUpgradeService modelUpgradeService;

    @Autowired
    private BardJdbcManager bardJdbcManager;

    @Autowired
    private Configuration yarnConfiguration;

    @Value("${dataplatform.customer.basedir}")
    private String userBaseDir;

    private String customer = "CitrixSaas";

    private String singularIdModelPath;

    private String tupleIdModelPath;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        bardJdbcManager.init("CitrixSaas_PLS2_DB_BARD", "");

        singularIdModelPath = userBaseDir + "/" + customer + "/models/Event_Table";
        tupleIdModelPath = userBaseDir + "/" + CustomerSpace.parse(customer) + "/models/Event_Table";
        HdfsUtils.rmdir(yarnConfiguration, singularIdModelPath);
        HdfsUtils.rmdir(yarnConfiguration, tupleIdModelPath);
        HdfsUtils.writeToFile(yarnConfiguration, singularIdModelPath
                + "/2208a85b-f096-484e-98e0-c72d84bf2616/container1/a.json", "");
        HdfsUtils.writeToFile(yarnConfiguration, tupleIdModelPath
                + "/245bf5af-c420-4657-996e-647e8a07ab63/container2/b.json", "");
        HdfsUtils.writeToFile(yarnConfiguration, singularIdModelPath
                + "/53e16023-5d32-467b-84e4-70792b623a2f/container3/c.json", "");
        HdfsUtils.writeToFile(yarnConfiguration, tupleIdModelPath
                + "/5f2bc0cf-a1cd-4a00-a52f-b346babefd62/container4/d.json", "");
        HdfsUtils.writeToFile(yarnConfiguration, singularIdModelPath
                + "/94a75212-6e6c-4205-a06b-b44a828d49e3/container5/e.json", "");
        HdfsUtils.writeToFile(yarnConfiguration, singularIdModelPath
                + "/e7ddfc25-8b58-4693-9750-e8900de494ef/container6/f.json", "");
    }

    @AfterClass(groups = "functional")
    public void cleanUp() throws Exception {
         //HdfsUtils.rmdir(yarnConfiguration, userBaseDir + "/" + customer);
         //HdfsUtils.rmdir(yarnConfiguration, userBaseDir + "/" + CustomerSpace.parse(customer));
    }

    @Test(groups = "functional")
    public void testExportModelsFromBardToHdfs() throws Exception {
        modelUpgradeService.exportModelsFromBardToHdfs("CitrixSaas");
        List<String> uuidList = Arrays.<String> asList(new String[]{"2208a85b-f096-484e-98e0-c72d84bf2616", "245bf5af-c420-4657-996e-647e8a07ab63", "53e16023-5d32-467b-84e4-70792b623a2f", "5f2bc0cf-a1cd-4a00-a52f-b346babefd62"
                ,"94a75212-6e6c-4205-a06b-b44a828d49e3", "e7ddfc25-8b58-4693-9750-e8900de494ef"});
        Set<String> modelNames = new HashSet<>();
        System.out.println(tupleIdModelPath);
        for(String uuid : uuidList){
            List<String> modelFile = HdfsUtils.getFilesForDirRecursive(yarnConfiguration, tupleIdModelPath + "/" + uuid, new HdfsFileFilter(){

                @Override
                public boolean accept(FileStatus file) {
                    return file.getPath().toString().contains("_model.json");
                }
            });
            if(CollectionUtils.isEmpty(modelFile)){
                throw new Exception("Cannot find model files " + uuid);
            }
            String modelContent = HdfsUtils.getHdfsFileContents(yarnConfiguration, modelFile.get(0));
            String name = new ObjectMapper().readTree(modelContent).get("Name").asText();
            System.out.println(name);
            modelNames.add(name);
        }
        assertEquals(modelNames.size(), 6);
    }
}
