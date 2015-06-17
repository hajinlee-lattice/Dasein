package com.latticeengines.upgrade.model.service.impl;

import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Component;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;

@Component("model_134_Upgrade")
public class Model_134_UpgradeServiceImpl extends ModelUpgradeServiceImpl{

    private static final String VERSION = "1.3.4";

    @Override
    public void upgrade() throws Exception{
        List<String> deploymentIds = getDeploymentIDs(VERSION);
        System.out.println(deploymentIds);
        //int i = 0;

        for (String deploymentId : deploymentIds) {
            //i++;
            //if (i > 3)
            //    break;
            setBardDBInfos(deploymentId);
            setToBardDBDataSource();

            String activeModelKey = getActiveModelKey();
            if(activeModelKey.equals(""))
                continue;
            modelGuid = StringUtils.remove(activeModelKey, "Model_");

            //uploadModelToHdfs(activeModelKey);
            populateTenantModelInfo();
            upgradeJdbcTemlate.setDataSource(dataSourceUpgrade);
            System.out.println("_______________________________________");
        }
    }

    private void uploadModelToHdfs(String activeModelKey) throws Exception{
            System.out.println("uploading model for: " + dlTenantName);
            String modelContent = getModelContent(activeModelKey);

            String uuid = StringUtils.remove(modelGuid, "ms__").substring(0, 36);
            String path = "/user/s-analytics/customers/" + CustomerSpace.parse(dlTenantName) + "/models/NoEventTableForUpradedModel/" + uuid
                    + "/1430367698445_0045/model.json";
            HdfsUtils.writeToFile(yarnConfiguration, path, modelContent);
    }

}
