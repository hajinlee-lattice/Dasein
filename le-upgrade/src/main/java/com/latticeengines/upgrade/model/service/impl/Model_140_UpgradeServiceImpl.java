package com.latticeengines.upgrade.model.service.impl;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Component;

@Component("model_140_Upgrade")
public class Model_140_UpgradeServiceImpl extends ModelUpgradeServiceImpl {

    private static final String VERSION = "1.4.0";

    @Override
    public void upgrade() throws Exception{
        List<String> deploymentIds = getDeploymentIDs(VERSION);
        System.out.println(deploymentIds);
        int i = 0;
        for (String deploymentId : deploymentIds) {
            i++;
            if (i > 3)
                break;
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

}
