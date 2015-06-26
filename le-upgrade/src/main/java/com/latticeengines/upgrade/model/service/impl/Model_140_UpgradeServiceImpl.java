package com.latticeengines.upgrade.model.service.impl;

import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;
import com.latticeengines.upgrade.domain.BardInfo;

@Component("model_140_Upgrade")
public class Model_140_UpgradeServiceImpl extends ModelUpgradeServiceImpl {

    private static final String VERSION = "1.4.0";

    @Override
    public void upgrade() throws Exception{
        setVersion(VERSION);
        populateTenantModelInfo();
        List<String> deploymentIds = authoritativeDBJdbcManager.getDeploymentIDs(VERSION);
        System.out.println(deploymentIds);
        int i = 0;
        for (String deploymentId : deploymentIds) {
            i++;
            if (i > 3)
                break;
            List<BardInfo> bardInfos = authoritativeDBJdbcManager.getBardDBInfos(deploymentId);
            setInfos(bardInfos);
            bardJdbcManager.init(bardDB, instance);

            List<String> activeModelKeyList = bardJdbcManager.getActiveModelKey();
            if(activeModelKeyList.size() != 1){
                System.out.println("_______________________________________");
                continue;
            }

            //uploadModelToHdfs(activeModelKey);
            populateTenantModelInfo();
            System.out.println("_______________________________________");
        }
    }

    @Override
    public boolean execute(String command, Map<String, Object> parameters) {
        System.out.println(VERSION + " upgrader is about to execute: " + command);
        this.setVersion(VERSION);
        boolean handledByParentUpgrader = super.execute(command, parameters);
        if (!handledByParentUpgrader) {
            return false;
        }
        return false;
    }

}
