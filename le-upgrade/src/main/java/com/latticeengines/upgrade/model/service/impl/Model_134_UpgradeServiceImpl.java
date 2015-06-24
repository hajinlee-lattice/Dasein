package com.latticeengines.upgrade.model.service.impl;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.upgrade.domain.BardInfo;
import com.latticeengines.upgrade.jdbc.AuthoritativeDBJdbcManager;
import com.latticeengines.upgrade.jdbc.BardJdbcManager;

@Component("model_134_Upgrade")
public class Model_134_UpgradeServiceImpl extends ModelUpgradeServiceImpl{

    private static final String VERSION = "1.3.4";

    @Autowired
    private AuthoritativeDBJdbcManager authoritativeDBJdbcManager;
    
    @Autowired
    private BardJdbcManager bardJdbcManager;

    @Override
    public void upgrade() throws Exception{
        List<String> deploymentIds = authoritativeDBJdbcManager.getDeploymentIDs(VERSION);
        System.out.println(deploymentIds);
        //int i = 0;

        for (String deploymentId : deploymentIds) {
            //i++;
            //if (i > 3)
            //    break;
            List<BardInfo> bardInfos = authoritativeDBJdbcManager.getBardDBInfos(deploymentId);
            setInfos(bardInfos);
            bardJdbcManager.init(bardDB, instance);

            String activeModelKey = bardJdbcManager.getActiveModelKey();
            if(activeModelKey.equals("")){
                System.out.println("_______________________________________");
                continue;
            }
            modelGuid = StringUtils.remove(activeModelKey, "Model_");

            //uploadModelToHdfs(activeModelKey);
            populateTenantModelInfo();
            System.out.println("_______________________________________");
        }
    }

    @Override
    public void execute(String command, Map<String, Object> parameters) {
        System.out.println(VERSION + " upgrader is about to execute: " + command);
        super.execute(command, parameters);
    }

}
