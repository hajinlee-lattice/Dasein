package com.latticeengines.upgrade.model.service.impl;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.upgrade.jdbc.AuthoritativeDBJdbcManager;
import com.latticeengines.upgrade.jdbc.BardJdbcManager;

@Component("model_140_Upgrade")
public class Model_140_UpgradeServiceImpl extends ModelUpgradeServiceImpl {

    private static final String VERSION = "1.4.0";

    @Autowired
    private AuthoritativeDBJdbcManager authoritativeDBJdbcManager;

    @Autowired
    private BardJdbcManager bardJdbcManager;

    @Override
    public void upgrade() throws Exception{
        List<String> deploymentIds = authoritativeDBJdbcManager.getDeploymentIDs(VERSION);
        System.out.println(deploymentIds);
        int i = 0;
        for (String deploymentId : deploymentIds) {
            i++;
            if (i > 3)
                break;
            setBardDBInfos(authoritativeDBJdbcManager.getBardDBInfos(deploymentId));
            bardJdbcManager.init(bardDB, instance);

            String activeModelKey = bardJdbcManager.getActiveModelKey();
            if(activeModelKey.equals(""))
                continue;
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
