package com.latticeengines.upgrade.jdbc;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component("authoritativeDBJdbcManager")
public class AuthoritativeDBJdbcManager {

    @Autowired
    private JdbcTemplate authoritativeDBJdbcTemlate;

    public List<String> getDeploymentIDs(String version) {
        return authoritativeDBJdbcTemlate.queryForList("select LEDeployment_ID from LEDeployment where " //
                + "deployment_type = 9 and isactive = 1 and Current_Version='" + version + "'", String.class);
    }

    public List<Map<String, Object>> getBardDBInfos(String deploymentId) throws Exception {
        List<Map<String, Object>> infosList = authoritativeDBJdbcTemlate
                .queryForList("select Display_Name, Instance, Name, Settings from LEComponent where " //
                        + "LEDeployment_ID = " + deploymentId);
        return infosList;
    }

}
