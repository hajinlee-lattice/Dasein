package com.latticeengines.upgrade.jdbc;

import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.upgrade.domain.BardInfo;

@Component("authoritativeDBJdbcManager")
public class AuthoritativeDBJdbcManager {

    @Autowired
    private JdbcTemplate authoritativeDBJdbcTemlate;

    public List<String> getDeploymentIDs(String version) {
        return authoritativeDBJdbcTemlate.queryForList("select LEDeployment_ID from LEDeployment where " //
                + "deployment_type = 9 and isactive = 1 and Current_Version='" + version + "'", String.class);
    }

    public List<BardInfo> getBardDBInfos(String deploymentId) throws Exception {
        List<BardInfo> infosList = authoritativeDBJdbcTemlate
                .query("select Display_Name, Instance, Name, Settings from LEComponent where " //
                        + "LEDeployment_ID = " + deploymentId, new BeanPropertyRowMapper<BardInfo>(BardInfo.class));
        return infosList;
    }

    public List<BardInfo> getBardDB(String customer) {
        List<BardInfo> infosList = authoritativeDBJdbcTemlate
                .query("select Display_Name, Instance, Name, Settings from LEComponent where " //
                        + "External_ID = '" + customer + "_DB_BARD'", new BeanPropertyRowMapper<BardInfo>(BardInfo.class));
        return infosList;
    }
}
