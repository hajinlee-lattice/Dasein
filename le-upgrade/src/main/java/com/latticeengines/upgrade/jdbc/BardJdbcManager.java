package com.latticeengines.upgrade.jdbc;

import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.upgrade.model.decrypt.ModelDecryptor;

@Component("bardJdbcManager")
public class BardJdbcManager {

    private JdbcTemplate bardJdbcTemplate;

    private static final String SQLServer200 = "BODCPRODVSQL200.prod.lattice.local\\SQL200";

    private static final String SQLServer100 = "BODCPRODVSQL100.prod.lattice.local\\SQL100";

    @Value("${upgrade.dao.datasource.user}")
    protected String user;

    @Value("${upgrade.dao.datasource.password.encrypted}")
    protected String pass;

    @Value("${upgrade.dao.bard.db.address.test}")
    private String useTestAddress;

    @Value("${dataloss.date}")
    private String dataLossDate; 

    private String getHostAddress(String instance) {
        if (StringUtils.isNotEmpty(useTestAddress))
            return useTestAddress.toLowerCase();
        String hostAddr = SQLServer200;
        if (instance.equals("SQL100")) {
            hostAddr = SQLServer100;
        }
        return hostAddr;
    }

    public void init(String bardDB, String instance) {
        String hostAddr = getHostAddress(instance);
        DataSource bardDataSource = new DriverManagerDataSource("jdbc:sqlserver://" + hostAddr + ";databaseName="
                + bardDB, user, pass);
        bardJdbcTemplate = new JdbcTemplate(bardDataSource);
    }

    public List<String> getActiveModelKey() throws Exception {
        List<String> activeModelKeys = new ArrayList<>();
        String modelInfo = bardJdbcTemplate.queryForObject(
                "select value from KeyValueStore where [Key] = 'ModelInfoDocument'", String.class);
        JsonNode jn = new ObjectMapper().readTree(ModelDecryptor.decrypt(modelInfo)).get("ActiveModelKeys");
        for (JsonNode subNode : jn) {
            activeModelKeys.add(subNode.asText());
        }
        return activeModelKeys;
    }

    public List<String> getModelGuidsWithinLast2Weeks(){
        List<String> modelGuids = bardJdbcTemplate.queryForList(
                "select [Key] from KeyValueStore where [Key] Like 'Model_ms__%' and [Creation_Date] > ' "+  dataLossDate + "'", String.class);
        return modelGuids;
    }

    public String getModelContent(String modelKey){
        return bardJdbcTemplate.queryForObject(
                String.format("select value from KeyValueStore where [key] = '%s'", modelKey), String.class);
    }

}
