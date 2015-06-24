package com.latticeengines.upgrade.jdbc;

import java.util.List;

import javax.sql.DataSource;

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
    
    private String getHostAddress(String instance){
        String hostAddr = SQLServer200;
        if (instance.equals("SQL100")) {
            hostAddr = SQLServer100;
        }
        //hostAdd = "10.41.1.250:1433";
        return hostAddr;
    }

    public void init(String bardDB, String instance){
        String hostAddr = getHostAddress(instance);
        DataSource bardDataSource = new DriverManagerDataSource("jdbc:sqlserver://" + hostAddr + ";databaseName="
                + bardDB, user, pass);
        bardJdbcTemplate = new JdbcTemplate(bardDataSource);
    }

    public String getActiveModelKey() throws Exception {
        String modelInfo = bardJdbcTemplate.queryForObject(
                "select value from KeyValueStore where [Key] = 'ModelInfoDocument'", String.class);
        JsonNode jn = new ObjectMapper().readTree(ModelDecryptor.decrypt(modelInfo)).get("ActiveModelKeys");
        if (jn.size() != 1) {
            for(int i = 0; i < jn.size(); i++){
                System.out.println(jn.get(i).asText());
            }
            //System.out.println(dlTenantName + " does not have 1 active models");
            return "";
        }
        return jn.path(0).asText();
    }
    
    public String getModelContent(String activeModelKey) throws Exception{
        String encryptedModelContent = bardJdbcTemplate.queryForObject(
              "select value from KeyValueStore where [Key] = '" + activeModelKey + "'", String.class);
        return ModelDecryptor.decrypt(encryptedModelContent);
    }

}
