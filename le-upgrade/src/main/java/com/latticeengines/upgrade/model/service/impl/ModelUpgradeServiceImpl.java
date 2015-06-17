package com.latticeengines.upgrade.model.service.impl;

import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.upgrade.model.decrypt.ModelDecryptor;
import com.latticeengines.upgrade.model.service.ModelUpgradeService;

@Component("modelUpgrade")
public class ModelUpgradeServiceImpl implements ModelUpgradeService {

    @Autowired
    protected DataSource dataSourceUpgrade;

    @Autowired
    protected JdbcTemplate upgradeJdbcTemlate;

    @Autowired
    protected Configuration yarnConfiguration;

    @Value("${upgrade.dao.tenant.model.info.jdbc}")
    protected String tenantModelInfoJDBC;

    @Value("${upgrade.dao.datasource.user}")
    protected String user;

    @Value("${upgrade.dao.datasource.password.encrypted}")
    protected String pass;

    @Value("${dataplatform.customer.basedir}")
    protected String customerBase;

    protected String bardDB;

    protected String instance;

    protected String dlTenantName;

    protected String modelGuid;

    public void upgrade() throws Exception {
    }

    protected List<String> getDeploymentIDs(String version) {
        return upgradeJdbcTemlate.queryForList("select LEDeployment_ID from LEDeployment where " //
                + "deployment_type = 9 and isactive = 1 and Current_Version='" + version + "'", String.class);
    }

    protected void setBardDBInfos(String deploymentId) throws Exception {
        List<Map<String, Object>> infosList = upgradeJdbcTemlate
                .queryForList("select Display_Name, Instance, Name, Settings from LEComponent where " //
                        + "LEDeployment_ID = " + deploymentId);
        setBardDBInfos(infosList);
    }

    protected void setBardDBInfos(List<Map<String, Object>> infosList) throws Exception {
        for (Map<String, Object> infos : infosList) {
            if (infos.get("Display_Name").equals("Bard DB")) {
                bardDB = (String) infos.get("Name");
                System.out.println(bardDB);
                instance = (String) infos.get("Instance");
                System.out.println("instance: " + instance);
            } else if (infos.get("Display_Name").equals("VisiDBDL")) {
                String settings = (String) infos.get("Settings");
                JsonNode parentNode = new ObjectMapper().readTree(settings);
                for (JsonNode node : parentNode) {
                    if (node.get("Key").asText().equals("CustomerName")) {
                        dlTenantName = node.get("Value").asText();
                        System.out.println("tenant name: " + dlTenantName);
                    }
                }
            }
        }
    }

    protected void setToBardDBDataSource(){
        String hostAdd = "BODCPRODVSQL200.prod.lattice.local\\SQL200";
        if (instance.equals("SQL100")) {
            hostAdd = "BODCPRODVSQL100.prod.lattice.local\\SQL100";
        }
        hostAdd = "10.41.1.250:1433";
        setToBardDBDataSource(hostAdd);
    }

    private void setToBardDBDataSource(String hostAdd){
        DataSource bardDBDataSource = new DriverManagerDataSource("jdbc:sqlserver://" + hostAdd + ";databaseName="
                + bardDB, user, pass);
        upgradeJdbcTemlate.setDataSource(bardDBDataSource);
    }

    protected String getActiveModelKey() throws Exception {
        String modelInfo = upgradeJdbcTemlate.queryForObject(
                "select value from KeyValueStore where [Key] = 'ModelInfoDocument'", String.class);
        JsonNode jn = new ObjectMapper().readTree(ModelDecryptor.decrypt(modelInfo)).get("ActiveModelKeys");
        if (jn.size() != 1) {
            System.out.println(dlTenantName + " does not have 1 active models");
            return "";
        }
        return jn.path(0).asText();
    }

    protected String getModelContent(String activeModelKey) throws Exception{
        String encryptedModelContent = upgradeJdbcTemlate.queryForObject(
              "select value from KeyValueStore where [Key] = '" + activeModelKey + "'", String.class);
        return ModelDecryptor.decrypt(encryptedModelContent);
    }



    protected void populateTenantModelInfo() {
        DataSource infoDataSource = new DriverManagerDataSource(tenantModelInfoJDBC, user, pass);
        upgradeJdbcTemlate.setDataSource(infoDataSource);
        upgradeJdbcTemlate.execute("IF NOT EXISTS (SELECT * FROM TenantModel_Info where TenantName = \'" + dlTenantName
                + //
                "\') insert into TenantModel_Info values (\'" + dlTenantName + "\', \'" + modelGuid + "\')");
    }

//    protected String findModelPath(String customer, String uuid) throws Exception {
//        List<String> paths = HdfsUtils.getFilesForDirRecursive(yarnConfiguration, customerBase + "/" + customer
//                + "/models", new HdfsFileFilter() {
//            public boolean accept(FileStatus fileStatus) {
//                if (fileStatus == null) {
//                    return false;
//                }
//                Pattern p = Pattern.compile(".*model.json");
//                Matcher matcher = p.matcher(fileStatus.getPath().getName());
//                return matcher.matches();
//            }
//        });
//        for (String path : paths) {
//            if (path.contains(uuid))
//                return path;
//        }
//        return null;
//    }

}
