package com.latticeengines.upgrade.model.service.impl;

import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.upgrade.model.decrypt.ModelDecryptor;
import com.latticeengines.upgrade.model.service.ModelUpgradeService;
import com.latticeengines.upgrade.yarn.YarnManager;

@Component("modelUpgrade")
abstract public class ModelUpgradeServiceImpl implements ModelUpgradeService {

    @Autowired
    protected DataSource dataSourceUpgrade;

    @Autowired
    protected JdbcTemplate upgradeJdbcTemlate;

    @Autowired
    protected YarnManager yarnManager;

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
        //hostAdd = "10.41.1.250:1433";
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
            for(int i = 0; i < jn.size(); i++){
                System.out.println(jn.get(i).asText());
            }
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
                + "\') insert into TenantModel_Info values (\'" + dlTenantName + "\', \'" + modelGuid + "\')");
    }

    private void copyCustomerToTupleId(String customer) throws Exception {
        System.out.print(String.format("Deleting destination folder %s ... ", CustomerSpace.parse(customer).toString()));
        yarnManager.deleteTupleIdCustomerRoot(customer);
        System.out.println("OK");

        System.out.print("Copying hdfs files to the destination folder ... ");
        yarnManager.copyCustomerFromSingularToTupleId(customer);
        System.out.println("OK");
    }

    private void copyCustomerModelToTupleId(String customer, String modelGuid) throws Exception {
        System.out.print(String.format("Create customer folder %s, if not exists ... ", CustomerSpace.parse(customer).toString()));
        yarnManager.createTupleIdCustomerRootIfNotExist(customer);
        System.out.println("OK");

        System.out.print("Copying model files to the destination folder ... ");
        yarnManager.copyModelFromSingularToTupleId(customer, modelGuid);
        System.out.println("OK");
    }

    private void copyCustomerDataToTupleId(String customer) throws Exception {
        System.out.print(String.format("Create customer folder %s, if not exists ... ", CustomerSpace.parse(customer).toString()));
        yarnManager.createTupleIdCustomerRootIfNotExist(customer);
        System.out.println("OK");

        System.out.print("Copying data files to the destination folder ... ");
        yarnManager.copyDataFromSingularToTupleId(customer);
        System.out.println("OK");
    }

    @Override
    public void execute(String command, Map<String, Object> parameters) throws Exception {
        String customer = (String) parameters.get("customer");
        String model = (String) parameters.get("model");

        switch (command) {
            case "cp_customer":
                copyCustomerToTupleId(customer);
                break;
            case "cp_model":
                copyCustomerModelToTupleId(customer, model);
                break;
            case "cp_data":
                copyCustomerDataToTupleId(customer);
                break;
            default:
                // handled by version specific upgrader
                break;
        }

    };
}
