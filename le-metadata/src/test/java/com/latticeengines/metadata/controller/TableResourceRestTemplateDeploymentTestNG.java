package com.latticeengines.metadata.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.web.client.RestTemplate;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata.AttributeMetadata;
import com.latticeengines.metadata.functionalframework.MetadataDeploymentTestNGBase;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.exposed.MagicAuthenticationHeaderHttpRequestInterceptor;

public class TableResourceRestTemplateDeploymentTestNG extends MetadataDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(TableResourceRestTemplateDeploymentTestNG.class);

    private MagicAuthenticationHeaderHttpRequestInterceptor addMagicAuthHeader = new MagicAuthenticationHeaderHttpRequestInterceptor(
            "");

    private RestTemplate restTemplate = HttpClientUtils.newRestTemplate();

    @Value("${common.test.microservice.url}")
    private String hostPort;

    @Override
    @BeforeClass(groups = "deployment")
    public void setup() {
        super.setup();

        addMagicAuthHeader = new MagicAuthenticationHeaderHttpRequestInterceptor();
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
    }

    @Test(groups = "deployment", dataProvider = "urlTypes")
    public void createTableWithResource(String urlType) {
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        Table table = createTable(null, TABLE2, tableLocation2.append(TABLE2).toString());

        log.info("Creating TABLE2 for " + customerSpace1 + " with url type " + urlType);
        String url = String.format("%s/metadata/customerspaces/%s/%s/%s", getRestAPIHostPort(), customerSpace1, urlType,
                table.getName());
        restTemplate.postForLocation(url, table);
        log.info("Creating TABLE2 for " + customerSpace2 + " with url type " + urlType);
        url = String.format("%s/metadata/customerspaces/%s/%s/%s", getRestAPIHostPort(), customerSpace2, urlType,
                table.getName());
        restTemplate.postForLocation(url, table);

        Table received = restTemplate.getForObject(url, Table.class, new HashMap<>());
        assertNotNull(received);
        assertEquals(received.getName(), table.getName());
        assertEquals(received.getNamespace(), table.getNamespace());
    }

    @Test(groups = "deployment", dataProvider = "urlTypes", enabled = true, dependsOnMethods = {
            "createTableWithResource" })
    public void updateTable(String urlType) {
        String url = String.format("%s/metadata/customerspaces/%s/%s/%s", //
                getRestAPIHostPort(), customerSpace1, urlType, TABLE2);

        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        Table table = restTemplate.getForObject(url, Table.class, new HashMap<>());
        assertNotNull(table);

        table.getExtracts().remove(0);

        log.info("Updating TABLE2 for " + customerSpace1 + " with url type " + urlType);
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        restTemplate.put(url, table, new HashMap<>());

        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        Table received = restTemplate.getForObject(url, Table.class, new HashMap<>());
        assertNotNull(received);
        assertEquals(received.getName(), table.getName());
        assertEquals(received.getExtracts().size(), table.getExtracts().size());
    }

    @Test(groups = "deployment", dataProvider = "urlTypes", enabled = true, dependsOnMethods = { "updateTable" })
    public void getTable(String urlType) {
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        String url = String.format("%s/metadata/customerspaces/%s/%s/%s", //
                getRestAPIHostPort(), customerSpace1, urlType, TABLE1);
        Table table = restTemplate.getForObject(url, Table.class, new HashMap<>());
        assertNotNull(table);
        assertEquals(table.getName(), TABLE1);

        assertEquals(table.getAttributes().size(), 22);
    }

    @Test(groups = "deployment", dataProvider = "urlTypes", dependsOnMethods = { "getTable" })
    public void getTableMetadata(String urlType) {
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        String url = String.format("%s/metadata/customerspaces/%s/%s/%s/metadata", //
                getRestAPIHostPort(), customerSpace1, urlType, TABLE1);
        ModelingMetadata modelingMetadata = restTemplate.getForObject(url, ModelingMetadata.class, new HashMap<>());
        AttributeMetadata attrMetadata = modelingMetadata.getAttributeMetadata().get(3);
        assertNotNull(attrMetadata);
        assertEquals(attrMetadata.getApprovedUsage().get(0), "Model");
        assertEquals(attrMetadata.getDataSource().get(0), "DerivedColumns");
        assertEquals(attrMetadata.getExtensions().get(0).getKey(), "Category");
        assertEquals(attrMetadata.getExtensions().get(0).getValue(), "Firmographics");
        assertEquals(attrMetadata.getExtensions().get(1).getKey(), "DataType");
        assertEquals(attrMetadata.getExtensions().get(1).getValue(), "Int");
        assertEquals(attrMetadata.getStatisticalType(), "ratio");
        assertEquals(attrMetadata.getFundamentalType(), "numeric");
        assertEquals(attrMetadata.getTags().get(0), "External");
    }

    @Test(groups = "deployment", dependsOnMethods = { "getTable" })
    public void getTableColumnsPageable() {
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        String url = String.format("%s/metadata/customerspaces/%s/tables/%s/attributes", //
                getRestAPIHostPort(), customerSpace1, TABLE1);
        Attribute[] attributeArr = restTemplate.getForObject(url, Attribute[].class, new HashMap<>());
        assertNotNull(attributeArr);
        log.info("Column Metadata Count for CustomerSpace-Table: {}-{} : {}", customerSpace1, TABLE1, attributeArr.length);
        
        int pageSize = Math.min(10,  attributeArr.length);
        if (pageSize > 0) {
            log.info("Testing TableColumns with PageSize: {}", pageSize);
            url = String.format("%s/metadata/customerspaces/%s/tables/%s/attributes?page={page}&size={size}&sort_dir={sort_dir}&sort_col={sort_col}", //
                    getRestAPIHostPort(), customerSpace1, TABLE1);
            Map<String, Object> uriVars = new HashMap<>();
            uriVars.put("page", 1);
            uriVars.put("size", pageSize);
            uriVars.put("sort_dir", "ASC");
            uriVars.put("sort_col", "displayName");
            attributeArr = restTemplate.getForObject(url, Attribute[].class, uriVars);
            assertNotNull(attributeArr);
            assertEquals(attributeArr.length, pageSize);
        }
    }

    @Test(groups = "deployment", dependsOnMethods = { "getTable" })
    public void cloneTable() {
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        String url = String.format("%s/metadata/customerspaces/%s/tables/%s/clone", //
                getRestAPIHostPort(), customerSpace1, TABLE1);
        Table clone = restTemplate.postForObject(url, null, Table.class);
        assertNotNull(clone);
        url = String.format("%s/metadata/customerspaces/%s/tables/%s", //
                getRestAPIHostPort(), customerSpace1, TABLE1);
        Table existing = restTemplate.getForObject(url, Table.class);
        assertNotNull(existing);
    }

    @Test(groups = "deployment", dataProvider = "urlTypes", dependsOnMethods = { "getTable" })
    public void getAttributeValidators(String urlType) {
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        String url = String.format("%s/metadata/customerspaces/%s/%s/%s", //
                getRestAPIHostPort(), customerSpace1, urlType, TABLE1);
        Table table = restTemplate.getForObject(url, Table.class, new HashMap<>());
        Attribute attribute = table.getAttributes().get(3);
        assertNotNull(attribute);
        assertEquals(attribute.getValidators().size(), 1);
    }

    @Test(groups = "deployment", dataProvider = "urlTypes", enabled = true, dependsOnMethods = { "getTableMetadata" })
    public void getTableBadHeader(String urlType) {
        addMagicAuthHeader.setAuthValue("xyz");
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        String url = String.format("%s/metadata/customerspaces/%s/%s/%s", //
                getRestAPIHostPort(), customerSpace1, urlType, TABLE1);

        boolean exception = false;
        try {
            restTemplate.getForObject(url, Table.class, new HashMap<>());
        } catch (Exception e) {
            exception = true;
            assertTrue(e.getMessage().contains("401"));
        }
        assertTrue(exception);
    }

    @SuppressWarnings("unchecked")
    @Test(groups = "deployment", dataProvider = "urlTypes", enabled = true, dependsOnMethods = { "getTableBadHeader" })
    public void getTables(String urlType) {
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        String url = String.format("%s/metadata/customerspaces/%s/%s", //
                getRestAPIHostPort(), customerSpace2, urlType);
        List<String> tables = restTemplate.getForObject(url, List.class);
        assertEquals(CollectionUtils.size(tables), 2);
    }

    @Test(groups = "deployment", dependsOnMethods = { "getTables" })
    public void validateMetadata() throws Exception {
        String metadataFile = ClassLoader.getSystemResource("com/latticeengines/metadata/controller/metadata.avsc")
                .getPath();

        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        String url = String.format("%s/metadata/customerspaces/%s/validations", //
                getRestAPIHostPort(), customerSpace2);
        ModelingMetadata metadata = JsonUtils.deserialize(FileUtils.readFileToString(new File(metadataFile), "utf8"),
                ModelingMetadata.class);
        SimpleBooleanResponse response = restTemplate.postForObject(url, metadata, SimpleBooleanResponse.class);
        assertNotNull(response);
        assertTrue(response.isSuccess());
    }

    @Test(groups = "deployment", dependsOnMethods = { "validateMetadata" })
    public void validateMetadataForInvalidPayload() throws Exception {
        String metadataFile = ClassLoader
                .getSystemResource("com/latticeengines/metadata/controller/invalidmetadata.avsc").getPath();

        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        String url = String.format("%s/metadata/customerspaces/%s/validations", //
                getRestAPIHostPort(), customerSpace2);
        ModelingMetadata metadata = JsonUtils.deserialize(FileUtils.readFileToString(new File(metadataFile), "utf8"),
                ModelingMetadata.class);
        SimpleBooleanResponse response = restTemplate.postForObject(url, metadata, SimpleBooleanResponse.class);
        assertFalse(response.isSuccess());
    }

    @Test(groups = "deployment", dependsOnMethods = { "validateMetadataForInvalidPayload" })
    public void resetTables() {
        String url = String.format("%s/metadata/customerspaces/%s/%s/%s", //
                getRestAPIHostPort(), customerSpace1, String.valueOf(getUrlTypes()[0][0]), TABLE2);

        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        Table table = restTemplate.getForObject(url, Table.class, new HashMap<>());
        assertNotNull(table);

        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        String importTableUrl = url.replace(String.valueOf(getUrlTypes()[0][0]), String.valueOf(getUrlTypes()[1][0]));
        Table importTable = restTemplate.getForObject(importTableUrl, Table.class, new HashMap<>());
        assertNotNull(importTable);

        log.info("Resetting TABLE2 for " + customerSpace1);
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        String resetTableUrl = String.format("%s/metadata/customerspaces/%s/%s/%s", //
                getRestAPIHostPort(), customerSpace1, String.valueOf(getUrlTypes()[0][0]), "reset");
        Boolean response = restTemplate.postForObject(resetTableUrl, null, Boolean.class);
        assertTrue(response);

        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        Table received = restTemplate.getForObject(url, Table.class, new HashMap<>());
        assertNull(received);

        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        Table receivedImportTable = restTemplate.getForObject(importTableUrl, Table.class, new HashMap<>());
        assertNotNull(receivedImportTable);
        assertEquals(receivedImportTable.getName(), importTable.getName());
        assertEquals(receivedImportTable.getExtracts().size(), 0);
        assertEquals(receivedImportTable.getAttributes().size(), importTable.getAttributes().size());
        assertNotEquals(receivedImportTable.getLastModifiedKey().getLastModifiedTimestamp(),
                importTable.getLastModifiedKey().getLastModifiedTimestamp());
    }

    @DataProvider(name = "urlTypes")
    public Object[][] getUrlTypes() {
        return new Object[][] { //
                { "tables" }, //
                { "importtables" } //
        };
    }

    private String getRestAPIHostPort() {
        return hostPort.endsWith("/") ? hostPort.substring(0, hostPort.length() - 1) : hostPort;
    }
}
