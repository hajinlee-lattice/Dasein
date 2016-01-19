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

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata.AttributeMetadata;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;
import com.latticeengines.security.exposed.Constants;

public class TableResourceTestNG extends MetadataFunctionalTestNGBase {

    private static final Logger log = Logger.getLogger(TableResourceTestNG.class);

    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();
    }

    @Test(groups = "functional", dataProvider = "urlTypes")
    public void createTableWithResource(String urlType) {
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        Table table = createTable(null, TABLE2, TABLE_LOCATION2);

        log.info("Creating TABLE2 for " + CUSTOMERSPACE1 + " with url type " + urlType);
        String url = String.format("%s/metadata/customerspaces/%s/%s/%s", getRestAPIHostPort(), CUSTOMERSPACE1,
                urlType, table.getName());
        restTemplate.postForLocation(url, table);
        log.info("Creating TABLE2 for " + CUSTOMERSPACE2 + " with url type " + urlType);
        url = String.format("%s/metadata/customerspaces/%s/%s/%s", getRestAPIHostPort(), CUSTOMERSPACE2, urlType,
                table.getName());
        restTemplate.postForLocation(url, table);

        Table received = restTemplate.getForObject(url, Table.class, new HashMap<>());
        assertNotNull(received);
        assertEquals(received.getName(), table.getName());
    }

    @Test(groups = "functional", dataProvider = "urlTypes", enabled = true, dependsOnMethods = { "createTableWithResource" })
    public void updateTable(String urlType) {
        String url = String.format("%s/metadata/customerspaces/%s/%s/%s", //
                getRestAPIHostPort(), CUSTOMERSPACE1, urlType, TABLE2);

        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        Table table = restTemplate.getForObject(url, Table.class, new HashMap<>());
        assertNotNull(table);

        table.getExtracts().remove(0);

        log.info("Updating TABLE2 for " + CUSTOMERSPACE1 + " with url type " + urlType);
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

    @Test(groups = "functional", dataProvider = "urlTypes", enabled = true, dependsOnMethods = { "updateTable" })
    public void getTable(String urlType) {
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        String url = String.format("%s/metadata/customerspaces/%s/%s/%s", //
                getRestAPIHostPort(), CUSTOMERSPACE1, urlType, TABLE1);
        Table table = restTemplate.getForObject(url, Table.class, new HashMap<>());
        assertEquals(table.getName(), TABLE1);

        assertEquals(table.getAttributes().size(), 22);
    }

    @Test(groups = "functional", dataProvider = "urlTypes", enabled = true, dependsOnMethods = { "getTable" })
    public void getTableMetadata(String urlType) {
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        String url = String.format("%s/metadata/customerspaces/%s/%s/%s/metadata", //
                getRestAPIHostPort(), CUSTOMERSPACE1, urlType, TABLE1);
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

    @Test(groups = "functional", dataProvider = "urlTypes", enabled = true, dependsOnMethods = { "getTableMetadata" })
    public void getTableBadHeader(String urlType) {
        addMagicAuthHeader.setAuthValue("xyz");
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        String url = String.format("%s/metadata/customerspaces/%s/%s/%s", //
                getRestAPIHostPort(), CUSTOMERSPACE1, urlType, TABLE1);

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
    @Test(groups = "functional", dataProvider = "urlTypes", enabled = true, dependsOnMethods = { "getTableBadHeader" })
    public void getTables(String urlType) {
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        String url = String.format("%s/metadata/customerspaces/%s/%s", //
                getRestAPIHostPort(), CUSTOMERSPACE2, urlType);
        List<String> tables = restTemplate.getForObject(url, List.class);
        assertEquals(tables.size(), 2);
    }

    @Test(groups = "functional", enabled = true, dependsOnMethods = { "getTables" })
    public void validateMetadata() throws Exception {
        String metadataFile = ClassLoader.getSystemResource("com/latticeengines/metadata/controller/metadata.avsc")
                .getPath();

        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        String url = String.format("%s/metadata/customerspaces/%s/validations", //
                getRestAPIHostPort(), CUSTOMERSPACE2);
        ModelingMetadata metadata = JsonUtils.deserialize(FileUtils.readFileToString(new File(metadataFile)),
                ModelingMetadata.class);
        SimpleBooleanResponse response = restTemplate.postForObject(url, metadata, SimpleBooleanResponse.class);
        assertTrue(response.isSuccess());
    }

    @Test(groups = "functional", enabled = true, dependsOnMethods = { "validateMetadata" })
    public void validateMetadataForInvalidPayload() throws Exception {
        String metadataFile = ClassLoader.getSystemResource(
                "com/latticeengines/metadata/controller/invalidmetadata.avsc").getPath();

        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        String url = String.format("%s/metadata/customerspaces/%s/validations", //
                getRestAPIHostPort(), CUSTOMERSPACE2);
        ModelingMetadata metadata = JsonUtils.deserialize(FileUtils.readFileToString(new File(metadataFile)),
                ModelingMetadata.class);
        SimpleBooleanResponse response = restTemplate.postForObject(url, metadata, SimpleBooleanResponse.class);
        assertFalse(response.isSuccess());
    }

    @Test(groups = "functional", enabled = true, dependsOnMethods = { "validateMetadataForInvalidPayload" })
    public void resetTables() {
        String url = String.format("%s/metadata/customerspaces/%s/%s/%s", //
                getRestAPIHostPort(), CUSTOMERSPACE1, String.valueOf(getUrlTypes()[0][0]), TABLE2);

        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        Table table = restTemplate.getForObject(url, Table.class, new HashMap<>());
        assertNotNull(table);

        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        String importTableUrl = url.replace(String.valueOf(getUrlTypes()[0][0]), String.valueOf(getUrlTypes()[1][0]));
        Table importTable = restTemplate.getForObject(importTableUrl, Table.class, new HashMap<>());
        assertNotNull(importTable);

        log.info("Resetting TABLE2 for " + CUSTOMERSPACE1);
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        String resetTableUrl = String.format("%s/metadata/customerspaces/%s/%s/%s", //
                getRestAPIHostPort(), CUSTOMERSPACE1, String.valueOf(getUrlTypes()[0][0]), "reset");
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
        assertNotEquals(receivedImportTable.getLastModifiedKey().getLastModifiedTimestamp(), importTable
                .getLastModifiedKey().getLastModifiedTimestamp());
    }

    @DataProvider(name = "urlTypes")
    public Object[][] getUrlTypes() {
        return new Object[][] { { "tables" }, //
                { "importtables" } //
        };
    }
}
