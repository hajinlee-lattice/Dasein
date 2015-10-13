package com.latticeengines.metadata.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.web.client.HttpClientErrorException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;
import com.latticeengines.security.exposed.Constants;

public class MetadataResourceTestNG extends MetadataFunctionalTestNGBase {
    
    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();
    }

    @Test(groups = "functional")
    public void createTable() {
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[]{addMagicAuthHeader}));
        Table table = createTable(null, TABLE3);
        String url = String.format("%s/metadata/customerspaces/%s/tables/%s",
                getRestAPIHostPort(), CUSTOMERSPACE1, table.getName());
        restTemplate.postForLocation(url, table);

        // Create again to ensure that it simply performs an update
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[]{addMagicAuthHeader}));
        restTemplate.postForLocation(url, table);


        Table received = restTemplate.getForObject(url, Table.class, new HashMap<>());
        assertNotNull(received);
        assertEquals(received.getName(), table.getName());
    }


    @Test(groups = "functional")
    public void updateTable() {
        String url = String.format("%s/metadata/customerspaces/%s/tables/%s", //
                getRestAPIHostPort(), CUSTOMERSPACE1, TABLE3);

        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[]{addMagicAuthHeader}));
        Table table = restTemplate.getForObject(url, Table.class, new HashMap<>());
        assertNotNull(table);

        Extract extract = createExtract("NewExtract");
        table.addExtract(extract);

        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[]{addMagicAuthHeader}));
        restTemplate.put(url, table, new HashMap<>());

        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[]{addMagicAuthHeader}));
        Table received = restTemplate.getForObject(url, Table.class, new HashMap<>());
        assertNotNull(received);
        assertEquals(received.getName(), table.getName());
        assertEquals(received.getExtracts().size(), table.getExtracts().size());
    }


    @Test(groups = "functional")
    public void getTable() {
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[]{addMagicAuthHeader}));
        String url = String.format("%s/metadata/customerspaces/%s/tables/%s", //
                getRestAPIHostPort(), CUSTOMERSPACE1, TABLE1);
        Table table = restTemplate.getForObject(url, Table.class, new HashMap<>());
        assertEquals(table.getName(), TABLE1);

        assertEquals(table.getAttributes().size(), 4);
    }

    @Test(groups = "functional", expectedExceptions = { HttpClientErrorException.class })
    public void getTableBadHeader() {
        addMagicAuthHeader.setAuthValue("xyz");
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[]{addMagicAuthHeader}));
        String url = String.format("%s/metadata/customerspaces/%s/tables/%s", //
                getRestAPIHostPort(), CUSTOMERSPACE1, TABLE1);
        restTemplate.getForObject(url, Table.class, new HashMap<>());
    }

    @SuppressWarnings("unchecked")
    @Test(groups = "functional")
    public void getTables() {
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        String url = String.format("%s/metadata/customerspaces/%s/tables", //
                getRestAPIHostPort(), CUSTOMERSPACE2, TABLE2);
        List<String> tables = restTemplate.getForObject(url, List.class);
        assertEquals(tables.size(), 1);
        assertEquals(tables.get(0), TABLE2);
    }
    
    @Test(groups = "functional")
    public void validateMetadata() throws Exception {
        String metadataFile = ClassLoader.getSystemResource(
                "com/latticeengines/metadata/controller/metadata.avsc").getPath();

        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        String url = String.format("%s/metadata/customerspaces/%s/tables/t1/validations", //
                getRestAPIHostPort(), CUSTOMERSPACE2);
        ModelingMetadata metadata = JsonUtils.deserialize(FileUtils.readFileToString(new File(metadataFile)), ModelingMetadata.class);
        SimpleBooleanResponse response = restTemplate.postForObject(url, metadata, SimpleBooleanResponse.class);
        assertTrue(response.isSuccess());
    }

    @Test(groups = "functional")
    public void validateMetadataForInvalidPayload() throws Exception {
        String metadataFile = ClassLoader.getSystemResource(
                "com/latticeengines/metadata/controller/invalidmetadata.avsc").getPath();

        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        String url = String.format("%s/metadata/customerspaces/%s/tables/t1/validations", //
                getRestAPIHostPort(), CUSTOMERSPACE2);
        ModelingMetadata metadata = JsonUtils.deserialize(FileUtils.readFileToString(new File(metadataFile)), ModelingMetadata.class);
        SimpleBooleanResponse response = restTemplate.postForObject(url, metadata, SimpleBooleanResponse.class);
        assertFalse(response.isSuccess());
    }
}
