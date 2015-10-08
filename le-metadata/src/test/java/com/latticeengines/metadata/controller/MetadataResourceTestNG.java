package com.latticeengines.metadata.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.web.client.HttpClientErrorException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
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

        assertEquals(table.getAttributes().size(), 2);
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
}
