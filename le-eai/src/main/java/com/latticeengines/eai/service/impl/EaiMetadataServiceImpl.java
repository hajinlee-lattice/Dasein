package com.latticeengines.eai.service.impl;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpRequest;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.http.client.support.HttpRequestWrapper;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.LastModifiedKey;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.eai.service.EaiMetadataService;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.exposed.service.TenantService;

@Component("eaiMetadataService")
public class EaiMetadataServiceImpl implements EaiMetadataService {

    private static final Log log = LogFactory.getLog(EaiMetadataServiceImpl.class);

    @Autowired
    private TenantService tenantService;

    @Autowired
    private RestTemplate restTemplate;

    @Value("${eai.metadata.url}")
    private String metadataUrl;

    @PostConstruct
    public void init() {
        restTemplate.setInterceptors(Arrays.<ClientHttpRequestInterceptor> asList( //
                new ClientHttpRequestInterceptor[] { new AuthorizationHeaderHttpRequestInterceptor() }));
    }

    @Override
    public void registerTables(List<Table> tablesMetadataFromImport, ImportContext importContext) {
        String customer = importContext.getProperty(ImportProperty.CUSTOMER, String.class);
        String customerSpace = CustomerSpace.parse(customer).toString();

        @SuppressWarnings("unchecked")
        Map<String, String> targetPaths = importContext.getProperty(ImportProperty.EXTRACT_PATH, Map.class);

        for (Table table : tablesMetadataFromImport) {
            addTenantToTable(table, customerSpace);
            addExtractToTable(table, targetPaths.get(table.getName()));
        }
        updateTables(customerSpace, tablesMetadataFromImport);
    }

    @Override
    public List<Table> getTables(String customerSpace) {
        Map<String, String> uriVariables = new HashMap<>();
        uriVariables.put("customerSpace", customerSpace);
        String[] tableNames = restTemplate.getForObject(metadataUrl + "customerspaces/{customerSpace}/tables",
                String[].class, uriVariables);
        List<Table> tables = new ArrayList<>();
        for (String tableName : tableNames) {
            uriVariables.put("tableName", tableName);
            Table table = getTable(customerSpace, tableName);
            if (table != null) {
                tables.add(table);
            }
        }
        return tables;
    }

    @Override
    public Table getTable(String customerSpace, String tableName) {
        Map<String, String> uriVariables = new HashMap<>();
        uriVariables.put("customerSpace", customerSpace);
        uriVariables.put("tableName", tableName);
        Table newTable = restTemplate.getForObject(metadataUrl + "customerspaces/{customerSpace}/tables/{tableName}",
                Table.class, uriVariables);
        return newTable;
    }

    @Override
    public void createTables(String customerSpace, List<Table> tables) {
        for (Table table : tables) {
            createTable(customerSpace, table);
        }
    }

    @Override
    public void createTable(String customerSpace, Table table) {
        Map<String, String> uriVariables = new HashMap<>();
        uriVariables.put("customerSpace", customerSpace);
        uriVariables.put("tableName", table.getName());
        restTemplate.postForObject(metadataUrl + "customerspaces/{customerSpace}/tables/{tableName}", table,
                String.class, uriVariables);
    }

    @Override
    public void updateTables(String customerSpace, List<Table> tables) {
        Map<String, String> uriVariables = new HashMap<>();
        uriVariables.put("customerSpace", customerSpace);

        for (Table table : tables) {
            uriVariables.put("tableName", table.getName());
            restTemplate.put(
                    String.format("%s/customerspaces/%s/tables/%s", metadataUrl, customerSpace, table.getName()), //
                    table);
        }
    }

    @Override
    public LastModifiedKey getLastModifiedKey(String customerSpace, Table table) {
        Map<String, String> uriVariables = new HashMap<>();
        uriVariables.put("customerSpace", customerSpace);
        uriVariables.put("tableName", table.getName());
        Table newTable = restTemplate.getForObject(metadataUrl + "customerspaces/{customerSpace}/tables/{tableName}",
                Table.class, uriVariables);

        if (newTable != null) {
            return newTable.getLastModifiedKey();
        }
        return null;
    }

    private void addTenantToTable(Table table, String customerSpace) {
        Tenant tenant = getTenant(customerSpace);
        table.setTenant(tenant);

        List<Attribute> attributes = table.getAttributes();
        for (Attribute attribute : attributes) {
            log.info("Attribute " + attribute.getDisplayName() + " : " + attribute.getPhysicalDataType());
        }

    }

    @VisibleForTesting
    void addExtractToTable(Table table, String path) {
        Extract e = new Extract();
        e.setName(StringUtils.substringAfterLast(path, "/"));
        e.setPath(PathUtils.stripoutProtocal(path));
        String dateTime = StringUtils.substringBetween(path, "/Extracts/", "/");
        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
        try {
            e.setExtractionTimestamp(f.parse(dateTime).getTime());
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        table.addExtract(e);
    }

    private Tenant getTenant(String customerSpace) {
        return tenantService.findByTenantId(customerSpace);
    }

    public class AuthorizationHeaderHttpRequestInterceptor implements ClientHttpRequestInterceptor {

        @Override
        public ClientHttpResponse intercept(HttpRequest request, byte[] body, ClientHttpRequestExecution execution)
                throws IOException {
            HttpRequestWrapper requestWrapper = new HttpRequestWrapper(request);
            requestWrapper.getHeaders().add(Constants.INTERNAL_SERVICE_HEADERNAME,
                    Constants.INTERNAL_SERVICE_HEADERVALUE);
            return execution.execute(requestWrapper, body);
        }
    }

    @Override
    public void setLastModifiedTimeStamp(List<Table> tableMetadata, ImportContext importContext) {
        @SuppressWarnings("unchecked")
        Map<String, Long> map = importContext.getProperty(ImportProperty.LAST_MODIFIED_DATE, Map.class);
        for (Table table : tableMetadata) {
            LastModifiedKey lmk = table.getLastModifiedKey();
            Long lastModifiedDateValue = map.get(table.getName());
            lmk.setLastModifiedTimestamp(lastModifiedDateValue);
        }
    }
}
