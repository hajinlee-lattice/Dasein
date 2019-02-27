package com.latticeengines.metadata.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.List;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StreamUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.service.MetadataProvisioningService;
import com.latticeengines.metadata.service.MetadataService;
import com.latticeengines.security.exposed.service.TenantService;

@Component("metadataProvisioningService")
public class MetadataProvisioningServiceImpl implements MetadataProvisioningService {

    private static final Logger log = LoggerFactory.getLogger(MetadataProvisioningServiceImpl.class);

    @Autowired
    private TenantService tenantService;

    @Autowired
    private MetadataService mdService;

    @Override
    public void provisionImportTables(CustomerSpace space) {

        try {
            ClassLoader classLoader = getClass().getClassLoader();
            InputStream tableRegistryStream = classLoader.getResourceAsStream("Tables/tables.json");

            String fileContents = StreamUtils.copyToString(tableRegistryStream, Charset.defaultCharset());
            ObjectMapper mapper = new ObjectMapper();
            JsonNode json;
            try {
                json = mapper.readValue(fileContents, JsonNode.class);
            } catch (IOException e) {
                // ignore
                log.error("Error loading import tables.", e);
                return;
            }

            JsonNode sourceTypes = json.get("source_types");
            if (!sourceTypes.isArray()) {
                throw new RuntimeException("source_types element must be an array.");
            }

            for (JsonNode sourceType : sourceTypes) {
                String s = sourceType.get("name").textValue();

                JsonNode tables = sourceType.get("tables");

                if (!tables.isArray()) {
                    throw new RuntimeException("tables element must be an array.");
                }

                for (JsonNode jsonTable : tables) {
                    String path = String.format("Tables/%s/%s.json", s, jsonTable.get("name").textValue());
                    InputStream is = classLoader.getResourceAsStream(path);
                    String tableContents = StreamUtils.copyToString(is, Charset.defaultCharset());
                    Table table = JsonUtils.deserialize(tableContents, Table.class);
                    Tenant tenant = tenantService.findByTenantId(space.toString());
                    table.setTenant(tenant);
                    table.setTableType(TableType.IMPORTTABLE);

                    DateTime date = new DateTime();
                    table.getLastModifiedKey().setLastModifiedTimestamp(date.minusYears(2).getMillis());
                    mdService.createTable(space, table);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void removeImportTables(CustomerSpace customerSpace) {
        try {
            List<Table> tables = mdService.getTables(customerSpace);
            for (Table table : tables) {
                mdService.deleteImportTableAndCleanup(customerSpace, table.getName());
            }
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }
}
