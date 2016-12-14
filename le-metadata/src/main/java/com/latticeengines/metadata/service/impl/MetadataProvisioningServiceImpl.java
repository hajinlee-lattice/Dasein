package com.latticeengines.metadata.service.impl;

import java.io.File;
import java.net.URL;
import java.net.URLDecoder;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

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

    private static final Log log = LogFactory.getLog(MetadataProvisioningServiceImpl.class);

    @Autowired
    private TenantService tenantService;

    @Autowired
    private MetadataService mdService;

    @Override
    public void provisionImportTables(CustomerSpace space) {
        try {
            URL url = getClass().getClassLoader().getResource("Tables");
            File tablesDir = new File(URLDecoder.decode(url.getPath(), "UTF-8"));
            File[] files = tablesDir.listFiles();
            for (File file : files) {
                String str = FileUtils.readFileToString(file);
                Table table = JsonUtils.deserialize(str, Table.class);
                Tenant tenant = tenantService.findByTenantId(space.toString());
                table.setTenant(tenant);
                table.setTableType(TableType.IMPORTTABLE);

                DateTime date = new DateTime();
                table.getLastModifiedKey().setLastModifiedTimestamp(date.minusYears(2).getMillis());
                mdService.createTable(space, table);
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
                mdService.deleteTable(customerSpace, table.getName());
            }
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }
}
