package com.latticeengines.metadata.provisioning;

import java.io.File;
import java.net.URL;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.service.MetadataService;
import com.latticeengines.security.exposed.service.TenantService;

@Component
public class MetadataComponentManager {

    private static final Log LOGGER = LogFactory.getLog(MetadataComponentManager.class);

    @Autowired
    private TenantService tenantService;

    @Autowired
    private MetadataService mdService;

    public void provisionTenant(CustomerSpace space, DocumentDirectory configDir) {
        // get tenant information
        String camilleTenantId = space.getTenantId();
        LOGGER.info(String.format("Provisioning tenant %s", space.toString()));

        try {
            URL url = getClass().getClassLoader().getResource("Tables");
            File tablesDir = new File(url.getFile());
            File[] files = tablesDir.listFiles();

            for (File file : files) {
                String str = FileUtils.readFileToString(file);
                Table table = JsonUtils.deserialize(str, Table.class);
                Tenant tenant = tenantService.findByTenantName(camilleTenantId);
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

}
