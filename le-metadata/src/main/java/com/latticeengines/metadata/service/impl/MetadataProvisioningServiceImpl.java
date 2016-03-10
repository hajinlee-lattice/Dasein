package com.latticeengines.metadata.service.impl;

import java.io.File;
import java.net.URL;
import java.net.URLDecoder;

import org.apache.commons.io.FileUtils;
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
}
