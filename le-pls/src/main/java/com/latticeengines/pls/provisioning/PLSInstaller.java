package com.latticeengines.pls.provisioning;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.camille.exposed.config.bootstrap.LatticeComponentInstaller;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.security.Tenant;

public class PLSInstaller extends LatticeComponentInstaller {

    private static final Log LOGGER = LogFactory.getLog(PLSInstaller.class);

    private PLSComponentManager componentManager;

    public PLSInstaller() { super(PLSComponent.componentName); }

    @Override
    public void installCore(CustomerSpace space, String serviceName, int dataVersion, DocumentDirectory configDir) {
        if (!serviceName.equals(PLSComponent.componentName)) { return; }

        // get tenant information
        String tenantId = space.getTenantId();
        String tenantName, emailListInJson;
        List<String> adminEmails = new ArrayList<>();
        try {
            emailListInJson = configDir.get("/AdminEmails").getDocument().getData();
            tenantName = configDir.get("/TenantName").getDocument().getData();
        } catch (NullPointerException e) {
            throw new LedpException(LedpCode.LEDP_18028, "Cannot parse input configuration", e);
        }

        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode aNode = mapper.readTree(emailListInJson);
            if (!aNode.isArray()) {
                throw new IOException("AdminEmails suppose to be a list of strings");
            }
            for (JsonNode node : aNode) {
                adminEmails.add(node.asText());
            }
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_18028, "Cannot parse AdminEmails to a list of valid emails", e);
        }


        Tenant tenant = new Tenant();
        tenant.setId(tenantId);
        tenant.setName(tenantName);

        LOGGER.info(String.format("Provisioning tenant %s", tenantId));

        componentManager.provisionTenant(tenant, adminEmails);
    }

    public void setComponentManager(PLSComponentManager manager) {
        this.componentManager = manager;
    }

}
