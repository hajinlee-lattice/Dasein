package com.latticeengines.upgrade.zk;

import java.util.Arrays;
import java.util.Collection;

import javax.annotation.PostConstruct;

import org.apache.zookeeper.ZooDefs;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.CamilleTransaction;
import com.latticeengines.camille.exposed.config.bootstrap.BootstrapStateUtil;
import com.latticeengines.camille.exposed.lifecycle.TenantLifecycleManager;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.admin.CRMTopology;
import com.latticeengines.domain.exposed.admin.SpaceConfiguration;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.CrmCredential;

@Component
public class ZooKeeperManager {

    private static final String DESCRIPTION = "A LPA tenant upgraded from 1.3.4 or 1.4.0";
    private static final String SPACE_CONFIG = "/SpaceConfiguration";
    private static final Collection<String> COMPONENTS = Arrays.asList(
            "PLS", "VisiDBDL", "VisiDBTemplate", "DLTemplate", "BardJams"
    );
    private static final BootstrapState MIGRATED = BootstrapState.constructMigratedState();

    private static String podId;
    private static Camille camille;
    private static CrmCredential marketoCredential;
    private static CrmCredential eloquaCredential;
    private static CrmCredential sfdcCredential;
    private static CrmCredential sfdcsandboxCredential;

    @Autowired
    private BatonService batonService;

    @PostConstruct
    private void readCamilleEnvironment() {
        podId = CamilleEnvironment.getPodId();
        camille = CamilleEnvironment.getCamille();
    }

    @PostConstruct
    private void setCrmCredentials() {
        String password = "";
        try {
            password = CipherUtils.encrypt("password");
        } catch (Exception e) {
            //ignore
        }

        marketoCredential = new CrmCredential();
        marketoCredential.setUserName(" ");
        marketoCredential.setPassword(password);
        marketoCredential.setUrl(" ");

        eloquaCredential = new CrmCredential();
        eloquaCredential.setUserName(" ");
        eloquaCredential.setPassword(password);
        eloquaCredential.setCompany(" ");

        sfdcCredential = new CrmCredential();
        sfdcCredential.setUserName(" ");
        sfdcCredential.setPassword(password);
        sfdcCredential.setSecurityToken("security-token");

        sfdcsandboxCredential = new CrmCredential();
        sfdcsandboxCredential.setUserName(" ");
        sfdcsandboxCredential.setPassword(password);
        sfdcsandboxCredential.setSecurityToken("security-token");
    }

    public void registerTenantIfNotExist(String tenantId) {
        CustomerSpace space = CustomerSpace.parse(tenantId);
        try {
            TenantLifecycleManager.exists(space.getContractId(), space.getTenantId());
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_24004,
                    String.format("Failed to check if tenant %s alread exists.", space.getContractId()), e);
        }
        CustomerSpaceProperties properties = new CustomerSpaceProperties(space.getTenantId(), DESCRIPTION, null, null);
        CustomerSpaceInfo spaceInfo = new CustomerSpaceInfo(properties, "");
        batonService.createTenant(tenantId, tenantId, CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID, spaceInfo);
    }

    public void uploadSpaceConfiguration(String tenantId, SpaceConfiguration spaceConfig) {
        CustomerSpace space = CustomerSpace.parse(tenantId);
        Path spaceConfigPath = PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(),
                space.getContractId(), space.getTenantId(), space.getSpaceId())
                .append(new Path(SPACE_CONFIG));
        batonService.loadDirectory(spaceConfig.toDocumentDirectory(), spaceConfigPath);
    }

    public void uploadCrmCredentials(String tenantId, CRMTopology topology) {
        if (topology.equals(CRMTopology.MARKETO)) {
            writeAsCredential("marketo", tenantId, true, marketoCredential);
        }

        if (topology.equals(CRMTopology.ELOQUA)) {
            writeAsCredential("eloqua", tenantId, true, eloquaCredential);
        }
    }

    public void setBootstrapStateToMigrate(String tenantId) {
        CustomerSpace space = CustomerSpace.parse(tenantId);
        Path servicesPath = PathBuilder.buildCustomerSpaceServicesPath(podId, space);
        for (String component: COMPONENTS) {
            CamilleTransaction transaction = new CamilleTransaction();
            Path serviceDirectoryPath = servicesPath.append(component);
            try {
                if (!camille.exists(serviceDirectoryPath)) {
                    camille.upsert(serviceDirectoryPath.parent(), ZooDefs.Ids.OPEN_ACL_UNSAFE);
                    camille.upsert(serviceDirectoryPath, ZooDefs.Ids.OPEN_ACL_UNSAFE);
                    BootstrapStateUtil.initializeState(serviceDirectoryPath, transaction, MIGRATED);
                }
                transaction.commit();
            } catch (Exception e) {
                throw new LedpException(LedpCode.LEDP_24004, "Failed to set BootstrapState for " + component, e);
            }
        }
    }

    private void writeAsCredential(String crmType, String tenantId, Boolean isProduction, CrmCredential crmCredential) {
        CustomerSpace customerSpace = CustomerSpace.parse(tenantId);
        Camille camille = CamilleEnvironment.getCamille();
        Path docPath = PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(), customerSpace.getContractId(),
                customerSpace.getTenantId(), customerSpace.getSpaceId());
        docPath = addExtraPath(crmType, docPath, isProduction);
        Document doc = new Document(JsonUtils.serialize(crmCredential));
        try {
            camille.upsert(docPath, doc, ZooDefs.Ids.OPEN_ACL_UNSAFE, true);
        } catch (Exception e) {
            throw new RuntimeException(String.format(
                    "Error inserting credential znodes for topology %s, customer %s",
                    crmType, customerSpace.getTenantId()));
        }
    }

    private Path addExtraPath(String crmType, Path docPath, Boolean isProduction) {
        docPath = docPath.append(crmType);
        if (crmType.equalsIgnoreCase("sfdc")) {
            docPath = docPath.append(isProduction ? "Production" : "Sandbox");
        }
        return docPath;
    }
}
