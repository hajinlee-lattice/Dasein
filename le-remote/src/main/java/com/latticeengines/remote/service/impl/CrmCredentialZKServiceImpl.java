package com.latticeengines.remote.service.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.ZooDefs;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.lifecycle.SpaceLifecycleManager;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.CrmConstants;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.domain.exposed.source.SourceCredentialType;
import com.latticeengines.remote.exposed.service.CrmCredentialZKService;

@Component("crmCredentialZKService")
public class CrmCredentialZKServiceImpl implements CrmCredentialZKService {

    private static final Log log = LogFactory.getLog(CrmCredentialZKServiceImpl.class);

    @Override
    public void writeToZooKeeper(String crmType, String tenantId, Boolean isProduction, CrmCredential crmCredential,
            boolean isWriteCustomerSpace) {
        try {
            if (isWriteCustomerSpace) {
                writeAsCustomerSpace(tenantId, isProduction, crmCredential);
            }

            crmCredential.setPassword(CipherUtils.encrypt(crmCredential.getPassword()));
            writeAsCredential(crmType, tenantId, isProduction, crmCredential);

        } catch (Exception ex) {
            throw new LedpException(LedpCode.LEDP_18030, ex);
        }
    }

    private void writeAsCredential(String crmType, String tenantId, Boolean isProduction, CrmCredential crmCredential)
            throws Exception {

        CustomerSpace customerSpace = CustomerSpace.parse(tenantId);

        Camille camille = CamilleEnvironment.getCamille();
        Path docPath = PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(), customerSpace.getContractId(),
                customerSpace.getTenantId(), customerSpace.getSpaceId());
        docPath = addExtraPath(crmType, docPath, isProduction);
        Document doc = new Document(JsonUtils.serialize(crmCredential));
        camille.upsert(docPath, doc, ZooDefs.Ids.OPEN_ACL_UNSAFE, true);
    }

    private void writeAsCustomerSpace(String tenantId, Boolean isProduction, CrmCredential crmCredential)
            throws Exception {
        CustomerSpaceInfo spaceInfo = null;
        CustomerSpace customerSpace = CustomerSpace.parse(tenantId);
        try {
            spaceInfo = SpaceLifecycleManager.getInfo(customerSpace.getContractId(), customerSpace.getTenantId(),
                    customerSpace.getSpaceId());
        } catch (Exception ex) {
            log.warn("Space does not exist!");
        }
        if (spaceInfo == null) {
            spaceInfo = new CustomerSpaceInfo(new CustomerSpaceProperties(), "");
        } else if (spaceInfo.properties == null) {
            spaceInfo.properties = new CustomerSpaceProperties();
        }

        if (Boolean.FALSE.equals(isProduction)) {
            spaceInfo.properties.sandboxSfdcOrgId = crmCredential.getOrgId();
        } else {
            spaceInfo.properties.sfdcOrgId = crmCredential.getOrgId();
        }
        SpaceLifecycleManager.create(customerSpace.getContractId(), customerSpace.getTenantId(),
                customerSpace.getSpaceId(), spaceInfo);
    }

    @Override
    public CrmCredential getCredential(String crmType, String tenantId, Boolean isProduction) {

        try {

            CustomerSpace customerSpace = CustomerSpace.parse(tenantId);

            Path docPath = PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(),
                    customerSpace.getContractId(), customerSpace.getTenantId(), customerSpace.getSpaceId());
            docPath = addExtraPath(crmType, docPath, isProduction);

            Camille camille = CamilleEnvironment.getCamille();
            Document doc = camille.get(docPath);
            CrmCredential crmCredential = JsonUtils.deserialize(doc.getData(), CrmCredential.class);
            crmCredential.setPassword(CipherUtils.decrypt(crmCredential.getPassword()));

            return crmCredential;

        } catch (Exception ex) {
            throw new LedpException(LedpCode.LEDP_18031, ex);
        }
    }

    @Override
    public void removeCredentials(String crmType, String tenantId, Boolean isProduction) {
        try {
            CustomerSpace customerSpace = CustomerSpace.parse(tenantId);

            Path docPath = PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(),
                    customerSpace.getContractId(), customerSpace.getTenantId(), customerSpace.getSpaceId());
            docPath = addExtraPath(crmType, docPath, isProduction);

            Camille camille = CamilleEnvironment.getCamille();
            if (camille.exists(docPath))
                camille.delete(docPath);
            log.info(String.format("Removing %s.%s credentials from tenant %s.", crmType,
                    isProduction ? SourceCredentialType.PRODUCTION.getName() : SourceCredentialType.SANDBOX.getName(),
                    tenantId));
        } catch (Exception ex) {
            throw new LedpException(LedpCode.LEDP_18031, ex);
        }
    }

    private Path addExtraPath(String crmType, Path docPath, Boolean isProduction) {
        docPath = docPath.append(crmType);
        if (crmType.equalsIgnoreCase(CrmConstants.CRM_SFDC)) {
            docPath = docPath.append(isProduction ? SourceCredentialType.PRODUCTION.getName() : SourceCredentialType.SANDBOX
                    .getName());
        }
        return docPath;
    }
}
