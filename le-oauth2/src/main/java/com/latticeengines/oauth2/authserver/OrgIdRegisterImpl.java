package com.latticeengines.oauth2.authserver;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.camille.exposed.lifecycle.SpaceLifecycleManager;
import com.latticeengines.camille.exposed.lifecycle.TenantLifecycleManager;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;

@Component
public class OrgIdRegisterImpl implements OrgIdRegister {

    private static final Log log = LogFactory.getLog(OrgIdRegisterImpl.class);

    @Autowired
    private BatonService batonService;

    @Override
    public void registerOrgId(String tenantId) {

        try {
            ServletRequestAttributes requestAttributes = (ServletRequestAttributes) RequestContextHolder
                    .getRequestAttributes();
            HttpServletRequest request = requestAttributes.getRequest();
            String orgId = request.getParameter("orgId");
            if (orgId == null) {
                return;
            }

            boolean isProduction = true;
            String isProductionParameter = request.getParameter("isProduction");
            if (isProductionParameter != null && "false".equalsIgnoreCase(isProductionParameter)) {
                isProduction = false;
            }

            CustomerSpaceInfo spaceInfo = null;
            CustomerSpace customerSpace = CustomerSpace.parse(tenantId);
            try {
                spaceInfo = SpaceLifecycleManager.getInfo(customerSpace.getContractId(), customerSpace.getTenantId(),
                        customerSpace.getSpaceId());
            } catch (Exception ex) {
                log.warn("Space does not exist! tenant=" + tenantId);
            }
            if (spaceInfo == null) {
                spaceInfo = new CustomerSpaceInfo(new CustomerSpaceProperties(), "");
                setProperties(tenantId, spaceInfo);
            } else if (spaceInfo.properties == null) {
                spaceInfo.properties = new CustomerSpaceProperties();
                setProperties(tenantId, spaceInfo);
            }

            if (isProduction) {
                spaceInfo.properties.sfdcOrgId = orgId;
            } else {
                spaceInfo.properties.sandboxSfdcOrgId = orgId;
            }

            if (!TenantLifecycleManager.exists(customerSpace.getContractId(), tenantId)) {
                batonService.createTenant(customerSpace.getContractId(), tenantId, customerSpace.getSpaceId(),
                        spaceInfo);
            } else {
                SpaceLifecycleManager.create(customerSpace.getContractId(), customerSpace.getTenantId(),
                        customerSpace.getSpaceId(), spaceInfo);
            }
            log.info("TenantID=" + tenantId + " registered orgId=" + orgId);

        } catch (Exception ex) {
            log.error("Failed to register orgId for tenant=" + tenantId, ex);
        }
    }

    private void setProperties(String tenantId, CustomerSpaceInfo spaceInfo) {
        spaceInfo.properties.displayName = tenantId;
        spaceInfo.properties.description = tenantId;
    }

    public void setBatonService(BatonService batonService) {
        this.batonService = batonService;
    }

}
