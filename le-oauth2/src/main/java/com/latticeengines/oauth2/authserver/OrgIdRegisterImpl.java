package com.latticeengines.oauth2.authserver;

import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import com.latticeengines.camille.exposed.lifecycle.SpaceLifecycleManager;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;

@Component
public class OrgIdRegisterImpl implements OrgIdRegister {

    private static final Logger log = LoggerFactory.getLogger(OrgIdRegisterImpl.class);

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
                log.warn("Space does not exist! tenant=" + tenantId, ex);
            }
            if (spaceInfo == null) {
                return;
            }

            if (isProduction) {
                spaceInfo.properties.sfdcOrgId = orgId;
            } else {
                spaceInfo.properties.sandboxSfdcOrgId = orgId;
            }

            SpaceLifecycleManager.create(customerSpace.getContractId(), customerSpace.getTenantId(),
                    customerSpace.getSpaceId(), spaceInfo);
            log.info("TenantID=" + tenantId + " registered orgId=" + orgId);

        } catch (Exception ex) {
            log.error("Failed to register orgId for tenant=" + tenantId, ex);
        }
    }

}
