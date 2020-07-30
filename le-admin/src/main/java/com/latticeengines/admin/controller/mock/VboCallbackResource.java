package com.latticeengines.admin.controller.mock;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dcp.vbo.VboCallback;
import com.latticeengines.domain.exposed.dcp.vbo.VboStatus;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.TenantEmailNotificationLevel;
import com.latticeengines.security.exposed.service.TenantService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;


@Api(value = "vbomock", description = "Resource for mocking the VBO callback")
@RestController
@RequestMapping("/vbomock")
public class VboCallbackResource {
    private static final Logger log = LoggerFactory.getLogger(VboCallbackResource.class);

    @Inject
    TenantService tenantService;

    @Value("${admin.vbo.callback.usemock}")
    boolean enabled;

    @PostMapping("")
    @ResponseBody
    @ApiOperation("Post a callback")
    public boolean receiveCallback(@RequestBody VboCallback callback) {
        if (!enabled) {
            log.error("Mock callback service is currently disabled!");
            return false;
        }
        try {
            log.info("Received callback");
            log.info(callback.toString());

            if (callback.customerCreation.transactionDetail.status.equals(VboStatus.SUCCESS)) {
                Tenant tenant = tenantService.findBySubscriberNumber(callback.customerCreation.customerDetail.subscriberNumber);
                if (tenant == null)
                    return false;
                tenant.getJobNotificationLevels().put("mock@vbo", TenantEmailNotificationLevel.NONE);
                log.info(JsonUtils.serialize(tenant));
                tenantService.updateTenant(tenant);
            }

            log.info("Exiting callback handler");
            return true;
        } catch (Exception e) {
            log.info("Unexpected error: Failed to save callback");
            return false;
        }
    }

}
