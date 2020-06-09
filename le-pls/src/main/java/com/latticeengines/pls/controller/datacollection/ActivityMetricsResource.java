package com.latticeengines.pls.controller.datacollection;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.transaction.ActivityType;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionConfiguration;
import com.latticeengines.domain.exposed.pls.ActivityMetricsWithAction;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics;
import com.latticeengines.pls.service.ActionService;
import com.latticeengines.proxy.exposed.cdl.ActivityMetricsProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "metrics", description = "REST resource for serving data about metrics")
@RestController
@RequestMapping("/datacollection/metrics")
@PreAuthorize("hasRole('View_PLS_CDL_Data')")
public class ActivityMetricsResource {

    private static final Logger log = LoggerFactory.getLogger(ActivityMetricsResource.class);

    private final ActivityMetricsProxy metricsProxy;

    @Inject
    private ActionService actionService;

    @Inject
    public ActivityMetricsResource(ActivityMetricsProxy metricsProxy) {
        this.metricsProxy = metricsProxy;
    }

    @GetMapping("/{type}/active")
    @ApiOperation(value = "Get all the active metrics for specific activity type")
    public List<ActivityMetrics> getActiveActivityMetrics(@PathVariable ActivityType type) {
        return metricsProxy.getActiveActivityMetrics(MultiTenantContext.getCustomerSpace().toString(), type);
    }

    @PostMapping("/{type}")
    @ApiOperation(value = "Save purchase metrics")
    public List<ActivityMetrics> saveActivityMetrics(@PathVariable ActivityType type,
            @RequestBody List<ActivityMetrics> metrics) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        try {
            ActivityMetricsWithAction am = metricsProxy.save(customerSpace, type, metrics);
            registerAction(am.getAction(), MultiTenantContext.getTenant());
            return am.getMetrics();
        } catch (LedpException e) {
            if (LedpCode.LEDP_40032.equals(e.getCode())) {
                throw e;
            } else {
                throw new RuntimeException(e);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void registerAction(Action action, Tenant tenant) {
        if (action != null) {
            action.setTenant(tenant);
            action.setActionInitiator(MultiTenantContext.getEmailAddress());
            log.info(String.format("Registering action %s", action));
            ActionConfiguration actionConfig = action.getActionConfiguration();
            if (actionConfig != null) {
                action.setDescription(actionConfig.serialize());
            }
            actionService.create(action);
        }
    }
}
