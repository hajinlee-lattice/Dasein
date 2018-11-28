package com.latticeengines.pls.controller.datacollection;

import java.util.List;

import javax.inject.Inject;

import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.BusinessCalendarValidation;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.cdl.BusinessCalendar;
import com.latticeengines.pls.service.ActionService;
import com.latticeengines.proxy.exposed.cdl.PeriodProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "periods", description = "REST resource for serving data about periods")
@RestController
@RequestMapping("/datacollection/periods")
@PreAuthorize("hasRole('View_PLS_CDL_Data')")
public class PeriodResource {

    private final PeriodProxy periodProxy;
    private final ActionService actionService;

    @Inject
    public PeriodResource(PeriodProxy periodProxy, ActionService actionService) {
        this.periodProxy = periodProxy;
        this.actionService = actionService;
    }

    @GetMapping(value = "/names", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all period names defined in a tenant")
    public List<String> getPeriodNames() {
        try {
            String customerSpace = MultiTenantContext.getCustomerSpace().toString();
            return periodProxy.getPeriodNames(customerSpace);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @GetMapping(value = "/calendar")
    @ApiOperation(value = "Get business calendar")
    public BusinessCalendar getBusinessCalendar() {
        try {
            String customerSpace = MultiTenantContext.getCustomerSpace().toString();
            return periodProxy.getBusinessCalendar(customerSpace);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @PreAuthorize("hasRole('Edit_PLS_CDL_Data')")
    @PostMapping(value = "/calendar")
    @ApiOperation(value = "Save business calendar")
    public BusinessCalendar saveBusinessCalendar(@RequestBody BusinessCalendar businessCalendar) {
        try {
            String customerSpace = MultiTenantContext.getCustomerSpace().toString();
            String email = MultiTenantContext.getEmailAddress();
            businessCalendar.setUpdatedBy(email);
            businessCalendar = periodProxy.saveBusinessCalendar(customerSpace, businessCalendar);
            createAction(MultiTenantContext.getTenant());
            return businessCalendar;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @PreAuthorize("hasRole('Edit_PLS_CDL_Data')")
    @DeleteMapping(value = "/calendar")
    @ApiOperation(value = "Delete business calendar")
    public void deleteBusinessCalendar() {
        try {
            String customerSpace = MultiTenantContext.getCustomerSpace().toString();
            periodProxy.deleteBusinessCalendar(customerSpace);
            createAction(MultiTenantContext.getTenant());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @PreAuthorize("hasRole('Edit_PLS_CDL_Data')")
    @PostMapping(value = "/calendar/validate")
    @ApiOperation(value = "Validate business calendar")
    public BusinessCalendarValidation validateBusinessCalendar(@RequestBody BusinessCalendar businessCalendar) {
        try {
            String customerSpace = MultiTenantContext.getCustomerSpace().toString();
            String note = periodProxy.validateBusinessCalendar(customerSpace, businessCalendar);
            return new BusinessCalendarValidation(note);
        } catch (LedpException e) {
            if (LedpCode.LEDP_40015.equals(e.getCode())) {
                throw e;
            } else {
                throw new RuntimeException(e);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void createAction(Tenant tenant) {
        Action action = new Action();
        action.setType(ActionType.BUSINESS_CALENDAR_CHANGE);
        action.setTenant(tenant);
        action.setActionInitiator(MultiTenantContext.getEmailAddress());
        actionService.create(action);
    }
}
