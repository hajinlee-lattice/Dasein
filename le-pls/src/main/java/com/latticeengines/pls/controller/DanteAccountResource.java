package com.latticeengines.pls.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dante.DanteAccount;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.proxy.exposed.dante.DanteAccountProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

/**
 * Retrieves Accounts for Talking Point UI from Dante database. Starting from
 * M16 we should not use this. Objectapi should be used instead
 */
@Deprecated
@Api(value = "dante/accounts", description = "REST resource for Dante Accounts")
@RestController
@RequestMapping("/dante/accounts")
@PreAuthorize("hasRole('View_PLS_Plays')")
public class DanteAccountResource {

    @Autowired
    private DanteAccountProxy danteAccountProxy;

    @RequestMapping(value = "/{count}", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "get a Dante Talking Point ")
    public List<DanteAccount> getAccounts(@PathVariable int count) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_38008);
        }
        return danteAccountProxy.getAccounts(count, customerSpace.toString());
    }
}
