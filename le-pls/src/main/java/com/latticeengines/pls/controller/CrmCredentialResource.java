package com.latticeengines.pls.controller;

import java.util.List;

import javax.inject.Inject;

import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;
import com.latticeengines.domain.exposed.pls.CrmConfig;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.pls.service.CrmConfigService;
import com.latticeengines.pls.service.CrmCredentialService;
import com.latticeengines.pls.service.TenantConfigService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "CRM Credential Verification", description = "REST resource for CRM credential verification")
@RestController
@RequestMapping(value = "/credentials")
@PreAuthorize("hasRole('View_PLS_CrmCredential')")
public class CrmCredentialResource {

    @Inject
    private CrmCredentialService crmCredentialService;

    @Inject
    private CrmConfigService crmConfigService;

    @Inject
    private TenantConfigService tenantConfigService;

    @RequestMapping(value = "/{crmType}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Verify CRM credential")
    @PreAuthorize("hasRole('Edit_PLS_CrmCredential')")
    public CrmCredential verifyCredential(@PathVariable String crmType,
            @RequestParam(value = "tenantId") String tenantId,
            @RequestParam(value = "isProduction", required = false) Boolean isProduction,
            @RequestParam(value = "verifyOnly", required = false) Boolean verifyOnly,
            @RequestBody CrmCredential crmCredential) {

        CrmCredential newCrmCredential = crmCredentialService.verifyCredential(crmType, tenantId, isProduction,
                crmCredential);
        List<LatticeProduct> products = tenantConfigService.getProducts(tenantId);
        FeatureFlagValueMap flags = tenantConfigService.getFeatureFlags(tenantId);
        if (!crmCredentialService.useEaiToValidate(flags) && products.contains(LatticeProduct.LPA)) {
            if ((verifyOnly == null || !verifyOnly) && (isProduction == null || isProduction)) {
                CrmConfig crmConfig = new CrmConfig();
                crmConfig.setCrmCredential(newCrmCredential);
                crmConfigService.config(crmType, tenantId, crmConfig);
            }
        }

        return newCrmCredential;

    }

    @RequestMapping(value = "/{crmType}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get CRM credential")
    public CrmCredential getCredential(@PathVariable String crmType, @RequestParam(value = "tenantId") String tenantId,
            @RequestParam(value = "isProduction", required = false) Boolean isProduction) {

        return crmCredentialService.getCredential(crmType, tenantId, isProduction);

    }
}
