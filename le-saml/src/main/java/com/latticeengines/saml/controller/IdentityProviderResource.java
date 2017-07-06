package com.latticeengines.saml.controller;

import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.saml.IdentityProvider;
import com.latticeengines.saml.service.IdentityProviderService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "identityproviders", description = "REST resource for managing SAML identity providers")
@RestController
@RequestMapping("identityproviders")
@PreAuthorize("hasRole('Edit_PLS_Data')")
public class IdentityProviderResource {
    private static final Logger log = Logger.getLogger(IdentityProviderResource.class);

    @Autowired
    private IdentityProviderService identityProviderService;

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Retrieve all identity providers")
    public List<IdentityProvider> findAll() {
        log.info("Retrieving all identity providers");
        return identityProviderService.findAll();
    }

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create an identity provider")
    public void create(@RequestBody IdentityProvider identityProvider) {
        log.info(String.format("Creating identity provider with entityId %s", identityProvider.getEntityId()));
        identityProviderService.create(identityProvider);
    }

    @RequestMapping(value = "/{entityId}", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Delete a identity provider with the specified entityId")
    public void delete(@PathVariable String entityId) {
        log.info(String.format("Deleting identity provider with entityId %s", entityId));
        identityProviderService.delete(entityId);
    }
}
