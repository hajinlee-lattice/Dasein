package com.latticeengines.ulysses.controller;

import java.util.Map;

import javax.inject.Inject;

import org.springframework.http.RequestEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.app.exposed.service.DataLakeService;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.proxy.exposed.oauth2.Oauth2RestApiProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "Contacts", description = "Common REST resource to lookup contacts")
@RestController
@RequestMapping("/contacts")
public class ContactResource {

    private final DataLakeService dataLakeService;
    private final Oauth2RestApiProxy tenantProxy;

    @Inject
    public ContactResource(DataLakeService dataLakeService, Oauth2RestApiProxy tenantProxy) {
        this.dataLakeService = dataLakeService;
        this.tenantProxy = tenantProxy;
    }

    @GetMapping(value = "/accounts/{accountId:.+}")
    @ResponseBody
    @ApiOperation(value = "Get given accountid and contactId")
    public DataPage getContactsByAccountId(RequestEntity<String> requestEntity, @PathVariable String accountId) {
        Map<String, String> orgInfo = tenantProxy.getOrgInfoFromOAuthRequest(requestEntity);

        return dataLakeService.getAllContactsByAccountId(accountId, orgInfo);
    }

}
