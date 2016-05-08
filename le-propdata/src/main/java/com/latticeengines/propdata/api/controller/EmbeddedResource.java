package com.latticeengines.propdata.api.controller;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.StatusDocument;
import com.latticeengines.propdata.match.service.EmbeddedDbService;
import com.latticeengines.security.exposed.InternalResourceBase;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import springfox.documentation.annotations.ApiIgnore;

@Api(value = "embedded db management", description = "REST resource for embedded H2 db")
@RestController
@RequestMapping("/embedded")
public class EmbeddedResource extends InternalResourceBase {

    @Autowired
    private EmbeddedDbService embeddedDbService;

    @RequestMapping(value = "/matchkeys", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiIgnore
    @ApiOperation(value = "Check if embedded db is loaded with match keys.")
    public StatusDocument matchKeysAreReady(HttpServletRequest request) {
        checkHeader(request);
        if (embeddedDbService.isReady()) {
            return new StatusDocument("YES");
        }
        return new StatusDocument("NO");
    }

    @RequestMapping(value = "/matchkeys", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiIgnore
    @ApiOperation(value = "Invalid embedded db to force using match keys on remote SQL server.")
    public StatusDocument invalidateMatchKeys(HttpServletRequest request) {
        checkHeader(request);
        embeddedDbService.invalidate();
        return StatusDocument.ok();
    }

    @RequestMapping(value = "/matchkeys", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiIgnore
    @ApiOperation(value = "Asynchronously refresh match keys stored in embedded db.")
    public StatusDocument refreshMatchKeys(HttpServletRequest request) {
        checkHeader(request);
        embeddedDbService.loadAsync();
        return StatusDocument.ok();
    }


}
