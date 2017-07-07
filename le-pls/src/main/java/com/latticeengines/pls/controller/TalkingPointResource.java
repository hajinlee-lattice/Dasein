package com.latticeengines.pls.controller;

import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dante.DantePreviewResources;
import com.latticeengines.domain.exposed.dante.DanteTalkingPoint;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.proxy.exposed.dante.TalkingPointProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;
import com.wordnik.swagger.annotations.Api;

import io.swagger.annotations.ApiOperation;

@Api(value = "dante/talkingpoints", description = "REST resource for Dante Talking Points")
@RestController
@RequestMapping("/dante/talkingpoints")
@PreAuthorize("hasRole('View_PLS_Plays')")
public class TalkingPointResource {

    private static final Logger log = Logger.getLogger(TalkingPointResource.class);

    @Autowired
    TalkingPointProxy talkingPointProxy;

    @RequestMapping(value = "", method = RequestMethod.POST)
    @ResponseBody
    @ResponseStatus(HttpStatus.CREATED)
    @ApiOperation(value = "Create a Dante Talking Point ")
    @PreAuthorize("hasRole('Edit_PLS_Plays')")
    public ResponseDocument<?> createOrUpdate(@RequestBody List<DanteTalkingPoint> talkingPoints) {
        return talkingPointProxy.createOrUpdate(talkingPoints);
    }

    @RequestMapping(value = "/{externalID}", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "get a Dante Talking Point ")
    public ResponseDocument<DanteTalkingPoint> findByExternalID(@PathVariable String externalID) {
        return talkingPointProxy.findByExternalID(externalID);
    }

    @RequestMapping(value = "/play/{playExternalID}", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "get a Dante Talking Point ")
    public ResponseDocument<List<DanteTalkingPoint>> findAllByPlayID(@PathVariable String playExternalID) {
        return talkingPointProxy.findAllByPlayID(playExternalID);
    }

    @RequestMapping(value = "/previewresources", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "get the server url and oAuth token for Dante authentication via Oauth")
    public ResponseDocument<DantePreviewResources> getPreviewResources() {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_38008);
        }
        return talkingPointProxy.getPreviewResources(customerSpace.toString());
    }

    @RequestMapping(value = "/{talkingPointExternalID}", method = RequestMethod.DELETE)
    @ResponseBody
    @ApiOperation(value = "Delete a Dante Talking Point ")
    @PreAuthorize("hasRole('Edit_PLS_Plays')")
    public ResponseDocument<?> delete(@PathVariable String talkingPointExternalID) {
        return talkingPointProxy.delete(talkingPointExternalID);
    }
}
