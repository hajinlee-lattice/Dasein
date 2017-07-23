package com.latticeengines.pls.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dante.DantePreviewResources;
import com.latticeengines.domain.exposed.dante.TalkingPointPreview;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.TalkingPointDTO;
import com.latticeengines.proxy.exposed.dante.TalkingPointProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "dante/talkingpoints", description = "REST resource for Dante Talking Points")
@RestController
@RequestMapping("/dante/talkingpoints")
@PreAuthorize("hasRole('View_PLS_Plays')")
public class TalkingPointResource {

    @Autowired
    private TalkingPointProxy talkingPointProxy;

    @RequestMapping(value = "", method = RequestMethod.POST)
    @ResponseBody
    @ResponseStatus(HttpStatus.CREATED)
    @ApiOperation(value = "Create a Talking Point")
    @PreAuthorize("hasRole('Edit_PLS_Plays')")
    public SimpleBooleanResponse createOrUpdate(@RequestBody List<TalkingPointDTO> talkingPoints) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_38008);
        }
        return talkingPointProxy.createOrUpdate(talkingPoints, customerSpace.toString());
    }

    @RequestMapping(value = "/{externalID}", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get a Talking Point")
    public ResponseDocument<TalkingPointDTO> findByName(@PathVariable String name) {
        return ResponseDocument.successResponse(talkingPointProxy.findByName(name));
    }

    @RequestMapping(value = "/play/{playName}", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Find all Talking Points defined for the given play")
    public ResponseDocument<List<TalkingPointDTO>> findAllByPlayName(@PathVariable String playName) {
        return ResponseDocument.successResponse(talkingPointProxy.findAllByPlayName(playName));
    }

    @RequestMapping(value = "/previewresources", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get the resources needed to preview a Dante Talking Point")
    public ResponseDocument<DantePreviewResources> getPreviewResources() {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_38008);
        }
        return ResponseDocument.successResponse(talkingPointProxy.getPreviewResources(customerSpace.toString()));
    }

    @RequestMapping(value = "/preview", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get Talking Point Preview Data for a given Play")
    public ResponseDocument<TalkingPointPreview> preview(@RequestParam("playName") String playName) {
        try {
            CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
            if (customerSpace == null) {
                throw new LedpException(LedpCode.LEDP_38008);
            }
            return ResponseDocument.successResponse(talkingPointProxy.getTalkingPointPreview(playName, customerSpace.toString()));
        } catch (Exception e) {
            // TODO: convert to some LedpException with Code
            throw(e);
        }
    }

    @RequestMapping(value = "/publish", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Publish given play's Talking Points to dante")
    @PreAuthorize("hasRole('Edit_PLS_Plays')")
    public SimpleBooleanResponse publish(@RequestParam("playName") String playName) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_38008);
        }
        return talkingPointProxy.publish(playName, customerSpace.toString());
    }

    @RequestMapping(value = "/{talkingPointName}", method = RequestMethod.DELETE)
    @ResponseBody
    @ApiOperation(value = "Delete a Dante Talking Point ")
    @PreAuthorize("hasRole('Edit_PLS_Plays')")
    public SimpleBooleanResponse delete(@PathVariable String talkingPointName) {
        return talkingPointProxy.delete(talkingPointName);
    }
}
