package com.latticeengines.pls.controller;

import java.util.List;

import javax.inject.Inject;

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

import com.latticeengines.domain.exposed.cdl.DantePreviewResources;
import com.latticeengines.domain.exposed.cdl.TalkingPointDTO;
import com.latticeengines.domain.exposed.cdl.TalkingPointNotionAttributes;
import com.latticeengines.domain.exposed.cdl.TalkingPointPreview;
import com.latticeengines.pls.service.TalkingPointService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "dante/talkingpoints", description = "REST resource for Dante Talking Points")
@RestController
@RequestMapping("/dante/talkingpoints")
@PreAuthorize("hasRole('View_PLS_Plays')")
public class TalkingPointResource {

    @Inject
    private TalkingPointService talkingPointService;

    @RequestMapping(value = "", method = RequestMethod.POST)
    @ResponseBody
    @ResponseStatus(HttpStatus.CREATED)
    @ApiOperation(value = "Create a Talking Point")
    @PreAuthorize("hasRole('Edit_PLS_Plays')")
    public List<TalkingPointDTO> createOrUpdate(@RequestBody List<TalkingPointDTO> talkingPoints) {
        return talkingPointService.createOrUpdate(talkingPoints);
    }

    @RequestMapping(value = "/{talkingPointName}", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get a Talking Point")
    public TalkingPointDTO findByName(@PathVariable String talkingPointName) {
        return talkingPointService.findByName(talkingPointName);
    }

    @RequestMapping(value = "/play/{playName}", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Find all Talking Points defined for the given play")
    public List<TalkingPointDTO> findAllByPlayName(@PathVariable String playName) {
        return talkingPointService.findAllByPlayName(playName);
    }

    @RequestMapping(value = "/previewresources", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get the resources needed to preview a Dante Talking Point")
    public DantePreviewResources getPreviewResources() {
        return talkingPointService.getPreviewResources();
    }

    @RequestMapping(value = "/preview", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get Talking Point Preview Data for a given Play")
    public TalkingPointPreview preview(@RequestParam("playName") String playName) {
        return talkingPointService.preview(playName);
    }

    @RequestMapping(value = "/publish", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Publish given play's Talking Points to dante")
    @PreAuthorize("hasRole('Edit_PLS_Plays')")
    public void publish(@RequestParam("playName") String playName) {
        talkingPointService.publish(playName);
    }

    @RequestMapping(value = "/revert", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(
            value = "Revert the given play's talking points to the version last published to dante")
    @PreAuthorize("hasRole('Edit_PLS_Plays')")
    public List<TalkingPointDTO> revert(@RequestParam("playName") String playName) {
        return talkingPointService.revert(playName);
    }

    @RequestMapping(value = "/{talkingPointName}", method = RequestMethod.DELETE)
    @ResponseBody
    @ApiOperation(value = "Delete a Dante Talking Point ")
    @PreAuthorize("hasRole('Edit_PLS_Plays')")
    public void delete(@PathVariable String talkingPointName) {
        talkingPointService.delete(talkingPointName);
    }

    @RequestMapping(value = "/attributes", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Get attributes for given notions")
    @PreAuthorize("hasRole('Edit_PLS_Plays')")
    public TalkingPointNotionAttributes getAttributesByNotions(@RequestBody List<String> notions) {
        return talkingPointService.getAttributesByNotions(notions);
    }
}
