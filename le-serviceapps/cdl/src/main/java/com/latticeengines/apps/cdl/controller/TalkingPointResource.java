package com.latticeengines.apps.cdl.controller;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.TalkingPointService;
import com.latticeengines.domain.exposed.cdl.DantePreviewResources;
import com.latticeengines.domain.exposed.cdl.TalkingPointDTO;
import com.latticeengines.domain.exposed.cdl.TalkingPointPreview;
import com.latticeengines.domain.exposed.query.AttributeLookup;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "talkingpoints", description = "REST resource for Talking Points related operations")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/talkingpoints")
public class TalkingPointResource {

    private static final Logger log = LoggerFactory.getLogger(TalkingPointResource.class);

    @Inject
    private TalkingPointService talkingPointService;

    @PostMapping("")
    @ResponseBody
    @ResponseStatus(HttpStatus.CREATED)
    @ApiOperation(value = "Create/Update a Talking Point. PID value of null or zero will create a TP.")
    public List<TalkingPointDTO> createOrUpdate(@PathVariable String customerSpace,
            @RequestBody List<TalkingPointDTO> talkingPoints) {
        return talkingPointService.createOrUpdate(talkingPoints);
    }

    @GetMapping("/{talkingPointName}")
    @ResponseBody
    @ApiOperation(value = "Get a Talking Point")
    public TalkingPointDTO findByName(@PathVariable String customerSpace, @PathVariable String talkingPointName) {
        return talkingPointService.findByName(talkingPointName);
    }

    @GetMapping("/play/{playName}")
    @ResponseBody
    @ApiOperation(value = "Find all Talking Points defined for the given play")
    public List<TalkingPointDTO> findAllByPlayName(@PathVariable String customerSpace, @PathVariable String playName,
            @RequestParam(name = "publishedonly", required = false, defaultValue = "false") boolean publishedOnly) {
        return talkingPointService.findAllByPlayName(playName, publishedOnly);
    }

    @GetMapping("/previewresources")
    @ResponseBody
    @ApiOperation(value = "Get the resources needed to preview a Dante Talking Point")
    public DantePreviewResources getPreviewResources(@PathVariable String customerSpace) {
        return talkingPointService.getPreviewResources();
    }

    @GetMapping("/preview")
    @ResponseBody
    @ApiOperation(value = "Get Talking Point Preview Data for a given Play")
    public TalkingPointPreview getTalkingPointPreview(@PathVariable String customerSpace,
            @RequestParam("playName") String playName) {
        return talkingPointService.getPreview(playName);
    }

    @PostMapping("/publish")
    @ResponseBody
    @ApiOperation(value = "Publish given play's Talking Points to dante")
    public void publish(@PathVariable String customerSpace, @RequestParam("playName") String playName) {
        talkingPointService.publish(playName);
    }

    @PostMapping("/revert")
    @ResponseBody
    @ApiOperation(value = "Revert the given play's talking points to the version last published to dante")
    public List<TalkingPointDTO> revert(@PathVariable String customerSpace, @RequestParam("playName") String playName) {
        return talkingPointService.revertToLastPublished(playName);
    }

    @DeleteMapping("/{talkingPointName}")
    @ResponseBody
    @ApiOperation(value = "Delete a Talking Point ")
    public void delete(@PathVariable String customerSpace, @PathVariable String talkingPointName) {
        talkingPointService.delete(talkingPointName);
    }

    @GetMapping("/attributes/{playName}")
    @ResponseBody
    @ApiOperation(value = "get all depending attributes in talkingpoint of a play")
    public List<AttributeLookup> getAttributesInTalkingPointOfPlay(@PathVariable String customerSpace,
            @PathVariable("playName") String playName) {
        return talkingPointService.getAttributesInTalkingPointOfPlay(playName);
    }

    @GetMapping("/all-published")
    @ResponseBody
    @ApiOperation(value = "Return all published Talking Points for the tenant")
    public List<TalkingPointDTO> findAllPublishedByTenant(@PathVariable String customerSpace) {
        return talkingPointService.findAllPublishedByTenant(customerSpace);
    }
}
