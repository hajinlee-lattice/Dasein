package com.latticeengines.dante.controller;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.dante.service.TalkingPointService;
import com.latticeengines.domain.exposed.dante.DantePreviewResources;
import com.latticeengines.domain.exposed.dante.TalkingPointPreview;
import com.latticeengines.domain.exposed.multitenant.TalkingPointDTO;
import com.latticeengines.network.exposed.dante.TalkingPointInterface;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "dante", description = "REST resource for Talking Points related operations")
@RestController
@RequestMapping("/talkingpoints")
public class TalkingPointResource implements TalkingPointInterface {
    private static final Logger log = LoggerFactory.getLogger(TalkingPointResource.class);

    @Autowired
    private TalkingPointService talkingPointService;

    @RequestMapping(value = "", method = RequestMethod.POST)
    @ResponseBody
    @ResponseStatus(HttpStatus.CREATED)
    @ApiOperation(value = "Create/Update a Talking Point. PID value of null or zero will create a TP.")
    public List<TalkingPointDTO> createOrUpdate(@RequestBody List<TalkingPointDTO> talkingPoints,
            @RequestParam("customerSpace") String customerSpace) {
        return talkingPointService.createOrUpdate(talkingPoints, customerSpace);
    }

    @RequestMapping(value = "/{name}", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get a Talking Point")
    public TalkingPointDTO findByName(@PathVariable String name) {
        return talkingPointService.findByName(name);
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
    public DantePreviewResources getPreviewResources(@RequestParam("customerSpace") String customerSpace) {
        return talkingPointService.getPreviewResources(customerSpace);
    }

    @RequestMapping(value = "/preview", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get Talking Point Preview Data for a given Play")
    public TalkingPointPreview getTalkingPointPreview(@RequestParam("playName") String playName,
            @RequestParam("customerSpace") String customerSpace) {
        return talkingPointService.getPreview(playName, customerSpace);
    }

    @RequestMapping(value = "/publish", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Publish given play's Talking Points to dante")
    public void publish(@RequestParam("playName") String playName,
            @RequestParam("customerSpace") String customerSpace) {
        talkingPointService.publish(playName, customerSpace);
    }

    @RequestMapping(value = "/revert", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Revert the given play's talking points to the version last published to dante")
    public List<TalkingPointDTO> revert(@RequestParam("playName") String playName,
            @RequestParam("customerSpace") String customerSpace) {
        return talkingPointService.revertToLastPublished(playName, customerSpace);
    }

    @RequestMapping(value = "/{talkingPointName}", method = RequestMethod.DELETE)
    @ResponseBody
    @ApiOperation(value = "Delete a Talking Point ")
    public void delete(@PathVariable String talkingPointName) {
        talkingPointService.delete(talkingPointName);
    }
}
