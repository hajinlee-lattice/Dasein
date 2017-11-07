package com.latticeengines.ulysses.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.dante.multitenant.TalkingPointDTO;
import com.latticeengines.proxy.exposed.dante.TalkingPointProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "TalkingPoints", description = "Common REST resource to lookup talking points")
@RestController
@RequestMapping("/talkingpoints")
public class TalkingPointResource {

    @Autowired
    private TalkingPointProxy talkingPointProxy;

    @RequestMapping(value = "/{talkingPointId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get an account by of attributes in a group")
    public TalkingPointDTO getTalkingPointById(@PathVariable String talkingPointId) {
        return talkingPointProxy.findByName(talkingPointId);
    }

    @RequestMapping(value = "/playid/{playId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get an account by of attributes in a group")
    public List<TalkingPointDTO> getTalkingPointByPlayId(@PathVariable String playId) {
        return talkingPointProxy.findAllByPlayName(playId);
    }
}
