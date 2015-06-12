package com.latticeengines.playmaker.controller;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.playmaker.entitymgr.PlaymakerRecommendationEntityMgr;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "Playmaker recommendation api", description = "REST resource for getting playmaker recomendationss")
@RestController
public class RecommendationResource {

    @Autowired
    private PlaymakerRecommendationEntityMgr playmakerRecommendationMgr;

    @RequestMapping(value = "/recommendations", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get recommendations")
    public Map<String, Object> getRecommendations(
            @RequestParam(value = "tenantName", required = false) String tenantName,
            @RequestParam(value = "startId", required = true) int startId,
            @RequestParam(value = "size", required = true) int size) {

        return playmakerRecommendationMgr.getRecommendations(tenantName, startId, size);
    }

    @RequestMapping(value = "/plays", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get plays")
    public Map<String, Object> getPlays(@RequestParam(value = "tenantName", required = false) String tenantName,
            @RequestParam(value = "startId", required = true) int startId,
            @RequestParam(value = "size", required = true) int size) {

        return playmakerRecommendationMgr.getPlays(tenantName, startId, size);
    }

    @RequestMapping(value = "/accountextensions", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get plays")
    public Map<String, Object> getAccountExtensions(
            @RequestParam(value = "tenantName", required = false) String tenantName,
            @RequestParam(value = "startId", required = true) int startId,
            @RequestParam(value = "size", required = true) int size) {

        return playmakerRecommendationMgr.getAccountextensions(tenantName, startId, size);
    }
}
