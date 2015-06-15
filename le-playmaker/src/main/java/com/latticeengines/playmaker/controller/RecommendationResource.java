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
            @RequestParam(value = "start", required = true) int start,
            @RequestParam(value = "offset", required = true) int offset,
            @RequestParam(value = "maximum", required = true) int maximum) {

        return playmakerRecommendationMgr.getRecommendations(tenantName, start, offset, maximum);
    }

    @RequestMapping(value = "/recommendationcount", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get recommendation count")
    public int getRecommendationCount(@RequestParam(value = "tenantName", required = false) String tenantName,
            @RequestParam(value = "start", required = true) int start) {

        return playmakerRecommendationMgr.getRecommendationCount(tenantName, start);
    }

    @RequestMapping(value = "/plays", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get plays")
    public Map<String, Object> getPlays(@RequestParam(value = "tenantName", required = false) String tenantName,
            @RequestParam(value = "start", required = true) int start,
            @RequestParam(value = "offset", required = true) int offset,
            @RequestParam(value = "maximum", required = true) int maximum) {

        return playmakerRecommendationMgr.getPlays(tenantName, start, offset, maximum);
    }

    @RequestMapping(value = "/playcount", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get play count")
    public int getPlays(@RequestParam(value = "tenantName", required = false) String tenantName,
            @RequestParam(value = "start", required = true) int start) {

        return playmakerRecommendationMgr.getPlayCount(tenantName, start);
    }

    @RequestMapping(value = "/accountextensions", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get account extensions")
    public Map<String, Object> getAccountExtensions(
            @RequestParam(value = "tenantName", required = false) String tenantName,
            @RequestParam(value = "start", required = true) int start,
            @RequestParam(value = "offset", required = true) int offset,
            @RequestParam(value = "maximum", required = true) int maximum) {

        return playmakerRecommendationMgr.getAccountextensions(tenantName, start, offset, maximum);
    }

    @RequestMapping(value = "/accountextensioncount", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get account extension count")
    public int getAccountExtensionCount(@RequestParam(value = "tenantName", required = false) String tenantName,
            @RequestParam(value = "start", required = true) int start) {

        return playmakerRecommendationMgr.getAccountextensionCount(tenantName, start);
    }

    @RequestMapping(value = "/accountextensionschema", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get account extensions")
    public List<Map<String, Object>> getAccountExtensionSchema(
            @RequestParam(value = "tenantName", required = false) String tenantName) {

        return playmakerRecommendationMgr.getAccountExtensionSchema(tenantName);
    }

    @RequestMapping(value = "/playvalues", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get flexible play values")
    public Map<String, Object> getPlayValues(@RequestParam(value = "tenantName", required = false) String tenantName,
            @RequestParam(value = "start", required = true) int start,
            @RequestParam(value = "offset", required = true) int offset,
            @RequestParam(value = "maximum", required = true) int maximum) {

        return playmakerRecommendationMgr.getPlayValues(tenantName, start, offset, maximum);
    }

    @RequestMapping(value = "/playvaluecount", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get flexible play value count")
    public int getPlayValues(@RequestParam(value = "tenantName", required = false) String tenantName,
            @RequestParam(value = "start", required = true) int start) {

        return playmakerRecommendationMgr.getPlayValueCount(tenantName, start);
    }
}
