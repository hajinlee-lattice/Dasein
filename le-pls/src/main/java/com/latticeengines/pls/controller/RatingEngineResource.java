package com.latticeengines.pls.controller;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;

import edu.emory.mathcs.backport.java.util.Arrays;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "ratingengine", description = "REST resource for rating engine")
@RestController
@RequestMapping("/ratingengines")
@PreAuthorize("hasRole('View_PLS_RatingEngines')")
public class RatingEngineResource {

    private static final Logger log = LoggerFactory.getLogger(RatingEngineResource.class);

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=applicaiton/json")
    @ResponseBody
    @ApiOperation(value = "Get all Rating Engines for a tenant")
    public List<RatingEngine> getRatingEngines() {
        return createRatingEngineList();
    }

    @SuppressWarnings("unchecked")
    @RequestMapping(value = "/types", method = RequestMethod.GET, headers = "Accept=applicaiton/json")
    @ResponseBody
    @ApiOperation(value = "Get types for Rating Engines")
    public List<RatingEngineType> getRatingEngineTypes() {
        return Arrays.asList(RatingEngineType.values());
    }

    @RequestMapping(value = "/{ratingEngineId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get a Rating Engine given its id")
    public RatingEngine getRatingEngine(@PathVariable String ratingEngineId, HttpServletRequest request,
            HttpServletResponse response) {
        return createRatingEngine();
    }

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Register a Rating Engine")
    @PreAuthorize("hasRole('Create_PLS_Rating_Engines')")
    public RatingEngine createRatingEngine(@RequestBody RatingEngine ratingEngine, HttpServletRequest request) {
        return createRatingEngine();
    }

    @RequestMapping(value = "/{ratingEngineId}", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update a Rating Engine given its id")
    @PreAuthorize("hasRole('Create_PLS_Rating_Engines')")
    public RatingEngine updateRatingEngine( //
            @RequestBody RatingEngine ratingEngine, //
            @PathVariable String ratingEngineId, //
            HttpServletRequest request) {
        return createRatingEngine();
    }

    @RequestMapping(value = "/{ratingEngineId}", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Delete a Rating Engine given its id")
    @PreAuthorize("hasRole('Edit_PLS_Rating_Engines')")
    public Boolean deleteRatingEngine(@PathVariable String ratingEngineId, HttpServletRequest request) {
        return true;
    }

    @RequestMapping(value = "/{ratingEngineId}/ratingmodels", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get Rating Models associated with a Rating Engine given its id")
    public Set<RatingModel> getRatingModels(@PathVariable String ratingEngineId, HttpServletRequest request,
            HttpServletResponse response) {
        return createRatingModels();
    }

    @RequestMapping(value = "/{ratingEngineId}/ratingmodels/{ratingModelId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get a particular Rating Model associated with a Rating Engine given its Rating Engine id and Rating Model id")
    public RatingModel getRatingModel(@PathVariable String ratingEngineId, //
            @PathVariable String ratingModelId, //
            HttpServletRequest request, //
            HttpServletResponse response) {
        return (RatingModel) createRatingModels().toArray()[0];
    }

    @RequestMapping(value = "/{ratingEngineId}/plays", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get Plays associated with a Rating Engine given its id")
    public Set<Play> getPlays(@PathVariable String ratingEngineId, //
            HttpServletRequest request, //
            HttpServletResponse response) {
        return createPlays();
    }

    private List<RatingEngine> createRatingEngineList() {
        List<RatingEngine> list = new ArrayList<>();
        RatingEngine re1 = new RatingEngine();
        list.add(re1);
        return list;
    }

    private RatingEngine createRatingEngine() {
        RatingEngine re = new RatingEngine();
        return re;
    }

    private Set<RatingModel> createRatingModels() {
        Set<RatingModel> set = new HashSet<>();
        set.add(new RuleBasedModel());
        return set;
    }

    private Set<Play> createPlays() {
        Set<Play> set = new HashSet<>();
        set.add(new Play());
        return set;
    }

}
