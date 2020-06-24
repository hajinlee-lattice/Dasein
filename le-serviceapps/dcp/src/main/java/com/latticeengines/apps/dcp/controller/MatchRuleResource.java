package com.latticeengines.apps.dcp.controller;

import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.dcp.service.MatchRuleService;
import com.latticeengines.domain.exposed.dcp.match.MatchRule;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "MatchRule")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/matchrules")
public class MatchRuleResource {

    @Inject
    private MatchRuleService matchRuleService;

    @PutMapping
    @ResponseBody
    @ApiOperation(value = "Update Match Rule")
    public MatchRule updateMatchRule(@PathVariable String customerSpace, @RequestBody MatchRule matchRule) {
        return null;
    }

    @PostMapping
    @ResponseBody
    @ApiOperation(value = "Create Match Rule")
    public MatchRule createMatchRule(@PathVariable String customerSpace, @RequestBody MatchRule matchRule) {
        return null;
    }

    @DeleteMapping("/{matchRuleId}")
    @ApiOperation(value = "Create Match Rule")
    public void deleteMatchRule(@PathVariable String customerSpace, @PathVariable String matchRuleId) {

    }

    @GetMapping("/sourceId/{sourceId}")
    @ResponseBody
    @ApiOperation(value = "List Match Rule")
    public List<MatchRule> getMatchRuleList(@PathVariable String customerSpace, @PathVariable String sourceId,
                                            @RequestParam(required = false, defaultValue = "false") Boolean includeArchived,
                                            @RequestParam(required = false, defaultValue = "false") Boolean includeInactive) {
        return Collections.emptyList();
    }
}
