package com.latticeengines.pls.controller.dcp;

import java.util.List;

import javax.inject.Inject;

import org.springframework.security.access.prepost.PreAuthorize;
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

import com.latticeengines.domain.exposed.dcp.match.MatchRule;
import com.latticeengines.pls.service.dcp.MatchRuleService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "Match Rule")
@RestController
@PreAuthorize("hasRole('Edit_DCP_Projects')")
@RequestMapping("/matchrules")
public class MatchRuleResource {


    @Inject
    private MatchRuleService matchRuleService;

    @PutMapping
    @ResponseBody
    @ApiOperation(value = "Update Match Rule")
    public MatchRule updateMatchRule(@RequestBody MatchRule matchRule,
                                     @RequestParam(required = false, defaultValue = "false") Boolean mock) {
        return matchRuleService.updateMatchRule(matchRule, mock);
    }

    @PostMapping
    @ResponseBody
    @ApiOperation(value = "Create Match Rule")
    public MatchRule createMatchRule(@RequestBody MatchRule matchRule,
                                     @RequestParam(required = false, defaultValue = "false") Boolean mock) {
        return matchRuleService.createMatchRule(matchRule, mock);
    }

    @DeleteMapping("/{matchRuleId}")
    @ApiOperation(value = "Create Match Rule")
    public void deleteMatchRule(@PathVariable String matchRuleId,
                                @RequestParam(required = false, defaultValue = "false") Boolean mock) {
        matchRuleService.archiveMatchRule(matchRuleId, mock);
    }

    @GetMapping("/sourceId/{sourceId}")
    @ResponseBody
    @ApiOperation(value = "List Match Rule")
    public List<MatchRule> getMatchRuleList(@PathVariable String sourceId,
                                            @RequestParam(required = false, defaultValue = "false") Boolean includeArchived,
                                            @RequestParam(required = false, defaultValue = "false") Boolean includeInactive,
                                            @RequestParam(required = false, defaultValue = "false") Boolean mock) {
        return matchRuleService.getMatchRuleList(sourceId, includeArchived, includeInactive, mock);
    }
}
