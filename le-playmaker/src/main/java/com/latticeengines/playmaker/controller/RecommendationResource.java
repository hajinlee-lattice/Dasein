package com.latticeengines.playmaker.controller;

import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.web.SpringBootServletInitializer;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.playmaker.controller.Oauth2Utils;
import com.latticeengines.playmaker.entitymgr.PlaymakerRecommendationEntityMgr;
import com.latticeengines.playmaker.entitymgr.PlaymakerTenantEntityMgr;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;

@Api(value = "Playmaker recommendation api", description = "REST resource for getting playmaker recomendationss")
@Configuration
@EnableAutoConfiguration
@RestController
@ImportResource(value = { "classpath:playmaker-context.xml", "classpath:playmaker-properties-context.xml" })
@RequestMapping(value = "/playmaker")
public class RecommendationResource extends SpringBootServletInitializer {

    private final Log log = LogFactory.getLog(this.getClass());

    @Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
        return application.sources(RecommendationResource.class);
    }

    public static void main(String[] args) {
        SpringApplication.run(RecommendationResource.class, args);
    }

    @Autowired
    private PlaymakerRecommendationEntityMgr playmakerRecommendationMgr;

    @Autowired
    private PlaymakerTenantEntityMgr playmakerEntityMgr;

    @RequestMapping(value = "/recommendations", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get recommendations")
    public Map<String, Object> getRecommendations(
            HttpServletRequest request,
            @ApiParam(value = "Last Modification date in Unix timestamp", required = true) @RequestParam(value = "start", required = true) int start,
            @ApiParam(value = "First record number from start", required = true) @RequestParam(value = "offset", required = true) int offset,
            @ApiParam(value = "Maximum records returned above offset", required = true) @RequestParam(value = "maximum", required = true) int maximum,
            @ApiParam(value = "Synchronization Destination: SFDC | MAP | SFDC_AND_MAP", required = true) @RequestParam(value = "destination", required = true) String destination) {

        String tenantName = getTenantName(request);
        return playmakerRecommendationMgr.getRecommendations(tenantName, start, offset, maximum,
                SynchronizationDestinationEnum.mapToIntType(destination));
    }

    @RequestMapping(value = "/recommendationcount", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get recommendation count")
    public Map<String, Object> getRecommendationCount(
            HttpServletRequest request,
            @ApiParam(value = "Last Modification date in Unix timestamp", required = true) @RequestParam(value = "start", required = true) int start,
            @ApiParam(value = "Synchronization Destination: SFDC | MAP | SFDC_AND_MAP", required = true) @RequestParam(value = "destination", required = true) String destination) {

        String tenantName = getTenantName(request);
        return playmakerRecommendationMgr.getRecommendationCount(tenantName, start,
                SynchronizationDestinationEnum.mapToIntType(destination));
    }

    @RequestMapping(value = "/plays", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get plays")
    public Map<String, Object> getPlays(
            HttpServletRequest request,
            @ApiParam(value = "Last Modification date in Unix timestamp", required = true) @RequestParam(value = "start", required = true) int start,
            @ApiParam(value = "First record number from start", required = true) @RequestParam(value = "offset", required = true) int offset,
            @ApiParam(value = "Maximum records returned above offset", required = true) @RequestParam(value = "maximum", required = true) int maximum) {

        String tenantName = getTenantName(request);
        return playmakerRecommendationMgr.getPlays(tenantName, start, offset, maximum);
    }

    @RequestMapping(value = "/playcount", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get play count")
    public Map<String, Object> getPlays(
            HttpServletRequest request,
            @ApiParam(value = "Last Modification date in Unix timestamp", required = true) @RequestParam(value = "start", required = true) int start) {

        String tenantName = getTenantName(request);
        return playmakerRecommendationMgr.getPlayCount(tenantName, start);
    }

    @RequestMapping(value = "/accountextensions", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get account extensions")
    public Map<String, Object> getAccountExtensions(
            HttpServletRequest request,
            @ApiParam(value = "Last Modification date in Unix timestamp", required = true) @RequestParam(value = "start", required = true) int start,
            @ApiParam(value = "First record number from start", required = true) @RequestParam(value = "offset", required = true) int offset,
            @ApiParam(value = "Maximum records returned above offset", required = true) @RequestParam(value = "maximum", required = true) int maximum) {

        String tenantName = getTenantName(request);
        return playmakerRecommendationMgr.getAccountextensions(tenantName, start, offset, maximum);
    }

    @RequestMapping(value = "/accountextensioncount", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get account extension count")
    public Map<String, Object> getAccountExtensionCount(
            HttpServletRequest request,
            @ApiParam(value = "Last Modification date in Unix timestamp", required = true) @RequestParam(value = "start", required = true) int start) {

        String tenantName = getTenantName(request);
        return playmakerRecommendationMgr.getAccountextensionCount(tenantName, start);
    }

    @RequestMapping(value = "/accountextensionschema", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get account extensions")
    public List<Map<String, Object>> getAccountExtensionSchema(HttpServletRequest request) {
        String tenantName = getTenantName(request);
        return playmakerRecommendationMgr.getAccountExtensionSchema(tenantName);
    }

    @RequestMapping(value = "/playvalues", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get flexible play values")
    public Map<String, Object> getPlayValues(
            HttpServletRequest request,
            @ApiParam(value = "Last Modification date in Unix timestamp", required = true) @RequestParam(value = "start", required = true) int start,
            @ApiParam(value = "First record number from start", required = true) @RequestParam(value = "offset", required = true) int offset,
            @ApiParam(value = "Maximum records returned above offset", required = true) @RequestParam(value = "maximum", required = true) int maximum) {

        String tenantName = getTenantName(request);
        return playmakerRecommendationMgr.getPlayValues(tenantName, start, offset, maximum);
    }

    @RequestMapping(value = "/playvaluecount", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get flexible play value count")
    public Map<String, Object> getPlayValues(
            HttpServletRequest request,
            @ApiParam(value = "Last Modification date in Unix timestamp", required = true) @RequestParam(value = "start", required = true) int start) {

        String tenantName = getTenantName(request);
        return playmakerRecommendationMgr.getPlayValueCount(tenantName, start);
    }

    @RequestMapping(value = "/workflowtypes", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get workflow types' IDs and Names")
    public List<Map<String, Object>> getWorkflowTypes(HttpServletRequest request) {

        String tenantName = getTenantName(request);
        return playmakerRecommendationMgr.getWorkflowTypes(tenantName);
    }

    private String getTenantName(HttpServletRequest request) {
        try {
            String token = Oauth2Utils.extractHeaderToken(request);
            if (token == null) {
                throw new LedpException(LedpCode.LEDP_22003);
            }
            String tokenId = Oauth2Utils.extractTokenKey(token);
            if (tokenId == null) {
                throw new LedpException(LedpCode.LEDP_22004);
            }
            String tenantName = playmakerEntityMgr.findTenantByTokenId(tokenId);
            if (tenantName == null) {
                throw new LedpException(LedpCode.LEDP_22006);
            }
            return tenantName;
        } catch (Exception ex) {
            log.error("Can not get tenant!", ex);
            throw new LedpException(LedpCode.LEDP_22005, ex);
        }
    }
}
