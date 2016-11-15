package com.latticeengines.playmaker.controller;

import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.velocity.VelocityAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.web.SpringBootServletInitializer;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.oauth2db.exposed.entitymgr.OAuthUserEntityMgr;
import com.latticeengines.oauth2db.exposed.util.OAuth2Utils;
import com.latticeengines.playmaker.entitymgr.PlaymakerRecommendationEntityMgr;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import com.wordnik.swagger.annotations.ApiParam;

@Api(value = "Playmaker recommendation api", description = "REST resource for getting playmaker recomendationss")
@Configuration
@EnableAutoConfiguration(exclude = { VelocityAutoConfiguration.class })
@RestController
@ImportResource(value = { "classpath:playmaker-context.xml", "classpath:common-properties-context.xml" })
@RequestMapping(value = "/playmaker")
public class RecommendationResource extends SpringBootServletInitializer {

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
    private OAuthUserEntityMgr oAuthUserEntityMgr;

    @RequestMapping(value = "/recommendations", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get recommendations")
    public Map<String, Object> getRecommendations(
            HttpServletRequest request,
            @ApiParam(value = "Last Modification date in Unix timestamp", required = true) @RequestParam(value = "start", required = true) long start,
            @ApiParam(value = "First record number from start", required = true) @RequestParam(value = "offset", required = true) int offset,
            @ApiParam(value = "Maximum records returned above offset", required = true) @RequestParam(value = "maximum", required = true) int maximum,
            @ApiParam(value = "Synchronization Destination: SFDC | MAP | SFDC_AND_MAP", required = true) @RequestParam(value = "destination", required = true) String destination,
            @ApiParam(value = "Play's Id whose recommendations are returned", required = false) @RequestParam(value = "playId", required = false) List<Integer> playIds) {

        String tenantName = OAuth2Utils.getTenantName(request, oAuthUserEntityMgr);
        return playmakerRecommendationMgr.getRecommendations(tenantName, start, offset, maximum,
                SynchronizationDestinationEnum.mapToIntType(destination), playIds);
    }

    @RequestMapping(value = "/recommendationcount", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get recommendation count")
    public Map<String, Object> getRecommendationCount(
            HttpServletRequest request,
            @ApiParam(value = "Last Modification date in Unix timestamp", required = true) @RequestParam(value = "start", required = true) long start,
            @ApiParam(value = "Synchronization Destination: SFDC | MAP | SFDC_AND_MAP", required = true) @RequestParam(value = "destination", required = true) String destination,
            @ApiParam(value = "Play's Id whose recommendations are returned; all play Ids if not specified", required = false) @RequestParam(value = "playId", required = false) List<Integer> playIds) {

        String tenantName = OAuth2Utils.getTenantName(request, oAuthUserEntityMgr);
        return playmakerRecommendationMgr.getRecommendationCount(tenantName, start,
                SynchronizationDestinationEnum.mapToIntType(destination), playIds);
    }

    @RequestMapping(value = "/plays", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get plays")
    public Map<String, Object> getPlays(
            HttpServletRequest request,
            @ApiParam(value = "Last Modification date in Unix timestamp", required = true) @RequestParam(value = "start", required = true) long start,
            @ApiParam(value = "First record number from start", required = true) @RequestParam(value = "offset", required = true) int offset,
            @ApiParam(value = "Maximum records returned above offset", required = true) @RequestParam(value = "maximum", required = true) int maximum,
            @ApiParam(value = "Play group's Id whose plays are returned; all play group Ids if not specified", required = false) @RequestParam(value = "playgroupId", required = false) List<Integer> playgroupIds) {

        String tenantName = OAuth2Utils.getTenantName(request, oAuthUserEntityMgr);
        return playmakerRecommendationMgr.getPlays(tenantName, start, offset, maximum, playgroupIds);
    }

    @RequestMapping(value = "/playcount", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get play count")
    public Map<String, Object> getPlayCount(
            HttpServletRequest request,
            @ApiParam(value = "Last Modification date in Unix timestamp", required = true) @RequestParam(value = "start", required = true) long start,
            @ApiParam(value = "Play group's Id whose plays are returned; all play group Ids if not specified", required = false) @RequestParam(value = "playgroupId", required = false) List<Integer> playgroupIds) {

        String tenantName = OAuth2Utils.getTenantName(request, oAuthUserEntityMgr);
        return playmakerRecommendationMgr.getPlayCount(tenantName, start, playgroupIds);
    }

    @RequestMapping(value = "/accountextensions", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get account extensions")
    public Map<String, Object> getAccountExtensions(
            HttpServletRequest request,
            @ApiParam(value = "Last Modification date in Unix timestamp", required = true) @RequestParam(value = "start", required = true) long start,
            @ApiParam(value = "First record number from start", required = true) @RequestParam(value = "offset", required = true) int offset,
            @ApiParam(value = "Maximum records returned above offset", required = true) @RequestParam(value = "maximum", required = true) int maximum,
            @ApiParam(value = "Account Id whose extension columns are returned; all account Ids if not specified. This is mutual exclusive to filberBy/recStart.", required = false) @RequestParam(value = "accountId", required = false) List<Integer> accountIds,
            @ApiParam(value = "filterBy is a flag to filter Account Extensions with Recommendations, NoRecommendations or All, which "
                    + "are also its predefined values. NOTE: in terms of Recommendations and NoRecommendations, parameter recStart needs to be used to locate recommendations modified since recStart. "
                    + "This is mutual exclusive to accountId.", required = false) @RequestParam(value = "filterBy", required = false) String filterBy,
            @ApiParam(value = "The Last Modification date in unix timestamp on Recommendation, only used together with filterBy=Recommendations or NoRecommendations", required = false) @RequestParam(value = "recStart", required = false) Long recStart,
            @ApiParam(value = "columns are selected column names for output; column names are delimited by a comma.", required = false) @RequestParam(value = "columns", required = false) String columns,
            @ApiParam(value = "true - populate column SfdcContactId, false - does NOT populate column SfdcContactId", required = false) @RequestParam(value = "hasSfdcContactId", required = false) String hasSfdcContactId) {

        String tenantName = OAuth2Utils.getTenantName(request, oAuthUserEntityMgr);
        Map<String, Object> accountExtensions = playmakerRecommendationMgr.getAccountExtensions(tenantName, start,
                offset, maximum, accountIds, filterBy, recStart, columns, "true".equals(hasSfdcContactId));
        return accountExtensions;
    }

    @RequestMapping(value = "/accountextensioncount", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get record count of account extension")
    public Map<String, Object> getAccountExtensionCount(
            HttpServletRequest request,
            @ApiParam(value = "Last Modification date in Unix timestamp on Account Extension", required = true) @RequestParam(value = "start", required = true) long start,
            @ApiParam(value = "Account Id whose extension columns are returned; all account Ids if not specified. This is mutual exclusive to filberBy/recStart.", required = false) @RequestParam(value = "accountId", required = false) List<Integer> accountIds,
            @ApiParam(value = "filterBy is a flag to filter Account Extensions with Recommendations, NoRecommendations or All, which "
                    + "are also its predefined values. NOTE: in terms of Recommendations and NoRecommendations, parameter recStart needs to be used to locate recommendations modified since recStart. "
                    + "This is mutual exclusive to accountId.", required = false) @RequestParam(value = "filterBy", required = false) String filterBy,
            @ApiParam(value = "The Last Modification date in unix timestamp on Recommendation, only used together with filterBy=Recommendations or NoRecommendations", required = false) @RequestParam(value = "recStart", required = false) Long recStart) {

        String tenantName = OAuth2Utils.getTenantName(request, oAuthUserEntityMgr);
        return playmakerRecommendationMgr.getAccountextExsionCount(tenantName, start, accountIds, filterBy, recStart);
    }

    @RequestMapping(value = "/accountextensionschema", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get account extensions")
    public List<Map<String, Object>> getAccountExtensionSchema(HttpServletRequest request) {
        String tenantName = OAuth2Utils.getTenantName(request, oAuthUserEntityMgr);
        return playmakerRecommendationMgr.getAccountExtensionSchema(tenantName);
    }

    @RequestMapping(value = "/accountextensioncolumncount", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get column count of account extension")
    public Map<String, Object> getAccountExtensionColumnCount(HttpServletRequest request) {
        String tenantName = OAuth2Utils.getTenantName(request, oAuthUserEntityMgr);
        return playmakerRecommendationMgr.getAccountExtensionColumnCount(tenantName);
    }

    @RequestMapping(value = "/contacts", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get contacts")
    public Map<String, Object> getContacts(
            HttpServletRequest request,
            @ApiParam(value = "Last Modification date in Unix timestamp", required = true) @RequestParam(value = "start", required = true) long start,
            @ApiParam(value = "First record number from start", required = true) @RequestParam(value = "offset", required = true) int offset,
            @ApiParam(value = "Maximum records returned above offset", required = true) @RequestParam(value = "maximum", required = true) int maximum,
            @ApiParam(value = "Contact Id whose contacts are returned; all contact Ids if not specified", required = false) @RequestParam(value = "contactId", required = false) List<Integer> contactIds) {

        String tenantName = OAuth2Utils.getTenantName(request, oAuthUserEntityMgr);
        return playmakerRecommendationMgr.getContacts(tenantName, start, offset, maximum, contactIds);
    }

    @RequestMapping(value = "/contactcount", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get contact count")
    public Map<String, Object> getContactCount(
            HttpServletRequest request,
            @ApiParam(value = "Last Modification date in Unix timestamp", required = true) @RequestParam(value = "start", required = true) long start,
            @ApiParam(value = "Contact Id whose plays are returned; all play group Ids if not specified", required = false) @RequestParam(value = "contactId", required = false) List<Integer> contactIds) {

        String tenantName = OAuth2Utils.getTenantName(request, oAuthUserEntityMgr);
        return playmakerRecommendationMgr.getContactCount(tenantName, start, contactIds);
    }

    @RequestMapping(value = "/contactextensions", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get contact extensions")
    public Map<String, Object> getContactExtensions(
            HttpServletRequest request,
            @ApiParam(value = "Last Modification date in Unix timestamp", required = true) @RequestParam(value = "start", required = true) long start,
            @ApiParam(value = "First record number from start", required = true) @RequestParam(value = "offset", required = true) int offset,
            @ApiParam(value = "Maximum records returned above offset", required = true) @RequestParam(value = "maximum", required = true) int maximum,
            @ApiParam(value = "Contact Id whose extension columns are returned; all contact Ids if not specified", required = false) @RequestParam(value = "contactId", required = false) List<Integer> contactIds) {

        String tenantName = OAuth2Utils.getTenantName(request, oAuthUserEntityMgr);
        return playmakerRecommendationMgr.getContactExtensions(tenantName, start, offset, maximum, contactIds);
    }

    @RequestMapping(value = "/contactextensioncount", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get record count of contact extension")
    public Map<String, Object> getContactExtensionCount(
            HttpServletRequest request,
            @ApiParam(value = "Last Modification date in Unix timestamp", required = true) @RequestParam(value = "start", required = true) long start,
            @ApiParam(value = "Contact Id whose extension columns are returned; all contact Ids if not specified", required = false) @RequestParam(value = "contactId", required = false) List<Integer> contactIds) {

        String tenantName = OAuth2Utils.getTenantName(request, oAuthUserEntityMgr);
        return playmakerRecommendationMgr.getContactExtensionCount(tenantName, start, contactIds);
    }

    @RequestMapping(value = "/contactextensionschema", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get Contact extensions")
    public List<Map<String, Object>> getContactExtensionSchema(HttpServletRequest request) {
        String tenantName = OAuth2Utils.getTenantName(request, oAuthUserEntityMgr);
        return playmakerRecommendationMgr.getContactExtensionSchema(tenantName);
    }

    @RequestMapping(value = "/contactextensioncolumncount", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get column count of contact extension")
    public Map<String, Object> getContactExtensionColumnCount(HttpServletRequest request) {
        String tenantName = OAuth2Utils.getTenantName(request, oAuthUserEntityMgr);
        return playmakerRecommendationMgr.getContactExtensionColumnCount(tenantName);
    }

    @RequestMapping(value = "/playvalues", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get flexible play values")
    public Map<String, Object> getPlayValues(
            HttpServletRequest request,
            @ApiParam(value = "Last Modification date in Unix timestamp", required = true) @RequestParam(value = "start", required = true) long start,
            @ApiParam(value = "First record number from start", required = true) @RequestParam(value = "offset", required = true) int offset,
            @ApiParam(value = "Maximum records returned above offset", required = true) @RequestParam(value = "maximum", required = true) int maximum,
            @ApiParam(value = "Play group's Id whose plays are returned; all play group Ids if not specified", required = false) @RequestParam(value = "playgroupId", required = false) List<Integer> playgroupIds) {

        String tenantName = OAuth2Utils.getTenantName(request, oAuthUserEntityMgr);
        return playmakerRecommendationMgr.getPlayValues(tenantName, start, offset, maximum, playgroupIds);
    }

    @RequestMapping(value = "/playvaluecount", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get flexible play value count")
    public Map<String, Object> getPlayValues(
            HttpServletRequest request,
            @ApiParam(value = "Last Modification date in Unix timestamp", required = true) @RequestParam(value = "start", required = true) long start,
            @ApiParam(value = "Play group's Id whose plays are returned; all play group Ids if not specified", required = false) @RequestParam(value = "playgroupId", required = false) List<Integer> playgroupIds) {

        String tenantName = OAuth2Utils.getTenantName(request, oAuthUserEntityMgr);
        return playmakerRecommendationMgr.getPlayValueCount(tenantName, start, playgroupIds);
    }

    @RequestMapping(value = "/workflowtypes", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get workflow types' IDs and Names")
    public List<Map<String, Object>> getWorkflowTypes(HttpServletRequest request) {

        String tenantName = OAuth2Utils.getTenantName(request, oAuthUserEntityMgr);
        return playmakerRecommendationMgr.getWorkflowTypes(tenantName);
    }

    @RequestMapping(value = "/playgroupcount", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get record count of play groups")
    public Map<String, Object> getPlayGroupCount(
            HttpServletRequest request,
            @ApiParam(value = "Last Modification date in Unix timestamp", required = true) @RequestParam(value = "start", required = true) long start) {

        String tenantName = OAuth2Utils.getTenantName(request, oAuthUserEntityMgr);
        return playmakerRecommendationMgr.getPlayGroupCount(tenantName, start);
    }

    @RequestMapping(value = "/playgroups", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all play groups' IDs and Names")
    public List<Map<String, Object>> getPlayGroups(
            HttpServletRequest request,
            @ApiParam(value = "Last Modification date in Unix timestamp", required = true) @RequestParam(value = "start", required = true) long start,
            @ApiParam(value = "First record number from start", required = true) @RequestParam(value = "offset", required = true) int offset,
            @ApiParam(value = "Maximum records returned above offset", required = true) @RequestParam(value = "maximum", required = true) int maximum) {

        String tenantName = OAuth2Utils.getTenantName(request, oAuthUserEntityMgr);
        return playmakerRecommendationMgr.getPlayGroups(tenantName, start, offset, maximum);
    }

}
