package com.latticeengines.playmaker.controller;

import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.playmakercore.SynchronizationDestinationEnum;
import com.latticeengines.oauth2db.exposed.entitymgr.OAuthUserEntityMgr;
import com.latticeengines.oauth2db.exposed.util.OAuth2Utils;
import com.latticeengines.playmaker.entitymgr.PlaymakerRecommendationEntityMgr;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

@Api(value = "Playmaker recommendation api", description = "REST resource for getting playmaker recomendationss")
@RestController
@RequestMapping(value = "/playmaker")
public class RecommendationResource {

    @Autowired
    private PlaymakerRecommendationEntityMgr playmakerRecommendationMgr;

    @Autowired
    private OAuthUserEntityMgr oAuthUserEntityMgr;

    @RequestMapping(value = "/recommendations", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get recommendations")
    public Map<String, Object> getRecommendations(HttpServletRequest request,
            @RequestHeader(value = "PREDICTIVE_PLATFORM", required = false) String lookupSource,
            @ApiParam(value = "Last Modification date in Unix timestamp", required = true) @RequestParam(value = "start", required = true) long start,
            @ApiParam(value = "First record number from start", required = true) @RequestParam(value = "offset", required = true) int offset,
            @ApiParam(value = "Maximum records returned above offset", required = true) @RequestParam(value = "maximum", required = true) int maximum,
            @ApiParam(value = "Synchronization Destination: SFDC | MAP | SFDC_AND_MAP", required = true) @RequestParam(value = "destination", required = true) String destination,
            @ApiParam(value = "Play's Id whose recommendations are returned", required = false) @RequestParam(value = "playId", required = false) List<String> playIds) {

        String tenantName = OAuth2Utils.getTenantName(request, oAuthUserEntityMgr);
        return playmakerRecommendationMgr.getRecommendations(tenantName, lookupSource, start, offset, maximum,
                SynchronizationDestinationEnum.mapToIntType(destination), playIds);
    }

    @RequestMapping(value = "/recommendationcount", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get recommendation count")
    public Map<String, Object> getRecommendationCount(HttpServletRequest request,
            @RequestHeader(value = "PREDICTIVE_PLATFORM", required = false) String lookupSource,
            @ApiParam(value = "Last Modification date in Unix timestamp", required = true) @RequestParam(value = "start", required = true) long start,
            @ApiParam(value = "Synchronization Destination: SFDC | MAP | SFDC_AND_MAP", required = true) @RequestParam(value = "destination", required = true) String destination,
            @ApiParam(value = "Play's Id whose recommendations are returned; all play Ids if not specified", required = false) @RequestParam(value = "playId", required = false) List<String> playIds) {

        String tenantName = OAuth2Utils.getTenantName(request, oAuthUserEntityMgr);
        return playmakerRecommendationMgr.getRecommendationCount(tenantName, lookupSource, start,
                SynchronizationDestinationEnum.mapToIntType(destination), playIds);
    }

    @RequestMapping(value = "/plays", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get plays")
    public Map<String, Object> getPlays(HttpServletRequest request,
            @RequestHeader(value = "PREDICTIVE_PLATFORM", required = false) String lookupSource,
            @ApiParam(value = "Last Modification date in Unix timestamp", required = true) @RequestParam(value = "start", required = true) long start,
            @ApiParam(value = "First record number from start", required = true) @RequestParam(value = "offset", required = true) int offset,
            @ApiParam(value = "Maximum records returned above offset", required = true) @RequestParam(value = "maximum", required = true) int maximum,
            @ApiParam(value = "Play group's Id whose plays are returned; all play group Ids if not specified", required = false) @RequestParam(value = "playgroupId", required = false) List<Integer> playgroupIds) {

        String tenantName = OAuth2Utils.getTenantName(request, oAuthUserEntityMgr);
        return playmakerRecommendationMgr.getPlays(tenantName, lookupSource, start, offset, maximum, playgroupIds);
    }

    @RequestMapping(value = "/playcount", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get play count")
    public Map<String, Object> getPlayCount(HttpServletRequest request,
            @RequestHeader(value = "PREDICTIVE_PLATFORM", required = false) String lookupSource,
            @ApiParam(value = "Last Modification date in Unix timestamp", required = true) @RequestParam(value = "start", required = true) long start,
            @ApiParam(value = "Play group's Id whose plays are returned; all play group Ids if not specified", required = false) @RequestParam(value = "playgroupId", required = false) List<Integer> playgroupIds) {

        String tenantName = OAuth2Utils.getTenantName(request, oAuthUserEntityMgr);
        return playmakerRecommendationMgr.getPlayCount(tenantName, lookupSource, start, playgroupIds);
    }

    @RequestMapping(value = "/accountextensions", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get account extensions")
    public Map<String, Object> getAccountExtensions(HttpServletRequest request,
            @RequestHeader(value = "PREDICTIVE_PLATFORM", required = false) String lookupSource,
            @ApiParam(value = "Last Modification date in Unix timestamp", required = true) @RequestParam(value = "start", required = true) long start,
            @ApiParam(value = "First record number from start", required = true) @RequestParam(value = "offset", required = true) int offset,
            @ApiParam(value = "Maximum records returned above offset", required = true) @RequestParam(value = "maximum", required = true) int maximum,
            @ApiParam(value = "Account Id whose extension columns are returned; all account Ids if not specified. This is mutual exclusive to filberBy/recStart.", required = false) @RequestParam(value = "accountId", required = false) List<String> accountIds,
            @ApiParam(value = "filterBy is a flag to filter Account Extensions with Recommendations, NoRecommendations or All, which "
                    + "are also its predefined values. NOTE: in terms of Recommendations and NoRecommendations, parameter recStart needs to be used to locate recommendations modified since recStart. "
                    + "This is mutual exclusive to accountId.", required = false) @RequestParam(value = "filterBy", required = false) String filterBy,
            @ApiParam(value = "The Last Modification date in unix timestamp on Recommendation, only used together with filterBy=Recommendations or NoRecommendations", required = false) @RequestParam(value = "recStart", required = false) Long recStart,
            @ApiParam(value = "columns are selected column names for output; column names are delimited by a comma.", required = false) @RequestParam(value = "columns", required = false) String columns,
            @ApiParam(value = "true - populate column SfdcContactId, false - does NOT populate column SfdcContactId", required = false) @RequestParam(value = "hasSfdcContactId", required = false) String hasSfdcContactId) {

        String tenantName = OAuth2Utils.getTenantName(request, oAuthUserEntityMgr);
        Map<String, Object> accountExtensions = playmakerRecommendationMgr.getAccountExtensions(tenantName,
                lookupSource, start, offset, maximum, accountIds, filterBy, recStart, columns,
                "true".equals(hasSfdcContactId));
        return accountExtensions;
    }

    @RequestMapping(value = "/accountextensioncount", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get record count of account extension")
    public Map<String, Object> getAccountExtensionCount(HttpServletRequest request,
            @RequestHeader(value = "PREDICTIVE_PLATFORM", required = false) String lookupSource,
            @ApiParam(value = "Last Modification date in Unix timestamp on Account Extension", required = true) @RequestParam(value = "start", required = true) long start,
            @ApiParam(value = "Account Id whose extension columns are returned; all account Ids if not specified. This is mutual exclusive to filberBy/recStart.", required = false) @RequestParam(value = "accountId", required = false) List<String> accountIds,
            @ApiParam(value = "filterBy is a flag to filter Account Extensions with Recommendations, NoRecommendations or All, which "
                    + "are also its predefined values. NOTE: in terms of Recommendations and NoRecommendations, parameter recStart needs to be used to locate recommendations modified since recStart. "
                    + "This is mutual exclusive to accountId.", required = false) @RequestParam(value = "filterBy", required = false) String filterBy,
            @ApiParam(value = "The Last Modification date in unix timestamp on Recommendation, only used together with filterBy=Recommendations or NoRecommendations", required = false) @RequestParam(value = "recStart", required = false) Long recStart) {

        String tenantName = OAuth2Utils.getTenantName(request, oAuthUserEntityMgr);
        return playmakerRecommendationMgr.getAccountextExsionCount(tenantName, lookupSource, start, accountIds,
                filterBy, recStart);
    }

    @RequestMapping(value = "/accountextensionschema", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get account extensions")
    public List<Map<String, Object>> getAccountExtensionSchema(HttpServletRequest request,
            @RequestHeader(value = "PREDICTIVE_PLATFORM", required = false) String lookupSource) {
        String tenantName = OAuth2Utils.getTenantName(request, oAuthUserEntityMgr);
        return playmakerRecommendationMgr.getAccountExtensionSchema(tenantName, lookupSource);
    }

    @RequestMapping(value = "/accountextensioncolumncount", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get column count of account extension")
    public Map<String, Object> getAccountExtensionColumnCount(HttpServletRequest request,
            @RequestHeader(value = "PREDICTIVE_PLATFORM", required = false) String lookupSource) {
        String tenantName = OAuth2Utils.getTenantName(request, oAuthUserEntityMgr);
        return playmakerRecommendationMgr.getAccountExtensionColumnCount(tenantName, lookupSource);
    }

    @RequestMapping(value = "/contacts", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get contacts")
    public Map<String, Object> getContacts(HttpServletRequest request,
            @RequestHeader(value = "PREDICTIVE_PLATFORM", required = false) String lookupSource,
            @ApiParam(value = "Last Modification date in Unix timestamp", required = true) @RequestParam(value = "start", required = true) long start,
            @ApiParam(value = "First record number from start", required = true) @RequestParam(value = "offset", required = true) int offset,
            @ApiParam(value = "Maximum records returned above offset", required = true) @RequestParam(value = "maximum", required = true) int maximum,
            @ApiParam(value = "Lattice Contact Id whose contacts are returned; all contacts returned if not specified", required = false) @RequestParam(value = "contactId", required = false) List<Integer> contactIds,
            @ApiParam(value = "Lattice Account Id whose contacts are returned; all contacts returned if not specified", required = false) @RequestParam(value = "accountId", required = false) List<Integer> accountIds) {

        String tenantName = OAuth2Utils.getTenantName(request, oAuthUserEntityMgr);
        return playmakerRecommendationMgr.getContacts(tenantName, lookupSource, start, offset, maximum, contactIds,
                accountIds);
    }

    @RequestMapping(value = "/contactcount", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get contact count")
    public Map<String, Object> getContactCount(HttpServletRequest request,
            @RequestHeader(value = "PREDICTIVE_PLATFORM", required = false) String lookupSource,
            @ApiParam(value = "Last Modification date in Unix timestamp", required = true) @RequestParam(value = "start", required = true) long start,
            @ApiParam(value = "Lattice Contact Id whose contacts are returned; all contacts returned if not specified", required = false) @RequestParam(value = "contactId", required = false) List<Integer> contactIds,
            @ApiParam(value = "Lattice Account Id whose contacts are returned; all contacts returned if not specified", required = false) @RequestParam(value = "accountId", required = false) List<Integer> accountIds) {

        String tenantName = OAuth2Utils.getTenantName(request, oAuthUserEntityMgr);
        return playmakerRecommendationMgr.getContactCount(tenantName, lookupSource, start, contactIds, accountIds);
    }

    @RequestMapping(value = "/contactextensions", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get contact extensions")
    public Map<String, Object> getContactExtensions(HttpServletRequest request,
            @RequestHeader(value = "PREDICTIVE_PLATFORM", required = false) String lookupSource,
            @ApiParam(value = "Last Modification date in Unix timestamp", required = true) @RequestParam(value = "start", required = true) long start,
            @ApiParam(value = "First record number from start", required = true) @RequestParam(value = "offset", required = true) int offset,
            @ApiParam(value = "Maximum records returned above offset", required = true) @RequestParam(value = "maximum", required = true) int maximum,
            @ApiParam(value = "Contact Id whose extension columns are returned; all contact Ids if not specified", required = false) @RequestParam(value = "contactId", required = false) List<Integer> contactIds) {

        String tenantName = OAuth2Utils.getTenantName(request, oAuthUserEntityMgr);
        return playmakerRecommendationMgr.getContactExtensions(tenantName, lookupSource, start, offset, maximum,
                contactIds);
    }

    @RequestMapping(value = "/contactextensioncount", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get record count of contact extension")
    public Map<String, Object> getContactExtensionCount(HttpServletRequest request,
            @RequestHeader(value = "PREDICTIVE_PLATFORM", required = false) String lookupSource,
            @ApiParam(value = "Last Modification date in Unix timestamp", required = true) @RequestParam(value = "start", required = true) long start,
            @ApiParam(value = "Contact Id whose extension columns are returned; all contact Ids if not specified", required = false) @RequestParam(value = "contactId", required = false) List<Integer> contactIds) {

        String tenantName = OAuth2Utils.getTenantName(request, oAuthUserEntityMgr);
        return playmakerRecommendationMgr.getContactExtensionCount(tenantName, lookupSource, start, contactIds);
    }

    @RequestMapping(value = "/contactextensionschema", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get Contact extensions")
    public List<Map<String, Object>> getContactExtensionSchema(HttpServletRequest request,
            @RequestHeader(value = "PREDICTIVE_PLATFORM", required = false) String lookupSource) {
        String tenantName = OAuth2Utils.getTenantName(request, oAuthUserEntityMgr);
        return playmakerRecommendationMgr.getContactExtensionSchema(tenantName, lookupSource);
    }

    @RequestMapping(value = "/contactextensioncolumncount", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get column count of contact extension")
    public Map<String, Object> getContactExtensionColumnCount(HttpServletRequest request,
            @RequestHeader(value = "PREDICTIVE_PLATFORM", required = false) String lookupSource) {
        String tenantName = OAuth2Utils.getTenantName(request, oAuthUserEntityMgr);
        return playmakerRecommendationMgr.getContactExtensionColumnCount(tenantName, lookupSource);
    }

    @RequestMapping(value = "/playvalues", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get flexible play values")
    public Map<String, Object> getPlayValues(HttpServletRequest request,
            @RequestHeader(value = "PREDICTIVE_PLATFORM", required = false) String lookupSource,
            @ApiParam(value = "Last Modification date in Unix timestamp", required = true) @RequestParam(value = "start", required = true) long start,
            @ApiParam(value = "First record number from start", required = true) @RequestParam(value = "offset", required = true) int offset,
            @ApiParam(value = "Maximum records returned above offset", required = true) @RequestParam(value = "maximum", required = true) int maximum,
            @ApiParam(value = "Play group's Id whose plays are returned; all play group Ids if not specified", required = false) @RequestParam(value = "playgroupId", required = false) List<Integer> playgroupIds) {

        String tenantName = OAuth2Utils.getTenantName(request, oAuthUserEntityMgr);
        return playmakerRecommendationMgr.getPlayValues(tenantName, lookupSource, start, offset, maximum, playgroupIds);
    }

    @RequestMapping(value = "/playvaluecount", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get flexible play value count")
    public Map<String, Object> getPlayValues(HttpServletRequest request,
            @RequestHeader(value = "PREDICTIVE_PLATFORM", required = false) String lookupSource,
            @ApiParam(value = "Last Modification date in Unix timestamp", required = true) @RequestParam(value = "start", required = true) long start,
            @ApiParam(value = "Play group's Id whose plays are returned; all play group Ids if not specified", required = false) @RequestParam(value = "playgroupId", required = false) List<Integer> playgroupIds) {

        String tenantName = OAuth2Utils.getTenantName(request, oAuthUserEntityMgr);
        return playmakerRecommendationMgr.getPlayValueCount(tenantName, lookupSource, start, playgroupIds);
    }

    @RequestMapping(value = "/workflowtypes", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get workflow types' IDs and Names")
    public List<Map<String, Object>> getWorkflowTypes(HttpServletRequest request,
            @RequestHeader(value = "PREDICTIVE_PLATFORM", required = false) String lookupSource) {

        String tenantName = OAuth2Utils.getTenantName(request, oAuthUserEntityMgr);
        return playmakerRecommendationMgr.getWorkflowTypes(tenantName, lookupSource);
    }

    @RequestMapping(value = "/playgroupcount", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get record count of play groups")
    public Map<String, Object> getPlayGroupCount(HttpServletRequest request,
            @RequestHeader(value = "PREDICTIVE_PLATFORM", required = false) String lookupSource,
            @ApiParam(value = "Last Modification date in Unix timestamp", required = true) @RequestParam(value = "start", required = true) long start) {

        String tenantName = OAuth2Utils.getTenantName(request, oAuthUserEntityMgr);
        return playmakerRecommendationMgr.getPlayGroupCount(tenantName, lookupSource, start);
    }

    @RequestMapping(value = "/playgroups", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all play groups' IDs and Names")
    public List<Map<String, Object>> getPlayGroups(HttpServletRequest request,
            @RequestHeader(value = "PREDICTIVE_PLATFORM", required = false) String lookupSource,
            @ApiParam(value = "Last Modification date in Unix timestamp", required = true) @RequestParam(value = "start", required = true) long start,
            @ApiParam(value = "First record number from start", required = true) @RequestParam(value = "offset", required = true) int offset,
            @ApiParam(value = "Maximum records returned above offset", required = true) @RequestParam(value = "maximum", required = true) int maximum) {
        String tenantName = OAuth2Utils.getTenantName(request, oAuthUserEntityMgr);
        return playmakerRecommendationMgr.getPlayGroups(tenantName, lookupSource, start, offset, maximum);
    }

    @RequestMapping(value = "/oauthtotenant", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get tenant info from OAuth token")
    public String getOauthTokenToTenant(HttpServletRequest request) {
        return OAuth2Utils.getTenantName(request, oAuthUserEntityMgr);
    }
}
