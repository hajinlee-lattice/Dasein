package com.latticeengines.pls.controller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.AmAttribute;
import com.latticeengines.pls.entitymanager.AmAttributeEntityMgr;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "amattibute", description = "REST resource for account master attributes")
@RestController
@RequestMapping("/amattributes")
@PreAuthorize("hasRole('View_PLS_Reports')")
public class AmAttributeResource {

    private static final Log log = LogFactory.getLog(AmAttributeResource.class);

    @Autowired
    private AmAttributeEntityMgr amAttributeEntityMgr;

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of atributes which meet defined criterias")
    public List<List<AmAttribute>> getAttributes(@RequestParam(value = "populate", required = false, defaultValue = "false") String populateStr,
                                                 @RequestParam(value = "queries") String qString) {

        boolean populate = Boolean.parseBoolean(populateStr);
        List<Map<String, String>> paramList;

        try {
            ObjectMapper objMapper = new ObjectMapper();
            TypeReference<List<Map<String,String>>> typeRef
                = new TypeReference<List<Map<String,String>>>(){};

            paramList = objMapper.readValue(qString, typeRef);
        }  catch (JsonGenerationException e) {
            throw new LedpException(LedpCode.LEDP_18075, new String[] { qString });
        } catch (JsonMappingException e) {
            throw new LedpException(LedpCode.LEDP_18075, new String[] { qString });
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_18075, new String[] { qString });
        }
        ArrayList<List<AmAttribute>> attrList = new ArrayList<List<AmAttribute>>();
        for (Map<String, String> params : paramList) {
            attrList.add(getAttributesByKey(params, populate));
        }
        return attrList;
    }

    public List<AmAttribute> getAttributesByKey(Map<String,String> params, boolean populate) {
        String key = params.get("AttrKey");
        String parentKey = params.get("ParentKey");
        String parentValue = params.get("ParentValue");

        log.info("search attributes Key=" + key + " pKey=" + parentKey + " pValue=" + parentValue + " populate=" + populate);

        return amAttributeEntityMgr.findAttributes(key, parentKey, parentValue, populate);
    }

    public AmAttributeEntityMgr getEntityMgr() {
        return this.amAttributeEntityMgr;
    }

}
