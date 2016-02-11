package com.latticeengines.pls.controller;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.pls.Contact;
import com.latticeengines.pls.entitymanager.ContactEntityMgr;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "contact", description = "REST resource for contacts")
@RestController
@RequestMapping("/contacts")
// @PreAuthorize("hasRole('View_PLS_Data')")
public class ContactResource {

    @Autowired
    private ContactEntityMgr contactEntityMgr;

    @RequestMapping(value = "/count", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get number of contacts which meet select criterias")
    public Long getContactCount(@RequestParam Map<String,String> reqParams) {

        return contactEntityMgr.findContactCount(reqParams);
    }

    @RequestMapping(value = "/{contactId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get detail for specific contact")
    public Contact getContact(@PathVariable String contactId) {
        return contactEntityMgr.findById(contactId);
    }

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of contacts which meet select criterias")
    public List<Contact> getContacts(@RequestParam Map<String,String> reqParams) {

        return contactEntityMgr.findContacts(reqParams);
    }
}
