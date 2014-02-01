package com.latticeengines.api.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.latticeengines.api.domain.AppSubmission;
import com.latticeengines.dataplatform.exposed.domain.Model;
import com.latticeengines.dataplatform.exposed.service.ModelingService;

@Controller
public class ModelResource {
	
	@Autowired
	private ModelingService modelingService;

    @RequestMapping(value = "/submit", method = RequestMethod.POST, 
    				headers="Accept=application/xml, application/json")
    @ResponseBody
	public AppSubmission submit(@RequestBody Model model) {
    	AppSubmission submission = new AppSubmission(modelingService.submitModel(model));
    	return submission;
    }
}
