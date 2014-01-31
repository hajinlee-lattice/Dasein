package com.latticeengines.dataplatform.exposed.service.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerTypeInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.latticeengines.dataplatform.exposed.service.YarnService;

@Component("yarnService")
public class YarnServiceImpl implements YarnService {

	private RestTemplate rmRestTemplate = new RestTemplate();
	
	@Autowired
	private Configuration yarnConfiguration;
	
	@Override
	public SchedulerTypeInfo getSchedulerInfo() {
		String rmRestEndpointBaseUrl = yarnConfiguration.get("yarn.resourcemanager.webapp.address"); 
		return rmRestTemplate.getForObject("http://" + rmRestEndpointBaseUrl + "/ws/v1/cluster/scheduler", SchedulerTypeInfo.class);
	}
}
