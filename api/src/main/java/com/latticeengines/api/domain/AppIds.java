package com.latticeengines.api.domain;

import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.yarn.api.records.ApplicationId;

@XmlRootElement(name="applicationIds")
public class AppIds {

	private List<ApplicationId> applicationIds;
	
	public AppIds() {
		
	}
	
	public AppIds(List<ApplicationId> applicationIds) {
		this.setApplicationIds(applicationIds);
	}

	public List<ApplicationId> getApplicationIds() {
		return applicationIds;
	}

	public void setApplicationIds(List<ApplicationId> applicationIds) {
		this.applicationIds = applicationIds;
	}
}
 