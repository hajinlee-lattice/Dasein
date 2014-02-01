package com.latticeengines.dataplatform.exposed.domain;

import org.codehaus.jackson.annotate.JsonProperty;

public class Algorithm implements HasName {

	private String name;
	private String script;
	
	@Override
	@JsonProperty("name")
	public String getName() {
		return name;
	}

	@Override
	@JsonProperty("name")
	public void setName(String name) {
		this.name = name;
	}

	@JsonProperty("script")
	public String getScript() {
		return script;
	}

	@JsonProperty("script")
	public void setScript(String script) {
		this.script = script;
	}

}
