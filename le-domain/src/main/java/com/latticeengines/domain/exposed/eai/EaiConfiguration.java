package com.latticeengines.domain.exposed.eai;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class EaiConfiguration {

	private String customer;
	private List<Table> tables = new ArrayList<>();
	private String targetPath;

	@JsonProperty("customer")
	public String getCustomer() {
		return customer;
	}

	@JsonProperty("customer")
	public void setCustomer(String customer) {
		this.customer = customer;
	}

	@JsonProperty("tables")
	public List<Table> getTables() {
		return tables;
	}

	@JsonProperty("tables")
	public void setTables(List<Table> tables) {
		this.tables = tables;
	}

	@JsonProperty("target_path")
	public String getTargetPath() {
		return targetPath;
	}

	@JsonProperty("target_path")
	public void setTargetPath(String targetPath) {
		this.targetPath = targetPath;
	}
}
