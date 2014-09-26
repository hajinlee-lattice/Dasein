package com.latticeengines.camille.initialization;

public abstract class Initializer {
	private String podId = null;
	private String connectionString = null;
	
	protected Initializer() { }
	
	public String getPodId() {
		return podId;
	}
	
	public void setPodId(String podId) {
		this.podId = podId;
	}
	
	public String getConnectionString() {
		return connectionString;
	}
	
	public void setConnectionString(String connectionString) {
		this.connectionString = connectionString;
	}
}
