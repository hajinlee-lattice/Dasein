package com.latticeengines.propdata.service.db;

public class PropDataKey {
	
	public enum CommandsKey {
		COMMAND_NAME("commandName"),
		CONTRACT_EXTERNAL_ID("contractExternalID"),
		DEPLOYMENT_EXTERNAL_ID("deploymentExternalID"),
		DESTTABLES("destTables"),
		IS_DOWNLOADING("isDownloading"),
		MAX_NUMR_ETRIES("maxNumRetries"),
		PROCESS_UID("processUID"),
		SOURCE_TABLE("sourceTable"),
		
		COMMAND_ID("commandId");
		
		String value;
		private CommandsKey(String value) {
			this.value = "commands-" + value;
		}
		public String getValue() {
			return  value;
		}
	}
	
	public enum CommandIdsKey {
		CREATED_BY("createdBy");
		
		String value;
		private CommandIdsKey(String value) {
			this.value = "commandIds-" + value;
		}
		public String getValue() {
			return value;
		}
	}
	
	public enum ImportExportKey {
		
		CUSTOMER("customer"),
		TABLE("table"),
		KEY_COLS("keyCols"),
		APPLICATION_ID("applicationId"),

		MAP_COLUMN("mapColumn");
		
		String value;
		private ImportExportKey(String value) {
			this.value = "ie-" + value;
		}
		public String getValue() {
			return value;
		}
	}

}
