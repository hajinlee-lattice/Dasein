package com.latticeengines.propdata.eai.service;

public class PropDataKey {

    public enum CommandsKey {
        COMMAND_NAME("commandName"), CONTRACT_EXTERNAL_ID("contractExternalID"), DEPLOYMENT_EXTERNAL_ID(
                "deploymentExternalID"), DESTTABLES("destTables"), IS_DOWNLOADING("isDownloading"), MAX_NUMR_ETRIES(
                "maxNumRetries"), PROCESS_UID("processUID"), SOURCE_TABLE("sourceTable"),

        COMMAND_ID("commandId");

        String key;

        private CommandsKey(String key) {
            this.key = "commands-" + key;
        }

        public String getKey() {
            return key;
        }
    }

    public enum CommandIdsKey {
        CREATED_BY("createdBy");

        String key;

        private CommandIdsKey(String key) {
            this.key = "commandIds-" + key;
        }

        public String getKey() {
            return key;
        }
    }

    public enum ImportExportKey {

        CUSTOMER("customer"), TABLE("table"), KEY_COLS("keyCols"), APPLICATION_ID("applicationId"),

        MAP_COLUMN("mapColumn");

        String key;

        private ImportExportKey(String key) {
            this.key = "ie-" + key;
        }

        public String getKey() {
            return key;
        }
    }

}
