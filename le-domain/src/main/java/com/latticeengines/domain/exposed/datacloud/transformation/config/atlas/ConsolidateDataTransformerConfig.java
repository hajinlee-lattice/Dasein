package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

public class ConsolidateDataTransformerConfig extends TransformerConfig {

    private String srcIdField;
    private String masterIdField;

    private boolean createTimestampColumn;

    private Set<String> columnsFromRight;
    private List<String> compositeKeys;
    private boolean dedupeSource;
    private boolean addTimestamps;
    private boolean mergeOnly;
    private boolean inputLast;
    private List<String> trimFields = getDefaultTrimFields();

    /*****************************************************************
     * Following operations apply to every source input before merge.
     *
     * Sequence: clone -> rename
     *****************************************************************/

    // String[][0]: original field;
    // String[][1]: NEW field copied value from original field
    @JsonProperty("CloneSrcFields")
    private String[][] cloneSrcFields;

    // String[][0]: old field; String[][1]: new field
    @JsonProperty("RenameSrcFields")
    private String[][] renameSrcFields;

    public static List<String> getDefaultTrimFields() {
        return new ArrayList<>(Arrays.asList(InterfaceName.CompanyName.name(),
                InterfaceName.City.name(), InterfaceName.State.name(), InterfaceName.Country.name(),
                InterfaceName.Website.name(), InterfaceName.ContactName.name()));
    }

    public String getSrcIdField() {
        return srcIdField;
    }

    public void setSrcIdField(String srcIdField) {
        this.srcIdField = srcIdField;
    }

    public String getMasterIdField() {
        return masterIdField;
    }

    public void setMasterIdField(String masterIdField) {
        this.masterIdField = masterIdField;
    }

    public boolean isCreateTimestampColumn() {
        return createTimestampColumn;
    }

    public void setCreateTimestampColumn(boolean createTimestampColumn) {
        this.createTimestampColumn = createTimestampColumn;
    }

    public Set<String> getColumnsFromRight() {
        return columnsFromRight;
    }

    public void setColumnsFromRight(Set<String> columnsFromRight) {
        this.columnsFromRight = columnsFromRight;
    }

    public List<String> getCompositeKeys() {
        return compositeKeys;
    }

    public void setCompositeKeys(List<String> compositeKeys) {
        this.compositeKeys = compositeKeys;
    }

    public boolean isDedupeSource() {
        return dedupeSource;
    }

    public void setDedupeSource(boolean isDedupeSource) {
        this.dedupeSource = isDedupeSource;
    }

    public boolean isAddTimestamps() {
        return addTimestamps;
    }

    public void setAddTimestamps(boolean addTimestamps) {
        this.addTimestamps = addTimestamps;
    }

    public boolean isMergeOnly() {
        return mergeOnly;
    }

    public void setMergeOnly(boolean mergeOnly) {
        this.mergeOnly = mergeOnly;
    }

    public boolean isInputLast() {
        return inputLast;
    }

    public void setInputLast(boolean inputLast) {
        this.inputLast = inputLast;
    }

    public List<String> getTrimFields() {
        return trimFields;
    }

    public void setTrimFields(List<String> trimFields) {
        this.trimFields = trimFields;
    }

    public String[][] getCloneSrcFields() {
        return cloneSrcFields;
    }

    public void setCloneSrcFields(String[][] cloneSrcFields) {
        this.cloneSrcFields = cloneSrcFields;
    }

    public String[][] getRenameSrcFields() {
        return renameSrcFields;
    }

    public void setRenameSrcFields(String[][] renameSrcFields) {
        this.renameSrcFields = renameSrcFields;
    }

    /**
     * ConsolidateDataTransformerConfig builder
     */
    public static class ConsolidateDataTxmfrConfigBuilder {
        private ConsolidateDataTransformerConfig config;

        public ConsolidateDataTxmfrConfigBuilder() {
            this.config = new ConsolidateDataTransformerConfig();
        }

        public ConsolidateDataTxmfrConfigBuilder(ConsolidateDataTransformerConfig config) {
            this.config = config;
        }

        public ConsolidateDataTxmfrConfigBuilder srcIdField(String srcIdField) {
            config.srcIdField = srcIdField;
            return this;
        }

        public ConsolidateDataTxmfrConfigBuilder masterIdField(String masterIdField) {
            config.masterIdField = masterIdField;
            return this;
        }

        public ConsolidateDataTxmfrConfigBuilder createTSColumn(boolean createTimestampColumn) {
            config.createTimestampColumn = createTimestampColumn;
            return this;
        }

        public ConsolidateDataTxmfrConfigBuilder columnsFromRight(Set<String> columnsFromRight) {
            config.columnsFromRight = columnsFromRight;
            return this;
        }

        public ConsolidateDataTxmfrConfigBuilder compositeKeys(List<String> compositeKeys) {
            config.compositeKeys = compositeKeys;
            return this;
        }

        public ConsolidateDataTxmfrConfigBuilder dedupeSource(boolean dedupeSource) {
            config.dedupeSource = dedupeSource;
            return this;
        }

        public ConsolidateDataTxmfrConfigBuilder addTimestamps(boolean addTimestamps) {
            config.addTimestamps = addTimestamps;
            return this;
        }

        public ConsolidateDataTxmfrConfigBuilder mergeOnly(boolean mergeOnly) {
            config.mergeOnly = mergeOnly;
            return this;
        }

        public ConsolidateDataTxmfrConfigBuilder inputLast(boolean inputLast) {
            config.inputLast = inputLast;
            return this;
        }

        public ConsolidateDataTxmfrConfigBuilder cloneSrcFields(String[][] cloneSrcFields) {
            config.cloneSrcFields = cloneSrcFields;
            return this;
        }

        public ConsolidateDataTxmfrConfigBuilder renameSrcFields(String[][] renameSrcFields) {
            config.renameSrcFields = renameSrcFields;
            return this;
        }

        public ConsolidateDataTransformerConfig build() {
            return config;
        }
    }
}
