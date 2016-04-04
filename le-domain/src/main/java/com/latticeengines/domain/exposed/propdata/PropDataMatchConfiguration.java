package com.latticeengines.domain.exposed.propdata;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;

public class PropDataMatchConfiguration {

    private String customer;
    private ColumnSelection.Predefined predefinedSelection;
    private ColumnSelection customizedSelection;
    private String rootOperationUid;
    private String inputDir;

    @JsonProperty("customer")
    public String getCustomer() {
        return customer;
    }

    @JsonProperty("customer")
    public void setCustomer(String customer) {
        this.customer = customer;
    }

    @JsonProperty("predefined_selection")
    public ColumnSelection.Predefined getPredefinedSelection() {
        return predefinedSelection;
    }

    @JsonProperty("predefined_selection")
    public void setPredefinedSelection(ColumnSelection.Predefined predefinedSelection) {
        this.predefinedSelection = predefinedSelection;
    }

    @JsonProperty("customized_selection")
    public ColumnSelection getCustomizedSelection() {
        return customizedSelection;
    }

    @JsonProperty("customized_selection")
    public void setCustomizedSelection(ColumnSelection customizedSelection) {
        this.customizedSelection = customizedSelection;
    }

    @JsonProperty("input_dir")
    public String getInputDir() {
        return inputDir;
    }

    @JsonProperty("input_dir")
    public void setInputDir(String inputDir) {
        this.inputDir = inputDir;
    }

    @JsonProperty("root_operation_uid")
    public String getRootOperationUid() {
        return rootOperationUid;
    }

    @JsonProperty("root_operation_uid")
    public void setRootOperationUid(String rootOperationUid) {
        this.rootOperationUid = rootOperationUid;
    }
}
