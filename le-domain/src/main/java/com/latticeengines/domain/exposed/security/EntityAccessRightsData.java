package com.latticeengines.domain.exposed.security;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

public class EntityAccessRightsData {
    private boolean mayEdit = false;
    private boolean mayCreate = false;
    private boolean mayExecute = false;
    private boolean mayView = false;

    public EntityAccessRightsData() {

    }

    @JsonProperty("MayEdit")
    public boolean isMayEdit() {
        return mayEdit;
    }

    @JsonProperty("MayEdit")
    public void setMayEdit(boolean mayEdit) {
        this.mayEdit = mayEdit;
    }

    @JsonProperty("MayCreate")
    public boolean isMayCreate() {
        return mayCreate;
    }

    @JsonProperty("MayCreate")
    public void setMayCreate(boolean mayCreate) {
        this.mayCreate = mayCreate;
    }

    @JsonProperty("MayExecute")
    public boolean isMayExecute() {
        return mayExecute;
    }

    @JsonProperty("MayExecute")
    public void setMayExecute(boolean mayExecute) {
        this.mayExecute = mayExecute;
    }

    @JsonProperty("MayView")
    public boolean isMayView() {
        return mayView;
    }

    @JsonProperty("MayView")
    public void setMayView(boolean mayView) {
        this.mayView = mayView;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }
}
