package com.latticeengines.domain.exposed.datacloud.match.patch;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.manage.PatchBook;

import java.util.List;

/**
 * Error to indicate a constraint violation of one or more {@link PatchBook}s
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PatchBookValidationError {

    @JsonProperty("Message")
    private String message;

    @JsonProperty("PatchBookIds")
    private List<Long> patchBookIds;

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public List<Long> getPatchBookIds() {
        return patchBookIds;
    }

    public void setPatchBookIds(List<Long> patchBookIds) {
        this.patchBookIds = patchBookIds;
    }
}
