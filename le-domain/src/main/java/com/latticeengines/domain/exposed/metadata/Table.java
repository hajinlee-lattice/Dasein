package com.latticeengines.domain.exposed.metadata;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasName;

public class Table extends AttributeOwner implements HasName {

    private String name;
    private String displayName;

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @JsonProperty("display_name")
    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    @JsonProperty("display_name")
    public String getDisplayName() {
        return displayName;
    }

	@Override
	public String toString() {
		return JsonUtils.serialize(this);
	}
}
