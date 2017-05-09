package com.latticeengines.domain.exposed.pls;

import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;

public interface HasAttributeCustomizations {

    Map<AttributeUseCase, JsonNode> getAttributeFlagsMap();

    void setAttributeFlagsMap(Map<AttributeUseCase, JsonNode> attributeFlagsMap);

    String getSubcategory();

    String getCategoryAsString();

    String getColumnId();
}
