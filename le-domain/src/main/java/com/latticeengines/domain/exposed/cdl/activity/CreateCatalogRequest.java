package com.latticeengines.domain.exposed.cdl.activity;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.validator.annotation.NotNull;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class CreateCatalogRequest {
    private final String catalogName;
    private final String dataFeedTaskUniqueId;
    private final String primaryKeyColumn;

    @JsonCreator
    public CreateCatalogRequest(@NotNull @JsonProperty("CatalogName") String catalogName, //
            @JsonProperty("TaskId") String dataFeedTaskUniqueId,
            @JsonProperty("PrimaryKeyColumn") String primaryKeyColumn) {
        Preconditions.checkArgument(StringUtils.isNotBlank(catalogName), "Catalog name should not be blank");
        this.catalogName = catalogName;
        this.dataFeedTaskUniqueId = dataFeedTaskUniqueId;
        this.primaryKeyColumn = primaryKeyColumn;
    }

    @JsonProperty("CatalogName")
    public String getCatalogName() {
        return catalogName;
    }

    @JsonProperty("TaskId")
    public String getDataFeedTaskUniqueId() {
        return dataFeedTaskUniqueId;
    }

    @JsonProperty("PrimaryKeyColumn")
    public String getPrimaryKeyColumn() {
        return primaryKeyColumn;
    }
}
