package com.latticeengines.domain.exposed.cdl.activity;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.transaction.NullMetricsImputation;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CreateActivityMetricsGroupRequest {
    @JsonProperty
    public String streamName;

    @JsonProperty
    public ActivityTimeRange timeRange;

    @JsonProperty
    public StreamAttributeDeriver aggregation;

    @JsonProperty
    public CategorizeValConfig categorizeValConfig;

    @JsonProperty
    public Category category;

    @JsonProperty
    public Map<ActivityMetricsGroup.ColumnSelectionStatus, Set<ColumnSelection.Predefined>> csOverwrite;

    @JsonProperty
    public BusinessEntity entity;

    @JsonProperty
    public String groupName;

    @JsonProperty
    public String javaClass;

    @JsonProperty
    public NullMetricsImputation nullImputation;

    @JsonProperty
    public ActivityRowReducer reducer;

    @JsonProperty
    public Boolean useLatestVersion;

    @JsonProperty
    public List<String> rollupDimensions;

    @JsonProperty
    public String descriptionTmpl;

    @JsonProperty
    public String displayNameTmpl;

    @JsonProperty
    public String secSubCategoryTmpl;

    @JsonProperty
    public String subCategoryTmpl;
}
