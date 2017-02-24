package com.latticeengines.domain.exposed.query.frontend;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.Sort;

@JsonIgnoreProperties(ignoreUnknown = true)
public class FrontEndQuery {
    @JsonProperty("restriction")
    private FlattenedRestriction restriction;
    @JsonProperty("sort")
    private Sort sort;
    @JsonProperty("page_filter")
    private PageFilter pageFilter;
    @JsonProperty("free_form_restriction")
    private String freeFormRestriction;

    public FlattenedRestriction getRestriction() {
        return restriction;
    }

    public void setRestriction(FlattenedRestriction restriction) {
        this.restriction = restriction;
    }

    public Sort getSort() {
        return sort;
    }

    public void setSort(Sort sort) {
        this.sort = sort;
    }

    public PageFilter getPageFilter() {
        return pageFilter;
    }

    public void setPageFilter(PageFilter pageFilter) {
        this.pageFilter = pageFilter;
    }

    public String getFreeFormRestriction() {
        return freeFormRestriction;
    }

    public void setFreeFormRestriction(String freeFormRestriction) {
        this.freeFormRestriction = freeFormRestriction;
    }
}
