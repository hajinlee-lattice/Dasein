package com.latticeengines.domain.exposed.query.frontend;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.Sort;

@JsonIgnoreProperties(ignoreUnknown = true)
public class FrontEndQuery {
    @JsonProperty("restriction")
    private FrontEndRestriction restriction;
    @JsonProperty("sort")
    private Sort sort;
    @JsonProperty("page_filter")
    private PageFilter pageFilter;
    @JsonProperty("free_form_text_search")
    private String freeFormTextSearch;

    public FrontEndRestriction getRestriction() {
        return restriction;
    }

    public void setRestriction(FrontEndRestriction restriction) {
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

    public String getFreeFormTextSearch() {
        return freeFormTextSearch;
    }

    public void setFreeFormTextSearch(String freeFormTextSearch) {
        this.freeFormTextSearch = freeFormTextSearch;
    }
}
