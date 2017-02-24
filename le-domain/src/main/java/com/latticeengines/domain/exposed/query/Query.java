package com.latticeengines.domain.exposed.query;

import java.util.List;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Query {
    @JsonProperty("object_type")
    private SchemaInterpretation objectType;
    @JsonProperty("lookups")
    private List<Lookup> lookups;
    @JsonProperty("restriction")
    private Restriction restriction;
    @JsonProperty("sort")
    private Sort sort;
    @JsonProperty("page_filter")
    private PageFilter pageFilter;
    @JsonProperty("free_form_restriction")
    private String freeFormRestriction;

    public Query(SchemaInterpretation objectType, List<Lookup> lookups, Restriction restriction, Sort sort,
            PageFilter pageFilter, String freeFromRestriction) {
        this.objectType = objectType;
        this.lookups = lookups;
        this.restriction = restriction;
        this.sort = sort;
        this.pageFilter = pageFilter;
        this.freeFormRestriction = freeFromRestriction;
    }

    public Query() {
    }

    public Restriction getRestriction() {
        return restriction;
    }

    public void setRestriction(Restriction restriction) {
        this.restriction = restriction;
    }

    public List<Lookup> getLookups() {
        return lookups;
    }

    public void setLookups(List<Lookup> lookups) {
        this.lookups = lookups;
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

    public SchemaInterpretation getObjectType() {
        return objectType;
    }

    public void setObjectType(SchemaInterpretation objectType) {
        this.objectType = objectType;
    }

    public String getFreeFormRestriction() {
        return freeFormRestriction;
    }

    public void setFreeFormRestriction(String freeFormRestriction) {
        this.freeFormRestriction = freeFormRestriction;
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }

    @Override
    public boolean equals(Object obj) {
        return EqualsBuilder.reflectionEquals(this, obj);
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}
