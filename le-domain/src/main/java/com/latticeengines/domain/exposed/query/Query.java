package com.latticeengines.domain.exposed.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.graph.traversal.impl.DepthFirstSearch;
import com.latticeengines.common.exposed.visitor.Visitor;
import com.latticeengines.common.exposed.visitor.VisitorContext;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class Query implements GraphNode {
    @JsonProperty("object_type")
    private SchemaInterpretation objectType;
    @JsonProperty("lookups")
    private List<Lookup> lookups = new ArrayList<>();
    @JsonProperty("restriction")
    private Restriction restriction;
    @JsonProperty("sort")
    private Sort sort;
    @JsonProperty("page_filter")
    private PageFilter pageFilter;
    @JsonProperty("free_form_text_search")
    private String freeFormTextSearch;

    public Query(SchemaInterpretation objectType, List<Lookup> lookups, Restriction restriction, Sort sort,
            PageFilter pageFilter, String freeFromRestriction) {
        this.objectType = objectType;
        this.lookups = lookups;
        this.restriction = restriction;
        this.sort = sort;
        this.pageFilter = pageFilter;
        this.freeFormTextSearch = freeFromRestriction;
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

    @SuppressWarnings("unchecked")
    public void setLookups(SchemaInterpretation objectType, String... columnNames) {
        this.lookups = new ArrayList<String>(Arrays.asList(columnNames)).stream() //
                .map((columnName) -> new ColumnLookup(objectType, columnName)) //
                .collect(Collectors.toList());
    }

    public void addLookup(Lookup lookup) {
        lookups.add(lookup);
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

    public String getFreeFormTextSearch() {
        return freeFormTextSearch;
    }

    public void setFreeFormTextSearch(String freeFormTextSearch) {
        this.freeFormTextSearch = freeFormTextSearch;
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

    @SuppressWarnings("unchecked")
    public List<JoinSpecification> getNecessaryJoins() {
        DepthFirstSearch search = new DepthFirstSearch();
        List<JoinSpecification> joins = new ArrayList<>();

        search.run(this, (object, ctx) -> {
            GraphNode node = (GraphNode) object;
            if (node instanceof ColumnLookup) {
                ColumnLookup lookup = (ColumnLookup) node;
                if (lookup.getObjectType() != null && !lookup.getObjectType().equals(getObjectType())) {
                    joins.add(new JoinSpecification(getObjectType(), lookup.getObjectType(), ObjectUsage.LOOKUP));
                }
            } else if (node instanceof ExistsRestriction) {
                ExistsRestriction exists = (ExistsRestriction) node;
                joins.add(new JoinSpecification(getObjectType(), exists.getObjectType(), ObjectUsage.EXISTS));
            }
        });

        return joins;
    }

    @Override
    public Collection<? extends GraphNode> getChildren() {
        List<GraphNode> children = lookups.stream().collect(Collectors.toList());
        children.add(restriction);
        children.add(sort);
        return children;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Collection<? extends GraphNode>> getChildMap() {
        Map<String, Collection<? extends GraphNode>> map = new HashMap<>();
        map.put("lookups", lookups);
        map.put("restriction", Collections.singletonList(restriction));
        map.put("sort", Collections.singletonList(sort));
        return map;
    }

    @Override
    public void accept(Visitor visitor, VisitorContext ctx) {
        visitor.visit(this, ctx);
    }

}
