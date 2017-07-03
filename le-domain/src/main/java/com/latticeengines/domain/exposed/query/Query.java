package com.latticeengines.domain.exposed.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.graph.traversal.impl.DepthFirstSearch;
import com.latticeengines.common.exposed.visitor.Visitor;
import com.latticeengines.common.exposed.visitor.VisitorContext;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;

/**
 * NOTE:
 *
 * SchemaInterpretation and ColumnLookup based implementation is deprecated
 * The new framework uses BusinessEntity and AttributeLookup.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
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

    @JsonProperty("free_form_text_search")
    private List<FreeFormTextSearchAttribute> freeFormTextSearchAttributes = new ArrayList<>();

    @JsonIgnore
    private Set<BusinessEntity> entitiesForJoin;

    @JsonIgnore
    private Set<BusinessEntity> entitiesForExists;

    @JsonIgnore
    private List<JoinSpecification> lookupJoins;

    @JsonIgnore
    private List<JoinSpecification> existsJoins;

    public static QueryBuilder builder() {
        return new QueryBuilder();
    }

    public Query(SchemaInterpretation objectType, List<Lookup> lookups, Restriction restriction, Sort sort,
            PageFilter pageFilter, String freeFromRestriction) {
        this.objectType = objectType;
        this.lookups = lookups;
        this.restriction = restriction;
        this.sort = sort;
        this.pageFilter = pageFilter;
        this.freeFormTextSearch = freeFromRestriction;
    }

    Query() {
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

    public void setLookups(BusinessEntity businessEntity, String... attrNames) {
        this.lookups = new ArrayList<String>(Arrays.asList(attrNames)).stream() //
                .map((attrName) -> new AttributeLookup(businessEntity, attrName)) //
                .collect(Collectors.toList());
    }

    public void addLookups(BusinessEntity businessEntity, String... attrNames) {
        List<Lookup> moreLookups = new ArrayList<String>(Arrays.asList(attrNames)).stream() //
                .map((attrName) -> new AttributeLookup(businessEntity, attrName)) //
                .collect(Collectors.toList());
        lookups.addAll(moreLookups);
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

    public List<FreeFormTextSearchAttribute> getFreeFormTextSearchAttributes() {
        return freeFormTextSearchAttributes;
    }

    public void setFreeFormTextSearchAttributes(List<FreeFormTextSearchAttribute> freeFormTextSearchAttributes) {
        this.freeFormTextSearchAttributes = freeFormTextSearchAttributes;
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

    public void analyze() {
        traverseEntities();
        generateJoins();
    }

    private void traverseEntities() {
        DepthFirstSearch search = new DepthFirstSearch();
        entitiesForJoin = new HashSet<>();
        entitiesForExists = new HashSet<>();
        search.run(this, (object, ctx) -> {
            GraphNode node = (GraphNode) object;
            if (node instanceof EntityLookup) {
                EntityLookup unityLookup = (EntityLookup) node;
                entitiesForJoin.add(unityLookup.getEntity());
            } else if (node instanceof AttributeLookup) {
                AttributeLookup lookup = (AttributeLookup) node;
                entitiesForJoin.add(lookup.getEntity());
            } else if (node instanceof ExistsRestriction) {
                ExistsRestriction exists = (ExistsRestriction) node;
                entitiesForExists.add(exists.getEntity());
            }
        });
    }

    private void generateJoins() {
        BusinessEntity entity = getMainEntity();
        Set<JoinSpecification> lookupJoinsSet = entitiesForJoin.stream() //
                .filter(j -> !entity.equals(j)) //
                .map(j -> new JoinSpecification(entity, j, ObjectUsage.LOOKUP)) //
                .collect(Collectors.toSet());
        entitiesForJoin.forEach(j1 -> {
            entitiesForJoin.forEach(j2 -> {
                if (!j1.equals(j2)) {
                    lookupJoinsSet.add(new JoinSpecification(j1, j2, ObjectUsage.LOOKUP));
                }
            });
        });

        Set<JoinSpecification> existsJoinsSet = entitiesForExists.stream() //
                .filter(e -> !entity.equals(e)) //
                .map(e -> new JoinSpecification(entity, e, ObjectUsage.EXISTS)) //
                .collect(Collectors.toSet());

        lookupJoins = new ArrayList<>(lookupJoinsSet);
        existsJoins = new ArrayList<>(existsJoinsSet);
    }

    public BusinessEntity getMainEntity() {
        if (entitiesForJoin.contains(BusinessEntity.Account)) {
            return BusinessEntity.Account;
        }
        return entitiesForJoin.iterator().next();
    }

    public List<JoinSpecification> from(BusinessEntity entity) {
        analyze();
        List<JoinSpecification> joins = new ArrayList<>(lookupJoins);
        joins.addAll(existsJoins);
        return joins;
    }

    public Set<BusinessEntity> getEntitiesForJoin() {
        return entitiesForJoin;
    }

    public Set<BusinessEntity> getEntitiesForExists() {
        return entitiesForExists;
    }

    public List<JoinSpecification> getLookupJoins() {
        return lookupJoins;
    }

    public List<JoinSpecification> getExistsJoins() {
        return existsJoins;
    }

    @Override
    public Collection<? extends GraphNode> getChildren() {
        List<GraphNode> children = lookups.stream().collect(Collectors.toList());
        children.add(restriction);
        children.add(sort);
        return children;
    }

    @Override
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

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
    public static class FreeFormTextSearchAttribute {
        @JsonProperty("entity")
        private BusinessEntity entity;

        @JsonProperty("attribute")
        private String attribute;

        FreeFormTextSearchAttribute(BusinessEntity entity, String attribute) {
            this.entity = entity;
            this.attribute = attribute;
        }

        public BusinessEntity getEntity() {
            return entity;
        }

        public void setEntity(BusinessEntity entity) {
            this.entity = entity;
        }

        public String getAttribute() {
            return attribute;
        }

        public void setAttribute(String attribute) {
            this.attribute = attribute;
        }
    }
}
