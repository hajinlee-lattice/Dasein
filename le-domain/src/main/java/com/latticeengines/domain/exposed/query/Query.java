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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.graph.traversal.impl.DepthFirstSearch;
import com.latticeengines.common.exposed.visitor.Visitor;
import com.latticeengines.common.exposed.visitor.VisitorContext;

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

    @JsonProperty("main_entity")
    private BusinessEntity mainEntity;

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

    @JsonProperty("free_form_text_search_attributes")
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

    public void addLookups(BusinessEntity businessEntity, String... attrNames) {
        List<Lookup> moreLookups = new ArrayList<>(Arrays.asList(attrNames)).stream() //
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

    public void analyze() {
        traverseEntities();
        resolveMainEntity();
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

    private void resolveMainEntity() {
        if (mainEntity != null) {
            entitiesForJoin.add(mainEntity);
        } else {
            if (entitiesForJoin.contains(BusinessEntity.Account)) {
                mainEntity = BusinessEntity.Account;
            } else {
                mainEntity = entitiesForJoin.iterator().next();
            }
        }
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
        return mainEntity;
    }

    public void setMainEntity(BusinessEntity mainEntity) {
        this.mainEntity = mainEntity;
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

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
    public static class FreeFormTextSearchAttribute {
        @JsonProperty("entity")
        private BusinessEntity entity;

        @JsonProperty("attribute")
        private String attribute;

        public FreeFormTextSearchAttribute(BusinessEntity entity, String attribute) {
            this.entity = entity;
            this.attribute = attribute;
        }

        public FreeFormTextSearchAttribute() {
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
