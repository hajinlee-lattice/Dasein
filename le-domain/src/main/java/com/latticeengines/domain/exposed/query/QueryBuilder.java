package com.latticeengines.domain.exposed.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.latticeengines.domain.exposed.query.Query.FreeFormTextSearchAttribute;

public class QueryBuilder {

    private List<Lookup> lookups = new ArrayList<>();
    private GroupBy groupBy;
    private BusinessEntity mainEntity;
    private List<SubQuery> subQueryList = new ArrayList<>();
    private SubQuery subQuery;
    private Restriction restriction;
    private Sort sort;
    private PageFilter pageFilter;
    private String freeFormTextSearch;
    private List<FreeFormTextSearchAttribute> freeFormTextSearchAttributes = new ArrayList<>();
    private Boolean distinct = Boolean.FALSE;
    private Boolean isCount;

    QueryBuilder() {
    }

    public QueryBuilder find(BusinessEntity entity) {
        lookups.add(new EntityLookup(entity));
        return this;
    }

    public QueryBuilder select(BusinessEntity entity, String... attrs) {
        for (String attr : attrs) {
            lookups.add(new AttributeLookup(entity, attr));
        }
        return this;
    }

    public QueryBuilder select(Lookup... lookupArray) {
        Collections.addAll(lookups, lookupArray);
        return this;
    }

    public QueryBuilder where(Restriction restriction) {
        this.restriction = restriction;
        return this;
    }

    public QueryBuilder exist(BusinessEntity entity) {
        return exist(entity, null);
    }

    public QueryBuilder exist(BusinessEntity entity, Restriction innerRestriction) {
        this.restriction = new ExistsRestriction(entity, false, innerRestriction);
        return this;
    }

    public QueryBuilder orderBy(BusinessEntity entity, String... attrs) {
        return orderBy(false, entity, attrs);
    }

    public QueryBuilder orderBy(boolean descending, BusinessEntity entity, String... attrs) {
        Sort sort = new Sort();
        sort.setLookups(entity, attrs);
        sort.setDescending(descending);
        this.sort = sort;
        return this;
    }

    public QueryBuilder orderBy(Sort sort) {
        this.sort = sort;
        return this;
    }

    public QueryBuilder page(PageFilter pageFilter) {
        this.pageFilter = pageFilter;
        return this;
    }

    @Deprecated
    public QueryBuilder freeText(String freeFormTextSearch, AttributeLookup... attrs) {
        this.freeFormTextSearch = freeFormTextSearch;
        for (AttributeLookup attr : attrs) {
            freeFormTextSearchAttributes
                    .add(new FreeFormTextSearchAttribute(attr.getEntity(), attr.getAttribute()));
        }
        return this;
    }

    public QueryBuilder from(BusinessEntity entity) {
        this.mainEntity = entity;
        return this;
    }

    public QueryBuilder from(SubQuery subQuery) {
        this.subQuery = subQuery;
        return this;
    }

    public QueryBuilder with(SubQuery... subQueryArray) {
        Collections.addAll(this.subQueryList, subQueryArray);
        return this;
    }

    public QueryBuilder groupBy(Lookup... groupByLookups) {
        groupBy = new GroupBy();
        groupBy.setLookups(Arrays.asList(groupByLookups));
        return this;
    }

    public QueryBuilder having(Restriction restriction) {
        groupBy.setHaving(restriction);
        return this;
    }

    public QueryBuilder distinct(Boolean distinct) {
        this.distinct = distinct;
        return this;
    }

    public QueryBuilder count(boolean count) {
        this.isCount = count;
        return this;
    }

    @SuppressWarnings("rawtypes")
    public List getSubQueryList() {
        return Collections.unmodifiableList(subQueryList);
    }

    public List<Lookup> getLookups() {
        return Collections.unmodifiableList(lookups);
    }

    public Query build() {
        Query query = new Query();
        query.setLookups(lookups);
        query.setRestriction(restriction);
        query.setSort(sort);
        query.setPageFilter(pageFilter);
        query.setFreeFormTextSearch(freeFormTextSearch);
        query.setFreeFormTextSearchAttributes(freeFormTextSearchAttributes);
        query.setMainEntity(mainEntity);
        query.setSubQuery(subQuery);
        query.setCommonTableQueryList(subQueryList);
        query.setGroupBy(groupBy);
        query.setDistinct(distinct);
        query.setCount(isCount);
        return query;
    }

}
