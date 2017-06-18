package com.latticeengines.domain.exposed.query;

import java.util.ArrayList;
import java.util.List;

public class QueryBuilder {

    private List<Lookup> lookups = new ArrayList<>();
    private Restriction restriction;
    private Sort sort;
    private PageFilter pageFilter;
    private String freeFormTextSearch;

    QueryBuilder(){}

    public QueryBuilder find(BusinessEntity entity) {
        lookups.add(new EntityLookup(entity));
        return this;
    }

    public QueryBuilder select(BusinessEntity entity, String... attrs) {
        for (String attr: attrs) {
            lookups.add(new AttributeLookup(entity, attr));
        }
        return this;
    }

    public QueryBuilder where(Restriction restriction) {
        this.restriction = restriction;
        return this;
    }

    public QueryBuilder exist(BusinessEntity entity) {
        this.restriction = new ExistsRestriction(entity, false, null);
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

    public QueryBuilder freeText(String freeFormTextSearch) {
        this.freeFormTextSearch = freeFormTextSearch;
        return this;
    }

    public Query build() {
        Query query = new Query();
        query.setLookups(lookups);
        query.setRestriction(restriction);
        query.setSort(sort);
        query.setPageFilter(pageFilter);
        query.setFreeFormTextSearch(freeFormTextSearch);
        return query;
    }

}
