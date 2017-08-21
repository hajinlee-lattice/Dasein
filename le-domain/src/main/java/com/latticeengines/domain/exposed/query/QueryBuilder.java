package com.latticeengines.domain.exposed.query;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.domain.exposed.query.Query.FreeFormTextSearchAttribute;

public class QueryBuilder {

    private List<Lookup> lookups = new ArrayList<>();
    private BusinessEntity mainEntity;
    private Restriction restriction;
    private Sort sort;
    private PageFilter pageFilter;
    private String freeFormTextSearch;
    private List<FreeFormTextSearchAttribute> freeFormTextSearchAttributes = new ArrayList<>();

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

    public QueryBuilder select(CaseLookup caseLookup) {
        lookups.add(caseLookup);
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

    public QueryBuilder freeText(String freeFormTextSearch, BusinessEntity entity, String... attrs) {
        this.freeFormTextSearch = freeFormTextSearch;
        if (StringUtils.isNotBlank(freeFormTextSearch)) {
            this.find(entity);
        }
        for (String attr : attrs) {
            freeFormTextSearchAttributes.add(new FreeFormTextSearchAttribute(entity, attr));
        }
        return this;
    }

    public QueryBuilder from(BusinessEntity entity) {
        this.mainEntity = entity;
        return this;
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
        return query;
    }

}
