package com.latticeengines.query.util;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.query.exposed.exception.QueryEvaluationException;
import com.querydsl.core.types.EntityPath;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.core.types.dsl.PathBuilder;
import com.querydsl.core.types.dsl.StringPath;

/**
 * This bean is deprecated, because we don't need AM attr repo anymore.
 * We will convert this bean to a utility class with static methods
 */
@Component("attrRepoUtils")
public class AttrRepoUtils {
    private ColumnMetadataProxy columnMetadataProxy;

    @Autowired
    public AttrRepoUtils(ColumnMetadataProxy columnMetadataProxy) {
        this.columnMetadataProxy = columnMetadataProxy;
    }

    @VisibleForTesting
    public void setColumnMetadataProxy(ColumnMetadataProxy columnMetadataProxy) {
        this.columnMetadataProxy = columnMetadataProxy;
    }

    private AttributeRepository getAMAttrRepo() {
        // AM Attr Repo is cached in the proxy
        return columnMetadataProxy.getAttrRepo();
    }

    public ColumnMetadata getAttribute(AttributeRepository attrRepo, AttributeLookup attributeLookup) {
        if (BusinessEntity.LatticeAccount.equals(attributeLookup.getEntity())) {
            return getAMAttrRepo().getColumnMetadata(attributeLookup);
        } else {
            return attrRepo.getColumnMetadata(attributeLookup);
        }
    }

    public StringPath getTablePath(AttributeRepository repository, BusinessEntity entity) {
        String tableName;
        if (isAM(entity)) {
            tableName = getTableName(getAMAttrRepo(), entity);
        } else {
            tableName = getTableName(repository, entity);
        }
        return Expressions.stringPath(tableName);
    }

    public EntityPath<String> getTablePathBuilder(AttributeRepository repository, BusinessEntity entity) {
        String tableName;
        if (isAM(entity)) {
            tableName = getTableName(getAMAttrRepo(), entity);
        } else {
            tableName = getTableName(repository, entity);
        }
        return new PathBuilder<>(String.class, tableName);
    }

    private String getTableName(AttributeRepository repository, BusinessEntity entity) {
        TableRoleInCollection tableRole = entity.getServingStore();
        if (tableRole == null) {
            throw new QueryEvaluationException("Cannot find a serving store for " + entity);
        }
        String tableName;
        if (isAM(entity)) {
            tableName = getAMAttrRepo().getTableName(tableRole);
        } else {
            tableName = repository.getTableName(tableRole);
        }
        if (tableName == null) {
            throw new QueryEvaluationException("Cannot find table of role " + tableRole + " in the repository.");
        }
        return tableName;
    }

    private boolean isAM(BusinessEntity entity) {
        return BusinessEntity.LatticeAccount.equals(entity);
    }

}
