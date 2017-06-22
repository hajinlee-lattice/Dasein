package com.latticeengines.query.util;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
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

@Component("attrRepoUtils")
public class AttrRepoUtils {

    private static final String AM_REPO = "AMCollection";

    private ColumnMetadataProxy columnMetadataProxy;
    private LoadingCache<String, AttributeRepository> amAttrRepoCache;

    @Autowired
    public AttrRepoUtils(ColumnMetadataProxy columnMetadataProxy) {
        this.columnMetadataProxy = columnMetadataProxy;
    }

    @VisibleForTesting
    public void setColumnMetadataProxy(ColumnMetadataProxy columnMetadataProxy) {
        this.columnMetadataProxy = columnMetadataProxy;
    }

    private AttributeRepository getAMAttrRepo() {
        if (amAttrRepoCache == null) {
            initLoadingCache();
        }
        try {
            return amAttrRepoCache.get(AM_REPO);
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to load am attr repo from cache.", e);
        }
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

    private void initLoadingCache() {
        amAttrRepoCache = CacheBuilder.newBuilder().maximumSize(1).refreshAfterWrite(30, TimeUnit.MINUTES)
                .build(new CacheLoader<String, AttributeRepository>() {
                    @Override
                    public AttributeRepository load(String repoId) throws Exception {
                        AttributeRepository attrRepo;
                        try {
                            attrRepo = columnMetadataProxy.getAttrRepo();
                        } catch (Exception e) {
                            throw new RuntimeException("Failed to load am attr repo via proxy.", e);
                        }
                        if (attrRepo == null) {
                            throw new RuntimeException("Failed to load am attr repo via proxy.");
                        }
                        return attrRepo;
                    }
                });
    }

}
