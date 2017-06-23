package com.latticeengines.query.util;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.ZK_WATCHER_AM_RELEASE;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.latticeengines.camille.exposed.watchers.NodeWatcher;
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
    private Cache<String, AttributeRepository> amAttrRepoCache;

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
        return amAttrRepoCache.getIfPresent(AM_REPO);
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
        amAttrRepoCache = CacheBuilder.newBuilder().maximumSize(1).build();
        amAttrRepoCache.put(AM_REPO, columnMetadataProxy.getAttrRepo());
        // watch on a zk node that will be updated by data cloud whe new am is released.
        NodeWatcher.registerWatcher(ZK_WATCHER_AM_RELEASE);
        NodeWatcher.registerListener(ZK_WATCHER_AM_RELEASE, new AMUpdateWatcher(amAttrRepoCache, columnMetadataProxy));
    }

    private static class AMUpdateWatcher implements NodeCacheListener {
        private static final Log log = LogFactory.getLog(AMUpdateWatcher.class);
        private final Cache<String, AttributeRepository> amAttrRepoCache;
        private ColumnMetadataProxy columnMetadataProxy;

        AMUpdateWatcher(Cache<String, AttributeRepository> amAttrRepoCache, ColumnMetadataProxy columnMetadataProxy) {
            this.amAttrRepoCache = amAttrRepoCache;
            this.columnMetadataProxy = columnMetadataProxy;
        }

        @Override
        public void nodeChanged() throws Exception {
            log.info("am update zk watch changed, updating am attr repo cache.");
            amAttrRepoCache.put(AM_REPO, columnMetadataProxy.getAttrRepo());
        }
    }

}
