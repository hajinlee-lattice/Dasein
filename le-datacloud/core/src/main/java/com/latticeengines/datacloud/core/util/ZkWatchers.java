package com.latticeengines.datacloud.core.util;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.ZK_WATCHER_AM_API_UPDATE;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.ZK_WATCHER_AM_MD_UPDATE;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.ZK_WATCHER_AM_RELEASE;

import java.util.Date;

import org.apache.curator.framework.recipes.cache.NodeCacheListener;

import com.latticeengines.camille.exposed.watchers.NodeWatcher;

public final class ZkWatchers {

    static {
        NodeWatcher.registerWatcher(ZK_WATCHER_AM_RELEASE);
        NodeWatcher.registerWatcher(ZK_WATCHER_AM_MD_UPDATE);
        NodeWatcher.registerWatcher(ZK_WATCHER_AM_API_UPDATE);
    }

    public static void listenToAMRelease(NodeCacheListener listener) {
        NodeWatcher.registerListener(ZK_WATCHER_AM_RELEASE, listener);
    }

    public static void updateAMReleaseWatcher(Date releaseTime) {
        String serialized = HdfsPathBuilder.dateFormat.format(releaseTime);
        NodeWatcher.updateWatchedData(ZK_WATCHER_AM_RELEASE, serialized);
    }

    public static void listenToAMMetadataUpdate(NodeCacheListener listener) {
        NodeWatcher.registerListener(ZK_WATCHER_AM_MD_UPDATE, listener);
    }

    public static void updateAMMetadataUpdateWatcher(Date updateTime) {
        String serialized = HdfsPathBuilder.dateFormat.format(updateTime);
        NodeWatcher.updateWatchedData(ZK_WATCHER_AM_MD_UPDATE, serialized);
    }

    public static void listenToAMApiUpdate(NodeCacheListener listener) {
        NodeWatcher.registerListener(ZK_WATCHER_AM_API_UPDATE, listener);
    }

    public static void updateAMApiWatcher(Date releaseTime) {
        String serialized = HdfsPathBuilder.dateFormat.format(releaseTime);
        NodeWatcher.updateWatchedData(ZK_WATCHER_AM_API_UPDATE, serialized);
    }

}
