package com.latticeengines.camille.exposed.watchers;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.zookeeper.ZooDefs;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;

public class NodeWatcher {

    private static final ConcurrentMap<String, NodeCache> watchers = new ConcurrentHashMap<>();
    private static Log log = LogFactory.getLog(NodeWatcher.class);

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> watchers.values().forEach(cache -> {
            try {
                cache.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        })));
    }

    public static void registerWatcher(String watcherName) {
        if (!watchers.containsKey(watcherName)) {
            Path lockPath = PathBuilder.buildLockPath(CamilleEnvironment.getPodId(), null, watcherName);
            NodeCache cache = CamilleEnvironment.getCamille().createNodeCache(lockPath.toString());
            try {
                cache.start();
            } catch (Exception e) {
                throw new RuntimeException("Failed to register watcher at " + lockPath, e);
            }
            watchers.putIfAbsent(watcherName, cache);
            log.info("Registered a new node cache " + watcherName + " at " + lockPath);
        }
    }

    public static void registerListener(String watcherName, NodeCacheListener listener) {
        if (!watchers.containsKey(watcherName)) {
            registerWatcher(watcherName);
        }
        NodeCache nodeCache = watchers.get(watcherName);
        if (nodeCache == null) {
            throw new RuntimeException("Failed to watcher named " + watcherName);
        }
        nodeCache.getListenable().addListener(listener);
    }

    private static Path getWatcherPath(String watcherName) {
        if (watchers.containsKey(watcherName)) {
            return PathBuilder.buildLockPath(CamilleEnvironment.getPodId(), null, watcherName);
        } else {
            return null;
        }
    }

    public static void updateWatchedData(String watcherName, String serializedData) {
        Path path = getWatcherPath(watcherName);
        if (path != null) {
            try {
                CamilleEnvironment.getCamille().upsert(path, new Document(serializedData), ZooDefs.Ids.OPEN_ACL_UNSAFE);
            } catch (Exception e) {
                throw new RuntimeException("Failed up update watcher " + watcherName);
            }
        }
    }

}
