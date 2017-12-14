package com.latticeengines.camille.exposed.watchers;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;

public class NodeWatcher {

    private static final ConcurrentMap<String, NodeCache> watchers = new ConcurrentHashMap<>();
    private static Logger log = LoggerFactory.getLogger(NodeWatcher.class);

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> watchers.values().forEach(cache -> {
            try {
                cache.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        })));
    }

    public static synchronized void registerWatcher(String watcherName) {
        if (!watchers.containsKey(watcherName)) {
            Path watcherPath = PathBuilder.buildWatcherPath(CamilleEnvironment.getPodId(), watcherName);
            NodeCache cache = CamilleEnvironment.getCamille().createNodeCache(watcherPath.toString());
            try {
                cache.start();
            } catch (Exception e) {
                throw new RuntimeException("Failed to register watcher at " + watcherPath, e);
            }
            watchers.putIfAbsent(watcherName, cache);
            log.info("Registered a new node cache " + watcherName + " at " + watcherPath);
        }
    }

    public static synchronized void registerListener(String watcherName, NodeCacheListener listener) {
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
        return PathBuilder.buildWatcherPath(CamilleEnvironment.getPodId(), watcherName);
    }

    public static synchronized void updateWatchedData(String watcherName, String serializedData) {
        Path path = getWatcherPath(watcherName);
        try {
            if (!CamilleEnvironment.getCamille().exists(path)) {
                log.info("Creating watched node " + path + " with data " + serializedData);
                CamilleEnvironment.getCamille().create(path, new Document(serializedData), ZooDefs.Ids.OPEN_ACL_UNSAFE);
            } else {
                log.info("Changing data at watched node " + path + " to " + serializedData);
                CamilleEnvironment.getCamille().upsert(path, new Document(serializedData), ZooDefs.Ids.OPEN_ACL_UNSAFE);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to update watcher " + watcherName, e);
        }
    }

    public static void notifyCacheWatchersAsync(String watcherName, String signal) {
        new Thread(() -> NodeWatcher.updateWatchedData(watcherName, signal)).run();
    }

    public static synchronized String getWatchedData(String watcherName) {
        Path path = getWatcherPath(watcherName);
        if (path != null) {
            try {
                if (CamilleEnvironment.getCamille().exists(path)) {
                    return CamilleEnvironment.getCamille().get(path).getData();
                } else {
                    return null;
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed get data at watcher " + watcherName, e);
            }
        }
        return null;
    }

}
