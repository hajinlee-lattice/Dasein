package com.latticeengines.camille;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;

public class CamilleCache {
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    private Map<Path, NodeCache> caches;
    private Camille camille;

    public CamilleCache() {
        caches = new HashMap<Path, NodeCache>();
        camille = CamilleEnvironment.getCamille();
    }

    /**
     * Retrieves the latest data for every item in the cache.
     */
    public synchronized void rebuild() throws Exception {
        for (Map.Entry<Path, NodeCache> entry : caches.entrySet()) {
            NodeCache cache = entry.getValue();
            cache.rebuild();
        }
    }

    /**
     * Retrieve the latest version of a document from the cache. If the
     * specified path is not currently being cached, it will be added.
     * 
     * Will throw if the document doesn't exist as far as this cache is aware,
     * since the usage pattern for this cache is for all cached documents to
     * exist.
     */
    public synchronized Document get(Path path) throws Exception {
        if (!caches.containsKey(path)) {
            log.debug("Not caching " + path + ". Adding an entry for it.");
            NodeCache cache = new NodeCache(camille.getCuratorClient(), path.toString());
            cache.start(true);
            caches.put(path, cache);
        }

        NodeCache cache = caches.get(path);
        ChildData data = cache.getCurrentData();
        if (data == null) {
            // The general assumption about this cache is that documents are
            // expected to exist
            throw new KeeperException.NoNodeException(path.toString());
        }

        Document document = DocumentSerializer.toDocument(data.getData());
        document.setVersion(data.getStat().getVersion());
        return document;
    }

    /**
     * Remove a path from the cache.
     */
    public synchronized void remove(Path path) throws IllegalArgumentException, IOException {
        if (!caches.containsKey(path)) {
            throw new IllegalArgumentException("Not caching path " + path);
        }
        NodeCache cache = caches.get(path);
        cache.close();
        caches.remove(path);
    }
}
