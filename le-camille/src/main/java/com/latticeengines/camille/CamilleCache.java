package com.latticeengines.camille;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;

import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;

public class CamilleCache {
    private Map<Path, NodeCache> caches;
    private Camille camille;

    public CamilleCache() {
        caches = new HashMap<Path, NodeCache>();
        camille = CamilleEnvironment.getCamille();
    }

    public synchronized void add(Path path) throws Exception {
        if (caches.containsKey(path)) {
            throw new IllegalArgumentException("Already caching path " + path);
        }
        NodeCache cache = new NodeCache(camille.getCuratorClient(), path.toString());
        cache.start();
        caches.put(path, cache);
    }
    
    public synchronized Document get(Path path) throws IllegalArgumentException, DocumentSerializationException {
        if (!caches.containsKey(path)) {
            throw new IllegalArgumentException("Not caching path " + path);
        }
        
        NodeCache cache = caches.get(path);
        ChildData data = cache.getCurrentData();
        Document document = DocumentSerializer.toDocument(data.getData());
        document.setVersion(data.getStat().getVersion());
        return document;
    }
    
    public synchronized void remove(Path path) throws IllegalArgumentException, IOException {
        if (!caches.containsKey(path)) {
            throw new IllegalArgumentException("Not caching path " + path);
        }
        NodeCache cache = caches.get(path);
        cache.close();
        caches.remove(path);
    }
}
