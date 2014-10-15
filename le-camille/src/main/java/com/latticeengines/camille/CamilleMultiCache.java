package com.latticeengines.camille;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;

public class CamilleMultiCache {
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    private Map<Path, CamilleCache> caches;

    public CamilleMultiCache() {
        caches = new HashMap<Path, CamilleCache>();
    }

    public synchronized void close() throws Exception {
        for (Map.Entry<Path, CamilleCache> entry : caches.entrySet()) {
            CamilleCache cache = entry.getValue();
            cache.close();
        }
    }
    
    public synchronized void rebuild() throws Exception {
        for (Map.Entry<Path, CamilleCache> entry : caches.entrySet()) {
            CamilleCache cache = entry.getValue();
            cache.rebuild();
        }
    }
    
    public synchronized void rebuild(Path path) throws Exception {
        if (!caches.containsKey(path)) {
            throw new IllegalArgumentException("Not caching path" + path);
        }
        CamilleCache cache = caches.get(path);
        cache.rebuild();
    }

    public synchronized Document get(Path path) throws DocumentSerializationException {
        if (!caches.containsKey(path)) {
            log.debug("Not caching " + path + ". Adding an entry for it.");
            CamilleCache cache = new CamilleCache(path);
            try {
                cache.start();
            }
            catch (Exception e) {
                log.error("Failed to start cache for path " + path);
                return null;
            }
            
            caches.put(path, cache);
        }

        CamilleCache cache = caches.get(path);
        return cache.get();
    }
    
    public synchronized boolean exists(Path path) throws DocumentSerializationException {
        return get(path) != null;
    }
    
    public synchronized void uncache(Path path) throws Exception {
        if (!caches.containsKey(path)) {
            throw new IllegalArgumentException("Not caching path " + path);
        }
        CamilleCache cache = caches.get(path);
        cache.close();
        caches.remove(path);
    }
}