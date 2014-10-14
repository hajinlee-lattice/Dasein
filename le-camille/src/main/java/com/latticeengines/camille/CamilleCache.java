package com.latticeengines.camille;

import org.apache.curator.framework.listen.ListenerContainer;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;

public class CamilleCache {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    private Path path;
    private NodeCache cache;

    public CamilleCache(Path path) {
        this.path = path;
        this.cache = new NodeCache(CamilleEnvironment.getCamille().getCuratorClient(), path.toString());
    }
    
    public void start() throws Exception {
        cache.start(true);
    }
    
    public void close() throws Exception {
        cache.close();
    }

    public void rebuild() throws Exception {
        cache.rebuild();
    }

    public Document get() throws DocumentSerializationException {
        ChildData data = cache.getCurrentData();
        if (data == null) {
            return null;
        }

        Document document = DocumentSerializer.toDocument(data.getData());
        document.setVersion(data.getStat().getVersion());
        return document;
    }
    
    public boolean exists() throws DocumentSerializationException {
        return get() != null;
    }

    public ListenerContainer<NodeCacheListener> getListeners() {
        return cache.getListenable();
    }
}
