package com.latticeengines.camille;

import org.apache.curator.framework.listen.ListenerContainer;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;

import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;

public class CamilleCache {
    private NodeCache cache;

    public CamilleCache(Path path) throws Exception {
        cache = new NodeCache(CamilleEnvironment.getCamille().getCuratorClient(), path.toString());
        cache.start(true);
    }

    public void close() throws Exception {
        cache.close();
    }

    public void rebuild() throws Exception {
        cache.rebuild();
    }

    public Document get() {
        ChildData data = cache.getCurrentData();
        if (data == null) {
            return null;
        }

        Document document = new Document(new String(data.getData()));
        document.setVersion(data.getStat().getVersion());
        return document;
    }

    public ListenerContainer<NodeCacheListener> getListeners() {
        return cache.getListenable();
    }
}
