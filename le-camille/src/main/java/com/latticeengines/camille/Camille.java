package com.latticeengines.camille;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.DocumentHierarchy;
import com.latticeengines.domain.exposed.camille.Path;

public class Camille {
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    private CuratorFramework client;

    // package visibility is deliberate
    Camille(CuratorFramework client) {
        this.client = client;
    }

    CuratorFramework getCuratorClient() {
        return client;
    }

    public void create(Path path, Document doc, List<ACL> acls) throws Exception {
        client.inTransaction().create().withACL(acls).forPath(path.toString(), doc.getData().getBytes()).and().commit();
    }

    public void set(Path path, Document doc) throws Exception {
        set(path, doc, false);
    }

    public void set(Path path, Document doc, boolean force) throws Exception {
        if (force) {
            // TODO: catch version exception and retry with backoff
            throw new UnsupportedOperationException();
        } else {
            client.inTransaction().setData().forPath(path.toString(), doc.getData().getBytes()).and().commit();
        }
    }

    public Document get(Path path) throws Exception {
        // TODO: do something about DocumentMetadata
        return new Document(new String(client.getData().forPath(path.toString())), null);
    }

    public Document get(Path path, CuratorWatcher watcher) throws Exception {
        // TODO: do something about DocumentMetadata
        return new Document(new String(client.getData().usingWatcher(watcher).forPath(path.toString())), null);
    }

    /**
     * Gets direct children only (not a fully hierarchy).
     * 
     * @throws Exception
     */
    public List<Pair<Document, Path>> getChildren(Path path) throws Exception {
        List<String> childPaths = client.getChildren().forPath(path.toString());

        List<Pair<Document, Path>> out = new ArrayList<Pair<Document, Path>>(childPaths.size());

        for (String childPath : childPaths) {
            out.add(Pair.of(new Document(new String(client.getData().forPath(childPath)), null), new Path(childPath)));
        }

        return out;
    }

    public DocumentHierarchy getHierarchy(Path path) {
        // TODO: code DocumentHierarchy class, then do this
        throw new UnsupportedOperationException();
    }

    public void delete(Path path) throws Exception {
        client.delete().forPath(path.toString());
    }

    public boolean exists(Path path) throws Exception {
        return client.checkExists().forPath(path.toString()) != null;
    }
}
