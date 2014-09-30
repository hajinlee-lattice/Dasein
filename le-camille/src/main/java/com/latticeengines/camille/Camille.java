package com.latticeengines.camille;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.api.SetDataBuilder;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
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
        client.create().withACL(acls).forPath(path.toString(), doc.getData().getBytes());
    }

    public Stat set(Path path, Document doc) throws Exception {
        return set(path, doc, false);
    }

    public Stat set(Path path, Document doc, boolean force) throws Exception {
        SetDataBuilder builder = client.setData();
        if (!force)
            builder.withVersion(doc.getVersion());
        return builder.forPath(path.toString(), doc.getData().getBytes());
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
        List<String> relativeChildPaths = client.getChildren().forPath(path.toString());

        List<Pair<Document, Path>> out = new ArrayList<Pair<Document, Path>>(relativeChildPaths.size());

        for (String relativePath : relativeChildPaths) {
            Path childPath = new Path(String.format("%s/%s", path, relativePath));
            out.add(Pair.of(new Document(new String(client.getData().forPath(childPath.toString())), null), childPath));
        }

        return out;
    }

    public DocumentHierarchy getHierarchy(Path path) throws Exception {
        DocumentHierarchy h = new DocumentHierarchy(get(path));
        addChildren(h.getRoot(), path);
        return h;
    }

    private void addChildren(DocumentHierarchy.Node parentNode, Path parentPath) throws Exception {
        for (Pair<Document, Path> child : getChildren(parentPath)) {
            DocumentHierarchy.Node n = new DocumentHierarchy.Node(child.getLeft());
            parentNode.getChildren().add(n);
            addChildren(n, child.getRight());
        }
    }

    public void delete(Path path) throws Exception {
        client.delete().forPath(path.toString());
    }

    /**
     * Returns a Stat object if exists, otherwise returns null.
     */
    public Stat exists(Path path) throws Exception {
        return client.checkExists().forPath(path.toString());
    }
}
