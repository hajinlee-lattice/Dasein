package com.latticeengines.camille;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    private final CuratorFramework client;

    // package visibility is deliberate
    Camille(CuratorFramework client) {
        this.client = client;
    }

    CuratorFramework getCuratorClient() {
        return client;
    }

    public void create(Path path, List<ACL> acls) throws Exception {
        client.create().withACL(acls).forPath(path.toString(), DocumentSerializer.toByteArray(new Document()));
    }

    public void create(Path path, Document doc, List<ACL> acls) throws Exception {
        client.create().withACL(acls).forPath(path.toString(), DocumentSerializer.toByteArray(doc));
        doc.setVersion(0);
    }

    public Stat set(Path path, Document doc) throws Exception {
        return set(path, doc, false);
    }

    public Stat set(Path path, Document doc, boolean force) throws Exception {
        final SetDataBuilder builder = client.setData();
        if (!force)
            builder.withVersion(doc.getVersion());
        final Stat stat = builder.forPath(path.toString(), DocumentSerializer.toByteArray(doc));
        doc.setVersion(stat.getVersion());
        return stat;
    }

    public Document get(Path path) throws Exception {
        final Stat stat = new Stat();
        final Document doc = DocumentSerializer.toDocument(client.getData().storingStatIn(stat)
                .forPath(path.toString()));
        doc.setVersion(stat.getVersion());
        return doc;
    }

    public Document get(Path path, CuratorWatcher watcher) throws Exception {
        final Stat stat = new Stat();
        final Document doc = DocumentSerializer.toDocument(client.getData().storingStatIn(stat).usingWatcher(watcher)
                .forPath(path.toString()));
        doc.setVersion(stat.getVersion());
        return doc;
    }

    /**
     * Gets direct children only (not a fully hierarchy).
     * 
     * @throws Exception
     */
    public List<Pair<Document, Path>> getChildren(Path path) throws Exception {
        final List<String> relativeChildPaths = client.getChildren().forPath(path.toString());

        final List<Pair<Document, Path>> out = new ArrayList<Pair<Document, Path>>(relativeChildPaths.size());

        for (final String relativePath : relativeChildPaths) {
            final Path childPath = new Path(String.format("%s/%s", path, relativePath));
            out.add(Pair.of(get(childPath), childPath));
        }

        return out;
    }

    public DocumentHierarchy getHierarchy(Path path) throws Exception {
        final DocumentHierarchy h = new DocumentHierarchy(get(path));
        addChildren(h.getRoot(), path);
        return h;
    }

    private void addChildren(DocumentHierarchy.Node parentNode, Path parentPath) throws Exception {
        final List<Pair<Document, Path>> children = getChildren(parentPath);
        Collections.sort(children, new Comparator<Pair<Document, Path>>() {
            @Override
            public int compare(Pair<Document, Path> p0, Pair<Document, Path> p1) {
                return p0.getRight().toString().compareToIgnoreCase(p1.getRight().toString());
            }
        });
        for (final Pair<Document, Path> child : children) {
            final DocumentHierarchy.Node n = new DocumentHierarchy.Node(child.getLeft());
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
    public boolean exists(Path path) throws Exception {
        return client.checkExists().forPath(path.toString()) != null;
    }
}
