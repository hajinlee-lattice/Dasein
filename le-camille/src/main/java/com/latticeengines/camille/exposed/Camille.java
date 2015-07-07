package com.latticeengines.camille.exposed;

import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.api.SetDataBuilder;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.DocumentDirectory.Node;
import com.latticeengines.domain.exposed.camille.Path;

public class Camille {
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    private final CuratorFramework client;

    public Camille(CuratorFramework client) {
        this.client = client;
    }

    public CuratorFramework getCuratorClient() {
        return client;
    }

    public void create(Path path, List<ACL> acls) throws Exception {
        create(path, new Document(), acls, true);
    }

    public void create(Path path, Document doc, List<ACL> acls) throws Exception {
        create(path, doc, acls, true);
    }

    public void create(Path path, List<ACL> acls, boolean createParents) throws Exception {
        create(path, new Document(), acls, createParents);
    }

    public void create(Path path, Document doc, List<ACL> acls, boolean createParents) throws Exception {
        if (createParents) {
            client.create().creatingParentsIfNeeded().withACL(acls).forPath(path.toString(), doc.getData().getBytes());
        } else {
            client.create().withACL(acls).forPath(path.toString(), doc.getData().getBytes());
        }
        // log.info(String.format("Camille creating doc at %s", path));

        doc.setVersion(0);
    }

    public void set(Path path, Document doc) throws Exception {
        set(path, doc, false);
    }

    public void set(Path path, Document doc, boolean force) throws Exception {
        SetDataBuilder builder = client.setData();
        if (!force)
            builder.withVersion(doc.getVersion());
        Stat stat = builder.forPath(path.toString(), doc.getData().getBytes());
        // log.info(String.format("Camille setting doc at %s", path));
        doc.setVersion(stat.getVersion());
    }

    public void upsert(Path path, List<ACL> acls) throws Exception {
        upsert(path, new Document(), acls);
    }

    public void upsert(Path path, Document doc, List<ACL> acls) throws Exception {
        upsert(path, doc, acls, false);
    }

    public void upsert(Path path, Document doc, List<ACL> acls, boolean force) throws Exception {
        try {
            create(path, doc, acls);
        } catch (KeeperException.NodeExistsException ex) {
            set(path, doc, force);
        }
    }

    public Document get(Path path) throws Exception {
        Stat stat = new Stat();
        Document doc = new Document(new String(client.getData().storingStatIn(stat).forPath(path.toString())));
        // log.info(String.format("Camille getting doc at %s", path));
        doc.setVersion(stat.getVersion());
        return doc;
    }

    public Document get(Path path, CuratorWatcher watcher) throws Exception {
        Stat stat = new Stat();
        Document doc = new Document(new String(client.getData().storingStatIn(stat).usingWatcher(watcher)
                .forPath(path.toString())));
        // log.info(String.format("Camille getting doc at %s", path));
        doc.setVersion(stat.getVersion());
        return doc;
    }

    /**
     * Gets direct children only (not a full hierarchy).
     * 
     * @throws Exception
     */
    public List<AbstractMap.SimpleEntry<Document, Path>> getChildren(Path path) throws Exception {
        List<String> relativeChildPaths = client.getChildren().forPath(path.toString());
        // log.info(String.format("Camille getting children at %s", path));

        List<AbstractMap.SimpleEntry<Document, Path>> out = new ArrayList<>(relativeChildPaths.size());

        for (String relativePath : relativeChildPaths) {
            Path childPath = path.append(relativePath);
            out.add(new SimpleEntry<Document, Path>(get(childPath), childPath));
        }

        return out;
    }

    public DocumentDirectory getDirectory(Path path) {
        DocumentDirectory directory = new DocumentDirectory(path,
                new Function<Path, List<Map.Entry<Document, Path>>>() {
                    @Override
                    public List<Entry<Document, Path>> apply(Path input) {
                        try {
                            return asMapEntry(getChildren(input));
                        } catch (Exception e) {
                            log.error("error getting children of path " + input, e);
                            return null;
                        }
                    }

                    @SuppressWarnings("unchecked")
                    private <E extends Map.Entry<Document, Path>> List<E> asMapEntry(
                            List<AbstractMap.SimpleEntry<Document, Path>> pairs) {
                        return (List<E>) pairs;
                    }
                });
        return directory;
    }

    public void delete(Path path) throws Exception {
        client.delete().deletingChildrenIfNeeded().forPath(path.toString());
        // log.info(String.format("Camille deleting doc at %s", path));
    }

    public boolean exists(Path path) throws Exception {
        return client.checkExists().forPath(path.toString()) != null;
    }

    public void createDirectory(Path parent, DocumentDirectory directory, List<ACL> acls) throws Exception {
        for (Iterator<Node> iter = directory.breadthFirstIterator(); iter.hasNext();) {
            Node node = iter.next();
            create(parent.append(node.getPath()), node.getDocument(), acls);
        }
    }

    public void upsertDirectory(Path parent, DocumentDirectory directory, List<ACL> acls) throws Exception {
        for (Iterator<Node> iter = directory.breadthFirstIterator(); iter.hasNext();) {
            Node node = iter.next();
            if (exists(parent.append(node.getPath()))) {
                set(parent.append(node.getPath()), node.getDocument());
            } else {
                create(parent.append(node.getPath()), node.getDocument(), acls);
            }
        }
    }
}
