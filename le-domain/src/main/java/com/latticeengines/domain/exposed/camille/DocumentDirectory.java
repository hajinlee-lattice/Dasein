package com.latticeengines.domain.exposed.camille;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;

import org.apache.commons.lang3.ObjectUtils;

import com.google.common.base.Function;

public class DocumentDirectory implements Serializable {
    private static final long serialVersionUID = 1L;

    private Path root;
    private List<Node> children;

    public DocumentDirectory() {
        this(new Path("/"));
    }

    public DocumentDirectory(Path root) {
        this.root = root;
        children = new ArrayList<>();
    }

    public DocumentDirectory(Path root, Function<Path, List<Map.Entry<Document, Path>>> getChildren) {
        List<Entry<Document, Path>> childPairs = getChildren.apply(this.root = root);
        if (childPairs == null) {
            children = new ArrayList<>(0);
        } else {
            children = new ArrayList<>(childPairs.size());
            for (Entry<Document, Path> childPair : childPairs) {
                children.add(new Node(childPair, getChildren));
            }
            Collections.sort(children);
        }
    }

    public DocumentDirectory(DocumentDirectory source) {
        this(source.root);

        Iterator<Node> iter = source.breadthFirstIterator();
        while (iter.hasNext()) {
            Node node = iter.next();
            add(node.getPath(), node.getDocument());
        }
    }

    public List<Node> getChildren() {
        return children;
    }

    public Path getRootPath() {
        return root;
    }

    public void makePathsLocal() {
        Iterator<Node> iter = depthFirstIterator();
        while (iter.hasNext()) {
            Node node = iter.next();
            node.setPath(node.getPath().local(root));
        }
        root = new Path("/");
    }

    public void makePathsAbsolute(Path newroot) {
        Iterator<Node> iter = depthFirstIterator();
        while (iter.hasNext()) {
            Node node = iter.next();
            node.setPath(node.getPath().prefix(newroot));
        }
        root = newroot;
    }

    public Node get(String relativePath) {
        return get(relativePath, false);
    }

    public Node get(String path, boolean pathIsAbsolute) {
        if (pathIsAbsolute) {
            return get(new Path(path));
        } else {
            return get(this.root.append(new Path(path)));
        }
    }

    public Node get(Path path) {
        Iterator<Node> iter = depthFirstIterator();
        while (iter.hasNext()) {
            Node node = iter.next();
            if (node.getPath().equals(path)) {
                return node;
            }
        }

        return null;
    }

    public Node add(String relativePath, String data) {
        return add(relativePath, data, false, false);
    }

    public Node add(String relativePath, String data, boolean withParent) {
        return add(relativePath, data, false, withParent);
    }

    public Node add(String path, String data, boolean pathIsAbsolute, boolean withParent) {
        Path relativePath = new Path(path);
        if (data == null) data = "";
        if (pathIsAbsolute) {
            return add(relativePath, new Document(data), withParent);
        } else {
            return add(this.root.append(relativePath), new Document(data), withParent);
        }
    }

    public Node add(Path path) {
        return add(path, new Document());
    }

    public Node add(Path path, Document document) {
        return add(path, document, false);
    }

    public Node add(Path path, Document document, boolean withParent) {
        Node node = get(path);
        if (node != null) {
            node.setDocument(document);
            return node;
        }
        Path parentPath = path.parent();
        if (parentPath.equals(root)) {
            node = new Node(path, document);
            children.add(node);
        } else {
            Node parent = get(parentPath);
            if (!withParent && parent == null) {
                throw new IllegalArgumentException("Cannot add path " + path
                        + ".  No such parent node exists with path " + parentPath);
            } else if (withParent) {
                while (parent == null) {
                    parent = add(parentPath, new Document(), true);
                }
            }
            node = new Node(path, document);
            parent.children.add(node);
        }

        return node;
    }

    public void delete(Path path) {
        Path parentPath = path.parent();
        List<Node> siblings;
        if (parentPath.equals(root)) {
            siblings = children;
        } else {
            Node parent = get(parentPath);
            if (parent == null) {
                throw new IllegalArgumentException("Cannot delete path " + path
                        + ".  No such parent node exists with path " + path.parent());
            }
            siblings = parent.children;
        }

        Iterator<Node> iter = siblings.iterator();
        while (iter.hasNext()) {
            Node sibling = iter.next();
            if (sibling.getPath().equals(path)) {
                iter.remove();
                return;
            }
        }

        throw new IllegalArgumentException("Cannot delete node with " + path + ".  No such node exists");
    }

    public Iterator<Node> leafFirstIterator() {
        return breadthFirstIterator(true);
    }

    public Iterator<Node> breadthFirstIterator() {
        return breadthFirstIterator(false);
    }

    private ListIterator<Node> breadthFirstIterator(boolean reverse) {
        Queue<Node> q = new LinkedList<Node>(nullSafe(children));
        Set<Node> visited = new LinkedHashSet<Node>();
        for (Node n = q.poll(); n != null; n = q.poll()) {
            if (visited.add(n)) {
                q.addAll(nullSafe(n.getChildren()));
            }
        }
        return new IteratorWrapper(visited, reverse);
    }

    public Iterator<Node> depthFirstIterator() {
        Set<Node> visited = new LinkedHashSet<Node>();
        for (Node child : nullSafe(children)) {
            traverse(child, visited);
        }
        return new IteratorWrapper(visited, false);
    }

    private static void traverse(Node parent, Set<Node> visited) {
        if (visited.add(parent)) {
            for (Node child : nullSafe(parent.getChildren())) {
                traverse(child, visited);
            }
        }
    }

    private static <T> List<T> nullSafe(List<T> list) {
        return list == null ? Collections.<T> emptyList() : list;
    }

    private static class IteratorWrapper implements ListIterator<Node> {
        private final ListIterator<Node> iter;

        IteratorWrapper(Collection<Node> c, boolean reverse) {
            ArrayList<Node> list = new ArrayList<Node>(c);
            if (reverse)
                Collections.reverse(list);
            iter = list.listIterator();
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public Node next() {
            return iter.next();
        }

        @Override
        public boolean hasPrevious() {
            return iter.hasPrevious();
        }

        @Override
        public Node previous() {
            return iter.previous();
        }

        @Override
        public int nextIndex() {
            return iter.nextIndex();
        }

        @Override
        public int previousIndex() {
            return iter.previousIndex();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void set(Node e) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void add(Node e) {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        if (children != null) {
            for (Iterator<Node> iter = breadthFirstIterator(); iter.hasNext();) {
                Node node = iter.next();
                result = prime * result + ((node == null) ? 0 : node.hashCode());
            }
        }
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (!(obj instanceof DocumentDirectory))
            return false;
        DocumentDirectory that = (DocumentDirectory) obj;
        if (children == null) {
            if (that.children != null)
                return false;
        } else if (that.children == null) {
            return false;
        } else {
            Iterator<Node> thisIter = breadthFirstIterator(), thatIter = that.breadthFirstIterator();
            while (thisIter.hasNext() && thatIter.hasNext()) {
                if (!thisIter.next().equals(thatIter.next()))
                    return false;
            }
            if (thisIter.hasNext() || thatIter.hasNext())
                return false;
        }
        return true;
    }

    public static class Node implements Comparable<Node>, Serializable {
        private static final long serialVersionUID = 1L;

        private Path path;
        private Document document;
        private List<Node> children;

        private Node(Map.Entry<Document, Path> entry, Function<Path, List<Entry<Document, Path>>> getChildren) {
            path = entry.getValue();
            document = entry.getKey();
            List<Entry<Document, Path>> childPairs = getChildren.apply(path);
            if (childPairs == null) {
                children = new ArrayList<Node>(0);
            } else {
                children = new ArrayList<Node>(childPairs.size());
                for (Entry<Document, Path> childPair : childPairs) {
                    children.add(new Node(childPair, getChildren));
                }
                Collections.sort(children);
            }
        }

        public Node(Path path, Document document) {
            this.path = path;
            this.document = document;
            this.children = new ArrayList<Node>();
        }

        public Path getPath() {
            return path;
        }

        public void setPath(Path path) {
            this.path = path;
        }

        public Document getDocument() {
            return document;
        }

        public void setDocument(Document document) {
            this.document = document;
        }

        public List<Node> getChildren() {
            return children;
        }

        public Node getChild(String pathSuffix) {
            for (Node child : children) {
                if (child.getPath().getSuffix().equals(pathSuffix)) {
                    return child;
                }
            }
            return null;
        }

        @Override
        public int compareTo(Node other) {
            return ObjectUtils.compare(path != null ? path.toString() : null,
                    other != null && other.path != null ? other.path.toString() : null);
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((path == null) ? 0 : path.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (!(obj instanceof Node))
                return false;
            Node other = (Node) obj;
            if (path == null) {
                if (other.path != null)
                    return false;
            } else if (!path.equals(other.path))
                return false;
            return true;
        }
    }

    public Node getChild(String pathSuffix) {
        for (Node child : children) {
            if (child.getPath().getSuffix().equals(pathSuffix)) {
                return child;
            }
        }
        return null;
    }
}