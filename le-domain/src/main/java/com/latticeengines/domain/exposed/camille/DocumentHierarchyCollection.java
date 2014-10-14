package com.latticeengines.domain.exposed.camille;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;

import org.apache.commons.lang.ObjectUtils;

import com.google.common.base.Function;

public class DocumentHierarchyCollection implements Serializable {
    private static final long serialVersionUID = 1L;

    private List<Node> children;

    public DocumentHierarchyCollection() {
        children = new ArrayList<Node>();
    }

    public DocumentHierarchyCollection(Path root, Function<Path, List<Map.Entry<Document, Path>>> getChildren) {
        List<Entry<Document, Path>> childPairs = getChildren.apply(root);
        children = new ArrayList<Node>(childPairs.size());
        for (Entry<Document, Path> childPair : childPairs) {
            children.add(new Node(childPair, getChildren));
        }
        Collections.sort(children);
    }

    public List<Node> getChildren() {
        return children;
    }

    public void setChildren(List<Node> children) {
        this.children = children;
    }

    public Iterator<Node> breadthFirstIterator() {
        Queue<Node> q = new LinkedList<Node>(children);
        Set<Node> visited = new LinkedHashSet<Node>();
        for (Node n = q.poll(); n != null; n = q.poll()) {
            if (visited.add(n)) {
                q.addAll(n.getChildren());
            }
        }
        return new IteratorWrapper(visited);
    }

    public Iterator<Node> depthFirstIterator() {
        Set<Node> visited = new LinkedHashSet<Node>();
        for (Node child : children) {
            traverse(child, visited);
        }
        return new IteratorWrapper(visited);
    }

    private static void traverse(Node parent, Set<Node> visited) {
        if (visited.add(parent)) {
            for (Node child : parent.getChildren()) {
                traverse(child, visited);
            }
        }
    }

    private static class IteratorWrapper implements Iterator<Node> {
        private final Iterator<Node> iter;

        IteratorWrapper(Collection<Node> c) {
            iter = c.iterator();
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
        public void remove() {
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
        if (!(obj instanceof DocumentHierarchyCollection))
            return false;
        DocumentHierarchyCollection that = (DocumentHierarchyCollection) obj;
        if (children == null) {
            if (that.children != null)
                return false;
        } else if (that.children == null) {
            return false;
        } else if (children.size() != that.children.size()) {
            return false;
        } else {
            for (Iterator<Node> thisIter = breadthFirstIterator(), thatIter = that.breadthFirstIterator(); thisIter
                    .hasNext();) {
                if (!thisIter.next().equals(thatIter.next()))
                    return false;
            }
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
            children = new ArrayList<Node>(childPairs.size());
            for (Entry<Document, Path> childPair : childPairs) {
                children.add(new Node(childPair, getChildren));
            }
            Collections.sort(children);
        }

        public Node() {
            children = new ArrayList<Node>();
        }

        public Path getPath() {
            return path;
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

        public void setChildren(List<Node> children) {
            this.children = children;
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
}