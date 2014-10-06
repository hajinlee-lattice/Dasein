package com.latticeengines.domain.exposed.camille;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

public class DocumentHierarchy {

    private Node root;

    public DocumentHierarchy(Document rootDocument) {
        root = new Node(rootDocument);
    }

    public Node getRoot() {
        return root;
    }

    public Iterator<Node> breadthFirstIterator() {
        Queue<Node> q = new LinkedList<Node>(Arrays.asList(root));
        Set<Node> visited = new LinkedHashSet<Node>();
        for (Node n = q.poll(); n != null; n = q.poll()) {
            if (!visited.contains(n)) {
                visited.add(n);
                q.addAll(n.getChildren());
            }
        }
        return new IteratorWrapper(visited);
    }

    public Iterator<Node> depthFirstIterator() {
        Set<Node> visited = new LinkedHashSet<Node>();
        traverse(root, visited);
        return new IteratorWrapper(visited);
    }

    private static void traverse(Node parent, Set<Node> visited) {
        if (!visited.contains(parent)) {
            visited.add(parent);
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

    public static final class Node {
        private Document document;
        private List<Node> children = new ArrayList<Node>();

        public Node(Document document) {
            this.document = document;
        }

        public Document getDocument() {
            return document;
        }

        public List<Node> getChildren() {
            return children;
        }
    }
}