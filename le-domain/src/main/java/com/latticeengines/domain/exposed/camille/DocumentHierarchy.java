package com.latticeengines.domain.exposed.camille;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class DocumentHierarchy {

    private Node root;

    public DocumentHierarchy(Document rootDocument) {
        root = new Node(rootDocument);
    }

    public Node getRoot() {
        return root;
    }

    public Iterator<DocumentHierarchy.Node> breadthFirstIterator() {
        List<DocumentHierarchy.Node> out = new ArrayList<DocumentHierarchy.Node>();
        Queue<DocumentHierarchy.Node> q = new LinkedList<DocumentHierarchy.Node>(Arrays.asList(root));
        for (Node n = q.poll(); n != null; n = q.poll()) {
            out.add(n);
            q.addAll(n.getChildren());
        }
        return new IteratorWrapper(out);
    }

    public Iterator<DocumentHierarchy.Node> depthFirstIterator() {
        List<DocumentHierarchy.Node> out = new ArrayList<DocumentHierarchy.Node>();
        traverse(root, out);
        return new IteratorWrapper(out);
    }

    private static void traverse(Node parent, List<DocumentHierarchy.Node> out) {
        out.add(parent);
        for (Node child : parent.getChildren()) {
            traverse(child, out);
        }
    }

    private static class IteratorWrapper implements Iterator<DocumentHierarchy.Node> {
        private final Iterator<DocumentHierarchy.Node> iter;

        IteratorWrapper(List<DocumentHierarchy.Node> list) {
            iter = list.iterator();
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

    public static class Node {
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