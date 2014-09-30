package com.latticeengines.domain.exposed.camille;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DocumentHierarchy implements Iterable<DocumentHierarchy.Node> {

    private Node root;
    
    public DocumentHierarchy(Document rootDocument) {
        root = new Node(rootDocument);
    }
    
    public Node getRoot() {
        return root;
    }
    
    public static class Node {
        private Document document;
        List<Node> children = new ArrayList<Node>();
        
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

    @Override
    public Iterator<DocumentHierarchy.Node> iterator() {
        // TODO
        return null;
    }
}
