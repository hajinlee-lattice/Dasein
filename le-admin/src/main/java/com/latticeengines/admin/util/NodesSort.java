package com.latticeengines.admin.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory.Node;

public final class NodesSort {

    protected NodesSort() {
        throw new UnsupportedOperationException();
    }

    public static void sortByServiceName(SerializableDocumentDirectory dir, String serviceName) {
        List<Node> sortedNodes = new ArrayList<>();
        Collection<Node> collections = dir.getNodes();
        if (collections == null || collections.size() == 0) {
            return;
        }

        if ("PLS".equals(serviceName)) {
            List<Node> emailNodes = collections.stream().filter(node -> node.getNode().contains("Emails"))
                    .sorted(Comparator.comparing(Node::getNode)).collect(Collectors.toList());
            List<Node> otherNodes = collections.stream().filter(node -> !node.getNode().contains("Emails"))
                    .sorted(Comparator.comparing(Node::getNode)).collect(Collectors.toList());
            sortedNodes.addAll(emailNodes);
            sortedNodes.addAll(otherNodes);
        } else {
            sortedNodes.addAll(collections);
            sortedNodes.sort(Comparator.comparing(Node::getNode));
        }
        dir.setNodes(sortedNodes);
    }

}
