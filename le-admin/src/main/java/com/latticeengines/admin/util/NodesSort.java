package com.latticeengines.admin.util;

import java.util.ArrayList;
import java.util.Collection;
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

        if (serviceName.equals("PLS")) {
            List<Node> emailNodes = collections.stream().filter(node -> node.getNode().contains("Emails"))
                    .collect(Collectors.toList());
            emailNodes.sort((n1, n2) -> n1.getNode().compareTo(n2.getNode()));
            List<Node> otherNodes = collections.stream().filter(node -> !node.getNode().contains("Emails"))
                    .collect(Collectors.toList());
            otherNodes.sort((n1, n2) -> n1.getNode().compareTo(n2.getNode()));
            sortedNodes.addAll(emailNodes);
            sortedNodes.addAll(otherNodes);
        } else {
            sortedNodes.addAll(collections);
            sortedNodes.sort((n1, n2) -> n1.getNode().compareTo(n2.getNode()));
        }
        dir.setNodes(sortedNodes);
    }

}
