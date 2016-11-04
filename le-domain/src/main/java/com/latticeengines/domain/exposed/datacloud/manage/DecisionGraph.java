package com.latticeengines.domain.exposed.datacloud.manage;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Transient;

import org.hibernate.annotations.Index;

import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Access(AccessType.FIELD)
@Table(name = "DecisionGraph")
public class DecisionGraph implements HasPid, Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Index(name = "IX_NAME")
    @Column(name = "GraphName", unique = true, nullable = false, length = 100)
    private String graphName;

    @Column(name = "Vertices", nullable = false, length = 1000)
    private String vertices;

    @Column(name = "StartingVertices", length = 1000)
    private String startingVertices;

    @Column(name = "Edges", length = 1000)
    private String edges;

    @Transient
    private List<Node> startingNodes;

    @Transient
    private Map<String, Node> nodeMap;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public String getGraphName() {
        return graphName;
    }

    private void setGraphName(String graphName) {
        this.graphName = graphName;
    }

    private String getVertices() {
        return vertices;
    }

    private void setVertices(String vertices) {
        this.vertices = vertices;
    }

    private String getStartingVertices() {
        return startingVertices;
    }

    private void setStartingVertices(String startingVertices) {
        this.startingVertices = startingVertices;
    }

    private String getEdges() {
        return edges;
    }

    private void setEdges(String edges) {
        this.edges = edges;
    }

    public List<Node> getStartingNodes() {
        if (startingNodes == null) {
            constructGraph();
        }
        return startingNodes;
    }

    public Node getNode(String nodeName) {
        if (nodeMap == null) {
            constructGraph();
        }
        return nodeMap.get(nodeName);
    }

    private void constructGraph() {
        nodeMap = new HashMap<>();
        List<String> nodeNameList = new ArrayList<>();
        for (String nodeName: getVertices().split(",")) {
            Node node = new Node(nodeName);
            nodeMap.put(nodeName, node);
            nodeNameList.add(nodeName);
        }

        Map<Integer, List<Integer>> edgeMap = parseEdges();
        for (Map.Entry<Integer, List<Integer>> entry: edgeMap.entrySet()) {
            Integer idx = entry.getKey();
            Node node = nodeMap.get(nodeNameList.get(idx));
            for (Integer idx2: entry.getValue()) {
                Node child = nodeMap.get(nodeNameList.get(idx2));
                node.addChild(child);
            }
        }

        startingNodes = new ArrayList<>();
        for (String idxStr: getStartingVertices().split(",")) {
            String nodeName = nodeNameList.get(Integer.valueOf(idxStr));
            startingNodes.add(nodeMap.get(nodeName));
        }
    }

    private Map<Integer, List<Integer>> parseEdges() {
        String[] groups = getEdges().split("\\|");
        Map<Integer, List<Integer>> toReturn = new HashMap<>();
        for (String group: groups) {
            String[] ends = group.split(":");
            if (ends.length != 2) {
                throw new IllegalArgumentException("Invalid edge definition " + group);
            }
            Integer fromVertex = Integer.valueOf(ends[0]);
            List<Integer> toVertices = new ArrayList<>();
            for (String v: ends[1].split(",")) {
                toVertices.add(Integer.valueOf(v));
            }
            toReturn.put(fromVertex, toVertices);
        }
        return toReturn;
    }

    public static class Node {
        private final String name;
        private List<Node> children = new ArrayList<>();

        Node(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        void addChild(Node child) {
            if (!this.children.contains(child) && !this.equals(child)) {
                this.children.add(child);
            }
        }

        public List<Node> getChildren() {
            return children;
        }

        @Override
        public boolean equals(Object that) {
            if (!(that instanceof Node)) {
                return false;
            }

            Node thatNode = (Node) that;
            return this.getName().equals(thatNode.getName());
        }
    }

}
