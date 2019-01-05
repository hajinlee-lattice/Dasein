package com.latticeengines.domain.exposed.datacloud.manage;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Table;
import javax.persistence.Transient;

import org.apache.commons.lang3.StringUtils;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.domain.exposed.actors.ActorType;
import com.latticeengines.domain.exposed.datacloud.match.utils.MatchActorUtils;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

@Entity
@Access(AccessType.FIELD)
@Table(name = "DecisionGraph", indexes = { @Index(name = "IX_NAME", columnList = "GraphName") })
public class DecisionGraph implements HasPid, Serializable {
    private static final long serialVersionUID = -803250604271710254L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @Column(name = "GraphName", unique = true, nullable = false, length = 100)
    private String graphName;

    // Content format
    // Actor name abbreviations separated by comma
    @Column(name = "Vertices", nullable = false, length = 1000)
    private String vertices;

    @Column(name = "StartingVertices", length = 1000)
    private String startingVertices;

    @Column(name = "Description", length = 1000)
    private String description;

    @Column(name = "Edges", length = 1000)
    private String edges;

    // Content format
    // JunctionActorNameAbbr1:DecisionGraph1,JunctionActorNameAbbr2:DecisionGraph2,...
    @Column(name = "JunctionGraphs", length = 1000)
    private String junctionGraphs;

    @Column(name = "Entity", length = 100)
    private String entity;

    @Transient
    private List<Node> startingNodes;

    @Transient
    private Map<String, Node> nodeMap;

    // JunctionName (short actor name) -> decision graph
    @Transient
    private Map<String, String> junctionGraphMap;

    /*******************************
     * Constructor & Getter/Setter
     *******************************/

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

    private String getVertices() {
        return vertices;
    }

    private String getStartingVertices() {
        return startingVertices;
    }

    public String getDescription() {
        return description;
    }

    private String getEdges() {
        return edges;
    }

    public String getJunctionGraphs() {
        return junctionGraphs;
    }

    public void setJunctionGraphs(String junctionGraphs) {
        this.junctionGraphs = junctionGraphs;
    }

    public String getEntity() {
        return entity;
    }

    @VisibleForTesting
    public void setEntity(String entity) {
        this.entity = entity;
    }

    /********************
     * Business methods
     ********************/

    /**
     * Based on current junction name, decide next decision graph to jump to
     * 
     * @param junctionName
     * @return
     */
    public String getNextGraphForJunction(String junctionName) {
        junctionName = MatchActorUtils.getShortActorName(junctionName, ActorType.JUNCION);
        return getJunctionGraphMap().get(junctionName);
    }

    /**
     * 
     * @return Junction name -> decision graph to jump to
     */
    public Map<String, String> getJunctionGraphMap() {
        if (junctionGraphMap == null) {
            if (StringUtils.isBlank(junctionGraphs)) {
                junctionGraphMap = new HashMap<>();
            } else {
                junctionGraphMap = Stream.of(junctionGraphs.split(","))
                        .collect(Collectors.toMap(x -> x.split(":")[0], x -> x.split(":")[1]));
            }
        }
        return junctionGraphMap;
    }

    public List<Node> getStartingNodes() {
        if (startingNodes == null) {
            synchronized (this) {
                if (startingNodes == null) {
                    constructGraph();
                }
            }
        }
        return startingNodes;
    }

    public Node getNode(String nodeName) {
        if (startingNodes == null) {
            synchronized (this) {
                if (startingNodes == null) {
                    constructGraph();
                }
            }
        }
        return nodeMap.get(nodeName);
    }

    private void constructGraph() {
        nodeMap = new HashMap<>();
        List<String> nodeNameList = new ArrayList<>();
        for (String nodeName : getVertices().split(",")) {
            Node node = new Node(nodeName);
            nodeMap.put(nodeName, node);
            nodeNameList.add(nodeName);
        }

        Map<Integer, List<Integer>> edgeMap = parseEdges();
        for (Map.Entry<Integer, List<Integer>> entry : edgeMap.entrySet()) {
            Integer idx = entry.getKey();
            Node node = nodeMap.get(nodeNameList.get(idx));
            for (Integer idx2 : entry.getValue()) {
                Node child = nodeMap.get(nodeNameList.get(idx2));
                node.addChild(child);
            }
        }

        startingNodes = new ArrayList<>();
        for (String idxStr : getStartingVertices().split(",")) {
            String nodeName = nodeNameList.get(Integer.valueOf(idxStr));
            startingNodes.add(nodeMap.get(nodeName));
        }
    }

    private Map<Integer, List<Integer>> parseEdges() {
        String[] groups = getEdges().split("\\|");
        Map<Integer, List<Integer>> toReturn = new HashMap<>();
        for (String group : groups) {
            String[] ends = group.split(":");
            if (ends.length != 2) {
                throw new IllegalArgumentException("Invalid edge definition " + group);
            }
            Integer fromVertex = Integer.valueOf(ends[0]);
            List<Integer> toVertices = new ArrayList<>();
            for (String v : ends[1].split(",")) {
                toVertices.add(Integer.valueOf(v));
            }
            toReturn.put(fromVertex, toVertices);
        }
        return toReturn;
    }

    public static class Node {
        private final String name; // Actor name abbreviation, not full name
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
