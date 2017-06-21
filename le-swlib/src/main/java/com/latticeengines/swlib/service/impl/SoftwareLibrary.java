package com.latticeengines.swlib.service.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.graph.traversal.impl.TopologicalTraverse;

public enum SoftwareLibrary implements GraphNode {

    DataCloud("datacloud"), //
    LeadPrioritization("leadprioritization"), //
    ProspectDiscovery("prospectdicovery"), //
    CDL("cdl");

    private static final Map<String, SoftwareLibrary> nameMap = new HashMap<>();

    private final String name;
    private List<SoftwareLibrary> depdencies;

    static {
        for (SoftwareLibrary swlib: SoftwareLibrary.values()) {
            nameMap.put(swlib.getName(), swlib);
        }

        DataCloud.depdencies = Collections.emptyList();
        LeadPrioritization.depdencies = Collections.singletonList(DataCloud);
        CDL.depdencies = Collections.singletonList(DataCloud);
        ProspectDiscovery.depdencies = Collections.singletonList(DataCloud);
    }

    public static SoftwareLibrary fromName(String name) {
        return nameMap.get(name);
    }

    public static List<SoftwareLibrary> getLoadingSequence(List<SoftwareLibrary> libs) {
        TopologicalTraverse traverse = new TopologicalTraverse();
        List<SoftwareLibrary> deps = new ArrayList<>();
        traverse.traverse(libs, (object, ctx) -> {
            deps.add((SoftwareLibrary) object);
        });
        return deps;
    }

    SoftwareLibrary(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public List<SoftwareLibrary> getLoadingSequence() {
        return getLoadingSequence(Collections.singletonList(this));
    }

    @Override
    public Collection<? extends GraphNode> getChildren() {
        return depdencies;
    }

    @Override
    public Map<String, Collection<? extends GraphNode>> getChildMap() {
        return Collections.emptyMap();
    }


}
