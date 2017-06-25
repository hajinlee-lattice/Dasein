package com.latticeengines.datacloud.dataflow.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.common.exposed.graph.traversal.impl.TopologicalTraverse;
import com.latticeengines.domain.exposed.datacloud.dataflow.AttrDimension;

public final class DimensionUtils {

    public static List<List<AttrDimension>> getRollupPaths(List<AttrDimension> tree) {
        TopologicalTraverse traverse = new TopologicalTraverse();
        List<List<AttrDimension>> paths = new ArrayList<>();
        List<AttrDimension> reversed = traverse.sort(tree, AttrDimension::clone);
        reversed.forEach(dim -> {
            List<List<AttrDimension>> newPaths = new ArrayList<>();
            for (List<AttrDimension> path: paths) {
                if (path.containsAll(dim.getChildren())) {
                    List<AttrDimension> newPath = new ArrayList<>(path);
                    newPath.add(new AttrDimension(dim.getName()));
                    newPaths.add(newPath);
                }
            }
            if (dim.getChildren().isEmpty()) {
                newPaths.add(Collections.singletonList(new AttrDimension(dim.getName())));
            }
            paths.addAll(newPaths);
        });
        return paths;
    }

    public static String pathToString(Collection<AttrDimension> path) {
        return StringUtils.join(path.stream().map(AttrDimension::getName).collect(Collectors.toList()), "->");
    }

}
