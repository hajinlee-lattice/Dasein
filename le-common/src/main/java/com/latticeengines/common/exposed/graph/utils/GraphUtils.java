package com.latticeengines.common.exposed.graph.utils;

import java.util.ArrayList;
import java.util.List;

import com.latticeengines.common.exposed.graph.GraphNode;
import com.latticeengines.common.exposed.graph.traversal.impl.DepthFirstSearch;

public class GraphUtils {

    public static <T> List<T> getAllOfType(GraphNode root, Class<T> clazz) {
        DepthFirstSearch search = new DepthFirstSearch();
        List<T> all = new ArrayList<>();
        if (root == null) {
            return all;
        }
        search.run(root, (object, ctx) -> {
            GraphNode node = (GraphNode) object;
            if (clazz.isInstance(node)) {
                all.add(clazz.cast(node));
            }
        });
        return all;
    }

}
