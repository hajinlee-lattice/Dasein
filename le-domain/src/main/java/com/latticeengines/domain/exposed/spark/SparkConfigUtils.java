package com.latticeengines.domain.exposed.spark;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit.DataFormat;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;

import reactor.core.publisher.Flux;

public class SparkConfigUtils {

    private SparkConfigUtils() {
    }

    public static List<HdfsDataUnit> getTargetUnits(String workspace, Map<Integer, DataFormat> specialTargets,
            int numTargets) {
        Map<Integer, DataUnit.DataFormat> specialFmts = specialTargets;
        String root;
        if (workspace.endsWith("/")) {
            root = workspace.substring(0, workspace.lastIndexOf("/"));
        } else {
            root = workspace;
        }
        return Flux.range(0, numTargets).map(idx -> {
            HdfsDataUnit dataUnit = HdfsDataUnit.fromPath(root + "/Output" + (idx + 1));
            if (specialFmts != null && specialFmts.containsKey(idx)) {
                dataUnit.setDataFormat(specialFmts.get(idx));
            }
            return dataUnit;
        }).collectList().block();
    }
}
