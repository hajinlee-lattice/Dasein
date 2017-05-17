package com.latticeengines.dataflow.runtime.cascading.propdata.util.stats.bucket;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.latticeengines.dataflow.runtime.cascading.propdata.util.stats.bucket.impl.AttrStatsDetailsAddMergeUtil;
import com.latticeengines.dataflow.runtime.cascading.propdata.util.stats.bucket.impl.AttrStatsDetailsDedupMergeUtil;

public class AttrStatsDetailsMergeFactory {
    private static Map<MergeType, AttrStatsDetailsMergeTool> utilMap = new ConcurrentHashMap<>();

    public static AttrStatsDetailsMergeTool getUtil(MergeType type) {

        AttrStatsDetailsMergeTool util = utilMap.get(type);

        if (util == null) {
            if (type == MergeType.ADD) {
                util = new AttrStatsDetailsAddMergeUtil();
            } else {
                util = new AttrStatsDetailsDedupMergeUtil();
            }

            utilMap.put(type, util);
        }

        return util;
    }

    public enum MergeType {
        ADD, //
        DEDUP;
    }
}
