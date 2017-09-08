package com.latticeengines.datacloud.match.exposed.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.MatchStatistics;

public class MatchUtils {

    private static final Logger log = LoggerFactory.getLogger(MatchUtils.class);
    private static final String DEFAULT_VERSION_FOR_DERIVED_COLUMN_CACHE_BASED_MATCHING = "1.";

    private static final String DEFAULT_VERSION_FOR_ACCOUNT_MASTER_BASED_MATCHING = "2.";

    public static MatchOutput mergeOutputs(MatchOutput output, MatchOutput newOutput) {
        if (output == null) {
            return newOutput;
        }
        output.setStatistics(mergeStatistics(output.getStatistics(), newOutput.getStatistics()));
        output.getResult().addAll(newOutput.getResult());
        return output;
    }

    public static MatchStatistics mergeStatistics(MatchStatistics stats, MatchStatistics newStats) {
        MatchStatistics mergedStats = new MatchStatistics();
        List<Integer> columnCounts = new ArrayList<>();
        if (stats.getColumnMatchCount() != null && stats.getColumnMatchCount().size() <= 10000) {
            for (int i = 0; i < stats.getColumnMatchCount().size(); i++) {
                columnCounts.add(stats.getColumnMatchCount().get(i) + newStats.getColumnMatchCount().get(i));
            }
            mergedStats.setColumnMatchCount(columnCounts);
        }
        mergedStats.setRowsMatched(stats.getRowsMatched() + newStats.getRowsMatched());
        return mergedStats;
    }

    public static String toAvroGlobs(String avroDirOrFile) {
        String avroGlobs = avroDirOrFile;
        if (!avroDirOrFile.endsWith(".avro")) {
            avroGlobs += "/*.avro";
        }
        return avroGlobs;
    }

    public static boolean isValidForRTSBasedMatch(String version) {
        return StringUtils.isEmpty(version)
                || version.trim().startsWith(DEFAULT_VERSION_FOR_DERIVED_COLUMN_CACHE_BASED_MATCHING);
    }

    public static boolean isValidForAccountMasterBasedMatch(String version) {
        return !StringUtils.isEmpty(version)
                && version.trim().startsWith(DEFAULT_VERSION_FOR_ACCOUNT_MASTER_BASED_MATCHING);
    }

}
