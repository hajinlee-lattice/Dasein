package com.latticeengines.datacloud.match.exposed.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.MatchStatistics;

public class MatchUtils {

    @SuppressWarnings("unused")
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
        log.error("$JAW$ Merging Match Stats");
        mergedStats.setOrphanedNoMatchCount(stats.getOrphanedNoMatchCount() + newStats.getOrphanedNoMatchCount());
        log.error("   Stats Orphaned No Match: " + stats.getOrphanedNoMatchCount());
        log.error("   New Stats Orphaned No Match: " + newStats.getOrphanedNoMatchCount());
        log.error("   Merges Stats Orphaned No Match: " + mergedStats.getOrphanedNoMatchCount());
        mergedStats.setOrphanedUnmatchedAccountIdCount(stats.getOrphanedUnmatchedAccountIdCount()
                + newStats.getOrphanedUnmatchedAccountIdCount());
        log.error("   Stats Orphaned Unmatched Account ID: " + stats.getOrphanedUnmatchedAccountIdCount());
        log.error("   New Stats Orphaned Unmatched Account ID: " + newStats.getOrphanedUnmatchedAccountIdCount());
        log.error("   Merges Stats Orphaned Unmatched Account ID: " + mergedStats.getOrphanedUnmatchedAccountIdCount());
        mergedStats.setMatchedByMatchKeyCount(stats.getMatchedByMatchKeyCount() + newStats.getMatchedByMatchKeyCount());
        log.error("   Stats Matched By MatchKey: " + stats.getMatchedByMatchKeyCount());
        log.error("   New Stats Matched By MatchKey: " + newStats.getMatchedByMatchKeyCount());
        log.error("   Merges Stats Matched By MatchKey: " + mergedStats.getMatchedByMatchKeyCount());
        mergedStats.setMatchedByAccountIdCount(stats.getMatchedByAccountIdCount()
                + newStats.getMatchedByAccountIdCount());
        log.error("   Stats Matched By Account ID: " + stats.getMatchedByAccountIdCount());
        log.error("   New Stats Matched By Account ID: " + newStats.getMatchedByAccountIdCount());
        log.error("   Merges Stats Matched By Account ID: " + mergedStats.getMatchedByAccountIdCount());
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
        return StringUtils.isNotEmpty(version)
                && version.trim().startsWith(DEFAULT_VERSION_FOR_ACCOUNT_MASTER_BASED_MATCHING);
    }

}
