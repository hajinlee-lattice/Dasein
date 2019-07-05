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
            // Creates a shallow copy of newOutput, which must be the first MatchOutput produced (since output is null),
            // and returns this MatchOutput.  Note that the MatchStatistics object inside MatchOutput is the only
            // component that is deeply copied.
            return newOutput.shallowCopy();
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
        mergedStats.setOrphanedNoMatchCount(stats.getOrphanedNoMatchCount() + newStats.getOrphanedNoMatchCount());
        mergedStats.setOrphanedUnmatchedAccountIdCount(stats.getOrphanedUnmatchedAccountIdCount()
                + newStats.getOrphanedUnmatchedAccountIdCount());
        mergedStats.setMatchedByMatchKeyCount(stats.getMatchedByMatchKeyCount() + newStats.getMatchedByMatchKeyCount());
        mergedStats.setMatchedByAccountIdCount(stats.getMatchedByAccountIdCount()
                + newStats.getMatchedByAccountIdCount());

        log.debug("Merged Match Statistics");
        log.debug("   Merged Stats Rows Matched: " + mergedStats.getRowsMatched());
        log.debug("   Merged Stats Orphaned No Match: " + mergedStats.getOrphanedNoMatchCount());
        log.debug("   Merged Stats Orphaned Unmatched Account ID: " + mergedStats.getOrphanedUnmatchedAccountIdCount());
        log.debug("   Merged Stats Matched By MatchKey: " + mergedStats.getMatchedByMatchKeyCount());
        log.debug("   Merged Stats Matched By Account ID: " + mergedStats.getMatchedByAccountIdCount());
        return mergedStats;
    }

    public static String toAvroGlobs(String avroDirOrFile) {
        String avroGlobs = avroDirOrFile;
        if (!avroDirOrFile.endsWith(".avro")) {
            avroGlobs += "/*.avro";
        }
        return avroGlobs;
    }

    /**
     * V1.0 SQL table based matcher -- No longer supported
     *
     * @param version
     * @return
     */
    public static boolean isValidForRTSBasedMatch(String version) {
        return StringUtils.isNotEmpty(version)
                && version.trim().startsWith(DEFAULT_VERSION_FOR_DERIVED_COLUMN_CACHE_BASED_MATCHING);
    }

    /**
     * V2.0 AccountMaster + DnB fuzzy match based matcher
     *
     * If coming match request doesn't have DataCloud version provided, populate
     * default version in MatchPlannerBase.setDataCloudVersion
     *
     * @param version
     * @return
     */
    public static boolean isValidForAccountMasterBasedMatch(String version) {
        return StringUtils.isNotEmpty(version)
                && version.trim().startsWith(DEFAULT_VERSION_FOR_ACCOUNT_MASTER_BASED_MATCHING);
    }

}
