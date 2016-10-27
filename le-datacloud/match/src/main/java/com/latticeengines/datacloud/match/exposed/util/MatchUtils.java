package com.latticeengines.datacloud.match.exposed.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.MatchStatistics;

public class MatchUtils {

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
        for (int i = 0; i < stats.getColumnMatchCount().size(); i++) {
            columnCounts.add(stats.getColumnMatchCount().get(i) + newStats.getColumnMatchCount().get(i));
        }
        mergedStats.setColumnMatchCount(columnCounts);
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

    public static boolean isAccountMaster(String dataCloudVersion) {
        return StringUtils.isNotBlank(dataCloudVersion) && dataCloudVersion.startsWith("2.");
    }
}
