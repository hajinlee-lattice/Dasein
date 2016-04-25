package com.latticeengines.propdata.match.util;

import java.util.ArrayList;
import java.util.List;

import com.latticeengines.domain.exposed.propdata.match.MatchOutput;
import com.latticeengines.domain.exposed.propdata.match.MatchStatistics;

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

}
