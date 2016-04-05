package com.latticeengines.domain.exposed.util;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.metadata.Table;

public class ExtractUtils {
    private static final Logger log = Logger.getLogger(ExtractUtils.class);

    /**
     * For cases in which we want to assert that there is a single extract path
     * tied to a table.
     */
    public static String getSingleExtractPath(Configuration yarnConfiguration, Table table) {
        if (table.getExtracts().size() == 0) {
            throw new RuntimeException(String.format("Expected at least one extract in table %s", table.getName()));
        }

        if (table.getExtracts().size() != 1) {
            log.error(String.format("Ignoring multiple extracts in table %s - only retrieving first extract",
                    table.getName()));
        }
        List<String> matches;
        try {
            matches = HdfsUtils.getFilesByGlob(yarnConfiguration, table.getExtracts().get(0).getPath());
        } catch (IOException e) {
            throw new RuntimeException(String.format("Failure looking up matches for path %s for extracts in table %s",
                    table.getExtracts().get(0).getPath(), table.getName()));
        }
        if (matches.size() == 0) {
            throw new RuntimeException(String.format("No matches for path %s in first extract of table %s", table
                    .getExtracts().get(0).getPath(), table.getName()));
        }

        if (matches.size() > 1) {
            log.error(String.format("Multiple matches for path %s for table with name %s.  Choosing first.", table
                    .getExtracts().get(0).getPath(), table.getName()));
        }

        return matches.get(0);
    }
}
