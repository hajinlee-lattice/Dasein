package com.latticeengines.domain.exposed.dataflow.operations;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.shaded.com.google.common.base.Preconditions;

import com.latticeengines.common.exposed.validator.annotation.NotNull;

public final class OperationLogUtils {

    protected OperationLogUtils() {
        throw new UnsupportedOperationException();
    }

    public static final String DEFAULT_FIELD_NAME = "LE_OperationLogs";

    /**
     * Log example [Step=AMSeedMarker, Code=FILTER, Log=NON-OOB]
     * 
     * @param step:
     *            dataflow name, transformer name, etc (optional)
     * @param code:
     *            operation code (required)
     * @param message:
     *            log message (optional)
     * @return
     */
    public static String buildLog(String step, @NotNull OperationCode code, String message) {
        Preconditions.checkNotNull(code);
        List<String> items = new ArrayList<>();
        if (StringUtils.isNotBlank(step)) {
            items.add(String.format("Step=%s", step));
        }
        items.add(String.format("Code=%s", code));
        if (StringUtils.isNotBlank(message) ) {
            items.add(String.format("Log=%s", message));
        }
        return "[" + String.join(",", items) + "]";
    }

    /**
     * Log example
     * [Step=AMSeedCleanup,Code=FILTER,Log=Non-OOB],[Step=AMSeedPriActFix,Code=PRIMARY_DOMAIN,Log=Highest
     * Alexa Rank]
     * 
     * @param logs:
     *            existing logs to append to (optional)
     * @param step:
     *            dataflow name, transformer name, etc (optional)
     * @param code:
     *            operation code (required)
     * @param message:
     *            log message (optional)
     * @return
     */
    public static String appendLog(String logs, String step, @NotNull OperationCode code, String message) {
        String appendLog = buildLog(step, code, message);
        return appendLog(logs, appendLog);
    }

    /**
     * @param existingLogs:
     *            [Step=X1,Code=XX1,Log=XXX1],[Step=X2,Code=XX2,Log=XXX2]
     * @param appendLog:
     *            [Step=X3,Code=XX3,Log=XXX3]
     * @return: [Step=X1,Code=XX1,Log=XXX1],[Step=X2,Code=XX2,Log=XXX2],[Step=X3,Code=XX3,Log=XXX3]
     */
    public static String appendLog(String existingLogs, String appendLog) {
        if (StringUtils.isBlank(existingLogs) && StringUtils.isBlank(appendLog)) {
            return null;
        }
        if (StringUtils.isBlank(existingLogs)) {
            return appendLog;
        }
        if (StringUtils.isBlank(appendLog)) {
            return existingLogs;
        }
        return existingLogs + "," + appendLog;
    }
}
