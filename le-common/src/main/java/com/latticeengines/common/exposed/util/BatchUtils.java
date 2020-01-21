package com.latticeengines.common.exposed.util;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BatchUtils {

    protected BatchUtils() {
        throw new UnsupportedOperationException();
    }

    private static final Logger log = LoggerFactory.getLogger(BatchUtils.class);

    /**
     * Based on total number of records, minimum number of records per batch,
     * maximum number of records per batch and maximum number of batches being
     * processed concurrently, determine total number of batches to divide to
     *
     * @param totalSize:
     *            total number of records
     * @param minBatchSize:
     *            minimum number of records per batch
     * @param maxBatchSize:
     *            maximum number of records per batch
     * @param maxConcurrentBatchCnt:
     *            maximum number of batches being processed concurrently
     * @return: total number of batches to divide to
     */
    public static int determineBatchCnt(long totalSize, int minBatchSize, int maxBatchSize, int maxConcurrentBatchCnt) {
        int batchCnt;
        if (totalSize < ((long) minBatchSize * maxConcurrentBatchCnt)) {
            batchCnt = Math.max((int) (totalSize / minBatchSize), 1);
        } else if (totalSize > ((long) maxBatchSize * maxConcurrentBatchCnt)) {
            batchCnt = (int) (totalSize / maxBatchSize);
        } else {
            batchCnt = maxConcurrentBatchCnt;
        }
        return batchCnt;
    }

    /**
     * Based on total number of records and total number of batches to divide
     * to, determine number of records for each batch
     *
     * @param totalSize:
     *            total number of records
     * @param batchCnt:
     *            total number of batches
     * @return: a list of record numbers for each batch
     */
    public static int[] divideBatches(long totalSize, int batchCnt) {
        long batchSize = totalSize / batchCnt;
        int[] batches = new int[batchCnt];
        long sum = 0L;
        for (int i = 0; i < batchCnt - 1; i++) {
            batches[i] = (int) batchSize;
            sum += (int) batchSize;
        }
        batches[batchCnt - 1] = (int) (totalSize - sum);
        log.info("Divide input into batches [" + StringUtils.join(batches, ", ") + "]");
        return batches;
    }
}
