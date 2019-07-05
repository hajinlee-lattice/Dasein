package com.latticeengines.common.exposed.util;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchUtils {

    private static Logger log = LoggerFactory.getLogger(BatchUtils.class);

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

    public static int[] divideBatches(long totalSize, int batchCnt) {
        long batchSize = totalSize / batchCnt;
        int[] batches = new int[batchCnt];
        long sum = 0L;
        for (int i = 0; i < batchCnt - 1; i++) {
            batches[i] = (int) batchSize;
            sum += (int) batchSize;
        }
        batches[batchCnt - 1] = (int) (totalSize - sum);
        log.info("Divide input into blocks [" + StringUtils.join(batches, ", ") + "]");
        return batches;
    }
}
