package com.latticeengines.serviceflows.workflow.util;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.collections4.CollectionUtils;

import com.latticeengines.domain.exposed.metadata.Table;

public final class ScalingUtils {

    public static int getMultiplier(long count) {
        double normalizedSize = count / 100_000.0;
        return (int) Math.max(1, Math.ceil(Math.log10(normalizedSize)));
    }

    public static long getTableCount(Table table) {
        AtomicLong maxCnt = new AtomicLong(0L);
        Long count = table.getCount();
        if (count != null) {
            maxCnt.set(count);
        } else if (CollectionUtils.isNotEmpty(table.getExtracts())) {
            table.getExtracts().forEach(extract -> {
                Long thisCount = extract.getProcessedRecords();
                if (thisCount != null) {
                    synchronized (maxCnt) {
                        maxCnt.set(maxCnt.get() + thisCount);
                    }
                }
            });
        }
        return maxCnt.get();
    }

}
