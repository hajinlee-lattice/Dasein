package com.latticeengines.datacloud.match.service.impl;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.datacloud.match.service.VboUsageService;
import com.latticeengines.domain.exposed.datacloud.usage.VboBatchUsageReport;
import com.latticeengines.domain.exposed.datacloud.usage.SubmitBatchReportRequest;

@Service
public class VboUsageServiceImpl implements VboUsageService {

    private static final Logger log = LoggerFactory.getLogger(VboUsageServiceImpl.class);

    private static final String BATCH = "batch";

    @Value("${datacloud.vbo.usage.s3.bucket}")
    private String s3Bucket;

    @Override
    public VboBatchUsageReport submitBatchUsageReport(SubmitBatchReportRequest request) {
        String batchRef = request.getBatchRef();
        if (StringUtils.isBlank(batchRef)) {
            batchRef = RandomStringUtils.randomAlphanumeric(6);
            log.info("Cannot find batchRef from request, assign a random ref: {}.", batchRef);
        }
        String prefix = generateS3Prefix(batchRef);

        VboBatchUsageReport report = new VboBatchUsageReport();
        report.setS3Bucket(s3Bucket);
        report.setS3Prefix(prefix);
        return report;
    }

    private String generateS3Prefix(String batchRef) {
        StringBuffer sb = new StringBuffer(BATCH);
        sb.append("/");
        Date now = new Date();
        SimpleDateFormat folderFmt = new SimpleDateFormat("YYYY/MM/dd");
        sb.append(folderFmt.format(now));
        sb.append("/");
        sb.append(NamingUtils.timestamp(batchRef, now));
        return sb.toString();
    }

}
