package com.latticeengines.datacloud.match.service.impl;

import java.text.SimpleDateFormat;
import java.util.Date;

import javax.inject.Inject;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.datacloud.match.entitymgr.VboUsageReportEntityMgr;
import com.latticeengines.datacloud.match.service.VboUsageService;
import com.latticeengines.domain.exposed.datacloud.manage.VboUsageReport;
import com.latticeengines.domain.exposed.datacloud.usage.SubmitBatchReportRequest;
import com.latticeengines.domain.exposed.datacloud.usage.VboBatchUsageReport;

@Service
public class VboUsageServiceImpl implements VboUsageService {

    private static final Logger log = LoggerFactory.getLogger(VboUsageServiceImpl.class);

    private static final String BATCH = "batch";

    @Value("${datacloud.vbo.usage.s3.bucket}")
    private String s3Bucket;

    @Inject
    private VboUsageReportEntityMgr entityMgr;


    @Override
    public VboBatchUsageReport submitBatchUsageReport(SubmitBatchReportRequest request) {
        String batchRef = request.getBatchRef();
        if (StringUtils.isBlank(batchRef)) {
            batchRef = RandomStringUtils.randomAlphanumeric(6);
            log.info("Cannot find batchRef from request, assign a random ref: {}.", batchRef);
        }
        String reportId = NamingUtils.timestamp(batchRef, new Date());
        String prefix = generateS3Prefix(reportId);

        VboUsageReport report = new VboUsageReport();
        report.setType(VboUsageReport.Type.Batch);
        report.setReportId(reportId);
        report.setNumRecords(request.getNumRecords());
        report.setS3Bucket(s3Bucket);
        report.setS3Path(prefix);
        report.setCreatedAt(new Date());
        report = entityMgr.create(report);

        VboBatchUsageReport response = new VboBatchUsageReport();
        response.setS3Bucket(report.getS3Bucket());
        response.setS3Prefix(report.getS3Path());

        return response;
    }

    private String generateS3Prefix(String reportId) {
        StringBuffer sb = new StringBuffer(BATCH);
        sb.append("/");
        Date now = new Date();
        SimpleDateFormat folderFmt = new SimpleDateFormat("YYYY/MM/dd");
        sb.append(folderFmt.format(now));
        sb.append("/");
        sb.append(reportId);
        return sb.toString();
    }

}
