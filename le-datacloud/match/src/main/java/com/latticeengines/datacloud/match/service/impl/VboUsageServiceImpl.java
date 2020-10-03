package com.latticeengines.datacloud.match.service.impl;

import static com.latticeengines.domain.exposed.datacloud.usage.UsageReportSubmissionSummary.Group.MOVE;
import static com.latticeengines.domain.exposed.datacloud.usage.UsageReportSubmissionSummary.Group.PENDING;
import static com.latticeengines.domain.exposed.datacloud.usage.UsageReportSubmissionSummary.Group.PREVIOUS;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.datacloud.match.entitymgr.VboUsageReportEntityMgr;
import com.latticeengines.datacloud.match.repository.reader.VboUsageReportReaderRepository;
import com.latticeengines.datacloud.match.service.VboUsageService;
import com.latticeengines.domain.exposed.datacloud.manage.VboUsageReport;
import com.latticeengines.domain.exposed.datacloud.usage.SubmitBatchReportRequest;
import com.latticeengines.domain.exposed.datacloud.usage.UsageReportSubmissionSummary;
import com.latticeengines.domain.exposed.datacloud.usage.VboBatchUsageReport;

@Service
public class VboUsageServiceImpl implements VboUsageService {

    private static final Logger log = LoggerFactory.getLogger(VboUsageServiceImpl.class);

    private static final String BATCH = "batch";

    @Value("${datacloud.vbo.usage.s3.bucket}")
    private String s3Bucket;

    @Value("${datacloud.vbo.usage.hourly.limit}")
    private long hourlyLimit;

    @Value("${datacloud.vbo.usage.s3.bucket.vbo}")
    private String vboS3Bucket;

    @Value("${datacloud.vbo.usage.s3.prefix.vbo}")
    private String vboS3Prefix;

    @Inject
    private S3Service s3Service;

    @Inject
    private VboUsageReportEntityMgr entityMgr;

    @Inject
    private VboUsageReportReaderRepository repository;

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

    @Override
    public UsageReportSubmissionSummary moveReportsToVbo() {
        Map<UsageReportSubmissionSummary.Group, List<UsageReportSubmissionSummary.Report>> reports = new HashMap<>();

        reports.put(PREVIOUS, new ArrayList<>());
        long inProcessCount = 0;
        List<VboUsageReport> inProcessReports = repository.findAllByQuietPeriodEndAfter(new Date());
        if (CollectionUtils.isNotEmpty(inProcessReports)) {
            long now = System.currentTimeMillis();
            for (VboUsageReport report: inProcessReports) {
                if (report.getNumRecords() != null) {
                    long remaining = Math.max(0, report.getQuietPeriodEnd().getTime() - now);
                    long total = report.getQuietPeriodEnd().getTime() - report.getSubmittedAt().getTime();
                    long remainingRecords = Math.max(0, Math.round(1. * report.getNumRecords() * remaining / total));
                    inProcessCount += remainingRecords;
                }
                reports.get(PREVIOUS).add(parseReportEntity(report));
            }
        }

        long quota = hourlyLimit - inProcessCount;
        List<VboUsageReport> newReports = repository.findAllBySubmittedAtIsNull();
        if (CollectionUtils.isNotEmpty(newReports)) {
            reports.put(MOVE, new ArrayList<>());
            reports.put(PENDING, new ArrayList<>());
            for (VboUsageReport report: newReports) {
                boolean moved = false;
                try {
                    if (quota > 0) {
                        moveReportToVbo(report);
                        reports.get(MOVE).add(parseReportEntity(report));
                        long cnt = report.getNumRecords() != null ? report.getNumRecords() : 0;
                        quota -= cnt;
                        moved = true;
                    }
                } catch (Exception e) {
                    log.warn("Failed to move report {} to VBO", report.getReportId(), e);
                }
                if (!moved) {
                    reports.get(PENDING).add(parseReportEntity(report));
                }
            }
        }

        UsageReportSubmissionSummary summary = new UsageReportSubmissionSummary();
        summary.setReports(reports);
        return summary;
    }

    private UsageReportSubmissionSummary.Report parseReportEntity(VboUsageReport entity) {
        UsageReportSubmissionSummary.Report report = new UsageReportSubmissionSummary.Report();
        report.setCreatedAt(entity.getCreatedAt());
        report.setSubmittedAt(entity.getSubmittedAt());
        report.setNumRecords(entity.getNumRecords());
        report.setReportId(entity.getReportId());
        return report;
    }

    private void moveReportToVbo(VboUsageReport report) {
        final String srcBucket = report.getS3Bucket();
        String srcFolder = report.getS3Path();
        List<S3ObjectSummary> objects = s3Service.listObjects(srcBucket, srcFolder);
        if (CollectionUtils.isNotEmpty(objects)) {
            Date date = new Date();
            String bundleId = NamingUtils.randomSuffix("Lattice", 6);
            log.info("Submitting report {} as bundle {} to VBO", report.getReportId(), bundleId);
            int idx = 1;
            for (S3ObjectSummary srcObj: objects) {
                final String srcKey = srcObj.getKey();
                String fileName = srcKey.substring(srcKey.lastIndexOf("/") + 1);
                if (fileName.endsWith(".csv") || fileName.endsWith(".gz") || fileName.endsWith(".zip")) {
                    final String tgtBucket = vboS3Bucket;
                    String tgtPrefix = vboS3Prefix;
                    if (!tgtPrefix.endsWith("/")) {
                        tgtPrefix += "/";
                    }
                    final String tgtKey = tgtPrefix //
                            + NamingUtils.timestamp(bundleId + "_" + idx++, date) + ".csv";
                    RetryTemplate retry = RetryUtils.getRetryTemplate(10, //
                            Collections.singleton(AmazonS3Exception.class), null);
                    retry.execute(context -> {
                        if (context.getRetryCount() > 0) {
                            log.info("(Attempt={}}) Retry copying file from from s3://{}/{} to s3://{}/{}", //
                                    context.getRetryCount() + 1, srcBucket, srcKey, tgtBucket, tgtKey);
                        }
                        s3Service.copyObject(srcBucket, srcKey, tgtBucket, tgtKey);
                        return true;
                    });
                    log.info("Copying file {} from s3://{}/{} to s3://{}/{}", //
                            fileName, srcBucket, srcKey, tgtBucket, tgtKey);
                } else {
                    log.warn("Invalid file name: {}", fileName);
                }
            }
        }
        report.setSubmittedAt(new Date());
        int quietHours = 1;
        if (report.getNumRecords() != null && report.getNumRecords() > hourlyLimit) {
            quietHours = (int) Math.ceil(1. * report.getNumRecords() / hourlyLimit);
        }
        report.setQuietPeriodEnd(new Date(System.currentTimeMillis() + TimeUnit.HOURS.toMillis(quietHours)));
        entityMgr.create(report);
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
