package com.latticeengines.datacloud.etl.ingestion.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.BatchUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.SleepUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.datacloud.core.entitymgr.PatchBookEntityMgr;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.core.util.PatchBookUtils;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionProgressService;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionVersionService;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.ingestion.PatchBookConfiguration;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;
import com.latticeengines.domain.exposed.datacloud.manage.IngestionProgress;
import com.latticeengines.domain.exposed.datacloud.manage.PatchBook;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.datacloud.match.patch.PatchMode;
import com.latticeengines.domain.exposed.datacloud.match.patch.PatchRequest;
import com.latticeengines.domain.exposed.datacloud.match.patch.PatchValidationResponse;
import com.latticeengines.proxy.exposed.matchapi.PatchProxy;

@Component("ingestionPatchBookProviderService")
public class IngestionPatchBookProviderServiceImpl extends IngestionProviderServiceImpl {

    private static final Logger log = LoggerFactory.getLogger(IngestionPatchBookProviderServiceImpl.class);

    @Value("${datacloud.patcher.ingest.batch.size.min}")
    private int minBatchSize;

    @Value("${datacloud.patcher.ingest.batch.size.max}")
    private int maxBatchSize;

    @Value("${datacloud.patcher.ingest.concurrent.batch.cnt.max}")
    private int maxConcurrentBatchCnt;

    @Value("${datacloud.patcher.ingest.concurrent.thread.cnt:4}")
    private int threadCount;

    @Value("${datacloud.patcher.ingest.transaction.batch.max:20000}")
    private int trxBatchMax;

    @Inject
    private PatchBookEntityMgr patchBookEntityMgr;

    @Inject
    private IngestionProgressService ingestionProgressService;

    @Inject
    private HdfsPathBuilder hdfsPathBuilder;

    @Inject
    private PatchProxy patchProxy;

    @Inject
    private IngestionVersionService ingestionVersionService;

    @Override
    public void ingest(IngestionProgress progress) throws Exception {
        Ingestion ingestion = progress.getIngestion();
        PatchBookConfiguration patchConfig = (PatchBookConfiguration) ingestion.getProviderConfiguration();
        Date currentDate = new Date();
        if (HdfsUtils.isDirectory(yarnConfiguration, progress.getDestination())) {
            log.info(progress.getDestination() + " already exists. Delete first.");
            HdfsUtils.rmdir(yarnConfiguration, progress.getDestination());
        }
        HdfsUtils.mkdir(yarnConfiguration, progress.getDestination());

        long totalSize = 0;
        Integer batchCnt = patchConfig.getBatchCnt();
        if (batchCnt == null) {
            Long minPid = patchConfig.getMinPid();
            Long maxPid = patchConfig.getMaxPid();
            // compute total num of records if minPid and maxPid provided
            if (minPid != null && maxPid != null) {
                // validate provided minPid & maxPid
                if (minPid < 0 || maxPid <= minPid) {
                    throw new RuntimeException(String.format("Invalid MinPid or MaxPid provided: MinPid=%d, MaxPid=%d",
                            patchConfig.getMinPid(), patchConfig.getMaxPid()));
                }
            } else {
                // ingest all records if minPid and maxPid not provided
                Map<String, Long> minMaxPid = patchBookEntityMgr.findMinMaxPid(patchConfig.getBookType());
                patchConfig.setMinPid(minMaxPid.get(PatchBookUtils.MIN_PID));
                // minPid is always inclusive, maxPid is inclusive in
                // findMinMaxPid(), but exclusive in PatchBookConfiguration due
                // to the API to get patch book list requires maxPid as
                // exclusive
                patchConfig.setMaxPid(minMaxPid.get(PatchBookUtils.MAX_PID) + 1);

            }
            totalSize = patchConfig.getMaxPid() - patchConfig.getMinPid();

            batchCnt = BatchUtils.determineBatchCnt(totalSize, minBatchSize, maxBatchSize, maxConcurrentBatchCnt);
        }
        if (batchCnt <= 0) {
            throw new RuntimeException(String.format("Invalid batch count provided/generated: %d", batchCnt));
        }
        log.info("PatchBookConfig = {}, TotalRecords = {}, TotalBatches = {}", JsonUtils.serialize(patchConfig),
                totalSize, batchCnt);
        int[] batches = BatchUtils.divideBatches(totalSize, batchCnt);

        List<Ingester> ingesters = initializeIngester(patchConfig, currentDate, progress, batches);
        ExecutorService executorService = ThreadPoolUtils.getFixedSizeThreadPool("PatchBookPool", threadCount);
        ThreadPoolUtils.runInParallel(executorService, ingesters, (int) TimeUnit.DAYS.toMinutes(1), 10);

        updateCurrentVersion(ingestion, progress.getVersion());

        progress = ingestionProgressService.updateProgress(progress).size(totalSize).status(ProgressStatus.FINISHED)
                .commit(true);
        log.info("Ingestion finished. Progress: " + progress.toString());
    }

    private List<Ingester> initializeIngester(PatchBookConfiguration patchConfig, Date currentDate,
            IngestionProgress progress, int[] batches) {
        List<Ingester> ingesters = new ArrayList<>();
        Preconditions.checkNotNull(patchConfig.getMinPid());
        long minPid = patchConfig.getMinPid();
        for (int i = 0; i < batches.length; i++) {
            Ingester ingester = new Ingester(patchConfig, currentDate, progress, i, batches[i], minPid);
            minPid += batches[i];
            ingesters.add(ingester);
        }
        return ingesters;
    }

    private List<PatchBook> getActiveBooks(List<PatchBook> books, Date currentDate, long minPid, long maxPid) {
        return books.stream() //
                .filter(book -> !PatchBookUtils.isEndOfLife(book, currentDate) && book.getPid() >= minPid
                        && book.getPid() < maxPid) //
                .collect(Collectors.toList());
    }

    private void postProcess(List<PatchBook> books, String dataCloudVersion, PatchBookConfiguration patchConfig,
            Date currentDate) {
        List<Long> pidsToClearHotFix = new ArrayList<>();
        List<Long> pidsToSetEOL = new ArrayList<>();
        List<Long> pidsToClearEOL = new ArrayList<>();
        List<Long> pidsToSetEffectiveSince = new ArrayList<>();
        List<Long> pidsToSetExpireAfter = new ArrayList<>();
        List<Long> pidsToClearExpireAfter = new ArrayList<>();
        books.forEach(book -> {
            // For hot-fixed items, need to reset HotFix flag to be false
            if (PatchMode.HotFix.equals(patchConfig.getPatchMode())) {
                pidsToClearHotFix.add(book.getPid()); // books are already
                                                      // filtered by HotFix flag
            }
            // For items with EOL flag not in sync with EffectiveSince and
            // ExpireAfter, need to update EOL flag
            boolean expectedEOL = PatchBookUtils.isEndOfLife(book, currentDate);
            if (book.isEndOfLife() != expectedEOL) {
                if (expectedEOL) {
                    pidsToSetEOL.add(book.getPid());
                } else {
                    pidsToClearEOL.add(book.getPid());
                }
            }
            // EffectiveSinceVersion is the first datacloud version when we
            // start to patch this item. If the item is effective first, then
            // expire, and then become effective again, EffectiveSinceVersion
            // does not change.
            if (!expectedEOL && StringUtils.isBlank(book.getEffectiveSinceVersion())) {
                pidsToSetEffectiveSince.add(book.getPid());
            }
            // Set ExpireAfterVersion with patched datacloud version when EOL =
            // 1 AND ExpireAfterVersion is empty
            if (expectedEOL && StringUtils.isBlank(book.getExpireAfterVersion())) {
                pidsToSetExpireAfter.add(book.getPid());
            }
            if (!expectedEOL) {
                pidsToClearExpireAfter.add(book.getPid());
            }
        });
        updateDB(dataCloudVersion, pidsToClearHotFix, pidsToSetEOL, pidsToClearEOL, pidsToSetEffectiveSince,
                pidsToSetExpireAfter, pidsToClearExpireAfter);
    }

    private void updateDB(String dataCloudVersion, List<Long> pidsToClearHotFix, List<Long> pidsToSetEOL,
            List<Long> pidsToClearEOL, List<Long> pidsToSetEffectiveSince, List<Long> pidsToSetExpireAfter,
            List<Long> pidsToClearExpireAfter) {

        doUpate(pidsToClearHotFix, (pids) -> {
            log.info("setHotFix=" + pids.size());
            patchBookEntityMgr.setHotFix(pids, false);
        });
        doUpate(pidsToSetEOL, (pids) -> {
            log.info("pidsToSetEOL=" + pids.size());
            patchBookEntityMgr.setEndOfLife(pids, true);
        });
        doUpate(pidsToClearEOL, (pids) -> {
            log.info("pidsToClearEOL=" + pids.size());
            patchBookEntityMgr.setEndOfLife(pids, false);
        });
        doUpate(pidsToClearEOL, (pids) -> {
            log.info("pidsToClearEOL=" + pids.size());
            patchBookEntityMgr.setEffectiveSinceVersion(pids, dataCloudVersion);
        });
        doUpate(pidsToSetExpireAfter, (pids) -> {
            log.info("pidsToSetExpireAfter=" + pids.size());
            patchBookEntityMgr.setExpireAfterVersion(pids, dataCloudVersion);
        });
        doUpate(pidsToClearExpireAfter, (pids) -> {
            log.info("pidsToClearExpireAfter=" + pids.size());
            patchBookEntityMgr.setExpireAfterVersion(pids, null);
        });

    }

    private void doUpate(List<Long> pids, Consumer<List<Long>> consumer) {
        List<Long> pidList = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(pids)) {
            for (Long pid : pids) {
                pidList.add(pid);
                if (pidList.size() == trxBatchMax) {
                    retryDoUpdate(consumer, pidList);
                }
            }
            if (!pidList.isEmpty()) {
                retryDoUpdate(consumer, pidList);
            }
        }
    }

    private void retryDoUpdate(Consumer<List<Long>> consumer, List<Long> pidList) {
        for (int i = 0; i < 3; i++) {
            try {
                consumer.accept(pidList);
                pidList.clear();
                return;
            } catch (Exception ex) {
                log.warn("Did not update for single transaction.", ex);
                SleepUtils.sleep(2000L);
            }
        }
        throw new RuntimeException("Failed to do update.");
    }

    private long importToHdfs(List<PatchBook> books, String destDir, String fileName,
            PatchBookConfiguration patchConfig) throws Exception {
        if (CollectionUtils.isEmpty(books)) {
            log.warn("Nothing to ingest for patch type " + patchConfig.getBookType() + " in mode "
                    + patchConfig.getPatchMode());
            return 0;
        }
        List<Pair<String, Class<?>>> schema = constructSchema();
        Object[][] data = prepareData(books);
        if (StringUtils.isBlank(fileName)) {
            fileName = "part-00000.avro";
        }
        AvroUtils.createAvroFileByData(yarnConfiguration, schema, data, destDir, fileName);
        String glob = new Path(destDir, fileName).toString();
        return AvroUtils.count(yarnConfiguration, glob);
    }

    /**
     * Only ingest columns which are needed or for reference in transformer
     *
     * For Domain, DUNS and Items, add PATCH_ prefix to avoid potential
     * attribute name conflict in dataflow
     *
     * @return
     */
    private List<Pair<String, Class<?>>> constructSchema() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of(PatchBook.COLUMN_PID, Long.class));
        columns.add(Pair.of(PatchBook.COLUMN_TYPE, String.class));
        columns.add(Pair.of(DataCloudConstants.ATTR_PATCH_DOMAIN, String.class));
        columns.add(Pair.of(DataCloudConstants.ATTR_PATCH_DUNS, String.class));
        columns.add(Pair.of(PatchBook.COLUMN_NAME, String.class));
        columns.add(Pair.of(PatchBook.COLUMN_COUNTRY, String.class));
        columns.add(Pair.of(PatchBook.COLUMN_STATE, String.class));
        columns.add(Pair.of(PatchBook.COLUMN_CITY, String.class));
        columns.add(Pair.of(PatchBook.COLUMN_ZIPCODE, String.class));
        columns.add(Pair.of(DataCloudConstants.ATTR_PATCH_ITEMS, String.class));
        columns.add(Pair.of(PatchBook.COLUMN_CLEANUP, Boolean.class));
        columns.add(Pair.of(PatchBook.COLUMN_HOTFIX, Boolean.class));
        columns.add(Pair.of(PatchBook.COLUMN_CREATEDATE, Long.class));
        columns.add(Pair.of(PatchBook.COLUMN_CREATEBY, String.class));
        columns.add(Pair.of(PatchBook.COLUMN_LASTMODIFIEDDATE, Long.class));
        columns.add(Pair.of(PatchBook.COLUMN_LASTMODIFIEDBY, String.class));
        columns.add(Pair.of(PatchBook.COLUMN_EFFECTIVE_SINCE, Long.class));
        columns.add(Pair.of(PatchBook.COLUMN_EXPIRE_AFTER, Long.class));
        return columns;
    }

    /**
     * Data and Schema (constructSchema) should be aligned
     *
     * @param books
     * @return
     */
    private Object[][] prepareData(List<PatchBook> books) {
        Object[][] objs = new Object[books.size()][18];
        IntStream.range(0, books.size()).forEach(idx -> {
            objs[idx][0] = books.get(idx).getPid();
            objs[idx][1] = books.get(idx).getType().name();
            objs[idx][2] = books.get(idx).getDomain();
            objs[idx][3] = books.get(idx).getDuns();
            objs[idx][4] = books.get(idx).getName();
            objs[idx][5] = books.get(idx).getCountry();
            objs[idx][6] = books.get(idx).getState();
            objs[idx][7] = books.get(idx).getCity();
            objs[idx][8] = books.get(idx).getZipcode();
            objs[idx][9] = JsonUtils.serialize(books.get(idx).getPatchItems());
            objs[idx][10] = books.get(idx).isCleanup();
            objs[idx][11] = books.get(idx).isHotFix();
            // Following columns are only for reference so that we have some
            // history to track since we don't support generating logs for
            // ingestion jobs for now
            objs[idx][12] = books.get(idx).getCreatedDate() == null ? null : books.get(idx).getCreatedDate().getTime();
            objs[idx][13] = books.get(idx).getCreatedBy();
            objs[idx][14] = books.get(idx).getLastModifiedDate() == null ? null
                    : books.get(idx).getLastModifiedDate().getTime();
            objs[idx][15] = books.get(idx).getLastModifiedBy();
            objs[idx][16] = books.get(idx).getEffectiveSince() == null ? null
                    : books.get(idx).getEffectiveSince().getTime();
            objs[idx][17] = books.get(idx).getExpireAfter() == null ? null : books.get(idx).getExpireAfter().getTime();
        });
        return objs;
    }

    @SuppressWarnings("static-access")
    private void updateCurrentVersion(Ingestion ingestion, String version) {
        PatchBookConfiguration config = (PatchBookConfiguration) ingestion.getProviderConfiguration();
        String hdfsPath = hdfsPathBuilder.constructIngestionDir(ingestion.getIngestionName(), version).toString();
        Path success = new Path(hdfsPath, hdfsPathBuilder.SUCCESS_FILE);
        try {
            HdfsUtils.writeToFile(yarnConfiguration, success.toString(), "");
        } catch (IOException e) {
            throw new RuntimeException("Fail to generate _SUCCESS file", e);
        }
        emailNotify(config, ingestion.getIngestionName(), version, hdfsPath);
        ingestionVersionService.updateCurrentVersion(ingestion, version);
    }

    // PATCH_BOOK is never automatically triggered
    @Override
    public List<String> getMissingFiles(Ingestion ingestion) {
        return null;
    }

    // for test mock
    @VisibleForTesting
    void setPatchBookEntityMgr(PatchBookEntityMgr patchBookEntityMgr) {
        this.patchBookEntityMgr = patchBookEntityMgr;
    }

    private class Ingester implements Runnable {

        private final PatchBookConfiguration patchConfig;
        private final Date currentDate;
        private final IngestionProgress progress;
        private final int batchSeq;
        private final long maxPid;
        private final long minPid;

        Ingester(PatchBookConfiguration patchConfig, Date currentDate, IngestionProgress progress, int batchSeq,
                int batchSize, long minPid) {
            this.patchConfig = patchConfig;
            this.currentDate = currentDate;
            this.progress = progress;
            this.batchSeq = batchSeq;
            this.minPid = minPid;
            // maxPid is exclusive
            this.maxPid = minPid + batchSize;
        }

        @Override
        public void run() {
            if (!patchConfig.isSkipValidation()) {
                validate();
            }
            ingest();
        }

        private void validate() {
            try (PerformanceTimer timer = new PerformanceTimer(
                    String.format("Validated PatchBook with type=%s, mode=%s, minPid=%d, maxPid(exclusive)=%d",
                            patchConfig.getBookType(), patchConfig.getPatchMode(), minPid, maxPid))) {
                PatchRequest patchRequest = new PatchRequest();
                patchRequest.setMode(patchConfig.getPatchMode());
                patchRequest.setDataCloudVersion(progress.getDataCloudVersion());
                patchRequest.setStartPid(minPid);
                patchRequest.setEndPid(maxPid);

                PatchValidationResponse patchResponse = patchProxy.validatePatchBook(patchConfig.getBookType(),
                        patchRequest);
                if (!patchResponse.isSuccess()) {
                    log.error(String.format("PatchBook validation failed. PatchRequest: %s. PatchResponse: %s",
                            JsonUtils.serialize(patchRequest), JsonUtils.serialize(patchResponse)));
                    throw new RuntimeException("PatchBook validation failed");
                }
            }
        }

        private void ingest() {
            try (PerformanceTimer timer = new PerformanceTimer(
                    String.format("Imported PatchBook with type=%s, mode=%s, minPid=%d, maxPid = %d",
                            patchConfig.getBookType(), patchConfig.getPatchMode(), minPid, maxPid))) {
                log.info(String.format("Importing PatchBook with type=%s, mode=%s, minPid=%d, maxPid= %d",
                        patchConfig.getBookType(), patchConfig.getPatchMode(), minPid, maxPid));
                List<PatchBook> books = patchBookEntityMgr.findByTypeAndHotFixWithPaginNoSort(minPid, maxPid,
                        patchConfig.getBookType(), PatchMode.HotFix.equals(patchConfig.getPatchMode()));
                List<PatchBook> activeBooks = getActiveBooks(books, currentDate, minPid, maxPid);
                String fileName = "part-" + batchSeq + ".avro";
                try {
                    long importSize = importToHdfs(activeBooks, progress.getDestination(), fileName, patchConfig);
                    if (importSize != activeBooks.size()) {
                        throw new RuntimeException(
                                String.format("For batch %d: expected to import %d rows, but actually imported %d rows",
                                        batchSeq, activeBooks.size(), importSize));
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                if (!CollectionUtils.isEmpty(books)) {
                    postProcess(books, progress.getDataCloudVersion(), patchConfig, currentDate);
                }
            }
        }

    }

}
