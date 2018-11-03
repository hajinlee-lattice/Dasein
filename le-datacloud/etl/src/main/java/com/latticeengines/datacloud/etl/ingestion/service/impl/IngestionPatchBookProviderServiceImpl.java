package com.latticeengines.datacloud.etl.ingestion.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.entitymgr.PatchBookEntityMgr;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.core.util.PatchBookUtils;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionProgressService;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionVersionService;
import com.latticeengines.domain.exposed.datacloud.ingestion.PatchBookConfiguration;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;
import com.latticeengines.domain.exposed.datacloud.manage.IngestionProgress;
import com.latticeengines.domain.exposed.datacloud.manage.PatchBook;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.datacloud.match.patch.PatchMode;

@Component("ingestionPatchBookProviderServiceImpl")
public class IngestionPatchBookProviderServiceImpl extends IngestionProviderServiceImpl {

    private static Logger log = LoggerFactory.getLogger(IngestionPatchBookProviderServiceImpl.class);

    @Inject
    private PatchBookEntityMgr patchBookEntityMgr;

    @Inject
    private IngestionProgressService ingestionProgressService;

    @Inject
    protected Configuration yarnConfiguration;

    @Inject
    private HdfsPathBuilder hdfsPathBuilder;

    @Inject
    private IngestionVersionService ingestionVersionService;


    // TODO:
    // Support partition (in case PatchBook list is not fit into
    // memory) / parallel ingestion
    // Support more general way to support ingesting from sql table to hdfs/s3
    // without sqoop
    @Override
    public void ingest(IngestionProgress progress) throws Exception {
        Ingestion ingestion = progress.getIngestion();
        PatchBookConfiguration patchConfig = (PatchBookConfiguration) ingestion.getProviderConfiguration();
        Date currentDate = new Date();

        List<PatchBook> books = patchBookEntityMgr.findByTypeAndHotFix(patchConfig.getBookType(),
                PatchMode.HotFix.equals(patchConfig.getPatchMode()));
        List<PatchBook> activeBooks = getActiveBooks(books, currentDate);

        long count = importToHdfs(activeBooks, progress.getDestination(), patchConfig);
        updateCurrentVersion(ingestion, progress.getVersion());

        postProcess(books, progress.getDataCloudVersion(), patchConfig, currentDate);

        progress = ingestionProgressService.updateProgress(progress).size(count).status(ProgressStatus.FINISHED)
                .commit(true);
        log.info("Ingestion finished. Progress: " + progress.toString());
    }

    private List<PatchBook> getActiveBooks(List<PatchBook> books, Date currentDate) {
        return books.stream() //
                .filter(book -> !PatchBookUtils.isEndOfLife(book, currentDate)) //
                .collect(Collectors.toList());
    }

    private void postProcess(List<PatchBook> books, String dataCloudVersion, PatchBookConfiguration patchConfig, Date currentDate) {
        List<Long> pidsToClearHotFix = new ArrayList<>();
        List<Long> pidsToSetEOL = new ArrayList<>();
        List<Long> pidsToClearEOL = new ArrayList<>();
        List<Long> pidsToSetEffectiveSince = new ArrayList<>();
        List<Long> pidsToSetExpireAfter = new ArrayList<>();
        List<Long> pidsToClearExpireAfter = new ArrayList<>();
        books.forEach(book -> {
            // For hot-fixed items, need to reset HotFix flag to be false
            if (PatchMode.HotFix.equals(patchConfig.getPatchMode())) {
                pidsToClearHotFix.add(book.getPid()); // books are already filtered by HotFix flag
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
        if (CollectionUtils.isNotEmpty(pidsToClearHotFix)) {
            patchBookEntityMgr.setHotFix(pidsToClearHotFix, false);
        }
        if (CollectionUtils.isNotEmpty(pidsToSetEOL)) {
            patchBookEntityMgr.setEndOfLife(pidsToSetEOL, true);
        }
        if (CollectionUtils.isNotEmpty(pidsToClearEOL)) {
            patchBookEntityMgr.setEndOfLife(pidsToClearEOL, false);
        }
        if (CollectionUtils.isNotEmpty(pidsToSetEffectiveSince)) {
            patchBookEntityMgr.setEffectiveSinceVersion(pidsToSetEffectiveSince, dataCloudVersion);
        }
        if (CollectionUtils.isNotEmpty(pidsToSetExpireAfter)) {
            patchBookEntityMgr.setExpireAfterVersion(pidsToSetExpireAfter, dataCloudVersion);
        }
        if (CollectionUtils.isNotEmpty(pidsToClearExpireAfter)) {
            patchBookEntityMgr.setExpireAfterVersion(pidsToClearExpireAfter, null);
        }
    }

    private long importToHdfs(List<PatchBook> books, String dest, PatchBookConfiguration patchConfig) throws Exception {
        if (CollectionUtils.isEmpty(books)) {
            throw new RuntimeException("Nothing to ingest for patch type " + patchConfig.getBookType() + " in mode "
                    + patchConfig.getPatchMode());
        }
        if (HdfsUtils.isDirectory(yarnConfiguration, dest)) {
            log.info(dest + " already exists. Delete first.");
            HdfsUtils.rmdir(yarnConfiguration, dest);
        }
        List<Pair<String, Class<?>>> schema = constructSchema();
        Object[][] data = prepareData(books);
        String fileName = "part-00000.avro";
        AvroUtils.createAvroFileByData(yarnConfiguration, schema, data, dest, fileName);
        String glob = new Path(dest, fileName).toString();
        return AvroUtils.count(yarnConfiguration, glob);
    }

    /**
     * Only ingest columns which are needed or for reference in transformer
     * 
     * @return
     */
    private List<Pair<String, Class<?>>> constructSchema() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of(PatchBook.COLUMN_PID, Long.class));
        columns.add(Pair.of(PatchBook.COLUMN_TYPE, String.class));
        columns.add(Pair.of(PatchBook.COLUMN_DOMAIN, String.class));
        columns.add(Pair.of(PatchBook.COLUMN_DUNS, String.class));
        columns.add(Pair.of(PatchBook.COLUMN_NAME, String.class));
        columns.add(Pair.of(PatchBook.COLUMN_COUNTRY, String.class));
        columns.add(Pair.of(PatchBook.COLUMN_STATE, String.class));
        columns.add(Pair.of(PatchBook.COLUMN_CITY, String.class));
        columns.add(Pair.of(PatchBook.COLUMN_ZIPCODE, String.class));
        columns.add(Pair.of(PatchBook.COLUMN_PATCH_ITEMS, String.class));
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
            objs[idx][12] = books.get(idx).getCreatedDate().getTime();
            objs[idx][13] = books.get(idx).getCreatedBy();
            objs[idx][14] = books.get(idx).getLastModifiedDate().getTime();
            objs[idx][15] = books.get(idx).getLastModifiedBy();
            objs[idx][16] = books.get(idx).getEffectiveSince().getTime();
            objs[idx][17] = books.get(idx).getExpireAfter().getTime();
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

}
