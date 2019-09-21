package com.latticeengines.datacloud.match.service.impl;

import static com.latticeengines.common.exposed.bean.BeanFactoryEnvironment.Environment.WebApp;
import static com.latticeengines.domain.exposed.camille.watchers.CamilleWatcher.AMReleaseBaseCache;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.support.RetryTemplate;

import com.google.common.base.Preconditions;
import com.latticeengines.aws.s3.S3KeyFilter;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.camille.exposed.watchers.NodeWatcher;
import com.latticeengines.common.exposed.bean.BeanFactoryEnvironment;
import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.AvroUtils.AvroStreamsIterator;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.match.annotation.MatchStep;
import com.latticeengines.datacloud.match.entitymgr.MetadataColumnEntityMgr;
import com.latticeengines.datacloud.match.exposed.service.MetadataColumnService;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.datacloud.manage.MetadataColumn;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;

import reactor.core.publisher.ParallelFlux;

public abstract class BaseMetadataColumnServiceImpl<E extends MetadataColumn> implements MetadataColumnService<E> {

    private static final Logger log = LoggerFactory.getLogger(BaseMetadataColumnServiceImpl.class);

    private static final String LOCAL_CACHE = "dc_mds";
    private static final int MDS_SPLITS = 4;

    @Value("${datacloud.collection.s3bucket}")
    private String s3Bucket;

    @Value("${datacloud.match.metadata.from.db}")
    private boolean mdsFromDB;

    @Inject
    private S3Service s3Service;

    @Inject
    private HdfsPathBuilder hdfsPathBuilder;

    protected abstract MetadataColumnEntityMgr<E> getMetadataColumnEntityMgr();

    // This is base cache which has all the metadatas for datacloud versions.
    // Upper-layer metadata related cache loads metadata from this base cache
    // instead of all loading from S3
    // datacloud version -> <column ID -> MetadataColumn>
    protected abstract ConcurrentMap<String, ConcurrentMap<String, E>> getWhiteColumnCache();

    // datacloud version -> [requested column IDs]
    protected abstract ConcurrentMap<String, ConcurrentSkipListSet<String>> getBlackColumnCache();

    protected abstract String getLatestVersion();

    /**
     * Get java class instance of MetadataColumn
     *
     * @return
     */
    protected abstract Class<E> getMetadataColumnClass();

    /**
     * Get table name of MetadataColumn
     *
     * @return
     */
    protected abstract String getMDSTableName();

    @PostConstruct
    private void postConstruct() {
        initWatcher();
    }

    @Override
    public List<E> findByColumnSelection(Predefined selectName, String dataCloudVersion) {
        List<E> columns = getMetadataColumns(dataCloudVersion);
        return columns.stream() //
                .filter(column -> column.containsTag(selectName.getName())) //
                .collect(Collectors.toList());
    }

    @Override
    public Long count(String dataCloudVersion) {
        if (StringUtils.isBlank(dataCloudVersion)) {
            dataCloudVersion = getLatestVersion();
        }
        return getMetadataColumnEntityMgr().count(dataCloudVersion);
    }

    @Override
    public ParallelFlux<E> scan(String dataCloudVersion, Integer page, Integer size) {
        if (StringUtils.isBlank(dataCloudVersion)) {
            dataCloudVersion = getLatestVersion();
        }
        if (page == null && size == null) {
            return getMetadataColumnEntityMgr().findAll(dataCloudVersion);
        } else if (size == null || page == null) {
            throw new IllegalArgumentException("Must specify page and size when asking for a particular page");
        } else {
            return getMetadataColumnEntityMgr().findByPage(dataCloudVersion, page, size).parallel();
        }
    }

    @Override
    @MatchStep
    public List<E> getMetadataColumns(List<String> columnIds, String dataCloudVersion) {
        List<E> toReturn = new ArrayList<>();
        ConcurrentMap<String, ConcurrentMap<String, E>> whiteColumnCaches = getWhiteColumnCache();

        if (!whiteColumnCaches.containsKey(dataCloudVersion)) {
            log.warn("Missed metadata cache for version " + dataCloudVersion + ", loading now.");
            refreshCacheForVersion(dataCloudVersion);
        }
        if (!whiteColumnCaches.containsKey(dataCloudVersion)) {
            throw new RuntimeException("Still cannot find " + dataCloudVersion + " in white column cache.");
        } else {
            ConcurrentSkipListSet<String> blackColumnCache = getBlackColumnCache().get(dataCloudVersion);
            for (String columnId : columnIds) {
                E column;
                if (blackColumnCache != null && blackColumnCache.contains(columnId)) {
                    column = null;
                } else if (!whiteColumnCaches.get(dataCloudVersion).containsKey(columnId)) {
                    column = loadColumnMetadataById(columnId, dataCloudVersion);
                } else {
                    column = whiteColumnCaches.get(dataCloudVersion).get(columnId);
                }
                if (column == null) {
                    log.warn("Cannot resolve metadata column for columnId=" + columnId);
                }
                toReturn.add(column);
            }
        }
        return toReturn;
    }

    @Override
    @MatchStep(threshold = 100L)
    public E getMetadataColumn(String columnId, String dataCloudVersion) {
        ConcurrentMap<String, ConcurrentMap<String, E>> whiteColumnCaches = getWhiteColumnCache();
        if (!whiteColumnCaches.containsKey(dataCloudVersion)
                || !whiteColumnCaches.get(dataCloudVersion).containsKey(columnId)) {
            return getMetadataColumnEntityMgr().findById(columnId, dataCloudVersion);
        }
        if (getBlackColumnCache().containsKey(dataCloudVersion)
                && getBlackColumnCache().get(dataCloudVersion).contains(columnId)) {
            return null;
        }
        if (!whiteColumnCaches.containsKey(dataCloudVersion)
                || !whiteColumnCaches.get(dataCloudVersion).containsKey(columnId)) {
            return loadColumnMetadataById(columnId, dataCloudVersion);
        } else {
            return whiteColumnCaches.get(dataCloudVersion).get(columnId);
        }
    }

    private E loadColumnMetadataById(String columnId, String dataCloudVersion) {
        synchronized (getWhiteColumnCache()) {
            return performThreadSafeColumnMetadataLoading(columnId, dataCloudVersion);
        }
    }

    private E performThreadSafeColumnMetadataLoading(String columnId, String dataCloudVersion) {
        E column;
        try {
            column = getMetadataColumnEntityMgr().findById(columnId, dataCloudVersion);
            log.debug("Loaded metadata from DB for columnId = " + columnId);
        } catch (Exception e) {
            log.error(String.format("Failed to retrieve column information for [%s]", columnId), e);
            return null;
        }
        if (column == null) {
            ConcurrentSkipListSet<String> blackColumnCache = getBlackColumnCache().computeIfAbsent(dataCloudVersion,
                    k -> new ConcurrentSkipListSet<>());
            blackColumnCache.add(columnId);
            return null;
        } else {
            ConcurrentMap<String, E> whiteColumnCache = getWhiteColumnCache().computeIfAbsent(dataCloudVersion,
                    k -> new ConcurrentHashMap<>());
            whiteColumnCache.put(columnId, column);
            return column;
        }
    }

    private void refreshCaches() {
        // refresh latest version for sure
        String latestVersion = getLatestVersion();
        refreshCacheForVersion(latestVersion);
        for (String cachedVersion : getWhiteColumnCache().keySet()) {
            if (!latestVersion.equals(cachedVersion)) {
                refreshCacheForVersion(cachedVersion);
            }
        }
        synchronized (getBlackColumnCache()) {
            getBlackColumnCache().clear();
        }
    }

    private void refreshCacheForVersion(String dataCloudVersion) {
        new Thread(() -> {
            log.info("Start loading white column caches for version " + dataCloudVersion);
            RetryTemplate retry = RetryUtils.getRetryTemplate(10);
            List<E> columns = retry.execute(ctx -> {
                if (ctx.getRetryCount() > 0) {
                    log.info("Attempt=" + (ctx.getRetryCount() + 1) //
                            + " get all columns for version " + dataCloudVersion);
                }
                if (mdsFromDB) {
                    return getMetadataColumnEntityMgr().findAll(dataCloudVersion).sequential().collectList().block();
                } else {
                    return loadFromS3(dataCloudVersion);
                }
            });
            log.info("Read " + columns.size() + " columns for version " + dataCloudVersion);
            ConcurrentMap<String, E> whiteColumnCache = new ConcurrentHashMap<>();
            for (E column : columns) {
                whiteColumnCache.put(column.getColumnId(), column);
            }
            synchronized (getWhiteColumnCache()) {
                getWhiteColumnCache().put(dataCloudVersion, whiteColumnCache);
            }
            log.info("Loaded " + whiteColumnCache.size() + " columns into white cache. version=" + dataCloudVersion);
        }).run();
    }

    private void initWatcher() {
        BeanFactoryEnvironment.Environment currentEnv = BeanFactoryEnvironment.getEnvironment();
        if (WebApp.equals(currentEnv)) {
            NodeWatcher.registerWatcher(AMReleaseBaseCache.name());
            NodeWatcher.registerListener(AMReleaseBaseCache.name(), () -> {
                int waitInSec = (int) (Math.random() * 30);
                log.info(String.format(
                        "ZK watcher %s is changed. To avoid refresh congestion, wait for %d seconds before start.",
                        AMReleaseBaseCache.name(), waitInSec));
                Thread.sleep(waitInSec * 1000);
                log.info(String.format("For changed ZK watcher %s, updating white and black columns caches ...",
                        AMReleaseBaseCache.name()));
                refreshCaches();
            });
        }
        log.info("Initial loading - updating white and black columns caches ...");
        refreshCaches();
    }

    @Override
    public synchronized long s3Publish(@NotNull String dataCloudVersion) {
        Preconditions.checkNotNull(dataCloudVersion);
        long total = getMetadataColumnEntityMgr().count(dataCloudVersion);
        if (total == 0) {
            return total;
        }

        Schema schema = AvroUtils.classToSchema(getMetadataColumnClass());
        // Prepare clean S3 prefix and clean local directory to cache metadata
        String s3Prefix = getMDSS3Prefix(dataCloudVersion);
        s3Service.cleanupPrefix(s3Bucket, s3Prefix);
        String localDir = LOCAL_CACHE + "/" + dataCloudVersion.replace(DataCloudVersion.SEPARATOR, "");
        try {
            FileUtils.deleteDirectory(new File(localDir));
            FileUtils.forceMkdir(new File(localDir));
        } catch (IOException e1) {
            throw new RuntimeException("Fail to re-create local directory to cache metadata: " + localDir);
        }
        // Dump metadata to local file
        int pageSize = (int) Math.ceil((double) total / MDS_SPLITS);
        total = 0;
        for (int page = 0; page < MDS_SPLITS; page++) {
            String localFile = localDir + "/" + "part-" + page + ".avro";
            try (PerformanceTimer timer = new PerformanceTimer(
                    String.format("Dump metadata page-%d to local file %s", page, localFile))) {
                List<E> mds = getMetadataColumnEntityMgr().findByPage(dataCloudVersion, page, pageSize).collectList().block();
                List<GenericRecord> records = AvroUtils.serialize(getMetadataColumnClass(), mds);
                try {
                    AvroUtils.writeToLocalFile(schema, records, localFile, true);
                } catch (IOException e) {
                    throw new RuntimeException(
                            String.format("Fail to write %d metadata for version %s to local file %s", records.size(),
                                    dataCloudVersion, localFile),
                            e);
                }
                total += records.size();
            }
        }
        // Copy local files to S3
        try (PerformanceTimer timer = new PerformanceTimer(
                String.format("Copy metadata file %s to S3 prefix %s", localDir, s3Prefix))) {
            s3Service.uploadLocalDirectory(s3Bucket, s3Prefix, localDir, true);
        }

        // Delete local directory to cache metadata
        try {
            FileUtils.deleteDirectory(new File(localDir));
        } catch (IOException e) {
            throw new RuntimeException("Fail to delete local metadata directory: " + localDir);
        }

        return total;
    }

    private List<E> loadFromS3(String dataCloudVersion) {
        String s3Prefix = getMDSS3Prefix(dataCloudVersion);
        List<E> list = new ArrayList<>();
        try (PerformanceTimer timer = new PerformanceTimer()) {
            timer.setThreshold(0);
            Iterator<InputStream> streamIter = s3Service.getObjectStreamIterator(s3Bucket, s3Prefix, new S3KeyFilter() {
            });
            try (AvroStreamsIterator iter = AvroUtils.iterateAvroStreams(streamIter)) {
                for (GenericRecord record : (Iterable<GenericRecord>) () -> iter) {
                    list.add(AvroUtils.deserialize(record, getMetadataColumnClass()));
                }
            }
            timer.setTimerMessage(String.format("Load %d metadata for version %s from S3 prefix %s", list.size(),
                    dataCloudVersion, s3Prefix));
        }
        return list;
    }

    /**
     * Get S3 prefix of metadata location
     *
     * Metadata is saved in S3 DataCoud source folder with snapshot version
     * derived from datacloud version instead of utc timestamp
     *
     * @param dataCloudVersion
     * @return
     */
    private String getMDSS3Prefix(String dataCloudVersion) {
        // Use the same path as hdfs, but remove the first /
        String path = hdfsPathBuilder
                .constructSnapshotDir(getMDSTableName(), dataCloudVersion.replace(DataCloudVersion.SEPARATOR, ""))
                .toString();
        if (path.startsWith("/")) {
            path = path.substring(1);
        }
        return path;
    }

    @Override
    public List<E> getMetadataColumns(String dataCloudVersion) {
        List<E> toReturn = new ArrayList<>();
        ConcurrentMap<String, ConcurrentMap<String, E>> whiteColumnCaches = getWhiteColumnCache();

        // Application only requests latest datacloud version which is
        // guaranteed to exist in cache. Old datacloud version could be
        // requested only when matchapi is manually called.
        if (!whiteColumnCaches.containsKey(dataCloudVersion)) {
            log.warn("Missed metadata cache for version " + dataCloudVersion + ", loading now.");
            refreshCacheForVersion(dataCloudVersion);
        }
        if (!whiteColumnCaches.containsKey(dataCloudVersion)) {
            throw new RuntimeException("Still cannot find " + dataCloudVersion + " in white column cache.");
        } else {
            toReturn.addAll(whiteColumnCaches.get(dataCloudVersion).values());
        }
        return toReturn;
    }
}
