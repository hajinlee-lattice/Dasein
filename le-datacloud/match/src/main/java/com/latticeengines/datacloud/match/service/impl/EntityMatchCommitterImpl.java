package com.latticeengines.datacloud.match.service.impl;

import static com.latticeengines.datacloud.match.util.EntityMatchUtils.newStandardizedTenant;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment.SERVING;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment.STAGING;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.service.EntityLookupEntryService;
import com.latticeengines.datacloud.match.service.EntityMatchCommitter;
import com.latticeengines.datacloud.match.service.EntityMatchVersionService;
import com.latticeengines.datacloud.match.service.EntityRawSeedService;
import com.latticeengines.datacloud.match.util.EntityMatchUtils;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityPublishStatistics;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityRawSeed;
import com.latticeengines.domain.exposed.security.Tenant;

@Lazy
@Component("entityMatchCommitter")
public class EntityMatchCommitterImpl implements EntityMatchCommitter {

    private static final Logger log = LoggerFactory.getLogger(EntityMatchCommitterImpl.class);

    private static final int WRITE_BATCH_SEED_SIZE = 25;
    private static final int READ_BATCH_SEED_SIZE = 20;

    @Value("${datacloud.match.entity.commit.queue.size}")
    private int queueSize;

    @Value("${datacloud.match.entity.commit.default.readers.num}")
    private int defaultNumReaders;

    @Value("${datacloud.match.entity.commit.default.writers.num}")
    private int defaultNumWriters;

    @Value("${datacloud.match.entity.commit.max.writer.lag.hr}")
    private int maxWriterLagInHours;

    @Inject
    private EntityRawSeedService entityRawSeedService;

    @Inject
    private EntityLookupEntryService entityLookupEntryService;

    @Inject
    private EntityMatchVersionService entityMatchVersionService;

    @Override
    public EntityPublishStatistics commit(@NotNull String entity, @NotNull Tenant tenant, Boolean destTTLEnabled) {
        check(entity, tenant);
        return commitAsync(entity, newStandardizedTenant(tenant), useTTL(destTTLEnabled), defaultNumReaders,
                defaultNumWriters);
    }

    @Override
    public EntityPublishStatistics commit(@NotNull String entity, @NotNull Tenant tenant, Boolean destTTLEnabled,
            int nReader, int nWriter) {
        check(entity, tenant);
        return commitAsync(entity, newStandardizedTenant(tenant), useTTL(destTTLEnabled), nReader, nWriter);
    }

    /*
     * Spawn background workers to write seed & lookup entries, scan seed in main thread
     */
    private EntityPublishStatistics commitAsync(@NotNull String entity, @NotNull Tenant tenant, boolean useTTL,
            int nReader, int nWriter) {
        ExecutorService readService = ThreadPoolUtils.getCachedThreadPool(String.format("em-commit-r-%s", entity));
        ExecutorService writeService = ThreadPoolUtils.getCachedThreadPool(String.format("em-commit-w-%s", entity));
        AtomicBoolean readFinished = new AtomicBoolean(false);
        AtomicBoolean writeFinished = new AtomicBoolean(false);
        // shared queues
        ArrayBlockingQueue<EntityRawSeed> readQueue = new ArrayBlockingQueue<>(queueSize);
        ArrayBlockingQueue<WriteSeedLookupRequest> writeQueue = new ArrayBlockingQueue<>(queueSize);
        // stats
        AtomicInteger nScannedSeeds = new AtomicInteger(0);
        AtomicInteger nWrittenSeeds = new AtomicInteger(0);
        AtomicInteger nWrittenLookups = new AtomicInteger(0);
        AtomicInteger nLookupNotInStaging = new AtomicInteger(0);
        // metrics
        Instant start = Instant.now();

        log.info("Start committing entity {} for tenant {}. TTL={}, nReader={}, nWriter={}", entity, tenant.getId(),
                useTTL, nReader, nWriter);

        // add r/w workers
        for (int i = 0; i < nReader; i++) {
            writeService.submit(
                    new SeedLookupWriter(tenant, writeFinished, nWrittenSeeds, nWrittenLookups, writeQueue, useTTL));
        }
        for (int i = 0; i < nWriter; i++) {
            readService.submit(new LookupEntryReader(tenant, readFinished, nLookupNotInStaging, writeQueue, readQueue));
        }

        // scanning seeds
        List<String> seedIds = new ArrayList<>();
        do {
            Map<Integer, List<EntityRawSeed>> seeds = entityRawSeedService.scan(STAGING, tenant, entity, seedIds, 1000,
                    entityMatchVersionService.getCurrentVersion(STAGING, tenant));
            seedIds.clear();
            if (MapUtils.isEmpty(seeds)) {
                continue;
            }

            for (Map.Entry<Integer, List<EntityRawSeed>> entry : seeds.entrySet()) {
                seedIds.add(entry.getValue().get(entry.getValue().size() - 1).getId());
                if (CollectionUtils.isEmpty(entry.getValue())) {
                    continue;
                }

                nScannedSeeds.addAndGet(entry.getValue().size());
                entry.getValue().forEach(seed -> {
                    try {
                        // TODO add timeout to prevent hanging
                        readQueue.put(seed);
                    } catch (InterruptedException e) {
                        log.error("Interrupted when putting seed(ID={}) into readQueue. error = {}", seed.getId(), e);
                    }
                });
            }
        } while (CollectionUtils.isNotEmpty(seedIds));

        Instant scanFinished = Instant.now();
        log.info("Finished scanning seeds. Total = {}, Duration = {}", nScannedSeeds.get(),
                Duration.between(start, scanFinished));

        // finish and wait for all writes to finish
        try {
            // need to terminate reader first to prevent race condition that all writers
            // finish first and then reader put some entries into queue
            readFinished.set(true);
            readService.shutdown();
            readService.awaitTermination(maxWriterLagInHours, TimeUnit.HOURS);
            writeFinished.set(true);
            writeService.shutdown();
            writeService.awaitTermination(maxWriterLagInHours, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            log.error("Commit entity {} for tenant {} is interrupted, error = {}", entity, tenant.getId(), e);
            throw new RuntimeException(e);
        }

        if (!readQueue.isEmpty() || !writeQueue.isEmpty()) {
            // some records not handled
            log.warn("Spotted unprocessed records in queue. readQueue size = {}, writeQueue size = {}",
                    readQueue.size(), writeQueue.size());
        }

        Instant workerExited = Instant.now();
        log.info(
                "All background writer terminated. Total written seeds = {}, Total written lookup entries = {}, Duration = {}, Writer Lag = {}",
                nWrittenSeeds.get(), nWrittenLookups.get(), Duration.between(start, workerExited),
                Duration.between(scanFinished, workerExited));

        return new EntityPublishStatistics(nWrittenSeeds.get(), nWrittenLookups.get(), nLookupNotInStaging.get());
    }

    /*
     * Worker that retrieves lookup entry mappings for seeds from staging table
     */
    private class LookupEntryReader implements Runnable {

        private final Tenant tenant;
        private final AtomicBoolean finished;
        private final AtomicInteger nLookupNotInStaging;
        private final ArrayBlockingQueue<WriteSeedLookupRequest> writeQueue;
        private final ArrayBlockingQueue<EntityRawSeed> readQueue;

        LookupEntryReader(Tenant tenant, AtomicBoolean finished, AtomicInteger nLookupNotInStaging,
                ArrayBlockingQueue<WriteSeedLookupRequest> writeQueue, ArrayBlockingQueue<EntityRawSeed> readQueue) {
            this.tenant = tenant;
            this.finished = finished;
            this.nLookupNotInStaging = nLookupNotInStaging;
            this.writeQueue = writeQueue;
            this.readQueue = readQueue;
        }

        @Override
        public void run() {
            while (!finished.get() || !readQueue.isEmpty()) {
                try {
                    Map<String, EntityRawSeed> seeds = new HashMap<>();
                    List<EntityLookupEntry> entries = new ArrayList<>();
                    while (entries.size() < READ_BATCH_SEED_SIZE) {
                        EntityRawSeed seed = readQueue.poll(5, TimeUnit.SECONDS);
                        if (seed == null) {
                            // timeout
                            break;
                        }

                        seeds.put(seed.getId(), seed);
                        entries.addAll(seed.getLookupEntries());
                    }
                    if (seeds.isEmpty()) {
                        continue;
                    }

                    List<String> mappedSeedIds = entityLookupEntryService.get(STAGING, tenant, entries);
                    Map<String, List<EntityLookupEntry>> entryMap = new HashMap<>();
                    for (int j = 0; j < mappedSeedIds.size(); j++) {
                        if (mappedSeedIds.get(j) == null) {
                            nLookupNotInStaging.incrementAndGet();
                            continue;
                        }
                        String entityId = mappedSeedIds.get(j);
                        if (seeds.containsKey(entityId)) {
                            entryMap.putIfAbsent(entityId, new ArrayList<>());
                            entryMap.get(entityId).add(entries.get(j));
                        }
                    }
                    for (String entityId : seeds.keySet()) {
                        try {
                            // TODO add timeout to prevent hanging
                            writeQueue.put(new WriteSeedLookupRequest(seeds.get(entityId), entryMap.get(entityId)));
                        } catch (InterruptedException e) {
                            log.error("Interrupted when putting seed(ID={}) into writeQueue. error = {}", entityId, e);
                        }
                    }
                } catch (InterruptedException e) {
                    log.error("Interrupted when reading lookup entries from seed", e);
                    return;
                } catch (Exception e) {
                    // TODO better error handling, consider failing in some cases
                    log.error("Failed to read lookup entries from seed", e);
                }
            }
            log.debug("Reader exited");
        }
    }

    /*
     * Worker that write seed and it's lookup entry mappings to serving table
     */
    private class SeedLookupWriter implements Runnable {
        private final Tenant tenant;
        private final AtomicBoolean finished;
        private final AtomicInteger nSeeds;
        private final AtomicInteger nLookups;
        private final ArrayBlockingQueue<WriteSeedLookupRequest> writeQueue;
        private final boolean useTTL;

        SeedLookupWriter(Tenant tenant, AtomicBoolean finished, AtomicInteger nSeeds, AtomicInteger nLookups,
                ArrayBlockingQueue<WriteSeedLookupRequest> writeQueue, boolean useTTL) {
            this.tenant = tenant;
            this.finished = finished;
            this.nSeeds = nSeeds;
            this.nLookups = nLookups;
            this.writeQueue = writeQueue;
            this.useTTL = useTTL;
        }

        @Override
        public void run() {
            while (!finished.get() || !writeQueue.isEmpty()) {
                try {
                    List<EntityRawSeed> seeds = new ArrayList<>();
                    List<Pair<EntityLookupEntry, String>> lookupEntryPairs = new ArrayList<>();
                    while (seeds.size() < WRITE_BATCH_SEED_SIZE) {
                        WriteSeedLookupRequest req = writeQueue.poll(5, TimeUnit.SECONDS);
                        if (req == null) {
                            break;
                        }

                        if (req.seed != null) {
                            seeds.add(req.seed);
                            if (CollectionUtils.isNotEmpty(req.entries)) {
                                lookupEntryPairs.addAll(req.entries.stream()
                                        .map(entry -> Pair.of(entry, req.seed.getId())).collect(Collectors.toList()));
                            }
                        }
                    }
                    if (seeds.isEmpty()) {
                        continue;
                    }

                    entityRawSeedService.batchCreate(SERVING, tenant, seeds, useTTL,
                            entityMatchVersionService.getCurrentVersion(SERVING, tenant));
                    entityLookupEntryService.set(SERVING, tenant, lookupEntryPairs, useTTL);
                    nSeeds.addAndGet(seeds.size());
                    nLookups.addAndGet(lookupEntryPairs.size());
                } catch (InterruptedException e) {
                    log.error("Interrupted when writing seeds and lookup entries", e);
                    return;
                } catch (Exception e) {
                    // TODO better error handling, consider failing in some cases
                    log.error("Failed to write seeds and lookup entries", e);
                }
            }
            log.debug("Writer exited");
        }
    }

    private class WriteSeedLookupRequest {
        final EntityRawSeed seed;
        final List<EntityLookupEntry> entries;

        WriteSeedLookupRequest(EntityRawSeed seed, List<EntityLookupEntry> entries) {
            this.seed = seed;
            this.entries = entries == null ? Collections.emptyList() : entries;
        }
    }

    private void check(String entity, Tenant tenant) {
        Preconditions.checkArgument(StringUtils.isNotBlank(entity), "entity to commit should not be blank");
        Preconditions.checkNotNull(tenant, "tenant should not be null");
        Preconditions.checkArgument(StringUtils.isNotBlank(tenant.getId()), "tenant id should not be blank");
    }

    private boolean useTTL(Boolean useTTL) {
        return useTTL == null ? EntityMatchUtils.shouldSetTTL(SERVING) : useTTL;
    }
}
