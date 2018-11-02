package com.latticeengines.datacloud.etl.ingestion.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
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
        List<PatchBook> books = patchBookEntityMgr.findByTypeAndHotFix(patchConfig.getBookType(),
                PatchMode.HotFix.equals(patchConfig.getPatchMode()));
        long count = importToHdfs(books, progress.getDestination(), patchConfig);
        updateCurrentVersion(ingestion, progress.getVersion());
        progress = ingestionProgressService.updateProgress(progress).size(count).status(ProgressStatus.FINISHED)
                .commit(true);
        log.info("Ingestion finished. Progress: " + progress.toString());
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
        columns.add(Pair.of(PatchBook.COLUMN_EOL, Boolean.class));
        columns.add(Pair.of(PatchBook.COLUMN_CREATEDATE, Long.class));
        columns.add(Pair.of(PatchBook.COLUMN_CREATEBY, String.class));
        columns.add(Pair.of(PatchBook.COLUMN_LASTMODIFIEDDATE, Long.class));
        columns.add(Pair.of(PatchBook.COLUMN_LASTMODIFIEDBY, String.class));
        return columns;
    }

    /**
     * Data and Schema (constructSchema) should be aligned
     * 
     * @param books
     * @return
     */
    private Object[][] prepareData(List<PatchBook> books) {
        Object[][] objs = new Object[books.size()][17];
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
            objs[idx][12] = books.get(idx).isEndOfLife();
            objs[idx][13] = books.get(idx).getCreatedDate().getTime();
            objs[idx][14] = books.get(idx).getCreatedBy();
            objs[idx][15] = books.get(idx).getLastModifiedDate().getTime();
            objs[idx][16] = books.get(idx).getLastModifiedBy();
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
