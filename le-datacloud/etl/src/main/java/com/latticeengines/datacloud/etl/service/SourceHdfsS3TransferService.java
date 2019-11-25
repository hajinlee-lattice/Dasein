package com.latticeengines.datacloud.etl.service;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.core.source.Source;

public interface SourceHdfsS3TransferService {

    /**
     * Transfer specified DataCloud source between HDFS and S3
     * 
     * Currently supported source type: DerivedSource &
     * IngestionSource(Ingestion)
     *
     * @param hdfsToS3:
     *            true: transfer source from hdfs to s3; false: from s3 to hdfs
     * @param source:
     *            source to transfer
     * @param version:
     *            version of source to transfer
     * @param tags:
     *            if transfer destination is s3, how to tag s3 objects --
     *            provide with mapping from tag names to tag values. if transfer
     *            desination is hdfs, it's ignored
     * @param skipEventualCheck:
     *            after transfer, whether to do verification on number files and
     *            file size
     * @param failIfExisted:
     *            if specified version of source already exists at destination,
     *            whether to fail the transfer; if not to fail, existed
     *            directory/file will be deleted and then re-copy
     */
    void transfer(boolean hdfsToS3, @NotNull Source source, @NotNull String version, List<Pair<String, String>> tags,
            boolean skipEventualCheck, boolean failIfExisted);
}
