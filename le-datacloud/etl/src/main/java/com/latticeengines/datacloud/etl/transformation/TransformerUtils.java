package com.latticeengines.datacloud.etl.transformation;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.TableSource;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.domain.exposed.metadata.Table;

/**
 * Some code can be shared by transformers
 */
public final class TransformerUtils {

    protected TransformerUtils() {
        throw new UnsupportedOperationException();
    }

    private static final Logger log = LoggerFactory.getLogger(TransformerUtils.class);

    public static String avroPath(Source source, String version, HdfsPathBuilder hdfsPathBuilder) {
        String avroPath;
        if (source instanceof TableSource) {
            Table table = ((TableSource) source).getTable();
            avroPath = table.getExtracts().get(0).getPath();
        } else {
            String avroDir = hdfsPathBuilder.constructSnapshotDir(source.getSourceName(), version).toString();
            if (!avroDir.endsWith(".avro")) {
                avroDir = avroDir.endsWith("/") ? avroDir : avroDir + "/";
                avroPath = avroDir + "*.avro";
            } else {
                avroPath = avroDir;
            }
        }
        return avroPath;
    }

    public static void removeAllButBiggestAvro(Configuration yarnConfiguration, String avroGlob) {
        try {
            List<String> files = HdfsUtils.getFilesByGlob(yarnConfiguration, avroGlob);
            String biggestFile = "";
            long maxFileSize = Integer.MIN_VALUE;
            for (String file : files) {
                long fileSize = HdfsUtils.getFileSize(yarnConfiguration, file);
                if (fileSize > maxFileSize) {
                    maxFileSize = fileSize;
                    biggestFile = file;
                }
            }
            if (StringUtils.isBlank(biggestFile)) {
                throw new RuntimeException("Cannot determine the biggest file in " + avroGlob);
            } else {
                for (String file : files) {
                    if (!file.equals(biggestFile)) {
                        HdfsUtils.rmdir(yarnConfiguration, file);
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to remove empty files", e);
        }
    }

    public static void removeEmptyAvros(Configuration yarnConfiguration, String avroGlob) {
        try {
            List<String> files = HdfsUtils.getFilesByGlob(yarnConfiguration, avroGlob);
            if (files == null) {
                return;
            }
            for (String file : files) {
                Iterator<GenericRecord> iterator = AvroUtils.iterator(yarnConfiguration, file);
                if (!iterator.hasNext()) {
                    log.info("Removing empty avro file " + file);
                    HdfsUtils.rmdir(yarnConfiguration, file);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to remove empty files", e);
        }
    }

}
