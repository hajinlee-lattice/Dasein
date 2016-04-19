package com.latticeengines.pls.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Set;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.io.ByteOrderMark;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.log4j.Logger;

import com.latticeengines.common.exposed.closeable.resource.CloseableResourcePool;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

public class ValidateCSVFileHeaderUtils {

    private static final Logger log = Logger.getLogger(ValidateCSVFileHeaderUtils.class);

    public static final int BIT_PER_BYTE = 1024;
    public static final int BYTE_NUM = 500;

    public static Set<String> getCSVHeaderFields(InputStream stream, CloseableResourcePool closeableResourcePool) {
        try {
            Set<String> headerFields = null;
            InputStreamReader reader = new InputStreamReader(new BOMInputStream(stream, false, ByteOrderMark.UTF_8,
                    ByteOrderMark.UTF_16LE, ByteOrderMark.UTF_16BE, ByteOrderMark.UTF_32LE, ByteOrderMark.UTF_32BE),
                    StandardCharsets.UTF_8);
            CSVFormat format = CSVFormat.RFC4180.withHeader().withDelimiter(',');
            CSVParser parser = new CSVParser(reader, format);
            closeableResourcePool.addCloseable(parser);
            headerFields = parser.getHeaderMap().keySet();
            return headerFields;

        } catch (IOException e) {
            log.error(e);
            throw new LedpException(LedpCode.LEDP_00002, e);
        }
    }

}
