package com.latticeengines.pls.service.impl;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.closeable.resource.CloseableResourcePool;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.pls.service.ScoringFileMetadataService;
import com.latticeengines.pls.util.ValidateCSVFileHeaderUtils;

@Component("scoringFileMetadataService")
public class ScoringFileMetadataServiceImpl implements ScoringFileMetadataService {

    private static final Log log = LogFactory.getLog(ScoringFileMetadataServiceImpl.class);

    @Override
    public InputStream validateHeaderFields(InputStream stream, List<String> requiredFileds,
            CloseableResourcePool leCsvParser, String displayName) {
        if (!stream.markSupported()) {
            stream = new BufferedInputStream(stream);
        }
        stream.mark(ValidateCSVFileHeaderUtils.BIT_PER_BYTE * ValidateCSVFileHeaderUtils.BYTE_NUM);
        Set<String> headerFields = ValidateCSVFileHeaderUtils.getCSVHeaderFields(stream, leCsvParser);
        try {
            stream.reset();
        } catch (IOException e) {
            log.error(e);
            throw new LedpException(LedpCode.LEDP_00002, e);
        }
        Set<String> missingRequiredFields = new HashSet<>();
        Iterator<String> iterator = requiredFileds.iterator();
        while (iterator.hasNext()) {
            String field = iterator.next();
            if (!headerFields.contains(field)) {
                missingRequiredFields.add(field);
            }
        }

        if (!missingRequiredFields.isEmpty()) {
            throw new LedpException(LedpCode.LEDP_18087, //
                    new String[] { StringUtils.join(missingRequiredFields, ","), displayName });
        }

        return stream;
    }
}
