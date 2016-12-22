package com.latticeengines.matchapi.service.impl;

import java.util.Iterator;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.datacloud.match.dnb.DnBMatchContext;
import com.latticeengines.datacloud.match.exposed.util.MatchUtils;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;
import com.latticeengines.matchapi.service.CacheLoaderConfig;

@Component("avroCacheLoaderService")
public class AvroCacheLoaderServiceImpl extends BaseCacheLoaderService<GenericRecord> {

    private static Log log = LogFactory.getLog(AvroCacheLoaderServiceImpl.class);

    @Override
    protected Iterator<GenericRecord> iterator(String dirPath) {
        return AvroUtils.iterator(yarnConfiguration, MatchUtils.toAvroGlobs(dirPath));
    }

    @Override
    protected DnBMatchContext createMatchContext(GenericRecord record, CacheLoaderConfig config) {
        String dunsField = getDunsField(config);
        if (record.get(dunsField) == null) {
            return null;
        }
        String duns = record.get(dunsField).toString();
        if (StringUtils.isEmpty(duns.trim())) {
            return null;
        }
        DnBMatchContext matchContext = new DnBMatchContext();
        createNameLocation(matchContext, record, config);
        return matchContext;
    }

    private String getDunsField(CacheLoaderConfig config) {
        String dunsField = config.getDunsField();
        if (StringUtils.isEmpty(dunsField)) {
            dunsField = defaultDunsField;
        }
        return dunsField;
    }

    private void createNameLocation(DnBMatchContext matchContext, GenericRecord record, CacheLoaderConfig config) {

        NameLocation nameLocation = new NameLocation();
        setFieldValues(record, nameLocation, config);
        matchContext.setInputNameLocation(nameLocation);

        String dunsField = getDunsField(config);
        matchContext.setDuns(record.get(dunsField).toString());
        matchContext.setConfidenceCode(8);
        matchContext.setMatchGrade("AAAAAAAAA");
        matchContext.setMatchStrategy(DnBMatchContext.DnBMatchStrategy.BATCH);
    }

    protected void setFieldValues(GenericRecord record, NameLocation nameLocation, CacheLoaderConfig config) {
        Map<String, String> fieldMap = config.getFieldMap();
        if (fieldMap == null || fieldMap.size() == 0) {
            fieldMap = defaultFieldMap;
        }
        for (String key : fieldMap.keySet()) {
            Object obj = record.get(key);
            String value = null;
            if (obj != null) {
                value = obj.toString();
            }
            try {
                BeanUtils.setProperty(nameLocation, fieldMap.get(key), value);
            } catch (Exception ex) {
                log.warn("Failed to setup field value, name location's fieldName=" + fieldMap.get(key)
                        + " record fieldName=" + key);
            }
        }
    }

}
