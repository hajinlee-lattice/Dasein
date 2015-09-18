package com.latticeengines.dataplatform.exposed.yarn.runtime;

import java.lang.reflect.ParameterizedType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.LineMapper;

import com.latticeengines.common.exposed.util.JsonUtils;

public abstract class SingleContainerYarnProcessor<T> implements ItemProcessor<T, String> {
    protected static final Log log = LogFactory.getLog(SingleContainerYarnProcessor.class);

    private LineMapper<T> lineMapper = new SingleContainerLineMapper();
    private ItemWriter<String> itemWriter = new SingleContainerWriter();

    private Class<T> type;

    @SuppressWarnings("unchecked")
    public SingleContainerYarnProcessor() {
        this.type = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    }

    public LineMapper<T> getLineMapper() {
        return lineMapper;
    }

    public void setLineMapper(LineMapper<T> lineMapper) {
        this.lineMapper = lineMapper;
    }

    public ItemWriter<String> getItemWriter() {
        return itemWriter;
    }

    public void setItemWriter(ItemWriter<String> itemWriter) {
        this.itemWriter = itemWriter;
    }
    
    public Class<T> getType() {
        return type;
    }

    public class SingleContainerLineMapper implements LineMapper<T> {

        @Override
        public T mapLine(String line, int lineNumber) throws Exception {
            log.info("Parsing line " + line);
            JSONParser parser = new JSONParser();
            JSONObject jsonObj = (JSONObject) parser.parse(line);
            return JsonUtils.deserialize(jsonObj.toString(), type);
        }

    }

}
