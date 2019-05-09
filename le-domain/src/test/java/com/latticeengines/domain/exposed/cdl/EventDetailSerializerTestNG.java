package com.latticeengines.domain.exposed.cdl;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import org.testng.annotations.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class EventDetailSerializerTestNG {

    // @Test(groups = "unit")
    public void testInitiatedEventDetail() {
        InitiatedEventDetail initiated = createInitiated();

        String ser = serialize(initiated);
        System.out.println(ser);
    }

    private InitiatedEventDetail createInitiated() {
        InitiatedEventDetail initiated = new InitiatedEventDetail();
        initiated.setBatchId(100L);
        return initiated;
    }

    // @Test(groups = "unit")
    public void testInProgressEventDetail() {
        ProgressEventDetail progress = createProgress();

        String ser = serialize(progress);
        System.out.println(ser);
    }

    @Test(groups = "unit")
    public void testEventDetailAsList() {
        List<EventDetail> lst = new ArrayList<>();
        lst.add(createInitiated());
        lst.add(createProgress());

        String ser = serialize(lst);
        System.out.println(ser);

        TypeReference<List<EventDetail>> type = new TypeReference<List<EventDetail>>() {
        };

        List<EventDetail> res = deserialize(ser, type);
        System.out.println(res);
    }

    private ProgressEventDetail createProgress() {
        ProgressEventDetail progress = new ProgressEventDetail();
        progress.setBatchId(120L);
        progress.setMessage("IN progress");
        return progress;
    }

    public static <T> String serialize(T object) {
        if (object == null) {
            return null;
        }
        ObjectMapper objectMapper = new ObjectMapper();
        StringWriter writer = new StringWriter();
        try {
            objectMapper.writeValue(writer, object);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        return writer.toString();
    }

    public static <T> T deserialize(String jsonStr, TypeReference<T> type) {
        T deserializedSchema;
        try {
            deserializedSchema = new ObjectMapper().readValue(jsonStr.getBytes(), type);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        return deserializedSchema;
    }
}
