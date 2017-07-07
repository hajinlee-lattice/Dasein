package com.latticeengines.domain.exposed.util;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.JoinColumn;

import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedImport;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;

public class DataFeedImportUtils {

    public static DataFeedImport createImportFromTask(DataFeedTask task) {

        Map<String, Field> map = new HashMap<>();
        Field[] fields = DataFeedTask.class.getDeclaredFields();
        for (Field taskField : fields) {
            if (taskField.isAnnotationPresent(Column.class) && !taskField.isAnnotationPresent(Id.class)) {
                map.put(taskField.getAnnotation(Column.class).name(), taskField);
            } else if (taskField.isAnnotationPresent(JoinColumn.class)) {
                map.put(taskField.getAnnotation(JoinColumn.class).name(), taskField);
            }
        }

        DataFeedImport datafeedImport = null;
        try {
            datafeedImport = DataFeedImport.class.newInstance();
            fields = DataFeedImport.class.getDeclaredFields();
            for (Field importField : fields) {
                importField.setAccessible(true);
                if (importField.isAnnotationPresent(Column.class)
                        && map.containsKey(importField.getAnnotation(Column.class).name())) {
                    Field taskField = map.get(importField.getAnnotation(Column.class).name());
                    taskField.setAccessible(true);
                    importField.set(datafeedImport, taskField.get(task));
                } else if (importField.isAnnotationPresent(JoinColumn.class)
                        && map.containsKey(importField.getAnnotation(JoinColumn.class).name())) {
                    Field taskField = map.get(importField.getAnnotation(JoinColumn.class).name());
                    taskField.setAccessible(true);
                    importField.set(datafeedImport, taskField.get(task));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return datafeedImport;
    }

}
