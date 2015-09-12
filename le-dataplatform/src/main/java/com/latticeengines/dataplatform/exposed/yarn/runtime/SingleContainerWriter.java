package com.latticeengines.dataplatform.exposed.yarn.runtime;

import java.util.List;

import org.springframework.batch.item.ItemWriter;

public class SingleContainerWriter implements ItemWriter<String> {
    @Override
    public void write(List<? extends String> items) throws Exception {
    }
}