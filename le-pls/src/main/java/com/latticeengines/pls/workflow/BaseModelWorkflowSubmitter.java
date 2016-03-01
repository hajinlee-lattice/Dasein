package com.latticeengines.pls.workflow;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.google.common.base.Predicate;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.SemanticType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

public abstract class BaseModelWorkflowSubmitter extends WorkflowSubmitter {
    @Autowired
    protected MetadataProxy metadataProxy;

    @Value("${pls.modelingservice.basedir}")
    protected String modelingServiceHdfsBaseDir;

    protected List<String> getEventColumns(Table table) {
        List<Attribute> events = table.findAttributes(new Predicate<Attribute>() {
            @Override
            public boolean apply(@Nullable Attribute attribute) {
                return attribute.getSemanticType() != null && attribute.getSemanticType() == SemanticType.Event;
            }
        });

        List<String> eventColumnNames = new ArrayList<>();
        for (Attribute column : events) {
            eventColumnNames.add(column.getName());
        }

        return eventColumnNames;
    }

}
