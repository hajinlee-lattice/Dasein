package com.latticeengines.dataflow.exposed.builder.operations;

import java.util.ArrayList;
import java.util.List;

import cascading.pipe.Pipe;
import cascading.pipe.assembly.Rename;

import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.util.DataFlowUtils;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

public class RenameOperation extends Operation {
    public RenameOperation(Input prior, FieldList previousNames, FieldList newNames) {
        Pipe rename = new Rename(prior.pipe, DataFlowUtils.convertToFields(previousNames.getFields()),
                DataFlowUtils.convertToFields(newNames.getFields()));
        List<FieldMetadata> metadata = new ArrayList<>(prior.metadata);
        renameFields(previousNames, newNames, metadata);
        this.pipe = rename;
        this.metadata = metadata;
    }

    private void renameFields(FieldList previousNames, FieldList newNames, List<FieldMetadata> metadata) {
        if (previousNames.getFields().length != newNames.getFields().length) {
            throw new RuntimeException("Previous and new name array lengths must be the same");
        }

        String[] previousNameStrings = previousNames.getFields();
        String[] newNameStrings = newNames.getFields();
        for (int i = 0; i < previousNameStrings.length; ++i) {
            String previousName = previousNameStrings[i];
            String newName = newNameStrings[i];

            boolean found = false;
            for (FieldMetadata field : metadata) {
                if (field.getFieldName().equals(previousName)) {
                    field.setFieldName(newName);
                    found = true;
                    break;
                }
            }

            if (!found) {
                throw new RuntimeException(String.format("Could not locate field with name %s in metadata",
                        previousName));
            }
        }
    }
}
