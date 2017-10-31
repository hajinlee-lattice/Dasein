package com.latticeengines.cdl.dataflow.stage;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.CreateStagingTableParameters;
import com.latticeengines.domain.exposed.pls.SourceFile;

@Component("createStagingTable")
public class CreateStagingTable extends TypesafeDataFlowBuilder<CreateStagingTableParameters> {

    @Override
    public Node construct(CreateStagingTableParameters parameters) {
        SourceFile file = parameters.getSourceFile();
        Node source = addSource(file.getTableName());
        source = addMissingColumns(source, file);
        return source;
    }
    
    private Node addMissingColumns(Node node, SourceFile sourceFile) {
        String categoryValue = "";
        switch (sourceFile.getBusinessEntity()) {
        case Account:
            categoryValue = Category.ACCOUNT_ATTRIBUTES.name();
            break;
        case Contact:
            categoryValue = Category.CONTACT_ATTRIBUTES.name();
            break;
        case Transaction:
        case Product:
            categoryValue = "Opportunity";
            break;
        default:
            return node;
        
        }
        return node.addColumnWithFixedValue("CategoryId", categoryValue, String.class);
    }

}
