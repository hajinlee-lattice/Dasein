package com.latticeengines.metadata.service.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.expection.AnnotationValidationError;
import com.latticeengines.common.exposed.validator.BeanValidationService;
import com.latticeengines.common.exposed.validator.impl.BeanValidationServiceImpl;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata.AttributeMetadata;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;
import com.latticeengines.metadata.service.MetadataService;

@Component("mdService")
public class MetadataServiceImpl implements MetadataService {

    private static final Logger log = Logger.getLogger(MetadataServiceImpl.class);

    @Autowired
    private TableEntityMgr tableEntityMgr;
    

    @Override
    public Table getTable(CustomerSpace customerSpace, String name) {
        return tableEntityMgr.findByName(name);
    }
    
    @Override
    public List<Table> getTables(CustomerSpace customerSpace) {
        return tableEntityMgr.findAll();
    }

    @Override
    public void createTable(CustomerSpace customerSpace, Table table) {
        if (tableEntityMgr.findByName(table.getName()) != null) {
            log.info("Table with name " + table.getName() + " already exists.  Updating instead");
            updateTable(customerSpace, table);
        }
        else {
            tableEntityMgr.create(table);
        }
    }

    @Override
    public void deleteTable(CustomerSpace customerSpace, String tableName) { 
        tableEntityMgr.delete(tableName);
    }
    
    @Override
    public void updateTable(CustomerSpace customerSpace, Table table) {
        if (tableEntityMgr.findByName(table.getName()) != null) {
            tableEntityMgr.delete(table.getName());
        }
        
        tableEntityMgr.create(table);
    }
    
    @Override
    public Map<String, Set<AnnotationValidationError>> validateTableMetadata(CustomerSpace customerSpace, ModelingMetadata modelingMetadata) {
        BeanValidationService validationService = new BeanValidationServiceImpl();
        try {
            Map<String, Set<AnnotationValidationError>> errors = new HashMap<>();
            for (AttributeMetadata m : modelingMetadata.getAttributeMetadata()) {
                Set<AnnotationValidationError> attributeErrors = validationService.validate(m);
                
                if (attributeErrors.size() > 0) {
                    errors.put(m.getColumnName(), attributeErrors);
                }
            }
            return errors;
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_00002, e);
        }
    }
}
