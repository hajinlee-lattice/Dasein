package com.latticeengines.pls.metadata.resolution;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.common.exposed.util.NameValidationUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.pls.metadata.standardschemas.SchemaRepository;

public abstract class MetadataResolutionStrategy {

    public abstract void calculate();

    public abstract List<ColumnTypeMapping> getUnknownColumns();

    public abstract boolean isMetadataFullyDefined();

    public abstract Table getMetadata();

    public void validateHeaderFields(Set<String> headerFields, SchemaInterpretation schema, String path) {

        SchemaRepository repository = SchemaRepository.instance();
        Table metadata = repository.getSchema(schema);

        Set<String> missingRequiredFields = new HashSet<>();
        List<Attribute> attributes = metadata.getAttributes();
        Iterator<Attribute> iterator = attributes.iterator();
        while (iterator.hasNext()) {
            Attribute attribute = iterator.next();
            boolean missing = !headerFields.contains(attribute.getName());
            if (missing && !attribute.isNullable()) {
                missingRequiredFields.add(attribute.getName());
            }
            if (missing) {
                iterator.remove();
            }
        }

        if (!missingRequiredFields.isEmpty()) {
            throw new LedpException(LedpCode.LEDP_18087, //
                    new String[] { StringUtils.join(missingRequiredFields, ","), path });
        }

        for (final String field : headerFields) {
            if (StringUtils.isEmpty(field)) {
                throw new LedpException(LedpCode.LEDP_18096, new String[] { path });
            } else if (!NameValidationUtils.validateColumnName(field)) {
                throw new LedpException(LedpCode.LEDP_18095, new String[] { field });
            }
        }
    }

}
