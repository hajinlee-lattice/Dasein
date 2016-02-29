package com.latticeengines.metadata.exposed.resolution;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import javax.annotation.Nullable;

import org.apache.avro.Schema;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.SchemaInterpretation;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.metadata.exposed.standardschemas.SchemaRepository;

public class UserDefinedMetadataResolutionStrategy extends MetadataResolutionStrategy {
    private String csvPath;
    private SchemaInterpretation schema;
    private List<ColumnTypeMapping> additionalColumns;
    private Configuration yarnConfiguration;

    private static class Result {
        public List<ColumnTypeMapping> unknownColumns;
        public Table metadata;
    }

    private Result result;

    // Interaction with front end:
    // - FE -> BE: uploadFile
    // - FE -> BE: getUnknownColumns(SourceFile)
    // - FE -> BE: resolveMetadata(SourceFile, unknowncolumns)
    public UserDefinedMetadataResolutionStrategy(String csvPath, SchemaInterpretation schema,
            List<ColumnTypeMapping> additionalColumns, Configuration yarnConfiguration) {
        this.csvPath = csvPath;
        this.schema = schema;
        this.additionalColumns = additionalColumns != null ? additionalColumns : new ArrayList<ColumnTypeMapping>();
        this.yarnConfiguration = yarnConfiguration;

    }

    @Override
    public void calculate() {
        result = new Result();
        SchemaRepository repository = SchemaRepository.instance();
        result.metadata = repository.getSchema(schema);
        result.unknownColumns = new ArrayList<>();

        // Get header
        Set<String> headerFields = getHeaderFields();

        // Remove unrequired columns from metadata that are not in header,
        // asserting that no required columns have been removed
        Set<String> missingRequiredFields = new HashSet<>();
        List<Attribute> attributes = result.metadata.getAttributes();
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
                    new String[] { StringUtils.join(missingRequiredFields, ","), csvPath });
        }

        // Add columns that are not in metadata to unknown columns
        for (final String field : headerFields) {
            if (StringUtils.isEmpty(field)) {
                throw new RuntimeException("Found empty column name in headers.");
            }
            if (!Iterables.any(attributes, new Predicate<Attribute>() {

                @Override
                public boolean apply(@Nullable Attribute attribute) {
                    return field.equals(attribute.getName());
                }
            })) {
                ColumnTypeMapping ctm = new ColumnTypeMapping();
                ctm.setColumnName(field);
                ctm.setColumnType(Schema.Type.STRING.toString());
                result.unknownColumns.add(ctm);
            }
        }

        // Go through unknown columns and remove ones that are specified in the
        // additionalColumns
        for (final ColumnTypeMapping additional : additionalColumns) {
            Iterables.removeIf(result.unknownColumns, new Predicate<ColumnTypeMapping>() {

                @Override
                public boolean apply(@Nullable ColumnTypeMapping unknown) {
                    return unknown.getColumnName().equals(additional.getColumnName());
                }
            });
        }

        // Resolve the metadata types for the additional columns
        for (final ColumnTypeMapping ctm : additionalColumns) {
            Attribute attribute = Iterables.find(attributes, new Predicate<Attribute>() {
                @Override
                public boolean apply(@Nullable Attribute attribute) {
                    return attribute.equals(ctm.getColumnName());
                }
            }, null);
            validateDataType(ctm);

            if (attribute != null) {
                attribute.setPhysicalDataType(ctm.getColumnType());
            } else {
                // Add an attribute
                attribute = new Attribute();
                attribute.setName(ctm.getColumnName());
                attribute.setPhysicalDataType(ctm.getColumnType());
                attribute.setDisplayName(ctm.getColumnName());
                attribute.setApprovedUsage(ModelingMetadata.MODEL_AND_ALL_INSIGHTS_APPROVED_USAGE);
                attribute.setNullable(true);
                attributes.add(attribute);
            }
        }

        // If there are any unknown columns, the metadata is not fully defined.
    }

    private void validateDataType(ColumnTypeMapping ctm) {
        try {
            Schema.Type.valueOf(ctm.getColumnType().toUpperCase());
        } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to validate data type %s of column %s",
                    ctm.getColumnName(), ctm.getColumnType()));
        }
    }

    private Set<String> getHeaderFields() {
        try {
            String localPath = "/tmp/UserDefinedMetadataResolutionStrategy/" + UUID.randomUUID().toString() + "/"
                    + new File(csvPath).getName();
            HdfsUtils.copyHdfsToLocal(yarnConfiguration, csvPath, localPath);
            CSVFormat format = CSVFormat.RFC4180.withHeader().withDelimiter(',');
            try (CSVParser parser = new CSVParser(new FileReader(localPath), format)) {
                return parser.getHeaderMap().keySet();
            } catch (IOException e) {
                throw new RuntimeException(String.format( //
                        "Failed to read csv from local path %s (originally copied from HDFS at %s) ", //
                        localPath, csvPath));
            }
        } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to retrieve header fields from csv file at path %s",
                    csvPath));
        }
    }

    @Override
    public List<ColumnTypeMapping> getUnknownColumns() {
        if (result == null) {
            return new ArrayList<>();
        }
        return result.unknownColumns;
    }

    @Override
    public boolean isMetadataFullyDefined() {
        if (result == null) {
            return false;
        }
        return result.unknownColumns.isEmpty();
    }

    @Override
    public Table getMetadata() {
        if (result == null) {
            return null;
        }
        if (!isMetadataFullyDefined()) {
            throw new RuntimeException("Metadata is not fully defined");
        }
        return result.metadata;
    }
}
