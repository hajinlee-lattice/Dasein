package com.latticeengines.pls.metadata.resolution;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.latticeengines.common.exposed.closeable.resource.CloseableResourcePool;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.pls.metadata.standardschemas.SchemaRepository;
import com.latticeengines.pls.util.ValidateFileHeaderUtils;

public class UserDefinedMetadataResolutionStrategy extends MetadataResolutionStrategy {
    private String csvPath;
    private SchemaInterpretation schema;
    private List<ColumnTypeMapping> additionalColumns;
    private static Configuration yarnConfiguration;

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
        result = new Result();

    }

    @Override
    public void calculateBasedOnExistingMetadata(Table metadataTable) {
        result.metadata = metadataTable;
        result.metadata.getExtracts().clear();
        result.unknownColumns = new ArrayList<>();
        calculateHelper();
    }

    @Override
    public void calculate() {
        SchemaRepository repository = SchemaRepository.instance();
        result.metadata = repository.getSchema(schema);
        result.unknownColumns = new ArrayList<>();
        calculateHelper();
    }

    private void calculateHelper() {
        // Get header
        Set<String> headerFields = getHeaderFields();

        // Shed columns from metadata that are not in the csv
        Set<String> missingRequiredFields = new HashSet<>();
        List<Attribute> attributes = result.metadata.getAttributes();
        Iterator<Attribute> attrIterator = attributes.iterator();

        while (attrIterator.hasNext()) {
            Attribute attribute = attrIterator.next();
            Iterator<String> headerIterator = headerFields.iterator();

            boolean foundMatchingAttribute = false;
            while (headerIterator.hasNext()) {
                String header = headerIterator.next();
                if (attribute.getAllowedDisplayNames() != null && attribute.getAllowedDisplayNames().contains(header)) {
                    headerIterator.remove();
                    attribute.setDisplayName(header);
                    foundMatchingAttribute = true;
                } else if (attribute.getDisplayName().equals(header)) {
                    headerIterator.remove();
                    foundMatchingAttribute = true;
                }
            }
            if (!foundMatchingAttribute) {
                if (!attribute.isNullable()) {
                    missingRequiredFields.add(attribute.getName());
                }
                attrIterator.remove();
            }
        }

        // Add columns that are not in metadata to unknown columns
        for (final String field : headerFields) {
            ColumnTypeMapping ctm = new ColumnTypeMapping();
            ctm.setColumnName(field);
            ctm.setColumnType(Schema.Type.STRING.toString());
            result.unknownColumns.add(ctm);
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
                attribute = generateAttributeBasedOnColumnTypeMapping(ctm);
                attributes.add(attribute);
            }
        }

        Attribute lastModified = result.metadata.getAttribute(InterfaceName.LastModifiedDate);
        if (lastModified == null) {
            result.metadata.setLastModifiedKey(null);
        }

        // If there are any unknown columns, the metadata is not fully defined.
    }

    Attribute generateAttributeBasedOnColumnTypeMapping(ColumnTypeMapping ctm) {
        Attribute attribute = new Attribute();
        attribute.setName(ctm.getColumnName().replaceAll("[^A-Za-z0-9_]", "_"));
        attribute.setPhysicalDataType(ctm.getColumnType());
        attribute.setDisplayName(ctm.getColumnName());
        attribute.setApprovedUsage(ModelingMetadata.MODEL_AND_ALL_INSIGHTS_APPROVED_USAGE);
        attribute.setCategory(ModelingMetadata.CATEGORY_LEAD_INFORMATION);
        attribute.setFundamentalType(generateFundamentalTypeBasedOnColumnType(ctm.getColumnType()));
        attribute.setStatisticalType(generateStatisticalTypeBasedOnColumnType(ctm.getColumnType()));
        attribute.setNullable(true);
        attribute.setTags(ModelingMetadata.INTERNAL_TAG);
        return attribute;
    }

    public static void main(String[] args) {
        UserDefinedMetadataResolutionStrategy userDefinedMetadataResolutionStrategy = new UserDefinedMetadataResolutionStrategy(
                "phf", SchemaInterpretation.SalesforceAccount, null, yarnConfiguration);
        ColumnTypeMapping mp = new ColumnTypeMapping();
        mp.setColumnName("jha");
        mp.setColumnType("String");
        Attribute attribute = userDefinedMetadataResolutionStrategy.generateAttributeBasedOnColumnTypeMapping(mp);
        System.out.println(attribute.getFundamentalType());
        System.out.println(attribute.getStatisticalType());
    }

    private String generateFundamentalTypeBasedOnColumnType(String columnType) {
        String fundamentalType = null;
        switch (columnType.toUpperCase()) {
        case "BOOLEAN":
            fundamentalType = ModelingMetadata.FT_BOOLEAN;
            break;
        case "DOUBLE":
            fundamentalType = ModelingMetadata.FT_NUMERIC;
            break;
        case "STRING":
        default:
            fundamentalType = ModelingMetadata.FT_ALPHA;
            break;
        }
        return fundamentalType;
    }

    private String generateStatisticalTypeBasedOnColumnType(String columnType) {
        String statType = null;
        switch (columnType.toUpperCase()) {
        case "BOOLEAN":
            statType = ModelingMetadata.NOMINAL_STAT_TYPE;
            break;
        case "DOUBLE":
            statType = ModelingMetadata.RATIO_STAT_TYPE;
            break;
        case "STRING":
            statType = ModelingMetadata.NOMINAL_STAT_TYPE;
            break;
        default:
            statType = ModelingMetadata.RATIO_STAT_TYPE;
            break;
        }
        return statType;
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
        CloseableResourcePool closeableResourcePool = new CloseableResourcePool();
        try {
            FileSystem fs = FileSystem.newInstance(yarnConfiguration);
            InputStream is = fs.open(new Path(csvPath));
            return ValidateFileHeaderUtils.getCSVHeaderFields(is, closeableResourcePool);
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_00002, e);
        } finally {
            try {
                closeableResourcePool.close();
            } catch (IOException e) {
                throw new RuntimeException("Problem when closing the pool", e);
            }
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
