package com.latticeengines.scoring.util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.SchemaBuilder.FieldBuilder;
import org.apache.avro.SchemaBuilder.RecordBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFileFilter;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.scoring.ScoreOutput;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.scoring.orchestration.service.ScoringDaemonService;

public class ScoringJobUtil {

    public static List<String> findModelUrlsToLocalize(Configuration yarnConfiguration, String tenant,
            String customerBaseDir, List<String> modelGuids) {
        List<String> modelFilePaths = findAllModelPathsInHdfs(yarnConfiguration, tenant, customerBaseDir);
        return findModelUrlsToLocalize(yarnConfiguration, tenant, modelGuids, modelFilePaths);
    }

    public static List<String> findAllModelPathsInHdfs(Configuration yarnConfiguration, String tenant,
            String customerBaseDir) {
        String customerModelPath = customerBaseDir + "/" + tenant + "/models";
        List<String> modelFilePaths = Collections.emptyList();
        try {
            modelFilePaths = HdfsUtils.getFilesForDirRecursive(yarnConfiguration, customerModelPath,
                    new HdfsFileFilter() {
                        @Override
                        public boolean accept(FileStatus fileStatus) {
                            if (fileStatus == null) {
                                return false;
                            }
                            Pattern p = Pattern.compile(".*model" + ScoringDaemonService.JSON_SUFFIX);
                            Matcher matcher = p.matcher(fileStatus.getPath().getName());
                            return matcher.matches();
                        }
                    });
        } catch (Exception e) {
            throw new RuntimeException("Customer " + tenant + "'s scoring job failed due to: " + e.getMessage(), e);
        }
        if (CollectionUtils.isEmpty(modelFilePaths)) {
            throw new LedpException(LedpCode.LEDP_20008, new String[] { tenant });
        }
        return modelFilePaths;
    }

    @VisibleForTesting
    static List<String> findModelUrlsToLocalize(Configuration yarnConfiguration, String tenant,
            List<String> modelGuids, List<String> modelFilePaths) {
        List<String> modelUrlsToLocalize = new ArrayList<>();
        label: for (String modelGuid : modelGuids) {
            String uuid = UuidUtils.extractUuid(modelGuid);
            for (String path : modelFilePaths) {
                if (uuid.equals(UuidUtils.parseUuid(path))) {
                    try {
                        HdfsUtils.getCheckSum(yarnConfiguration, path);
                    } catch (IOException e) {
                        throw new LedpException(LedpCode.LEDP_20021, new String[] { path, tenant });
                    }
                    modelUrlsToLocalize.add(path + "#" + uuid);
                    continue label;
                }
            }
            throw new LedpException(LedpCode.LEDP_18007, new String[] { modelGuid });
        }
        return modelUrlsToLocalize;
    }

    public static JsonNode generateDataTypeSchema(Schema schema) {
        List<Field> fields = schema.getFields();
        ObjectNode jsonObj = new ObjectMapper().createObjectNode();
        for (Field field : fields) {
            String type = AvroUtils.getType(field).getName();
            if (type.equals("string") || type.equals("bytes"))
                jsonObj.put(field.name(), 1);
            else
                jsonObj.put(field.name(), 0);
        }
        return jsonObj;
    }

    public static Schema createSchema(String name, Table table) {
        RecordBuilder<Schema> recordBuilder = SchemaBuilder.record(name);
        recordBuilder.prop("uuid", UUID.randomUUID().toString());
        FieldAssembler<Schema> fieldAssembler = recordBuilder.doc("").fields();
        FieldBuilder<Schema> fieldBuilder;

        for (Attribute attr : table.getAttributes()) {
            fieldBuilder = fieldAssembler.name(attr.getName());

            fieldBuilder = fieldBuilder.prop("displayName", attr.getDisplayName());

            if (attr.getLength() != null) {
                fieldBuilder = fieldBuilder.prop("length", attr.getLength().toString());
            }
            if (attr.getPrecision() != null) {
                fieldBuilder = fieldBuilder.prop("precision", attr.getPrecision().toString());
            }
            if (attr.getScale() != null) {
                fieldBuilder = fieldBuilder.prop("scale", attr.getScale().toString());
            }
            if (attr.getLogicalDataType() != null) {
                fieldBuilder = fieldBuilder.prop("logicalType", attr.getLogicalDataType().toString());
            }
            fieldBuilder = fieldBuilder.prop("sourceLogicalType", attr.getSourceLogicalDataType());
            fieldBuilder = fieldBuilder.prop("uuid", UUID.randomUUID().toString());

            for (Map.Entry<String, Object> entry : attr.getEntries()) {
                fieldBuilder.prop(entry.getKey(), entry.getValue() == null ? "" : entry.getValue().toString());
            }

            if (attr.getEnumValues().size() > 0) {
                fieldBuilder = fieldBuilder.prop("enumValues", StringUtils.join(attr.getEnumValues().toArray(), ","));
            }

            Type type = Type.valueOf(attr.getPhysicalDataType().toUpperCase());

            switch (type) {
            case DOUBLE:
                fieldAssembler = fieldBuilder.type().unionOf().doubleType().and().nullType().endUnion().noDefault();
                break;
            case FLOAT:
                fieldAssembler = fieldBuilder.type().unionOf().floatType().and().nullType().endUnion().noDefault();
                break;
            case INT:
                fieldAssembler = fieldBuilder.type().unionOf().intType().and().nullType().endUnion().noDefault();
                break;
            case LONG:
                fieldAssembler = fieldBuilder.type().unionOf().longType().and().nullType().endUnion().noDefault();
                break;
            case STRING:
                fieldAssembler = fieldBuilder.type().unionOf().stringType().and().nullType().endUnion().noDefault();
                break;
            case BOOLEAN:
                fieldAssembler = fieldBuilder.type().unionOf().booleanType().and().nullType().endUnion().noDefault();
                break;
            case ENUM:
                String[] enumValues = new String[attr.getEnumValues().size()];
                attr.getEnumValues().toArray(enumValues);
                fieldAssembler = fieldBuilder.type().enumeration(attr.getName()).symbols(enumValues).noDefault();
                break;
            default:
                break;
            }
        }
        return fieldAssembler.endRecord();
    }

    public static Table createGenericOutputSchema() {
        Table scoreResultTable = new Table();
        String tableName = "ScoreResult";
        scoreResultTable.setName(tableName);
        Attribute id = new Attribute();
        id.setName(InterfaceName.Id.name());
        id.setDisplayName(InterfaceName.Id.name());
        id.setPhysicalDataType(Type.STRING.name());
        id.setSourceLogicalDataType(InterfaceName.Id.name());

        Attribute percentile = new Attribute();
        percentile.setName(ScoreResultField.Percentile.name());
        percentile.setDisplayName(ScoreResultField.Percentile.name());
        percentile.setPhysicalDataType(ScoreResultField.Percentile.physicalDataType);
        percentile.setSourceLogicalDataType(ScoreResultField.Percentile.sourceLogicalDataType);

        Attribute rawScore = new Attribute();
        rawScore.setName(ScoreResultField.RawScore.name());
        rawScore.setDisplayName(ScoreResultField.RawScore.name());
        rawScore.setPhysicalDataType(ScoreResultField.RawScore.physicalDataType);
        rawScore.setSourceLogicalDataType(ScoreResultField.RawScore.sourceLogicalDataType);

        scoreResultTable.setAttributes(Arrays.<Attribute> asList(new Attribute[] { id, percentile, rawScore }));
        return scoreResultTable;
    }

    public static void writeScoreResultToAvroRecord(DataFileWriter<GenericRecord> dataFileWriter, List<ScoreOutput> resultList, File outputFile) throws IOException {
        Table scoreResultTable = ScoringJobUtil.createGenericOutputSchema();
        Schema schema = ScoringJobUtil.createSchema(scoreResultTable.getName(), scoreResultTable);

        dataFileWriter.create(schema, outputFile);
        for (ScoreOutput scoreOutput : resultList) {
            GenericRecordBuilder builder = new GenericRecordBuilder(schema);
            builder.set(InterfaceName.Id.name(), String.valueOf(scoreOutput.getLeadID()));
            builder.set(ScoreResultField.Percentile.name(), scoreOutput.getPercentile());
            builder.set(ScoreResultField.RawScore.name(), scoreOutput.getRawScore());
            dataFileWriter.append(builder.build());
        }
    }

}
