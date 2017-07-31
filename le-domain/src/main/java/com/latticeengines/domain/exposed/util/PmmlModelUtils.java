package com.latticeengines.domain.exposed.util;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.transform.sax.SAXSource;

import org.dmg.pmml.DataDictionary;
import org.dmg.pmml.DataField;
import org.dmg.pmml.DataType;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.FieldUsageType;
import org.dmg.pmml.InvalidValueTreatmentMethodType;
import org.dmg.pmml.MiningField;
import org.dmg.pmml.MiningSchema;
import org.dmg.pmml.MissingValueTreatmentMethodType;
import org.dmg.pmml.Model;
import org.dmg.pmml.PMML;
import org.jpmml.evaluator.UnsupportedFeatureException;
import org.jpmml.model.ImportFilter;
import org.jpmml.model.JAXBUtil;
import org.xml.sax.InputSource;
import org.xml.sax.XMLFilter;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

import com.latticeengines.domain.exposed.pmml.LEImportFilter;
import com.latticeengines.domain.exposed.pmml.PmmlField;
import com.latticeengines.domain.exposed.pmml.SkipFilter;

public class PmmlModelUtils {

    public static final Map<DataType, Object> dataTypeValueMap = new HashMap<>();

    static {
        for (DataType dataType : DataType.values()) {
            switch (dataType) {
            case STRING:
                dataTypeValueMap.put(dataType, "");
                break;
            case INTEGER:
                dataTypeValueMap.put(dataType, 0);
                break;
            case FLOAT:
                dataTypeValueMap.put(dataType, 0.0);
                break;
            case BOOLEAN:
                dataTypeValueMap.put(dataType, false);
                break;
            case DATE:
                dataTypeValueMap.put(dataType, null);
                break;
            case DATE_DAYS_SINCE_0:
                dataTypeValueMap.put(dataType, null);
                break;
            case DATE_DAYS_SINCE_1960:
                dataTypeValueMap.put(dataType, null);
                break;
            case DATE_DAYS_SINCE_1970:
                dataTypeValueMap.put(dataType, null);
                break;
            case DATE_DAYS_SINCE_1980:
                dataTypeValueMap.put(dataType, null);
                break;
            case DATE_TIME:
                dataTypeValueMap.put(dataType, null);
                break;
            case DATE_TIME_SECONDS_SINCE_0:
                dataTypeValueMap.put(dataType, null);
                break;
            case DATE_TIME_SECONDS_SINCE_1960:
                dataTypeValueMap.put(dataType, null);
                break;
            case DATE_TIME_SECONDS_SINCE_1970:
                dataTypeValueMap.put(dataType, null);
                break;
            case DATE_TIME_SECONDS_SINCE_1980:
                dataTypeValueMap.put(dataType, null);
                break;
            case DOUBLE:
                dataTypeValueMap.put(dataType, 0.0);
                break;
            case TIME:
                dataTypeValueMap.put(dataType, null);
                break;
            case TIME_SECONDS:
                dataTypeValueMap.put(dataType, null);
                break;
            default:
                break;
            }
        }

    }

    public static final List<PmmlField> getPmmlFields(PMML pmml) {
        Map<String, DataField> dataFields = getDataFields(pmml);
        Model model = pmml.getModels().get(0);
        List<MiningField> miningFields = model.getMiningSchema().getMiningFields();

        List<PmmlField> pmmlFields = new ArrayList<>();
        for (MiningField miningField : miningFields) {
            DataField f = dataFields.get(miningField.getName().getValue());

            if (f == null && miningField.getUsageType() != FieldUsageType.PREDICTED
                    && miningField.getUsageType() != FieldUsageType.TARGET) {
                continue;
            }
            pmmlFields.add(new PmmlField(miningField, f));
        }

        return pmmlFields;
    }

    public static final List<PmmlField> getPmmlFields(InputStream pmmlStream) throws Exception {
        PMML pmml = getPMML(pmmlStream);
        return getPmmlFields(pmml);
    }

    public static PMML getPMMLWithOriginalVersion(InputStream pmmlStream) throws Exception {
        InputSource source = new InputSource(pmmlStream);
        XMLReader reader = XMLReaderFactory.createXMLReader();
        LEImportFilter importFilter = new LEImportFilter(reader);
        XMLFilter skipSegmentationFilter = new SkipFilter(reader, "Segmentation");
        skipSegmentationFilter.setParent(importFilter);
        XMLFilter skipExtensionFilter = new SkipFilter(reader, "Extension");
        skipExtensionFilter.setParent(skipSegmentationFilter);
        SAXSource transformedSource = new SAXSource(skipExtensionFilter, source);

        PMML pmml = JAXBUtil.unmarshalPMML(transformedSource);
        pmml.setVersion(importFilter.getOriginalVersion());
        return pmml;
    }

    public static PMML getPMML(InputStream pmmlStream) throws Exception {
        return getPMML(pmmlStream, true);
    }

    public static PMML getPMML(InputStream pmmlStream, boolean skipSections) throws Exception {
        if (skipSections) {
            XMLReader reader = XMLReaderFactory.createXMLReader();
            ImportFilter importFilter = new ImportFilter(reader);
            XMLFilter skipSegmentationFilter = new SkipFilter(reader, "Segmentation");
            skipSegmentationFilter.setParent(importFilter);
            XMLFilter skipExtensionFilter = new SkipFilter(reader, "Extension");
            skipExtensionFilter.setParent(skipSegmentationFilter);
            SAXSource transformedSource = new SAXSource(skipExtensionFilter, new InputSource(pmmlStream));

            return JAXBUtil.unmarshalPMML(transformedSource);
        } else {
            return JAXBUtil.unmarshalPMML(ImportFilter.apply(new InputSource(pmmlStream)));
        }
    }

    private static Map<String, DataField> getDataFields(PMML pmml) {
        DataDictionary dataDictionary = pmml.getDataDictionary();
        List<DataField> dataFields = dataDictionary.getDataFields();
        Map<String, DataField> map = new HashMap<>();
        for (DataField dataField : dataFields) {
            map.put(dataField.getName().getValue(), dataField);
        }
        return map;
    }

    public static List<MiningField> getMiningFields(Model model) {
        MiningSchema miningSchema = model.getMiningSchema();
        return miningSchema.getMiningFields();
    }

    public static MiningField getTargetField(Model model) {

        MiningField result = null;

        List<MiningField> miningFields = getMiningFields(model);
        for (MiningField miningField : miningFields) {
            FieldUsageType fieldUsage = miningField.getUsageType();

            switch (fieldUsage) {
            case TARGET:
            case PREDICTED:
                if (result != null) {
                    throw new UnsupportedFeatureException(miningFields.toString());
                }
                result = miningField;
                break;
            default:
                break;
            }
        }

        return result;
    }

    public static void setDefaultValueForMiningField(Model model, DataDictionary dataDictionary) {
        List<MiningField> miningFields = getMiningFields(model);

        for (MiningField miningField : miningFields) {
            if (!miningField.getUsageType().equals(FieldUsageType.ACTIVE)) {
                continue;
            }
            miningField.setInvalidValueTreatment(InvalidValueTreatmentMethodType.AS_MISSING);
            if (miningField.getMissingValueTreatment() != null && miningField.getMissingValueReplacement() != null) {
                continue;
            }
            miningField.setMissingValueTreatment(MissingValueTreatmentMethodType.AS_VALUE);

            Map<FieldName, DataField> map = getFieldNameDataFieldMap(dataDictionary);
            if (!map.containsKey(miningField.getName())) {
                throw new RuntimeException(
                        String.format("MiningField %s not found in the map", miningField.getName()).toString());
            }
            DataField dataField = map.get(miningField.getName());
            DataType dataType = dataField.getDataType();
            switch (dataField.getOpType()) {
            case CATEGORICAL:
                miningField.setMissingValueReplacement(dataField.getValues().get(0).getValue());
                break;
            case CONTINUOUS:
                miningField.setMissingValueReplacement(String.valueOf(dataTypeValueMap.get(dataType)));
                break;
            case ORDINAL:
            default:
                break;
            }

        }
    }

    public static Map<FieldName, DataField> getFieldNameDataFieldMap(DataDictionary dataDictionary) {
        Map<FieldName, DataField> map = new HashMap<>();
        for (DataField dataField : dataDictionary.getDataFields()) {
            map.put(dataField.getName(), dataField);
        }
        return map;
    }
}
