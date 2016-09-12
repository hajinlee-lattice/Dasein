package com.latticeengines.domain.exposed.util;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.transform.sax.SAXSource;

import org.dmg.pmml.DataDictionary;
import org.dmg.pmml.DataField;
import org.dmg.pmml.FieldUsageType;
import org.dmg.pmml.MiningField;
import org.dmg.pmml.Model;
import org.dmg.pmml.PMML;
import org.jpmml.model.ImportFilter;
import org.jpmml.model.JAXBUtil;
import org.xml.sax.InputSource;
import org.xml.sax.XMLFilter;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

import com.latticeengines.domain.exposed.pmml.PmmlField;
import com.latticeengines.domain.exposed.pmml.SkipFilter;

public class PmmlModelUtils {

    public static final List<PmmlField> getPmmlFields(InputStream pmmlStream) throws Exception {
        InputSource source = new InputSource(pmmlStream);
        XMLReader reader = XMLReaderFactory.createXMLReader();
        XMLFilter importFilter = new ImportFilter(reader);
        XMLFilter skipSegmentationFilter = new SkipFilter(reader, "Segmentation");
        skipSegmentationFilter.setParent(importFilter);
        XMLFilter skipExtensionFilter = new SkipFilter(reader, "Extension");
        skipExtensionFilter.setParent(skipSegmentationFilter);
        SAXSource transformedSource = new SAXSource(skipExtensionFilter, source);

        PMML pmml = JAXBUtil.unmarshalPMML(transformedSource);
        Map<String, DataField> dataFields = getDataFields(pmml);

        Model model = pmml.getModels().get(0);
        List<MiningField> miningFields = model.getMiningSchema().getMiningFields();

        List<PmmlField> pmmlFields = new ArrayList<>();
        for (MiningField miningField : miningFields) {
            DataField f = dataFields.get(miningField.getName().getValue());

            if (f == null && miningField.getUsageType() != FieldUsageType.PREDICTED) {
                continue;
            }
            pmmlFields.add(new PmmlField(miningField, f));
        }

        return pmmlFields;
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
}
