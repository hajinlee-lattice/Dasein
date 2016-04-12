package com.latticeengines.scoringapi.transform;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.python.core.PyInteger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.voc.types.Dict;
import org.voc.types.Str;

import python.encoder.Make_float;
import python.encoder.Pivot;
import python.encoder.Replace_null_value;

import com.fasterxml.jackson.core.type.TypeReference;
import com.latticeengines.common.exposed.jython.JythonEngine;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.scoringapi.transform.impl.Encoder;

@Service
public class RecordTransformer {

    private static final Log log = LogFactory.getLog(RecordTransformer.class);

    @Autowired
    private TransformRetriever retriever;

    Dict argumentsMap;
    Dict recordMap;
    Dict pivotValues;
    Dict imputationsValues;

    private static Make_float makeFloat;
    private static Replace_null_value replaceNullValue;
    private static Pivot pivot;
    private static String pivotModelPath;
    private static String imputationModelPath;

    public Map<String, java.lang.Object> transform(String modelPath, List<TransformDefinition> definitions,
            Map<String, java.lang.Object> record) {

        synchronized (retriever) {
            Map<String, java.lang.Object> result = new HashMap<String, java.lang.Object>(record.size() + definitions.size());
            result.putAll(record);

            JythonEngine engine = null;

            recordMap = buildMapForFastTransform(record);

            for (TransformDefinition entry : definitions) {
                java.lang.Object value = null;
                boolean successfulInvocation = false;
                boolean jythonInvoke = false;
                Exception failedInvocationException = null;
                try{
                    argumentsMap = buildMapForFastTransform(entry.arguments);
                    if (entry.name.toLowerCase().equals("encoder") == true) {
                        value = Encoder.encode(entry.arguments, record, entry.type);
                        successfulInvocation = true;
                    } else if(entry.name.toLowerCase().equals("make_float") == true) {
                        if(makeFloat == null) {
                            makeFloat = new Make_float(null, null);
                        }
                        value = Make_float.transform( argumentsMap, recordMap);
                        successfulInvocation = true;
                    } else if (entry.name.toLowerCase().equals("pivot") == true) {
                        if(pivot == null) {
                            pivot = new Pivot(null, null);
                        }
                        String pivotFilePath = modelPath + "/pivotvalues.txt";
                        if(pivotFilePath.toString().equals(RecordTransformer.pivotModelPath) == false || pivotValues == null) {
                            RecordTransformer.pivotModelPath = pivotFilePath;
                            String fileString = FileUtils.readFileToString(new File(pivotFilePath));
                            fileString = fileString.replaceAll("u'", "'");
                            fileString = fileString.replace('(', '[');
                            fileString = fileString.replace(')', ']');
                            TypeReference<HashMap<String,List>> typeRef = new TypeReference<HashMap<String,List>>() {};
                            pivotValues =  buildMapOfListForFastTransform(JsonUtils.deserialize(fileString, typeRef, true));
                        }
                        value = Pivot.transform( argumentsMap, recordMap, pivotValues);
                        successfulInvocation = true;
                    } else if (entry.name.toLowerCase().equals("replace_null_value") == true) {
                        if(replaceNullValue == null) {
                            replaceNullValue = new Replace_null_value(null, null);
                        }
                        String imputationFilePath = modelPath + "/imputations.txt";
                        if(imputationFilePath.equals(RecordTransformer.imputationModelPath) == false || imputationsValues == null) {
                            RecordTransformer.imputationModelPath = imputationFilePath;
                            String fileString = FileUtils.readFileToString(new File(imputationFilePath));
                            TypeReference<HashMap<String,java.lang.Object>> typeRef = new TypeReference<HashMap<String,java.lang.Object>>() {};
                            imputationsValues =  buildMapForFastTransform(JsonUtils.deserialize(fileString, typeRef, true));
                        }
                        value = Replace_null_value.transform( argumentsMap, recordMap, imputationsValues);
                        successfulInvocation = true;
                    } else {
                        if(engine == null)
                            engine = new JythonEngine(modelPath);

                        value = engine.invoke(entry.name, entry.arguments, result, entry.type.type());
                        successfulInvocation = true;
                        jythonInvoke = true;
                    }
                } catch(Exception e){
                    System.out.println("Exception while doing fastTransforms" + e.getMessage());
                    System.out.println(e);
                    successfulInvocation = false;
                }

                // For Jython invocation, don't cast because Jython already casts
                if(jythonInvoke && successfulInvocation) {
                    log.warn("Jython invoked for entry.output" + entry.output + " " + value);
                    result.put(entry.output, value);
                }
                // Cast transformed value on successfulInvocation
                else if (successfulInvocation) {
                    if (entry.type.type() == Boolean.class) {
                        Integer value1 = ((PyInteger) value).getValue();
                        value = entry.type.type().cast(value1 == 1);
                    }
                    else if (entry.type.type() == Double.class) {
                        try{
                            if(value.toString().equals("null") == false && value.toString().equals("None") == false) {
                                value = entry.type.type().cast(Double.valueOf(value.toString()));
                            } else if(value.toString().toLowerCase().equals("true")) {
                                value = entry.type.type().cast(Double.valueOf("1.0")); 
                            } else if(value.toString().toLowerCase().equals("false")) {
                                value = entry.type.type().cast(Double.valueOf("0.0"));
                            } else {
                                value = null;
                            }
                        } catch(Exception e) {
                            log.warn(String.format("Problem parsing Python value to Java Double"), e);
                        }
                    }
                    else if (entry.type.type() == Long.class) {
                        try {
                            if(value.toString().equals("null") == false && value.toString().equals("None") == false) {
                                value = entry.type.type().cast(Long.valueOf(value.toString()));
                            } else {
                                value = null;
                            }
                        } catch(Exception e) {
                            log.warn(String.format("Problem parsing Python value to Java Long"), e);
                        }
                    } else {
                        value = null;
                    }

                    result.put(entry.output, value);
                } else {
                    if (log.isWarnEnabled()) {
                        log.warn(String.format("Problem invoking fast transform for %s", entry.name), failedInvocationException);
                    }
                }
            }

            return result;
        }
    }

    private org.voc.types.Dict buildMapForFastTransform(Map<String, java.lang.Object> mapObject) {
        org.voc.types.Dict vocDict = new org.voc.types.Dict();
        for(String s: mapObject.keySet()) {
            if(mapObject.get(s) != null) {
                vocDict.value.put(new Str(s), new Str(mapObject.get(s).toString()));
            } else {
                vocDict.value.put(new Str(s), new Str("null"));
            }
        }
        return vocDict;
    }

    private org.voc.types.Dict buildMapOfListForFastTransform(Map<String, List> mapObject) {
        org.voc.types.Dict vocDict = new org.voc.types.Dict();
        for(String s: mapObject.keySet()) {
            if(mapObject.get(s) != null) {
                org.voc.types.Dict subDict = new org.voc.types.Dict();
                Str keyName = new Str("1");
                org.voc.types.List subList = new org.voc.types.List();
                for(String value : mapObject.get(s).get(1).toString().split(",")) {
                    value = value.replace("[", "");
                    value = value.replace("]", "");
                    subList.value.add(new Str(value));
                }
                subDict.value.put(keyName, subList);
                vocDict.value.put(new Str(s), subDict);
            } else {
                vocDict.value.put(new Str(s), new Str("null"));
            }
        }
        return vocDict;
    }

    public Map<String, java.lang.Object> transformOld(String modelPath, List<TransformDefinition> definitions,
            Map<String, java.lang.Object> record) {
        Map<String, java.lang.Object> result = new HashMap<String, java.lang.Object>(record.size() + definitions.size());
        result.putAll(record);
        JythonEngine engine = new JythonEngine(modelPath);

        for (TransformDefinition entry : definitions) {
            java.lang.Object value = null;
            boolean successfulInvocation = false;
            Exception failedInvocationException = null;
            for (int numTries = 1; numTries < 3; numTries++) {
                try {
                    value = engine.invoke(entry.name, entry.arguments, result, entry.type.type());
                    successfulInvocation = true;
                    if (numTries > 1) {
                        log.warn(String.format("Transform invocation on %s succeeded on try #%d", entry.name, numTries));
                    }
                    break;
                } catch (Exception e) {
                    failedInvocationException = e;
                }
            }
            if (successfulInvocation) {
                result.put(entry.output, value);
            } else {
                if (log.isWarnEnabled()) {
                    log.warn(String.format("Problem invoking %s", entry.name), failedInvocationException);
                }
            }
        }

        return result;
    }

}
