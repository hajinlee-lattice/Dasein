package com.latticeengines.dataflow.runtime.cascading.propdata.patch;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.util.TypeConversionUtil;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

/**
 * Patch all AM attributes of multiple patchItems. PatchItems are JSON object strings identified by input field names.
 * Key of the object is the attribute name, value is the object to patch.
 */
@SuppressWarnings("rawtypes")
public class PatchAMAttributesFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = 5196059636110982949L;

    private int[] patchItemIndexes;
    private Map<String, Integer> positionMap;

    public PatchAMAttributesFunction(@NotNull Fields fieldDeclaration, @NotNull String[] patchItemFields) {
        super(fieldDeclaration);
        check(patchItemFields);

        // field name => column index mapping
        positionMap = IntStream.range(0, fieldDeclaration.size())
                .mapToObj(idx -> Pair.of((String) fieldDeclaration.get(idx), idx))
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
        // column indexes for each patchItems
        patchItemIndexes = Arrays.stream(patchItemFields).mapToInt(positionMap::get).toArray();
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        Map<String, Object> patchItems = mergePatchItems(arguments, patchItemIndexes);
        if (MapUtils.isEmpty(patchItems)) {
            // nothing to patch
            functionCall.getOutputCollector().add(arguments);
            return;
        }

        functionCall.getOutputCollector().add(updateAMAttributes(arguments, patchItems));
    }

    private TupleEntry updateAMAttributes(TupleEntry arguments, Map<String, Object> patchItems) {
        TupleEntry result = new TupleEntry(arguments);
        for (Map.Entry<String, Object> item : patchItems.entrySet()) {
            if (!positionMap.containsKey(item.getKey())) {
                throw new IllegalArgumentException(
                        "Key in patchItems does not exist in account master: " + item.getKey());
            }

            // update attribute
            int keyIdx = positionMap.get(item.getKey());
            Class<?> clz = getFieldDeclaration().getTypeClass(keyIdx);
            result.setRaw(keyIdx, convert(item.getValue(), clz));
        }
        return result;
    }

    /*
     * Convert input object to the target type
     */
    private Object convert(Object obj, Class<?> clz) {
        // TODO maybe support float
        // do not allow to update to null
        Preconditions.checkNotNull(obj);
        if (Double.class.equals(clz)) {
            return TypeConversionUtil.toDouble(obj);
        } else if (Long.class.equals(clz)) {
            return TypeConversionUtil.toLong(obj);
        } else if (Integer.class.equals(clz)) {
            return TypeConversionUtil.toInteger(obj);
        } else if (String.class.equals(clz)) {
            return TypeConversionUtil.toString(obj);
        } else if (Boolean.class.equals(clz)) {
            return TypeConversionUtil.toBoolean(obj);
        } else {
            throw new UnsupportedOperationException("Target type is not supported for conversion: " + clz);
        }
    }

    /**
     * Deserialize multiple patchItems to Maps and merge into one.
     * @param entry input row
     * @param patchItemIndexes indexes of patchItems (JSON object string)
     * @return merged map
     */
    private Map<String, Object> mergePatchItems(TupleEntry entry, int[] patchItemIndexes) {
        // NOTE this merge function does not detect conflict between different patch items
        // NOTE Conflict should NOT happen because validation API is supposed to catch it.
        return Arrays
                .stream(patchItemIndexes)
                .filter(idx -> entry.getObject(idx) instanceof String)
                .mapToObj(patchItemIdx -> {
                    String patchItemStr = (String) entry.getObject(patchItemIdx);
                    if (patchItemStr == null) {
                        return null;
                    }

                    Map<?, ?> rawMap = JsonUtils.deserialize(patchItemStr, Map.class);
                    return JsonUtils.convertMap(rawMap, String.class, Object.class);
                })
                .collect(HashMap::new, Map::putAll, Map::putAll);
    }

    private void check(@NotNull String[] patchItemFields) {
        Preconditions.checkNotNull(patchItemFields);
        Arrays.stream(patchItemFields).forEach(Preconditions::checkNotNull);
    }
}
