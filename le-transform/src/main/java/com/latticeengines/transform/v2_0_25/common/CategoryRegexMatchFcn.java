package com.latticeengines.transform.v2_0_25.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class CategoryRegexMatchFcn extends TransformWithImpAndVetoFunctionBase {

    class Category {
        public String name;
        public Pattern pattern;

        Category(String name, Pattern pattern) {
            this.name = name;
            this.pattern = pattern;
        }
    }

    private List<Category> categories = new ArrayList<>();
    private Map<String, Double> categoryValue;

    public CategoryRegexMatchFcn(Object mapAndImputationObj, String unusualCharSet, String vetoStringSet,
            List<List<String>> regexMap) {
        super(mapAndImputationObj, unusualCharSet, vetoStringSet);
        for (List<String> categoryRegex : regexMap) {
            String category = categoryRegex.get(0);
            String regex = ".*?(" + categoryRegex.get(1) + ").*?";
            categories.add(
                    new Category(category, Pattern.compile(regex, Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE)));
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void setImputation(Object mapAndImputationObj) {

        // The JSON artifact has both a category -> regex map and the
        // imputed
        // category; an example is
        // [ { "Consumer": 5.2, "Government": 0}, "Consumer" ]
        List<Object> mapAndImputation = ((List<Object>) mapAndImputationObj);

        // categoryValueRaw may contain either Doubles or Integers
        Map<String, Object> categoryValueRaw = (Map<String, Object>) mapAndImputation.get(0);

        categoryValue = new HashMap<>();
        for (Map.Entry<String, Object> entry : categoryValueRaw.entrySet()) {
            categoryValue.put(entry.getKey(), ((Number) entry.getValue()).doubleValue());
        }

        String imputedCategory = (String) mapAndImputation.get(1);
        super.setImputation(categoryValue.get(imputedCategory));
    }

    @Override
    public double execute(String s) {
        if (s == null) {
            return getImputation();
        }

        for (Category category : categories) {
            if (category.pattern.matcher(s).matches()) {
                return categoryValue.getOrDefault(category.name, 0.0);
            }
        }

        if (!isValid(s)) {
            return categoryValue.getOrDefault("invalid", 0.0);
        }

        if (categoryValue.containsKey("")) {
            return categoryValue.get("");
        }

        return 0.0;
    }

}
