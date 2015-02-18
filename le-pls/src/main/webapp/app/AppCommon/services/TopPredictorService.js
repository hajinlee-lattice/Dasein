angular.module('mainApp.appCommon.services.TopPredictorService', [
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.appCommon.utilities.AnalyticAttributeUtility'
])
.service('TopPredictorService', function (StringUtility, AnalyticAttributeUtility) {
    
    this.ShowBasedOnTags = function (predictor, tag) {
        
        //TODO:pierce This is a hack because DataLoader is not providing internal metadata
        if (predictor.Tags == null) {
            predictor.Tags = ["External"];
        }
        
        var toReturn = false;
        for (var x=0; x<predictor.Tags.length; x++) {
            if (tag == predictor.Tags[x]) {
                toReturn = true;
                break;
            }
        }
        
        return toReturn;
    };
    
    this.GetNumberOfAttributesByType = function (fullPredictorList, type) {
        var toReturn = 0;
        for (var i = 0; i < fullPredictorList.length; i++) {
            var predictor = fullPredictorList[i];
            if (this.ShowBasedOnTags(predictor, type) && AnalyticAttributeUtility.IsAllowedForInsights(predictor)) {
                //TODO:pierce Might need to check frequency on categoricals before adding the number
                toReturn += predictor.Elements.length;
            }
        }
        
        return toReturn;
    };
    
    this.GetNumberOfAttributesByCategory = function (categoryList, type, fullPredictorList) {
        var toReturn = {
            total: 0,
            categories: []
        };
        if (categoryList == null || type == null || fullPredictorList == null) {
            return toReturn;
        }
        for (var x = 0; x < categoryList.length; x++) {
            var category = categoryList[x];
            var displayCategory = {
                name: category.name,
                count: 0,
                color: category.color
            };
            for (var i = 0; i < fullPredictorList.length; i++) {
                var predictor = fullPredictorList[i];
                if (predictor.Category == category.name && 
                    this.ShowBasedOnTags(predictor, type) &&
                    AnalyticAttributeUtility.IsAllowedForInsights(predictor)) {
                        //TODO:pierce Might need to check frequency on categoricals before adding the number
                        toReturn.total += predictor.Elements.length;
                        displayCategory.count = displayCategory.count + predictor.Elements.length;
                    }
            }
            
            if (displayCategory.count > 0) {
                toReturn.categories.push(displayCategory);
            }
        }
        
        return toReturn;
    };
    
    this.GetColorByCategory = function (categoryName) {
        var toReturn;
        switch (categoryName) {
            case "Firmographics":
                toReturn = "#27D2AE";
                break;
            case "Growth Trends":
                toReturn = "#3279DF";
                break;
            case "Online Presence":
                toReturn = "#FF9403";
                break;
            case "Technologies":
                toReturn = "#BD8DF6";
                break;
            case "Behind Firewall Tech":
                toReturn = "#96E01E";
                break;
            case "Financial":
                toReturn = "#A8A8A8";
                break;
            case "Marketing Activity":
                toReturn = "#3279DF";
                break;
            case "Lead Information":
                toReturn = "#FF7A44";
                break;
            default:
                toReturn = "#000000";
                break;
        }
        
        return toReturn;
    };
    
    this.SortByPredictivePower = function (a, b) {
        if (a.UncertaintyCoefficient > b.UncertaintyCoefficient) {
            return -1;
        }
        if (a.UncertaintyCoefficient < b.UncertaintyCoefficient) {
            return 1;
        }
        // a must be equal to b
        return 0;
    };
    
    this.GetAttributesByCategory = function (predictorList, categoryName, categoryColor, maxNumber) {
        if (StringUtility.IsEmptyString(categoryName) || predictorList == null) {
            return [];
        }
        
        var totalPredictors = [];
        for (var i = 0; i < predictorList.length; i++) {
            if (categoryName == predictorList[i].Category) {
                totalPredictors.push(predictorList[i]);
            }
        }
        totalPredictors = totalPredictors.sort(this.SortByPredictivePower);
        
        var toReturn = [];
        for (var y = 0; y < totalPredictors.length; y++) {
            var predictor = totalPredictors[y];
            if (maxNumber == null || toReturn.length < maxNumber) {
                var displayPredictor = {
                  name: predictor.Name,
                  power: predictor.UncertaintyCoefficient,
                  size: 1,
                  color: categoryColor
                };
                toReturn.push(displayPredictor);
            } else {
                break;
            }
        }
        return toReturn;
        
    };
    
    this.CalculateAttributeSize = function (attributeList) {
        if (attributeList == null || attributeList.length === 0) {
            return null;
        }
        var numLargeCategories = Math.round(attributeList.length * 0.16);
        var numMediumCategories = Math.round(attributeList.length * 0.32);
        
        for (var i = 0; i < attributeList.length; i++) {
            var attribute = attributeList[i];
            if (numLargeCategories > 0) {
                attribute.size = 6.55;
                numLargeCategories--;
            } else if (numMediumCategories > 0) {
                attribute.size = 2.56;
                numMediumCategories--;
            } else {
                attribute.size = 1;
            }
        }
    };
    
    this.FormatDataForChart = function (modelSummary) {
        if (modelSummary == null || modelSummary.Predictors == null || modelSummary.Predictors.length === 0) {
            return null;
        }
        
        // First sort all predictors by UncertaintyCoefficient
        modelSummary.Predictors = modelSummary.Predictors.sort(this.SortByPredictivePower);
        
        // Then pull all unique categories
        var topCategories = [];
        var topCategoryNames = [];
        var category;
        for (var i = 0; i < modelSummary.Predictors.length; i++) {
            var predictor = modelSummary.Predictors[i];
            if (!StringUtility.IsEmptyString(predictor.Category) &&  topCategoryNames.indexOf(predictor.Category) === -1 && topCategoryNames.length < 8) {
                topCategoryNames.push(predictor.Category);
                category = {
                    name: predictor.Category,
                    power: predictor.UncertaintyCoefficient,
                    size: 1, // This doesn't matter because the inner ring takes on the saze of the outer
                    color: this.GetColorByCategory(predictor.Category),
                    children: []
                };
                topCategories.push(category);
            }
        }
        
        //And finally calculate the size based on predictive power
        var attributesPerCategory = topCategories.length >= 7 ? 3 : 5;
        var numLargeCategories = Math.round((topCategories.length * attributesPerCategory) * 0.16);
        var numMediumCategories = Math.round((topCategories.length * attributesPerCategory) * 0.32);
        var totalAttributes = [];
        for (var x = 0; x < topCategories.length; x++) {
            category = topCategories[x];
            category.children = this.GetAttributesByCategory(modelSummary.Predictors, category.name, category.color, attributesPerCategory);
            for (var y = 0; y < category.children.length; y++) {
                totalAttributes.push(category.children[y]);
            }
        }
        
        totalAttributes.Predictors = totalAttributes.sort(this.SortByPredictivePower);
        
        for (var z = 0; z < totalAttributes.length; z++) {
            var attribute = totalAttributes[z];
            if (numLargeCategories > 0) {
                attribute.size = 6.55;
                numLargeCategories--;
            } else if (numMediumCategories > 0) {
                attribute.size = 2.56;
                numMediumCategories--;
            } else {
                attribute.size = 1;
            }
            
        }
        
        var toReturn = {
            name: "root",
            size : 1,
            color: "#FFFFFF",
            attributesPerCategory: attributesPerCategory,
            children: topCategories
        };
        
        return toReturn;
    };
});