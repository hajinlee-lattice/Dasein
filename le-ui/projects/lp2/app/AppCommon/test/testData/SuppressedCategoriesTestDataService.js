angular.module('test.testData.SuppressedCategoriesTestDataService', [
    'mainApp.appCommon.utilities.MetadataUtility'])
.service('SuppressedCategoriesTestDataService', function() {

    this.GetTwoSampleSuppressedCategories = function () {

        var suppressedCategories = [];
        var category1 = {
                name: "category1",
                categoryName: "category1",
                UncertaintyCoefficient: 0.1,
                size: 1,
                color: null,
                children: []
            };
        var category2 = {
                name: "category2",
                categoryName: "category2",
                UncertaintyCoefficient: 0.1,
                size: 1,
                color: null,
                children: []
            };
        suppressedCategories.push(category1);
        suppressedCategories.push(category2);

        return suppressedCategories;
    }; //end GetTwoSampleSuppressedCategories()
})