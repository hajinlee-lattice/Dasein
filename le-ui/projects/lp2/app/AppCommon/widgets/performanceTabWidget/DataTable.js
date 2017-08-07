angular.module('mainApp.appCommon.widgets.performanceTab.DataTable', [
    'mainApp.appCommon.utilities.ResourceUtility'
])
.directive('dataTable', function () {
    return {
        templateUrl: 'app/AppCommon/widgets/performanceTabWidget/DataTableTemplate.html',
        scope: {data: "=", columns : "=", title: "@"},
        controller: function ($scope, ResourceUtility) {
        }
    };
});