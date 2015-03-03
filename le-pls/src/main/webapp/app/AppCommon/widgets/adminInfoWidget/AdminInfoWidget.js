angular.module('mainApp.appCommon.widgets.AdminInfoWidget', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.DateTimeFormatUtility'
])
.controller('AdminInfoWidgetController', function ($scope, $rootScope, $http, ResourceUtility) {
    $scope.ResourceUtility = ResourceUtility;
    $scope.showDownloadError = false;

    var data = $scope.data;
    $scope.ModelId = $scope.data.ModelId;
    console.log(data);

    function parseROC(score) {
        return score.toFixed(4).toString() + " Excelent";
    }

    $scope.ModelHealthScore = parseROC(data.ModelDetails.RocScore);
    $scope.LookupId = data.ModelDetails.LookupID;

    $scope.modelSummaryJSONClick = function(){
        $http.get("/pls/datafiles/modeljson/" + $scope.ModelId)
            .error(function(){ $scope.showDownloadError = true; });
    };

    $scope.topPredictorCSVClick = function(){
        $http.get("/pls/datafiles/predictorcsv/" + $scope.ModelId)
            .error(function(){ $scope.showDownloadError = true; });
    };

    $scope.readoutCSVClick = function(){
        $http.get("/pls/datafiles/readoutcsv/" + $scope.ModelId)
            .error(function(){ $scope.showDownloadError = true; });
    };

    $scope.scoresCSVClick = function(){
        $http.get("/pls/datafiles/scorescsv/" + $scope.ModelId)
            .error(function(){ $scope.showDownloadError = true; });
    };

    $scope.thresholdExplorerCSVClick = function(){
        $http.get("/pls/datafiles/thresholdExplorercsv/" + $scope.ModelId)
            .error(function(){ $scope.showDownloadError = true; });
    };

    $scope.RFModelCSVClick = function(){
        $http.get("/pls/datafiles/rfmodelcsv/" + $scope.ModelId)
            .error(function(){ $scope.showDownloadError = true; });
    };

})
.directive('adminInfoWidget', function ($compile) {
    var directiveDefinitionObject = {
        templateUrl: 'app/AppCommon/widgets/adminInfoWidget/AdminInfoWidgetTemplate.html'
    };
  
    return directiveDefinitionObject;
});