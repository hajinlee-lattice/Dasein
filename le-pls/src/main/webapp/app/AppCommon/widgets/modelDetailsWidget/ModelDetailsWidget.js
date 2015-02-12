angular.module('mainApp.appCommon.widgets.ModelDetailsWidget', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.DateTimeFormatUtility'
])

.controller('ModelDetailsWidgetController', function ($scope, ResourceUtility, DateTimeFormatUtility) {
    $scope.ResourceUtility = ResourceUtility;
    
    var widgetConfig = $scope.widgetConfig;
    var metadata = $scope.metadata;
    var data = $scope.data;

    //TODO:pierce Field names subject to change
    $scope.displayName = data[widgetConfig.NameProperty];
    $scope.status = data[widgetConfig.StatusProperty];
    $scope.score = data[widgetConfig.ScoreProperty];
    if ($scope.score != null && $scope.score < 1) {
        $scope.score = Math.round($scope.score * 100);
    }
    $scope.externalAttributes = data[widgetConfig.ExternalAttributesProperty];
    $scope.internalAttributes = data[widgetConfig.InternalAttributesProperty];
    $scope.createdDate = data[widgetConfig.CreatedDateProperty];
    
    $scope.totalLeads = data[widgetConfig.TotalLeadsProperty];
    $scope.testSet = data[widgetConfig.TestSetProperty];
    $scope.trainingSet = data[widgetConfig.TrainingSetProperty];
    $scope.totalSuccessEvents = data[widgetConfig.TotalSuccessEventsProperty];
    $scope.conversionRate = data[widgetConfig.ConversionRateProperty];
    if ($scope.conversionRate != null && $scope.conversionRate < 1) {
        $scope.conversionRate = $scope.conversionRate * 100;
    }
    $scope.leadSource = data[widgetConfig.LeadSourceProperty];
    $scope.opportunity = data[widgetConfig.OpportunityProperty];
    
    $("#more-datapoints").hide();

    $("#link-showmore a").click(function(){
        $("#more-datapoints").slideToggle('400');
        return false;
    });
    
})
.directive('modelDetailsWidget', function ($compile) {
    var directiveDefinitionObject = {
        templateUrl: 'app/AppCommon/widgets/modelDetailsWidget/ModelDetailsWidgetTemplate.html'
    };
  
    return directiveDefinitionObject;
});