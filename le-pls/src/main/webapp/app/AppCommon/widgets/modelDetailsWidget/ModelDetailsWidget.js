angular.module('mainApp.appCommon.widgets.ModelDetailsWidget', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.DateTimeFormatUtility'
])

.controller('ModelDetailsWidgetController', function ($scope, ResourceUtility, DateTimeFormatUtility) {
    $scope.ResourceUtility = ResourceUtility;
    
    var widgetConfig = $scope.widgetConfig;
    var metadata = $scope.metadata;
    var data = $scope.data.ModelDetails;

    $scope.displayName = data[widgetConfig.NameProperty];
    var isActive = data[widgetConfig.StatusProperty];
    
    if (isActive) {
        $scope.status = ResourceUtility.getString("MODEL_DETAILS_ACTIVE_LABEL");
    } else {
        $scope.status = ResourceUtility.getString("MODEL_DETAILS_INACTIVE_LABEL");
    }
    
    $scope.score = data[widgetConfig.ScoreProperty];
    if ($scope.score != null && $scope.score < 1) {
        $scope.score = Math.round($scope.score * 100);
    }
    
    //Need to calculate this
    $scope.externalAttributes = data[widgetConfig.ExternalAttributesProperty];
    //Need to calculate this
    $scope.internalAttributes = data[widgetConfig.InternalAttributesProperty];
    
    $scope.createdDate = data[widgetConfig.CreatedDateProperty];
    $scope.createdDate = new Date($scope.createdDate).toLocaleDateString();
    
    $scope.totalLeads = data[widgetConfig.TotalLeadsProperty];
    $scope.testSet = data[widgetConfig.TestSetProperty];
    $scope.trainingSet = data[widgetConfig.TrainingSetProperty];
    $scope.totalSuccessEvents = data[widgetConfig.TotalSuccessEventsProperty];
    $scope.conversionRate = $scope.totalSuccessEvents / ($scope.testSet + $scope.trainingSet);
    if ($scope.conversionRate != null && $scope.conversionRate < 1) {
        $scope.conversionRate = $scope.conversionRate * 100;
        $scope.conversionRate = $scope.conversionRate.toFixed(2);
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