angular.module('mainApp.appCommon.widgets.ModelDetailsWidget', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.DateTimeFormatUtility',
    'mainApp.core.utilities.GriotNavUtility',
    'mainApp.appCommon.utilities.StringUtility',
    'mainApp.appCommon.services.TopPredictorService'
])

.controller('ModelDetailsWidgetController', function ($scope, $rootScope, ResourceUtility, DateTimeFormatUtility, GriotNavUtility, StringUtility, TopPredictorService) {
    $scope.ResourceUtility = ResourceUtility;
    
    var widgetConfig = $scope.widgetConfig;
    var metadata = $scope.metadata;
    var data = $scope.data;
    var modelDetails = $scope.data.ModelDetails;

    $scope.displayName = modelDetails[widgetConfig.NameProperty];
    var isActive = modelDetails[widgetConfig.StatusProperty];
    
    if (isActive) {
        $scope.status = ResourceUtility.getString("MODEL_DETAILS_ACTIVE_LABEL");
    } else {
        $scope.status = ResourceUtility.getString("MODEL_DETAILS_INACTIVE_LABEL");
    }
    
    $scope.score = modelDetails[widgetConfig.ScoreProperty];
    if ($scope.score != null && $scope.score < 1) {
        $scope.score = Math.round($scope.score * 100);
    }
    
    //Need to calculate this
    $scope.externalAttributes = TopPredictorService.GetNumberOfAttributesByType(data.Predictors, "External");
    //Need to calculate this
    $scope.internalAttributes = TopPredictorService.GetNumberOfAttributesByType(data.Predictors, "Internal");
    
    $scope.createdDate = modelDetails[widgetConfig.CreatedDateProperty];
    $scope.createdDate = $scope.createdDate * 1000;
    $scope.createdDate = new Date($scope.createdDate).toLocaleDateString();
    
    $scope.totalLeads = modelDetails[widgetConfig.TotalLeadsProperty];
    $scope.totalLeads = StringUtility.AddCommas($scope.totalLeads);
    
    $scope.testSet = modelDetails[widgetConfig.TestSetProperty];
    $scope.testSet = StringUtility.AddCommas($scope.testSet);
    
    $scope.trainingSet = modelDetails[widgetConfig.TrainingSetProperty];
    $scope.trainingSet = StringUtility.AddCommas($scope.trainingSet);

    $scope.totalSuccessEvents = modelDetails[widgetConfig.TotalSuccessEventsProperty];
    $scope.totalSuccessEvents = StringUtility.AddCommas($scope.totalSuccessEvents);
    
    $scope.conversionRate = modelDetails[widgetConfig.TotalSuccessEventsProperty] / (modelDetails[widgetConfig.TestSetProperty] + modelDetails[widgetConfig.TrainingSetProperty]);
    if ($scope.conversionRate != null && $scope.conversionRate < 1) {
        $scope.conversionRate = $scope.conversionRate * 100;
        $scope.conversionRate = $scope.conversionRate.toFixed(2);
    }
    $scope.leadSource = modelDetails[widgetConfig.LeadSourceProperty];
    $scope.opportunity = modelDetails[widgetConfig.OpportunityProperty];
    
    $scope.dataExpanded = false;
    $scope.showMoreLabel = ResourceUtility.getString('MODEL_DETAILS_SHOW_MORE_BUTTON');
    $("#moreDataPoints").hide();
    
    $scope.backButtonClick = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }
        
        $rootScope.$broadcast(GriotNavUtility.MODEL_LIST_NAV_EVENT);
    };
    
    $scope.showMoreClicked = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }
        $scope.dataExpanded = !$scope.dataExpanded;
        if ($scope.dataExpanded) {
            $scope.showMoreLabel = ResourceUtility.getString('MODEL_DETAILS_SHOW_LESS_BUTTON');
        } else {
            $scope.showMoreLabel = ResourceUtility.getString('MODEL_DETAILS_SHOW_MORE_BUTTON');
        }
        
        $("#moreDataPoints").slideToggle('400');
    };
})
.directive('modelDetailsWidget', function ($compile) {
    var directiveDefinitionObject = {
        templateUrl: 'app/AppCommon/widgets/modelDetailsWidget/ModelDetailsWidgetTemplate.html'
    };
  
    return directiveDefinitionObject;
});