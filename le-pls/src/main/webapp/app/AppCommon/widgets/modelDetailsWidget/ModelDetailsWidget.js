angular.module('mainApp.appCommon.widgets.ModelDetailsWidget', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.DateTimeFormatUtility',
    'mainApp.core.utilities.GriotNavUtility',
    'mainApp.appCommon.utilities.StringUtility'
])

.controller('ModelDetailsWidgetController', function ($scope, $rootScope, ResourceUtility, DateTimeFormatUtility, GriotNavUtility, StringUtility) {
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
    $scope.createdDate = $scope.createdDate * 1000;
    $scope.createdDate = new Date($scope.createdDate).toLocaleDateString();
    
    $scope.totalLeads = data[widgetConfig.TotalLeadsProperty];
    $scope.totalLeads = StringUtility.AddCommas($scope.totalLeads);
    
    $scope.testSet = data[widgetConfig.TestSetProperty];
    $scope.testSet = StringUtility.AddCommas($scope.testSet);
    
    $scope.trainingSet = data[widgetConfig.TrainingSetProperty];
    $scope.trainingSet = StringUtility.AddCommas($scope.trainingSet);

    $scope.totalSuccessEvents = data[widgetConfig.TotalSuccessEventsProperty];
    $scope.totalSuccessEvents = StringUtility.AddCommas($scope.totalSuccessEvents);
    
    $scope.conversionRate = data[widgetConfig.TotalSuccessEventsProperty] / (data[widgetConfig.TestSetProperty] + data[widgetConfig.TrainingSetProperty]);
    if ($scope.conversionRate != null && $scope.conversionRate < 1) {
        $scope.conversionRate = $scope.conversionRate * 100;
        $scope.conversionRate = $scope.conversionRate.toFixed(2);
    }
    $scope.leadSource = data[widgetConfig.LeadSourceProperty];
    $scope.opportunity = data[widgetConfig.OpportunityProperty];
    
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