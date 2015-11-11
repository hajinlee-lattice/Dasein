angular.module('mainApp.appCommon.widgets.AnalyticAttributeListWidget', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.MetadataUtility',
    'mainApp.appCommon.utilities.AnalyticAttributeUtility',
    'mainApp.appCommon.services.WidgetFrameworkService'
])

.service('AnalyticAttributeListService', function (AnalyticAttributeUtility) {
    
    this.GroomAttributeList = function (rawAttributeList, modelId, widgetConfig, metadata) {
        if (rawAttributeList == null || rawAttributeList.length === 0 || widgetConfig == null) {
            return rawAttributeList;
        }
        
        var modelSummary = this.GetStoredNotion(widgetConfig.ModelNotion, modelId);
        if (modelSummary == null) {
            return null;
        }
        var attributeLimit = parseInt(widgetConfig.Limit);
        var approvedUsages = [
            AnalyticAttributeUtility.ApprovedUsage.IndividualDisplay,
            AnalyticAttributeUtility.ApprovedUsage.ModelAndAllInsights,
            AnalyticAttributeUtility.ApprovedUsage.ModelAndModelInsights
        ];
        
        var groomedAttributeList = [];
        for (var i = 0; i < rawAttributeList.length; i++) {
            var rawAttribute = rawAttributeList[i];
            var attributeMetadata = AnalyticAttributeUtility.FindAttributeMetadataData(modelSummary, rawAttribute);
            var attributeBucket = AnalyticAttributeUtility.FindAttributeBucket(modelSummary, rawAttribute);
            
            if (attributeMetadata != null && attributeBucket != null && attributeBucket.IsVisible === true &&
                AnalyticAttributeUtility.IsAllowedForInsights(attributeMetadata, approvedUsages) && 
                AnalyticAttributeUtility.ShowBasedOnTags(widgetConfig, attributeMetadata) &&
                AnalyticAttributeUtility.ShouldShowNullBucket(attributeBucket, widgetConfig.NullThreshold)) {
                    
                var showLift = widgetConfig.ShowLift == "true" ? true : false;
                var bucketName = AnalyticAttributeUtility.GetAttributeBucketName(attributeBucket, attributeMetadata);
                var attributeLift = AnalyticAttributeUtility.FormatLift(attributeBucket.Lift);
                var attribute = {
                    DisplayName: attributeMetadata.DisplayName,
                    Description: attributeMetadata.Description,
                    BucketName: bucketName,
                    Lift: attributeLift,
                    ShowLift: showLift
                };
                
                groomedAttributeList.push(attribute);
            }
            
        }
        
        // Sort by Lift
        groomedAttributeList = AnalyticAttributeUtility.SortAttributeList(groomedAttributeList);
        if (groomedAttributeList.length > attributeLimit) {
            groomedAttributeList = groomedAttributeList.slice(0, attributeLimit);
        }
        
        return groomedAttributeList;
    };
    
    this.GetStoredNotion = function (notionName, notionId) {
        var notion = null;
        var localData = $.jStorage.get(notionName);
        if (localData != null && localData.listData != null) {
            for (var i = 0; i < localData.listData.length; i++) {
                var notionData = localData.listData[i];
                if (notionData.Id == notionId ) {
                    notion = notionData.Data;
                    break;
                }
            }
        }
        return notion;
    };
})

.controller('AnalyticAttributeListWidgetController', function ($scope, ResourceUtility, MetadataUtility, AnalyticAttributeUtility, WidgetFrameworkService, AnalyticAttributeListService) {
    var widgetConfig = $scope.widgetConfig;
    var metadata = $scope.metadata;
    var data = $scope.data;
    
    $scope.ResourceUtility = ResourceUtility;
    $scope.liftDescending = true;
    $scope.hasAttributes = false;
    
    var showLift = widgetConfig != null && widgetConfig.ShowLift == "true" ? true : false;
    if (showLift) {
        $scope.liftLabel = ResourceUtility.getString('ANALYTIC_ATTRIBUTE_LIFT_HEADER_LABEL');
    } else {
        $scope.liftLabel = ResourceUtility.getString('ANALYTIC_ATTRIBUTE_PROBABILITY_HEADER_LABEL');
    }
    
    var targetNotionProperty = MetadataUtility.GetNotionAssociationMetadata(widgetConfig.Notion, widgetConfig.TargetNotion, metadata);
    if (targetNotionProperty == null) {
        return;
    }
    
    var groomedAttributeList = AnalyticAttributeListService.GroomAttributeList(
        data[targetNotionProperty.Name], 
        data[widgetConfig.ModelIdProperty], 
        widgetConfig, 
        metadata
    );
    
    $scope.hasAttributes = groomedAttributeList != null && groomedAttributeList.length > 0;
    
    $scope.liftSortClicked = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }
        
        $scope.liftDescending = !$scope.liftDescending;
        groomedAttributeList = AnalyticAttributeUtility.SortAttributeList(groomedAttributeList, $scope.liftDescending);
        $scope.createAttributeList();
    };
    
    $scope.createAttributeList = function () {
        var listContainer = $('.js-analytic-attribute-list', $scope.element);
        listContainer.empty();
        for (var i = 0; i < groomedAttributeList.length; i++) {
            var container = $('<div></div>');
            listContainer.append(container);
            
            var options = {
                element: container,
                widgetConfig: widgetConfig,
                metadata: metadata,
                data: data
            };
            WidgetFrameworkService.CreateChildWidgets(options, groomedAttributeList[i]);
        }
    };
    
    if ($scope.hasAttributes) {
        $scope.createAttributeList();
    }
})

.directive('analyticAttributeListWidget', function () {
    var directiveDefinitionObject = {
        templateUrl: '/app/widgets/analyticAttributeListWidget/AnalyticAttributeListWidgetTemplate.html'
    };
  
    return directiveDefinitionObject;
});