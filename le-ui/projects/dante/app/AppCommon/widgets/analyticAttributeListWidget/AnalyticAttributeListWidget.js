angular.module('mainApp.appCommon.widgets.AnalyticAttributeListWidget', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.MetadataUtility',
    'mainApp.appCommon.utilities.AnalyticAttributeUtility',
    'mainApp.appCommon.services.WidgetFrameworkService'
])

.service('AnalyticAttributeListService', function (AnalyticAttributeUtility) {

    this.GroomAttributeList = function (rawAttributeList, score, modelId, widgetConfig, metadata) {
        if (rawAttributeList == null || rawAttributeList.length === 0 || widgetConfig == null) {
            return rawAttributeList;
        }

        var modelSummary = this.GetStoredNotion(widgetConfig.ModelNotion, modelId);
        if (modelSummary == null) {
            return null;
        }

        var leadCount = AnalyticAttributeUtility.GetLeadCount(modelSummary);
        var attributeLimit = parseInt(widgetConfig.Limit);
        var groomedPositiveAttributeList = [];
        var groomedNegativeAttributeList = [];
        for (var i = 0; i < rawAttributeList.length; i++) {
            var rawAttribute = rawAttributeList[i];
            var attributeMetadata = AnalyticAttributeUtility.FindAttributeMetadataData(modelSummary, rawAttribute);
            var attributeBucket = AnalyticAttributeUtility.FindAttributeBucket(modelSummary, rawAttribute);
            if (attributeMetadata != null && attributeBucket != null && attributeBucket.IsVisible === true &&
                AnalyticAttributeUtility.IsAllowedForInsights(attributeMetadata) &&
                AnalyticAttributeUtility.ShowBasedOnTags(widgetConfig, attributeMetadata) &&
                AnalyticAttributeUtility.ShouldShowNullBucket(attributeBucket, widgetConfig.NullThreshold)) {

                var frequency = attributeBucket.Count / leadCount;
                if (frequency < 0.01)
                    continue;
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
                if (attributeLift > 1) {
                    groomedPositiveAttributeList.push(attribute);
                } else if (attributeLift !== 1) {
                    groomedNegativeAttributeList.push(attribute);
                }
            }

        }

        // Sort by Lift
        groomedPositiveAttributeList = AnalyticAttributeUtility.SortAttributeList(groomedPositiveAttributeList);
        groomedNegativeAttributeList = AnalyticAttributeUtility.SortAttributeList(groomedNegativeAttributeList);
        return AnalyticAttributeUtility.GetAttributeList(groomedPositiveAttributeList, groomedNegativeAttributeList, attributeLimit, score);
    };

    this.GetStoredNotion = function (notionName, notionId) {
        var notion = null;
        var localData = $.jStorage.get(notionName);
        if (localData != null && localData.listData != null) {
            for (var i = 0; i < localData.listData.length; i++) {
                var notionData = localData.listData[i];
                if (notionData.Id == notionId) {
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

    var notionMetadata = MetadataUtility.GetNotionMetadata(widgetConfig.Notion, metadata);
    var valueProperty = MetadataUtility.GetNotionProperty(widgetConfig.ValueProperty, notionMetadata);
    var score = null;
    if (valueProperty != null) {
        score = data[valueProperty.Name] || null;
        if (score != null) {
            score = Math.round(score);
        }
    }

    var groomedAttributeList = AnalyticAttributeListService.GroomAttributeList(
        data[targetNotionProperty.Name],
        score,
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

    $scope.learnMoreTooltip = ResourceUtility.getString("ANALYTIC_ATTRIBUTE_INTERNAL_LEARN_MORE");
    if (widgetConfig.RequiredTags != null && widgetConfig.RequiredTags.length !== 0 && widgetConfig.RequiredTags[0] === "External") {
        $scope.learnMoreTooltip = ResourceUtility.getString("ANALYTIC_ATTRIBUTE_EXTERNAL_LEARN_MORE");
    }

    $(".tooltip").tooltip({
        position: { my: "left+15 center", at: "right center" }
    });

    $scope.learnMoreClicked = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }
    };

    if ($scope.hasAttributes) {
        $scope.createAttributeList();
    }
})

.directive('analyticAttributeListWidget', function () {
    var directiveDefinitionObject = {
        templateUrl: 'app/AppCommon/widgets/analyticAttributeListWidget/AnalyticAttributeListWidgetTemplate.html'
    };

    return directiveDefinitionObject;
});