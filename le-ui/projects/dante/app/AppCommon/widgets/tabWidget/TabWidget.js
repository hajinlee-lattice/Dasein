angular.module('mainApp.appCommon.widgets.TabWidget', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.MetadataUtility',
    'mainApp.appCommon.services.WidgetFrameworkService',
    'common.utilities.browserstorage'
])

.controller('TabWidgetController', function ($scope, $element, ResourceUtility, MetadataUtility, WidgetFrameworkService, BrowserStorageUtility) {
    var widgetConfig = $scope.widgetConfig;
    var metadata = $scope.metadata;
    var data = $scope.data;
    var parentData = $scope.parentData;

    if (widgetConfig == null || widgetConfig.length === 0) {
        return;
    }

    var defaultTab = widgetConfig.DefaultTab != null ? parseInt(widgetConfig.DefaultTab) : 1;
    
    var checkTabWidgetData = function (widgetConfig) {
        var customSettings = BrowserStorageUtility.getCrmCustomSettings();
        if (widgetConfig && widgetConfig.ID === 'purchaseHistoryScreenWidget' &&
            customSettings && customSettings.ShowPurchaseHistory === true) {
            return true;
        }

        if (widgetConfig == null || widgetConfig.Notion == null || widgetConfig.TargetNotion == null) {
            return false;
        }

        var targetNotionProperty = MetadataUtility.GetNotionAssociationMetadata(widgetConfig.Notion, widgetConfig.TargetNotion, metadata);
        if (targetNotionProperty == null) {
            return false;
        }
        
        var tabData = data[targetNotionProperty.Name];
        if (tabData != null && tabData.length > 0) {
            return true;
        } else {
            return false;
        }
    };
    
    $scope.tabs = [];
    var createTabs = function () {
        for (var i = 0; i < widgetConfig.Widgets.length; i++) {
            var widget = widgetConfig.Widgets[i];
            var hasData = checkTabWidgetData(widget);
            if (!hasData) {
                if (defaultTab > i) {
                    defaultTab--;
                }
                continue;
            }
            var tab = {
                ID: widget.ID,
                IsActive: false,
                Title: ResourceUtility.getString(widget.TitleString),
                HasData: hasData,
                NoDataString: ResourceUtility.getString(widget.NoDataString)
            };
            $scope.tabs.push(tab);
        }

        if ($scope.tabs.length === 0) {
            return;
        }

        var foundActiveTab = false;
        for (var x = 0; x < $scope.tabs.length; x++) {
            var nextTab = $scope.tabs[x];
            if (nextTab.HasData && defaultTab === x) {
                foundActiveTab = true;
                nextTab.IsActive = true;
            }
        }
        if (!foundActiveTab) {
            $scope.tabs[0].IsActive = true;
        }
    };
    createTabs();

    //TODO:pierce There has to be a better to handle this
    setTimeout(function () {
        for (var i = 0; i < widgetConfig.Widgets.length; i++) {
            var tabWidgetConfig = widgetConfig.Widgets[i];
            var tabHasData = checkTabWidgetData(tabWidgetConfig);
            
            var childElement = $('#' + tabWidgetConfig.ID, $element);
            var container = $('<div></div>');
            childElement.append(container);
            
            if (childElement && tabHasData) {
                WidgetFrameworkService.CreateWidget({
                    element: container,
                    widgetConfig: tabWidgetConfig,
                    metadata: metadata,
                    data: data,
                    parentData: parentData
                });
            }
        }
    }, 0);
    
    $scope.tabClicked = function ($event, tab) {
        if ($event != null) {
            $event.preventDefault();
        }
        
        for (var i = 0; i < $scope.tabs.length; i++) {
            $scope.tabs[i].IsActive = false;
        }
        tab.IsActive = true;
    };
})

.directive('tabWidget', function () {
    var directiveDefinitionObject = {
        templateUrl: 'app/AppCommon/widgets/tabWidget/TabWidgetTemplate.html'
    };
  
    return directiveDefinitionObject;
});