angular.module('mainApp.appCommon.widgets.SimpleTabWidget', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.services.WidgetFrameworkService'
])

.controller('SimpleTabWidgetController', function ($scope, $element, ResourceUtility, WidgetFrameworkService) {
    
    var widgetConfig = $scope.widgetConfig;
    var data = $scope.data;
    var parentData = $scope.parentData;
    
    if (widgetConfig == null || widgetConfig.length === 0) {
        return;
    }
    
    var defaultTab = widgetConfig.DefaultTab != null ? parseInt(widgetConfig.DefaultTab) : 1;
    
    $scope.tabs = [];
    var createTabs = function () {
        for (var i = 0; i < widgetConfig.Tabs.length; i++) {
            var tab = widgetConfig.Tabs[i];
            tab.IsActive = false;
            tab.Title = ResourceUtility.getString(tab.TitleString);
            $scope.tabs.push(tab);
        }
        
        if ($scope.tabs.length === 0) {
            return;
        }
        
        var foundActiveTab = false;
        for (var x = 0; x < $scope.tabs.length; x++) {
            var nextTab = $scope.tabs[x];
            if (defaultTab === x) {
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
        for (var i = 0; i < widgetConfig.Tabs.length; i++) {
            var tab = widgetConfig.Tabs[i];
            
            for (var x = 0; x < tab.Widgets.length; x++) {
                var tabWidgetConfig = tab.Widgets[x];
                var childElement = $('#' + tab.ID, $element);
                var container = $('<div></div>');
                childElement.append(container);
                
                if (childElement) {
                    WidgetFrameworkService.CreateWidget({
                        element: container,
                        widgetConfig: tabWidgetConfig,
                        data: data,
                        parentData: parentData
                    });
                }
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

.directive('simpleTabWidget', function () {
    var directiveDefinitionObject = {
        templateUrl: 'app/AppCommon/widgets/simpleTabWidget/SimpleTabWidgetTemplate.html'
    };
  
    return directiveDefinitionObject;
});