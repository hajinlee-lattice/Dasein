angular.module('mainApp.core.controllers.NoNotionController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'common.utilities.browserstorage',
    'mainApp.appCommon.utilities.MetadataUtility'
])
.controller('NoNotionController', function ($scope, ResourceUtility, BrowserStorageUtility, MetadataUtility) {
    
    var notionName = "";
    if (BrowserStorageUtility.getRootApplication() == MetadataUtility.ApplicationContext.Lead) {
        notionName = "lead";
    } else {
        notionName = "account";
    }
    
    $scope.title = ResourceUtility.getString('NO_NOTION_FOUND_TITLE', [notionName]);
    $scope.message1 = ResourceUtility.getString('NO_NOTION_FOUND_MESSAGE_1', [notionName]);
    $scope.message2 = ResourceUtility.getString('NO_NOTION_FOUND_MESSAGE_2', [notionName]);
});