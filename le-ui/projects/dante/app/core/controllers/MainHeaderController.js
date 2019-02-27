angular.module('mainApp.core.controllers.MainHeaderController', [
    'mainApp.appCommon.utilities.ResourceUtility',
    'mainApp.appCommon.utilities.URLUtility',
    'mainApp.appCommon.utilities.MetadataUtility',
    'common.utilities.browserstorage'
])

.controller('MainHeaderController', function ($scope, $rootScope, ResourceUtility, URLUtility, BrowserStorageUtility, MetadataUtility) {

    $scope.ResourceUtility = ResourceUtility;
    var rootNotion = BrowserStorageUtility.getCurrentRootObject();
    $scope.headerLabel = rootNotion.HeaderName;
    var hasSalesprism = URLUtility.HasSalesprism() === "true" ? true : false;
    var isAccountContext = BrowserStorageUtility.getRootApplication() == MetadataUtility.ApplicationContext.Account;
    $scope.showSellingIdeas = isAccountContext;
    $scope.showSalesprismButton = hasSalesprism && isAccountContext;

    $scope.handleViewInSalesprismClick = function () {
        var goToSalesprismAccountEvent = "GoToSalesprismAccountEvent=" + rootNotion.SalesforceAccountID;
        window.parent.postMessage(goToSalesprismAccountEvent, "*");
    };

    $scope.handlePlayListClick = function ($event) {
        if ($event != null) {
            $event.preventDefault();
        }
        var selectedCrmObject = BrowserStorageUtility.getSelectedCrmObject();
        if (BrowserStorageUtility.getRootApplication() == MetadataUtility.ApplicationContext.Lead) {
            return;
        } else if (BrowserStorageUtility.getRootApplication() == MetadataUtility.ApplicationContext.Account && selectedCrmObject && selectedCrmObject.ID != null) {
            return;
        }

        BrowserStorageUtility.setSelectedCrmObject(null);
        $rootScope.$broadcast("ShowPlayList");
    };
});