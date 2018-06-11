angular.module('mainApp.appCommon.widgets.PurchaseHistoryWidget.controllers.PurchaseHistorySpendTable', [
    'mainApp.appCommon.utilities.PurchaseHistoryUtility',
    'mainApp.appCommon.services.PurchaseHistoryService',
    'mainApp.appCommon.widgets.PurchaseHistoryWidget.stores.PurchaseHistoryStore',
    'mainApp.appCommon.widgets.PurchaseHistoryWidget.directives.PurchaseHistoryTimeline',
    'mainApp.appCommon.services.PurchaseHistoryTooltip'
  ])
  .controller('PurchaseHistorySpendTableCtrl', function ($scope, $element, $rootScope, $compile, PurchaseHistoryService, PurchaseHistoryUtility, PurchaseHistoryStore, PurchaseHistoryTooltip) {
    var vm = this;
    vm.defaultColsize = 12;

    vm.toggleNode = function (product) {
      product.toggleExpand();
    };

    vm.expandAll = function () {
      $scope.tree.expandAll();
    };

    vm.collapseAll = function () {
      $scope.tree.collapseAll();
    };

    var tooltipEl = angular.element('#ph-tooltip');
    vm.showTooltip = function ($event, data) {
      $event.stopPropagation();

      PurchaseHistoryTooltip.show(tooltipEl, $event, data);
    };

    vm.hideTooltip = function () {
      PurchaseHistoryTooltip.hide(tooltipEl);
    };

    vm.searchProduct = function (item, index) {
      return item.searchTreeDescendants($scope.searchProduct);
    };

    vm.update = function () {
      var period = PurchaseHistoryStore.period;
      var periodRange = PurchaseHistoryStore.periodRange;

      $scope.formattedPeriodRange = periodRange.map(function (periodId) {
        return PurchaseHistoryUtility.formatDisplayPeriodId(period, periodId);
      });
      $scope.tree = PurchaseHistoryStore.productTreeSelectedPeriods;

      var periodRangeLen = periodRange.length;
      if (periodRangeLen < vm.defaultColsize) {
        $scope.colsize = periodRangeLen;
        $scope.periodStartIndex = 0;
      } else {
        $scope.colsize = vm.defaultColsize;
        $scope.periodStartIndex = periodRangeLen - $scope.colsize;
      }

      $scope.$broadcast('purchaseHistorySpendTableChange');
    };

    $scope.sortKey = 'data.accountProductTotalSpend';
    $scope.sortReverse = true;
    $scope.sortBy = function (key) {
      if ($scope.sortKey === key) {
        $scope.sortReverse = !$scope.sortReverse;
      } else {
        $scope.sortKey = key;
      }
    };

    $scope.$on('purchaseHistoryStoreChange', function (event, payload) {
      vm.update();
    });

    $scope.$on('purchaseHistoryFilterChange', function (event, payload) {
      vm.spendFilter = payload;
    });

    vm.update();
  });
