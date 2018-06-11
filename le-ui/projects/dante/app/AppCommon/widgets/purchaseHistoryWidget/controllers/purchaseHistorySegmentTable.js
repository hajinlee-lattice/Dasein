angular.module('mainApp.appCommon.widgets.PurchaseHistoryWidget.controllers.PurchaseHistorySegmentTable', [
    'mainApp.appCommon.widgets.PurchaseHistoryWidget.stores.PurchaseHistoryStore',
    'mainApp.appCommon.utilities.PurchaseHistoryUtility',
    'mainApp.appCommon.services.PurchaseHistoryService'
  ])
  .controller('PurchaseHistorySegmentTableCtrl', function ($scope, PurchaseHistoryStore, PurchaseHistoryUtility) {
    var vm = this;

    vm.toggleNode = function (product) {
      product.toggleExpand();
    };

    vm.expandAll = function () {
      $scope.tree.expandAll();
    };

    vm.collapseAll = function () {
      $scope.tree.collapseAll();
    };

    vm.calculateBarWidthPercentage = function (value, max) {
      var LIMIT = 70; // limit bar width to 70% to leave room for text

      var percentage;
      if (value > max) {
        percentage = LIMIT;
      } else {
        percentage = (value/max * LIMIT) || 0;
      }

      return PurchaseHistoryUtility.formatPercent(percentage);
    };

    vm.searchProduct = function (item, index) { 
      return item.searchTreeDescendants($scope.searchProduct);
    };

    vm.update = function () {
      $scope.tree = PurchaseHistoryStore.productTreeSelectedPeriods;
      maxSpendSegmentDiff = $scope.tree.data.maxSpendSegmentDiff;

      $scope.tree.depthFirstTraversal(function (node) {
        var segmentBarWidth = vm.calculateBarWidthPercentage(Math.abs(node.data.segmentDiff), maxSpendSegmentDiff);

        node.data.segmentBarStyle = {
          width: segmentBarWidth
        };
      });
    };
    
    $scope.sortKey = 'data.segmentProductTotalAverageSpend';
    $scope.sortReverse = true;
    $scope.sortBy = function (key) {
      if ($scope.sortKey === key) {
        $scope.sortReverse = !$scope.sortReverse;
      } else {
        $scope.sortKey = key;
      }
    };

    $scope.$on('purchaseHistoryStoreChange', function () {
      vm.update();
    });

    $scope.$on('purchaseHistoryFilterChange', function (event, payload) {
      vm.spendFilter = payload;
    });

    vm.update();
  });
