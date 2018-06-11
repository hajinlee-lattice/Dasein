angular.module('mainApp.appCommon.widgets.PurchaseHistoryWidget.controllers.PurchaseHistoryYearTable', [
    'mainApp.appCommon.widgets.PurchaseHistoryWidget.stores.PurchaseHistoryStore',
    'mainApp.appCommon.utilities.PurchaseHistoryUtility'
  ])
  .controller('PurchaseHistoryYearTableCtrl', function ($scope, PurchaseHistoryStore, PurchaseHistoryUtility) {
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
      var LIMIT = 90; // limit bar width to 90% to leave room for text

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
      var maxSpendYoy = $scope.tree.data.maxSpendYoy;

      $scope.tree.depthFirstTraversal(function (node) {
        var currentYearBarWidth = vm.calculateBarWidthPercentage(node.data.accountProductTotalSpend, maxSpendYoy);
        var prevYearBarWidth = vm.calculateBarWidthPercentage(node.data.accountProductTotalPrevYearSpend, maxSpendYoy);

        node.data.currentYearBarStyle = {
          width: currentYearBarWidth
        };

        node.data.prevYearBarStyle = {
          width: prevYearBarWidth
        };
      });
    };
    
    $scope.sortKey = 'data.yoyDiff';
    $scope.sortReverse = false;
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
