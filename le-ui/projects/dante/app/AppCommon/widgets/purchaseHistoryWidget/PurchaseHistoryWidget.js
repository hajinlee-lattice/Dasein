angular.module('mainApp.appCommon.widgets.PurchaseHistoryWidget', [
    'mainApp.appCommon.utilities.PurchaseHistoryUtility',
    'mainApp.appCommon.widgets.PurchaseHistoryWidget.controllers.PurchaseHistoryNav',
    'mainApp.appCommon.widgets.PurchaseHistoryWidget.controllers.PurchaseHistorySpendTable',
    'mainApp.appCommon.widgets.PurchaseHistoryWidget.controllers.PurchaseHistorySegmentTable',
    'mainApp.appCommon.widgets.PurchaseHistoryWidget.controllers.PurchaseHistoryYearTable',
    'mainApp.appCommon.widgets.PurchaseHistoryWidget.stores.PurchaseHistoryStore'
])
.filter('startFrom', function () {
    return function (input, start) {
        if (input) {
            start = +start;
            return input.slice(start);
        }
        return [];
    };
})
.filter('range', function () {
    return function (n) {
      var range = [];
      for (var i = 0; i < n; i++) {
        range.push(i);
      }
      return range;
    };
})
.filter('formatDollar', function (PurchaseHistoryUtility) {
    return function (n, abbreviate) {
        return (n === null) ? '' : PurchaseHistoryUtility.formatDollar(n, abbreviate);
    };
})
.filter('sortWithNulls', function ($filter, $parse) {
    var lastIndexOfNull = function (arr, cb) {
        var index = 0;
        for (var i = 0; i < arr.length; i++) {
            if (cb(arr[i]) !== null) {
                index = i;
                break;
            }
        }
        return index;
    };

    var rotateNullToEnd = function (arr, pivot) {
        var left = arr.slice(0, pivot);
        var right = arr.slice(pivot);
        return right.concat(left);
    };

    return function (arr, predicate, reverse) {
        var sorted = $filter('orderBy')(arr, predicate, reverse);
        if (!Array.isArray(sorted)) {
            return sorted;
        }
        if (!reverse) {
            return sorted;
        }

        var pivot;
        if (typeof predicate === 'string') {
            var get = $parse(predicate);
            if (get.constant) {
                var key = get();

                pivot = lastIndexOfNull(sorted, function (item) {
                    return item[key];
                });
                return rotateNullToEnd(sorted, pivot);
            } else {
                pivot = lastIndexOfNull(sorted, function (item) {
                    return get(item);
                });
                return rotateNullToEnd(sorted, pivot);
            }
        }

        pivot = sorted.lastIndexOf(null) + 1;
        return rotateNullToEnd(sorted, pivot);
    };
})
.controller('PurchaseHistoryWidgetController', function ($scope, $element, $compile, $rootScope, $q, $http, PurchaseHistoryUtility, $timeout, PurchaseHistoryStore) {
    var widgetConfig = $scope.widgetConfig;
    var metadata = $scope.metadata;
    
    var vm = this;
    vm.defaultView = 'spend-view';

    vm.renderTable = function (view) {
        var template;
        switch (view) {
            case 'spend-view':
                template = 'app/AppCommon/widgets/purchaseHistoryWidget/templates/purchaseHistorySpendTableTemplate.html';
                break;
            case 'yearly-view':
                template = 'app/AppCommon/widgets/purchaseHistoryWidget/templates/purchaseHistoryYearTableTemplate.html';
                break;
            case 'segment-view':
                template = 'app/AppCommon/widgets/purchaseHistoryWidget/templates/purchaseHistorySegmentTableTemplate.html';
                break;
        }

        var scope = $rootScope.$new();

        return $http.get(template).success(function (html) {
            $compile($("#purchaseHistoryMain").html(html))($scope);
        });
    };

    $scope.$on('purchaseHistoryViewChange', function(event, payload){
        vm.renderTable(payload.view);
    });

    // 'view resolve'
    PurchaseHistoryStore.initPurchaseHistory($scope.data.SalesforceAccountID, metadata)
        .then(function () {
            $http.get("app/AppCommon/widgets/purchaseHistoryWidget/templates/purchaseHistoryNavTemplate.html").success(function (html) {
                
                $('.initial-loading-spinner-container').remove();

                var scope = $rootScope.$new();
                scope.view = vm.defaultView;

                $compile($('#purchaseHistoryHeader').html(html))(scope);

                vm.renderTable(scope.view);
            });
        })
        .catch(function (errors) {
            $http.get("app/core/views/NoAssociationView.html").success(function (html) {
                var scope = $rootScope.$new();

                var notions = Object.keys(errors).join(', ');
                scope.message1 = 'No ' + notions + ' found.';
                scope.message1 = scope.message1.replace(/\,\s([^\,]+)$/, ' and $1');

                $element.replaceWith($compile(html)(scope));
            });
        });
})
.directive('purchaseHistoryWidget', function ($compile) {
    var directiveDefinitionObject = {
        templateUrl: 'app/AppCommon/widgets/purchaseHistoryWidget/PurchaseHistoryWidgetTemplate.html',
        link: function (scope, element, attr) {
            // purchase history table not  responsive, must allow scroll
            // IE can't use className, use setAttribute instead
            var widget = $('#purchaseHistoryScreenWidget')[0];
            widget.setAttribute('class', widget.className + ' ph-overflow-x');
        }
    };

    return directiveDefinitionObject;
});
