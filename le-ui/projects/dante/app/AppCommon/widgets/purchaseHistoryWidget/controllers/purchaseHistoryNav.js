angular.module('mainApp.appCommon.widgets.PurchaseHistoryWidget.controllers.PurchaseHistoryNav', [
    'mainApp.appCommon.widgets.PurchaseHistoryWidget.stores.PurchaseHistoryStore',
    'mainApp.appCommon.utilities.PurchaseHistoryUtility',
    'mainApp.appCommon.services.PurchaseHistoryService'
  ])
  .controller('PurchaseHistoryNavCtrl', function ($scope, $rootScope, PurchaseHistoryStore, PurchaseHistoryService, PurchaseHistoryUtility) {
      var vm = this;

      vm.handleViewChange = function (selectedView) {
        // IE doesn't handle pointer-event: none
        if (!selectedView.disabled && $scope.view !== selectedView.view) {
          $scope.view = selectedView.view;
          $scope.views.forEach(function (item) {
            item.selected = (item.view === selectedView.view);
          });

          // reset filter
          $scope.filterSelected = $scope.filterOptions[0];
          PurchaseHistoryStore.productTreeSelectedPeriods.collapseAll();

          $rootScope.$broadcast('purchaseHistoryViewChange', selectedView);
        }
      };

      vm.getHeaderContent = function (headerData) {
        var accountTotalSpend = headerData.accountProductTotalSpend,
            accountPrevYearSpend = headerData.accountProductTotalPrevYearSpend,
            segmentAvgSpend = headerData.segmentProductTotalAverageSpend,
            yearlyDiff = headerData.yoyDiff,
            segmentDiff = headerData.segmentDiff;

        var yearlyDiffPercentage = null;
        if (accountPrevYearSpend !== null) {
           yearlyDiffPercentage = Math.round((yearlyDiff / accountPrevYearSpend) * 100);
        }

        var totalSpend = '-';
        if (accountTotalSpend !== null) {
          totalSpend = PurchaseHistoryUtility.formatDollar(accountTotalSpend, false);
        }

        var yearlyCompareIcon = null;
        var yearlyCompare = '-';
        if (accountPrevYearSpend !== null || accountTotalSpend !== null) {
          yearlyCompare = (yearlyDiff >= 0) ? 'Up' : 'Down';
          yearlyCompareIcon = yearlyCompare;
          yearlyCompare += ' ';
          if (yearlyDiffPercentage !== null) {
            yearlyCompare += PurchaseHistoryUtility.formatPercent(Math.abs(yearlyDiffPercentage));
          }
          yearlyCompare += ' ';
          if (yearlyDiff < 0) {
            yearlyCompare += '(';
          }
          yearlyCompare += PurchaseHistoryUtility.formatDollar(Math.abs(yearlyDiff), false);
          if (yearlyDiff < 0) {
            yearlyCompare += ')';
          }
        }

        var segmentCompare = '-';
        if (segmentAvgSpend !== null) {
          segmentCompare = PurchaseHistoryUtility.formatDollar(Math.abs(segmentDiff), false);
          segmentCompare += ' ';
          segmentCompare += (segmentDiff >= 0) ? 'More' : 'Less';
          segmentCompare += ' Than Average';
        }

        var upOrDown = yearlyCompare.match(/(Down|Up)/);
        var arrow = '';
        if (upOrDown) {
          arrow = upOrDown[0].toLowerCase() + '-arrow-long';
        }

        return {
          totalSpend: {
            textHtml: totalSpend
          },
          yearlyCompare: {
            textHtml: yearlyCompare + '<span class="ph-header-icon '+ arrow + '"></span>'
          },
          segmentCompare: {
            textHtml: segmentCompare
          }
        };
      };

      vm.getPeriodIdOptions = function (period, startPeriodId, endPeriodId) {
        var periodRange = PurchaseHistoryUtility.getPeriodRange(period, startPeriodId, endPeriodId);

        return periodRange.map(function (periodId) {
          return {
            text: PurchaseHistoryUtility.formatDisplayPeriodId(period, periodId, true),
            value: periodId
          };
        });
      };

      vm.selectStartPeriodId = function (selected) {
        PurchaseHistoryStore.setPeriodIdRange(selected.value, endPeriodId);
      };

      vm.selectEndPeriodId = function (selected) {
        PurchaseHistoryStore.setPeriodIdRange(startPeriodId, selected.value);
      };

      vm.selectPeriod = function (selected) {
        // convert the max start and end period id between quarterly and monthly
        var prevMomentFormat = momentFormat;
        momentFormat = PurchaseHistoryUtility.periodToMomentFormat[selected.value];
        maxStartPeriodId = moment(PurchaseHistoryStore.periodStartDatePeriodId, prevMomentFormat).format(momentFormat);
        maxEndPeriodId = moment(PurchaseHistoryStore.periodEndDatePeriodId, prevMomentFormat).format(momentFormat);

        PurchaseHistoryStore.setPeriod(selected.value);
      };

      var period = PurchaseHistoryStore.period;
      var momentFormat = PurchaseHistoryUtility.periodToMomentFormat[period];
      var maxStartPeriodId = moment(PurchaseHistoryStore.periodStartDatePeriodId, momentFormat).format(momentFormat);
      var maxEndPeriodId = moment(PurchaseHistoryStore.periodEndDatePeriodId, momentFormat).format(momentFormat);
      var startPeriodId = PurchaseHistoryStore.startPeriodId;
      var endPeriodId = PurchaseHistoryStore.endPeriodId;

      vm.update = function () {
        var productTreeSelectedPeriods = PurchaseHistoryStore.productTreeSelectedPeriods;
        period = PurchaseHistoryStore.period;
        startPeriodId = PurchaseHistoryStore.startPeriodId;
        endPeriodId = PurchaseHistoryStore.endPeriodId;

        $scope.headerContents = vm.getHeaderContent(productTreeSelectedPeriods.data);

        $scope.periodIdStartOptions = vm.getPeriodIdOptions(period, maxStartPeriodId, endPeriodId);
        $scope.periodIdEndOptions = vm.getPeriodIdOptions(period, startPeriodId, maxEndPeriodId);

        var foundPeriodIdStartOption = false;
        for (var i = 0 ; i < $scope.periodIdStartOptions.length; i++) {
          if ($scope.periodIdStartOptions[i].value === startPeriodId) {
            $scope.periodIdStartSelected = $scope.periodIdStartOptions[i];
            foundPeriodIdStartOption = true;
          }
        }

        if (!foundPeriodIdStartOption) {
          $scope.periodIdStartSelected = $scope.periodIdStartOptions[$scope.periodIdStartOptions.length - 1];
        }

        var foundPeriodIdEndOption = false;
        for (var j = 0 ; j < $scope.periodIdEndOptions.length; j++) {
          if ($scope.periodIdEndOptions[j].value === endPeriodId) {
            $scope.periodIdEndSelected = $scope.periodIdEndOptions[j];
            foundPeriodIdEndOption = true;
          }
        }

        if (!foundPeriodIdEndOption) {
          $scope.periodIdEndSelected = $scope.periodIdEndOptions[0];
        }
      };

      var segmentName = PurchaseHistoryStore.segment.segmentName;
      $scope.views = [{
        titleHtml: 'Total Spend',
        contentKey: 'totalSpend',
        icon: 'chart',
        selected: $scope.view === 'spend-view',
        view: 'spend-view',
        disabled: false
      },{
        titleHtml: 'Spend Compared to Year Before',
        contentKey: 'yearlyCompare',
        icon: 'calendar',
        selected: $scope.view === 'yearly-view',
        view: 'yearly-view',
        disabled: false
      },{
        titleHtml: segmentName ? 'Spend Compared to Similar in ' + '<span class="ph-segment-label">' + segmentName + '</span>': 'No segment available',
        contentKey: 'segmentCompare',
        icon: 'building',
        selected: $scope.view === 'segment-view',
        view: 'segment-view',
        disabled: segmentName ? false : true
      }];

      $scope.periodOptions = [{
        text: 'Monthly',
        value: 'M'
      }, {
        text: 'Quarterly',
        value: 'Q'
      }];

      var foundPeriod = false;
      for (var i = 0; i < $scope.periodOptions.length; i++) {
        if ($scope.periodOptions[i].value === period) {
          $scope.periodSelected = $scope.periodOptions[i];
          foundPeriod = true;
          break;
        }
      }

      if (!foundPeriod) {
        $scope.periodSelected = $scope.periodOptions[0];
      }

      var hasSpendFilter = function (product, index) {
        var found = false;

        product.depthFirstTraversal(function (node) {
          if (node.data.accountProductTotalSpend !== null) {
            found = true;
          }
        });

        return found;
      };
      var noSpendFilter = function (product, index) {
        var found = false;

        product.depthFirstTraversal(function (node) {
          if (node.data.accountProductTotalSpend === null) {
            found = true;
          }
        });

        return found;
      };
      var allFilter = function (product, index) {
        return true;
      };
      vm.selectFilter = function (selected) {
        var spendFilter = allFilter;
        switch (selected.value) {
            case 'PH_FILTER_HAS_SPEND':
              spendFilter = hasSpendFilter;
              break;
            case 'PH_FILTER_NO_SPEND':
              spendFilter = noSpendFilter;
              break;
            case 'PH_FILTER_ALL':
              spendFilter = allFilter;
              break;
        }

        $rootScope.$broadcast('purchaseHistoryFilterChange', spendFilter);
      };

      $scope.filterOptions = [{
        text: 'Show All',
        value: 'PH_FILTER_ALL'
      }, {
        text: 'Show Only With Spend',
        value: 'PH_FILTER_HAS_SPEND'
      }, {
        text: 'Show Only Without Spend',
        value: 'PH_FILTER_NO_SPEND'
      }];

      $scope.filterSelected = $scope.filterOptions[0];

      $scope.$on('purchaseHistoryStoreChange', function (event, payload) {
        vm.update();
      });

      vm.update();
  });
