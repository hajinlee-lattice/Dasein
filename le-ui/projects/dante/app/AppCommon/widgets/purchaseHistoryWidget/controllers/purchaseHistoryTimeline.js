angular.module('mainApp.appCommon.widgets.PurchaseHistoryWidget.directives.PurchaseHistoryTimeline', [
    'mainApp.appCommon.utilities.PurchaseHistoryUtility',
    'mainApp.appCommon.services.PurchaseHistoryService',
    'mainApp.appCommon.widgets.PurchaseHistoryWidget.stores.PurchaseHistoryStore'
  ])
  .directive('purchaseHistoryTimeline', function ($rootScope, $compile, $window, $timeout, PurchaseHistoryStore, PurchaseHistoryUtility) {
      return {
        restrict: 'A',
        replace: true,
        scope: {
          periodStartIndex: '=',
          colsize: '='
        },
        link: function (scope, element, attr, PurchaseHistoryTimelineVm) {
          var line = function (x, y, key) {
            return d3.svg.line()
              .x(function(d, i) { return x(i); })
              .y(function(d) { return y(d[key]); });
          };

          var render = function () {
            var colsize = scope.colsize;
            var data = scope.periods.slice(scope.periodStartIndex, scope.periodStartIndex + colsize);

            var container = element.find('#ph-chart')[0];
            $(container).empty();

            var margin = {top: 24, right: 0, bottom: 0, left: 0},
                width = container.clientWidth - margin.left - margin.right,
                height = container.clientHeight - margin.top - margin.bottom;

            if (height <= 0) {
              // race condition, left page, prevent errors
              return;
            }

            var svg = d3.select(container)
                        .append("svg")
                        .attr("width", width + margin.left + margin.right)
                        .attr("height", height + margin.top + margin.bottom);
            var chart = svg.append("g")
                        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

            var x = d3.scale.ordinal()
                      .rangeRoundBands([0, width], (1/3), (1/6));
            var y = d3.scale.linear()
                      .range([height, 0]);

            var maxY = d3.max(data, function (d) {
              return Math.max(d.accountTotalSpend, d.accountPrevYearSpend);
            });

            x.domain(d3.range(0, colsize, 1));
            y.domain([0, maxY]);
            var rangeBand = x.rangeBand();

            if (rangeBand == Infinity) {
              return;
            }
            
            var bars = chart.append("g")
              .selectAll(".bar")
                .data(data)
              .enter().append("rect")
                .attr("class", "bar")
                .attr("x", function(d, i) {
                  return x(i);
                })
                .attr("width", rangeBand)
                .attr("y", function(d) {
                  return y(d.accountTotalSpend);
                })
                .attr("height", function(d) {
                  return height - y(d.accountTotalSpend);
                });

            var prevYearLine = chart.append("g")
              .attr("class", "prev-year-line")
              .attr("transform", "translate(" + rangeBand/2 + ", 0)");

            prevYearLine.append("path")
              .attr("d", line(x, y, 'accountPrevYearSpend')(data));

            var dots = prevYearLine.selectAll('dots')
                .data(data)
              .enter().append("circle")
                .attr("r", 4)
                .attr("cx", function(d, i) {
                  return x(i);
                })
                .attr("cy", function(d, i) {
                  return y(d.accountPrevYearSpend);
                });

            var chartLabels = chart.append("g")
              .attr("class", "bar-text")
              .attr("transform", "translate(0, -10)")
              .selectAll("text")
              .data(data)
              .enter()
              .append("text")
              .attr("dx", function (d, i) {
                var text = PurchaseHistoryUtility.formatDollar(d.accountTotalSpend, true);
                var offset = text.length/2 * 6;

                return x(i) + rangeBand/2 - offset;
              })
              .attr("dy", function (d, i) {
                return y(d.accountTotalSpend) + 5;
              })
              .text(function (d, i) {
                var value = d.accountTotalSpend;
                if (value && value > 0) {
                  return PurchaseHistoryUtility.formatDollar(d.accountTotalSpend, true);
                }
              });

            var appendTooltip = function (d, i) {
              var el = d3.select(this);
              var tooltipText = PurchaseHistoryTimelineVm.createToolTipContent(d);

              d3.select(container)
                .append("div")
                  .style("position", "absolute")
                  .style("bottom", function () {
                    var topVal = Math.max(d.accountTotalSpend, d.accountPrevYearSpend);
                    return height - y(topVal) + 24 + 'px';
                  })
                  .style("left", function () {
                    return x(i) + rangeBand/2 - 77 + 'px';
                  })
                  .attr("id", "chart-tooltip")
                  .html('<div class="ph-timeline-toolip">' +
                        '<p>'+tooltipText.title+'</p>' +
                        '<p>'+tooltipText.content+'</p>' +
                        '</div>');
            };

            var removeTooltip = function () {
              $("#chart-tooltip").remove();
            };

            bars.on("mouseover", appendTooltip);
            bars.on("mouseout", removeTooltip);

            chartLabels.on("mouseover", appendTooltip);
            chartLabels.on("mouseout", removeTooltip);

            dots.on("mouseover", appendTooltip);
            dots.on("mouseout", removeTooltip);
          };

          var update = function () {
            $timeout(function () {
                PurchaseHistoryTimelineVm.update();
                render();
              }, 1000, false);
          };

          var periodStartIndexWatcher = scope.$watch('periodStartIndex', function (newPeriodStartIndex, oldPeriodStartIndex) {
            if (newPeriodStartIndex === oldPeriodStartIndex) { return; }
            render();
          });

          update();

          window.onresize = function() {
              scope.$apply();
          };
          var ignoreFirstResize = true;
          scope.$watch(function () {
              return angular.element($window)[0].innerWidth;
          }, function () {
              if (ignoreFirstResize) {
                ignoreFirstResize = false;
                return;
              }
              render();
          });

          scope.$on('purchaseHistorySpendTableChange', function (event, payload) {
            update();
          });
        },
        controller: 'PurchaseHistoryTimelineCtrl',
        controllerAs: 'PurchaseHistoryTimelineVm',
        templateUrl: 'app/AppCommon/widgets/purchaseHistoryWidget/templates/purchaseHistoryTimelineTemplate.html'
      };
  })
  .controller('PurchaseHistoryTimelineCtrl', function ($scope, PurchaseHistoryService, PurchaseHistoryStore, PurchaseHistoryUtility) {
    // must set for button to be disabled on initialization
    $scope.max = 0;

    var vm = this;

    vm.update = function () {
      $scope.periods = PurchaseHistoryStore.productTreeSelectedPeriods.data.periods;
      $scope.max = $scope.periods.length - $scope.colsize;
    };

    vm.handlePeriodRangeClick = function (direction) {
      if (direction < 0) {
        if ($scope.periodStartIndex - 1 > -1) {
          $scope.periodStartIndex--;
        }
      } else if (direction > 0) {
        if (($scope.periodStartIndex + 1) <= $scope.max) {
          $scope.periodStartIndex ++;
        }
      }
    };

    vm.createToolTipContent = function (periodData) {
      var title = 'Compared to Last Year';
      var content = '';

      var accountTotalSpend = periodData.accountTotalSpend;
      var accountPrevYearSpend = periodData.accountPrevYearSpend;

      var diff = accountTotalSpend - accountPrevYearSpend;
      var percentChange = Math.abs(diff / accountPrevYearSpend);
      if (isNaN(percentChange)) {
        percentChange = 0;
      } else if (Math.abs(percentChange) === Infinity) {
        percentChange = 1;
      }

      if (diff === 0) {
        content += 'No Change';
      } else {
        content += PurchaseHistoryUtility.formatDollar(diff);
        content += ' ( ';
        content += diff < 0 ? 'Down' : 'Up';
        content += ' ';
        if (/(Up)/.test(content) && percentChange === 1) {
          content += '- ';
        } else {
          content += PurchaseHistoryUtility.formatPercent(percentChange * 100);
        }
        content += ' )';
      }

      return {
        title: title,
        content: content
      };
    };
  });
