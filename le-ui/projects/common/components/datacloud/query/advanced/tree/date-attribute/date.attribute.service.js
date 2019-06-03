import "./date-attribute-edit.scss";

angular
  .module("common.datacloud.query.builder.tree.edit.date.attribute")
  .service("QueryTreeDateAttributeStore", function(
    QueryTreeDateAttributeService
  ) {
    var QueryTreeDateAttributeStore = this;
    QueryTreeDateAttributeStore.periodsMap = {};
    QueryTreeDateAttributeStore.periods = [];
    this.dateAttributeMap = {
      EVER: "Ever",
      IN_CURRENT_PERIOD: "Current",
      WITHIN: "Previous",
      PRIOR_ONLY: "Only Prior to Last",
      BETWEEN: "Between Last",
      BETWEEN_DATE: "Between",
      BEFORE: "Before",
      AFTER: "After",
      IS_EMPTY: "Is Empty",
      LAST: "Last",
      LATEST_DAY: 'Latest Day'
    };
    this.getPeriodNumericalConfig = function() {
      return {
        from: {
          name: "from-period",
          value: undefined,
          position: 0,
          type: "Time",
          min: "0",
          max: "",
          pattern: "\\d*",
          step: 1
        },
        to: {
          name: "to-period",
          value: undefined,
          position: 1,
          type: "Time",
          min: "0",
          max: "",
          pattern: "\\d*",
          step: 1
        }
      };
    };

    this.getPeriodTimeConfig = function() {
      return {
        from: {
          name: "from-time",
          initial: undefined,
          position: 0,
          type: "Time",
          visible: true,
          pattern: "\\d*",
          step: 1
        },
        to: {
          name: "to-time",
          initial: undefined,
          position: 1,
          type: "Time",
          visible: true,
          pattern: "\\d*",
          step: 1
        }
      };
    };

    this.getCmpsList = function() {
      return [
        { name: "EVER", displayName: "Ever" },
        { name: "IN_CURRENT_PERIOD", displayName: "Current" },
        { name: "WITHIN", displayName: "Previous" },
        { name: "LAST", displayName: "Last" },
        { name: "BETWEEN", displayName: "Between Last" },
        { name: "BETWEEN_DATE", displayName: "Between" },
        { name: "BEFORE", displayName: "Before" },
        { name: "AFTER", displayName: "After" },
        { name: "IS_EMPTY", displayName: "Is Empty" },
        { name: "LATEST_DAY", displayName: "Latest Day" }
      ];
    };

    this.periodList = function() {
      if (QueryTreeDateAttributeStore.periods.length == 0) {
        QueryTreeDateAttributeService.getPeriods().then(function(result) {
          result.forEach(function(element) {
            if(!QueryTreeDateAttributeStore.periodsMap[element]){
              QueryTreeDateAttributeStore.periods.push({
                name: element,
                displayName: element + "(s)"
              });
              QueryTreeDateAttributeStore.periodsMap[element] = element + "(s)";
            }
          });
        });
      }
      return QueryTreeDateAttributeStore.periods;
    };

    this.getVal = function(type, cmp, bkt, position) {
      switch (type) {
        case "Date":
          if (cmp !== "BETWEEN_DATE" || cmp === "BEFORE" || cmp === "AFTER") {
            return undefined;
          } else {
            return this.getValue(cmp, bkt, position);
          }

        case "Numerical":
          if (cmp == "WITHIN" || cmp == "LAST" || cmp == "BETWEEN") {
            return this.getValue(cmp, bkt, position);
          } else {
            return undefined;
          }
        default:
          return undefined;
      }
    };
    this.getValue = function(cmp, bkt, position) {
      var valsArray = bkt.Fltr.Vals;
      switch (cmp) {
        case "BETWEEN":
        case "BETWEEN_DATE": {
          return valsArray[position];
        }
        case "WITHIN":
        case "LAST":
        case "BEFORE":
        case "AFTER": {
          if (position == valsArray.length - 1 || valsArray.length == 0) {
            return valsArray[valsArray.length - 1];
          } else {
            return undefined;
          }
        }
        default: {
          return undefined;
        }
      }
    };
    this.getPeriod = function(bkt) {
      if (bkt && bkt.Fltr) {
        return bkt.Fltr.Period;
      } else {
        return "";
      }
    };

    this.changeCmp = function(bkt, cmp, period, valsArray) {
      bkt.Fltr.Cmp = cmp;
      bkt.Fltr.Period = period;
      bkt.Fltr.Vals = valsArray;
    };

    this.changeValue = function(cmp, valsArray, position, value) {
      switch (cmp) {
        case "BETWEEN":
        case "BETWEEN_DATE": {
          valsArray[position] = value;
          break;
        }
        default: {
          valsArray[0] = value;
        }
      }
    };
    this.restValues = function(bkt) {
      bkt.Fltr.Vals = [];
    };

    this.getCmpValueReadable = function(displayName, bkt) {
      let cmp = QueryTreeDateAttributeStore.dateAttributeMap[bkt.Fltr.Cmp];
      let vals = bkt.Fltr.Vals;
      let period = `${bkt.Fltr.Period}(s)`;
      let valRedable = "";
      switch (bkt.Fltr.Cmp) {
        case "EVER":
        case "IS_EMPTY":
        case 'LATEST_DAY':
          valRedable = "";
          period = "";
          break;
        case "IN_CURRENT_PERIOD":
          valRedable = "";
          break;
        case "LAST":
        case "WITHIN":
          valRedable = vals[0];
          break;

        case "BEFORE":
        case "AFTER":
          valRedable = vals[0];
          period = "";
          break;
        case "BETWEEN":
        case "BETWEEN_DATE":
          valRedable = `${vals[0]} - ${vals[1]}`;
          period = "";
          break;
      }
      let ret = {
        label: displayName + ": ",
        value: `${cmp} ${valRedable} ${period}`
      };
      return ret;
    };
  })
  .service("QueryTreeDateAttributeService", function($http, $q) {
    this.getPeriods = function(resourceType, query) {
      var deferred = $q.defer();

      $http({
        method: "GET",
        url: "/pls/datacollection/periods/names"
      })
        .success(function(result) {
          deferred.resolve(result);
        })
        .error(function(result) {
          deferred.resolve(result);
        });

      return deferred.promise;
    };
  });
