import "./date-attribute-edit.scss";
angular
  .module("common.datacloud.query.builder.tree.edit.date.attribute", [
    "common.datacloud.query.builder.tree.edit.transaction.edit.numerical.range",
    "common.datacloud.query.builder.tree.edit.transaction.edit.date.range",
    "angularMoment",
    "common.datacloud.query.builder.tree.transaction.service"
  ])
  .component("dateAttributeEdit", {
    templateUrl:
      "/components/datacloud/query/advanced/tree/date-attribute/date-attribute-edit.html",

    bindings: {
      type: "<",
      bucketrestriction: "=",
      form: "=",
      vm: "="
    },
    controller: function(QueryTreeService, QueryTreeDateAttributeStore) {
      this.init = function() {
        // console.log("STarted");
        this.timeCmp = "EVER";
        this.timeframePeriod = "Week";
        this.showTimeFrame = false;
        this.showPeriodSelect = false;
        this.showPeriodNumber = false;
        this.showFromPeriod = false;
        this.showToPeriod = false;
        this.showFromTime = false;
        this.showToTime = false;

        this.periodTimeConfig = {
          from: {
            name: "from-time",
            initial: undefined,
            position: 0,
            type: "Time",
            visible: this.showFromTime,
            pattern: "\\d+"
          },
          to: {
            name: "to-time",
            initial: undefined,
            position: 1,
            type: "Time",
            visible: this.showToTime,
            pattern: "\\d+"
          }
        };
        this.periodNumberConfig = {
          from: {
            name: "from-period",
            value: undefined,
            position: 0,
            type: "Time",
            min: "1",
            max: "",
            pattern: "\\d*"
          },
          to: {
            name: "to-period",
            value: undefined,
            position: 1,
            type: "Time",
            min: "1",
            max: "",
            pattern: "\\d*"
          }
        };

        this.cmpsList = [
          { name: "EVER", displayName: "Ever" },
          { name: "IN_CURRENT_PERIOD", displayName: "Current" },
          { name: "WITHIN", displayName: "Previous" },
          { name: "LAST", displayName: "Last" },
          { name: "BETWEEN", displayName: "Between Last" },
          { name: "BETWEEN_DATE", displayName: "Between" },
          { name: "BEFORE", displayName: "Before" },
          { name: "AFTER", displayName: "After" },
          { name: "IS_EMPTY", displayName: "Is Empty" }
        ];
        this.periodsList = QueryTreeDateAttributeStore.periodList();
      };
      this.changeValue = function(type, position, value) {
        console.log("TYPE ", type, " POSITION ", position, " VALUE ", value);
      };
      this.changeCmp = function(value, type) {
        this.showTimeFrame = false;
        setTimeout(() => {
          switch (value) {
            case "EVER":
            case "IS_EMPTY":
              this.showTimeFrame = false;
              this.showPeriodSelect = false;
              this.showPeriodNumber = false;
              this.showFromPeriod = false;
              this.showToPeriod = false;
              this.showFromTime = false;
              this.showToTime = false;
              break;
            case "IN_CURRENT_PERIOD":
              this.showTimeFrame = false;
              this.showPeriodSelect = true;
              this.showPeriodNumber = false;
              this.showFromPeriod = false;
              this.showToPeriod = false;
              this.showFromTime = false;
              this.showToTime = false;
              break;
            case "WITHIN":
              this.showTimeFrame = false;
              this.showPeriodSelect = true;
              this.showPeriodNumber = true;
              this.showFromPeriod = false;
              this.showToPeriod = true;
              this.showFromTime = false;
              this.showToTime = false;
              break;
            case "LAST":
              this.showTimeFrame = false;
              this.showPeriodSelect = false;
              this.showPeriodNumber = true;
              this.showFromPeriod = true;
              this.showToPeriod = false;
              this.showFromTime = false;
              this.showToTime = false;
              break;
            case "BETWEEN":
              this.showTimeFrame = false;
              this.showPeriodSelect = true;
              this.showPeriodNumber = true;
              this.showFromPeriod = true;
              this.showToPeriod = true;
              this.showFromTime = false;
              this.showToTime = false;
              break;
            case "BETWEEN_DATE":
              this.showTimeFrame = true;
              this.showPeriodSelect = false;
              this.showPeriodNumber = false;
              this.showFromPeriod = false;
              this.showToPeriod = false;
              this.showFromTime = true;
              this.showToTime = true;
              break;

            case "BEFORE":
              this.showTimeFrame = true;
              this.showPeriodSelect = false;
              this.showPeriodNumber = false;
              this.showFromPeriod = false;
              this.showToPeriod = false;
              this.showFromTime = false;
              this.showToTime = true;
              break;

            case "AFTER":
              this.showTimeFrame = true;
              this.showPeriodSelect = false;
              this.showPeriodNumber = false;
              this.showFromPeriod = false;
              this.showToPeriod = false;
              this.showFromTime = true;
              this.showToTime = false;
              break;
          }
        }, 0);
      };
      this.changePeriod = function() {
        QueryTreeService.changeTimeframePeriod(this.bucketrestriction, this.type, {Period:this.timeframePeriod, Vals: []});
      };
      this.init();
    }
  });
