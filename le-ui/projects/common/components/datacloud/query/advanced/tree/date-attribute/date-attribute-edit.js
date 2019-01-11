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
      this.$onInit = function() {
        console.log("Started", this.bucketrestriction);
        this.timeCmp = QueryTreeService.getCmp(
          this.bucketrestriction,
          this.type,
          "Date"
        );
        this.timeframePeriod = QueryTreeService.getPeriodValue(
          this.bucketrestriction,
          this.type,
          "Date"
        );
        this.showTimeFrame = false;
        this.showPeriodSelect = false;
        this.showPeriodNumber = false;
        this.showFromPeriod = false;
        this.showToPeriod = false;
        this.showFromTime = false;
        this.showToTime = false;
        this.periodsList = QueryTreeDateAttributeStore.periodList();
       
        this.periodTimeConfig = {
          from: {
            name: "from-time",
            initial: QueryTreeDateAttributeStore.getVal(
              "Date",
              this.timeCmp,
              this.bucketrestriction.bkt,
              0
            ),
            position: 0,
            type: "Date",
            visible: this.showFromTime,
            pattern: "\\d+"
          },
          to: {
            name: "to-time",
            initial: QueryTreeDateAttributeStore.getVal(
              "Date",
              this.timeCmp,
              this.bucketrestriction.bkt,
              1
            ),
            position: 1,
            type: "Date",
            visible: this.showToTime,
            pattern: "\\d+"
          }
        };
        this.periodNumberConfig = {
          from: {
            name: "from-period",
            value: QueryTreeDateAttributeStore.getVal(
              "Numerical",
              this.timeCmp,
              this.bucketrestriction.bkt,
              0
            ),
            position: 0,
            type: "Numerical",
            min: "1",
            max: "",
            pattern: "\\d*"
          },
          to: {
            name: "to-period",
            value: QueryTreeDateAttributeStore.getVal(
              "Date",
              this.timeCmp,
              this.bucketrestriction.bkt,
              1
            ),
            position: 1,
            type: "Date",
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
        this.changeCmp(this.timeCmp);
      };

      this.changeCmp = function(value, type) {
        // this.showTimeFrame = false;
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
              QueryTreeDateAttributeStore.changeCmp(
                this.bucketrestriction.bkt,
                value,
                "Day",
                this.bucketrestriction.bkt.Fltr.Vals
              );
              break;
            case "IN_CURRENT_PERIOD":
              this.showTimeFrame = false;
              this.showPeriodSelect = true;
              this.showPeriodNumber = false;
              this.showFromPeriod = false;
              this.showToPeriod = false;
              this.showFromTime = false;
              this.showToTime = false;
              QueryTreeDateAttributeStore.changeCmp(
                this.bucketrestriction.bkt,
                value,
                this.timeframePeriod,
                this.bucketrestriction.bkt.Fltr.Vals
              );
              break;
            case "WITHIN":
              this.showTimeFrame = false;
              this.showPeriodSelect = true;
              this.showPeriodNumber = true;
              this.showFromPeriod = true;
              this.showToPeriod = false;
              this.showFromTime = false;
              this.showToTime = false;
              QueryTreeDateAttributeStore.changeCmp(
                this.bucketrestriction.bkt,
                value,
                this.timeframePeriod,
                this.bucketrestriction.bkt.Fltr.Vals
              );
              break;
            case "LAST":
              this.showTimeFrame = false;
              this.showPeriodSelect = false;
              this.showPeriodNumber = true;
              this.showFromPeriod = true;
              this.showToPeriod = false;
              this.showFromTime = false;
              this.showToTime = false;
              QueryTreeDateAttributeStore.changeCmp(
                this.bucketrestriction.bkt,
                value,
                "Day",
                this.bucketrestriction.bkt.Fltr.Vals
              );
              break;
            case "BETWEEN":
              this.showTimeFrame = false;
              this.showPeriodSelect = true;
              this.showPeriodNumber = true;
              this.showFromPeriod = true;
              this.showToPeriod = true;
              this.showFromTime = false;
              this.showToTime = false;
              QueryTreeDateAttributeStore.changeCmp(
                this.bucketrestriction.bkt,
                value,
                this.timeframePeriod,
                this.bucketrestriction.bkt.Fltr.Vals
              );
              break;
            case "BETWEEN_DATE":
              QueryTreeDateAttributeStore.restValues(
                this.bucketrestriction.bkt
              );
              this.showTimeFrame = true;
              this.showPeriodSelect = false;
              this.showPeriodNumber = false;
              this.showFromPeriod = false;
              this.showToPeriod = false;
              this.showFromTime = true;
              this.showToTime = true;
              this.periodTimeConfig.from.visible = true;
              this.periodTimeConfig.to.visible = true;

              QueryTreeDateAttributeStore.changeCmp(
                this.bucketrestriction.bkt,
                value,
                "Date",
                this.bucketrestriction.bkt.Fltr.Vals
              );
              break;

            case "BEFORE":
              QueryTreeDateAttributeStore.restValues(
                this.bucketrestriction.bkt
              );
              this.showTimeFrame = true;
              this.showPeriodSelect = false;
              this.showPeriodNumber = false;
              this.showFromPeriod = false;
              this.showToPeriod = false;
              this.showFromTime = true;
              this.showToTime = false;
              this.periodTimeConfig.from.visible = true;
              this.periodTimeConfig.to.visible = false;
              QueryTreeDateAttributeStore.changeCmp(
                this.bucketrestriction.bkt,
                value,
                "Date",
                this.bucketrestriction.bkt.Fltr.Vals
              );
              break;

            case "AFTER":
              QueryTreeDateAttributeStore.restValues(
                this.bucketrestriction.bkt
              );
              this.showTimeFrame = true;
              this.showPeriodSelect = false;
              this.showPeriodNumber = false;
              this.showFromPeriod = false;
              this.showToPeriod = false;
              this.showFromTime = true;
              this.showToTime = false;
              this.periodTimeConfig.from.visible = true;
              this.periodTimeConfig.to.visible = false;

              QueryTreeDateAttributeStore.changeCmp(
                this.bucketrestriction.bkt,
                value,
                "Date",
                this.bucketrestriction.bkt.Fltr.Vals
              );
              break;
          }
        }, 0);
      };
      this.changePeriod = function() {
        QueryTreeService.changeTimeframePeriod(
          this.bucketrestriction,
          this.type,
          {
            Period: this.timeframePeriod,
            Vals: this.bucketrestriction.bkt.Fltr.Vals
          }
        );
      };
      this.callbackChangedValue = function(type, position, value) {
        QueryTreeDateAttributeStore.changeValue(
          this.timeCmp,
          this.bucketrestriction.bkt.Fltr.Vals,
          position,
          value
        );
        // QueryTreeDateAttributeStore.changeValue(this.bucketrestriction, this.type, value, position, type);
        this.periodTimeConfig.from.initial = QueryTreeDateAttributeStore.getVal(
          "Date",
          this.timeCmp,
          this.bucketrestriction.bkt,
          0
        );
        this.periodTimeConfig.to.initial = QueryTreeDateAttributeStore.getVal(
          "Date",
          this.timeCmp,
          this.bucketrestriction.bkt,
          1
        );
      };
      // this.init();
    }
  });
