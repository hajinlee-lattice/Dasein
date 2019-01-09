import "./date-attribute-edit.scss";
angular
  .module("common.datacloud.query.builder.tree.edit.date.attribute")
  .component("dateAttribute", {
    templateUrl:
      "/components/datacloud/query/advanced/tree/date-attribute/date-attribute.component.html",

    bindings: {
      type: "=",
      bucketrestriction: "="
    },
    controller: function(QueryTreeService, QueryTreeDateAttributeStore) {
      this.init = function() {};

      this.getCmp = function(subType) {
        var ret = QueryTreeService.getCmp(
          this.bucketrestriction,
          this.type,
          subType
        );
        switch (subType) {
          case "Date": {
            var cmp =
              ret === "" ? "EVER" : QueryTreeDateAttributeStore.dateAttributeMap[ret];
            return cmp;
          }
          default: {
            return ret;
          }
        }
      };
      this.getValues = function(subType){
        var ret = QueryTreeService.getValues(this.bucketrestriction, this.type, subType);
        switch (ret.length) {
            case 0: {
                return '';
            }
            case 1: {
                return ret[0];
            }
            case 2: {
                return ret[0] + ' - ' + ret[1];
            }
            default:
                return '';
        }
      }
      this.getPeriod = function(){
        var period = QueryTreeService.getPeriodValue(this.bucketrestriction, this.type, 'Date');
        if(this.getCmp('Date') !== 'Ever' && this.getCmp('Date') !== 'IS_EMPTY'){
            return period+'(s)';
        }
      }
      this.init();
    }
  });
