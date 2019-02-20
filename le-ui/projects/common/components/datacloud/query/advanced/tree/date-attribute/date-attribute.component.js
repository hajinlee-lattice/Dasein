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
      this.init = function() {
        QueryTreeDateAttributeStore.periodList();
      };

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
        // console.log('READ VALUES ', this.bucketrestriction.bkt.Fltr);
        var ret = QueryTreeService.getValues(this.bucketrestriction, this.type, subType);
        // console.log('RET ', ret);
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
        // console.log('PERIOD', this.bucketrestriction.bkt.Fltr.Period);
        var period = QueryTreeDateAttributeStore.getPeriod(this.bucketrestriction.bkt);
        switch(this.bucketrestriction.bkt.Fltr.Cmp){
          case 'EVER':
          case 'IS_EMPTY':
          case 'BETWEEN_DATE':
          case 'BEFORE':
          case 'AFTER':

          return '';

          default:
          return period+'(s)';
        }
        // if(this.getCmp('Date') !== 'Ever' && this.getCmp('Date') !== 'IS_EMPTY'){
        //     return period+'(s)';
        // }
      }
      this.init();
    }
  });
