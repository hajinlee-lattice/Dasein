angular.module('common.datacloud.query.builder.tree.edit.percent.item', [])
    .component('percentAttribute', {
        templateUrl: '/components/datacloud/query/advanced/tree/percent/item/percent-item.component.html',

        bindings: {
            bucketrestriction: '=',
        },

        controller: function (QueryTreeService, PercentStore) {
            var self = this;

            this.$onInit = function () {

            };
            
            this.getDirection = function(){
                return PercentStore.getDirectionRedable(this.bucketrestriction);
            };

            this.getCmp = function() {
                return PercentStore.getCmpRedable(this.bucketrestriction);
            };

            this.getValues = function(){
                return PercentStore.getValuesFormatted(this.bucketrestriction);
            };

            
        }

    });