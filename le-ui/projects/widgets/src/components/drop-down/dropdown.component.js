import template from './dropdown.component.html';

angular.module('le.widgets.dropdown', [])
.component('leDropDown', {
    template: template,

    bindings: {
        selection: '<',
        options: '=',
        callback: '&'
    },

    controller: function () {


        function getOptions() {
            return this.options;
        }

        function valueChanged(val) {
            if (this.callback !== undefined) {
                var selection = JSON.stringify(this.selected);
                this.callback({
                    value: val
                });
            }
        }
        this.$onInit = function () {
            if (!this.options || this.options == null) {
                this.options = [];
            }
            this.options.unshift({
                value: -1,
                name: '-- Select Option --'
            });
            if (!this.selection) {
                this.selection = this.options[0];
            }

        };

        this.changed = valueChanged;
        this.getOptions = getOptions;
    }

});