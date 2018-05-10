import txt from './test.html';
angular.module('leWidgets.test', [])
.component('test', {
    template: txt,

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

            console.log('');

        };

        this.changed = valueChanged;
        this.getOptions = getOptions;
        this.val = 'Leo controller';
    }

});