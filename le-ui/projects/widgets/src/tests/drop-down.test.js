import "../components/drop-down/dropdown.component";
angular.module('dd.test', ['le.widgets.dropdown']).component('dropDownTest', {
    template: ` <div style=" display: flex; flex-direction: row;">
                    <le-drop-down options="$ctrl.options" selection="$ctrl.selected()" callback="$ctrl.mycallback(value)"></le-drop-down>
                    <p id="callbackMsg">On Selection changed you are going to see the position selected :-)</p>
                </div>`,
    bindings: {
        input: '<',
        selected: '<'
    },

    controller: function () {
        this.options = this.input;
        if (this.options) {
            var position = this.selected ? this.selected : 0;
            this.selection = this.options[position];
        }


        function getSelected() {
            return this.selection;
        }
        this.mycallback = function (value) {
            document.getElementById('callbackMsg').innerHTML = 'Position selected: ' + value;
            console.log('Called callback ', value);
        }
        this.selected = getSelected;
    }

});