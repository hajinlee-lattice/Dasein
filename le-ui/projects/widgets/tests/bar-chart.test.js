angular.module('barchart.test', ['le.widgets.barchart'])
    .component('barChartTest', {
    template: `<div style="border: 1px solid red;">
                    <le-bar-chart 
                        bktlist="$ctrl.getTestData()"
                        callback="$ctrl.clicked()"
                        config="$ctrl.getConfig()"
                        statcount="0">
                    </le-bar-chart>
                </div>`,

    controller: function () {
        
        console.log('Loading');
        this.statcount = 0;
        this.getTestData = function () {
            if (this.data === undefined) {
                this.data = [{
                        "Lbl": "B",
                        "Cnt": 10,
                        "Lift": "1.3",
                        "Id": 2,
                        "Cmp": "EQUAL",
                        "Vals": [
                            "B"
                        ]
                    },
                    {
                        "Lbl": "A",
                        "Cnt": 11,
                        "Lift": "0.3",
                        "Id": 1,
                        "Cmp": "EQUAL",
                        "Vals": [
                            "A"
                        ]
                    },
                    {
                        "Lbl": "F",
                        "Cnt": 14,
                        "Lift": "3.5",
                        "Id": 3,
                        "Cmp": "EQUAL",
                        "Vals": [
                            "F"
                        ]
                    },
                    {
                        "Lbl": "C",
                        "Cnt": 16,
                        "Lift": "0.8",
                        "Id": 3,
                        "Cmp": "EQUAL",
                        "Vals": [
                            "C"
                        ]
                    },
                    {
                        "Lbl": "D",
                        "Cnt": 18,
                        "Lift": "0.9",
                        "Id": 3,
                        "Cmp": "EQUAL",
                        "Vals": [
                            "D"
                        ]
                    }
                ];
            }
            return this.data;
        }

        this.getConfig = function () {

            if (this.config === undefined) {
                this.config = {
                    'data': {
                        'tosort': false,
                        'sortBy': 'Lbl',
                        'trim': false,
                        'top': 5,
                    },
                    'chart': {
                        'header':'Attributes Value',
                        'emptymsg': '',
                        'color': '#2E6099',
                        'mousehover': false,
                        'hovercolor': '#77aae5',
                        'type': '',
                        'showstatcount': true,
                        'maxVLines': 3,
                        'showVLines': false
                    },
                    'vlines':{
                        'suffix': 'x'
                    },
                    'columns': [{
                            'field': 'Lift',
                            'label': 'Lifts',
                            'type': 'string',
                            'sufix': 'x',
                            'chart': false
                        },
                        {
                            'field': 'Cnt',
                            'label': 'Records',
                            'type': 'number',
                            'chart': true,
                        }
                    ]
                };
            }
            return this.config;
        }

        this.clicked = function (stat) {
            console.log('You clicked ===> ', stat);
            this.statcount = this.statcount + 1;
        }
    }

});