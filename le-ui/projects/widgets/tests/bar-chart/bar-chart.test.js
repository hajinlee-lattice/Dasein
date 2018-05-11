angular.module('barchart.test', ['le.widgets.barchart'])
    .component('barChartTest', {
        templateUrl: './tests/bar-chart/bar-chart.test.html',


        controller: function ($scope) {
            this.type = 'long';
            this.draw = true;
            console.log('Loading');
            this.statcount = 0;



            this.getFunnyData = function () {
                if (this.data === undefined) {
                    this.data = [{
                        "bucket_name": "A",
                        "left_bound_score": 95,
                        "right_bound_score": 85,
                        "num_leads": 15,
                        "lift": 20.524444444444445,
                        "creation_timestamp": 1523337152898,
                        "last_modified_by_user": "dipei.he@lattice-engines.com"
                    }, {
                        "bucket_name": "B",
                        "left_bound_score": 84,
                        "right_bound_score": 75,
                        "num_leads": 79,
                        "lift": 3.8970464135021095,
                        "creation_timestamp": 1523337152898,
                        "last_modified_by_user": "dipei.he@lattice-engines.com"
                    }, {
                        "bucket_name": "C",
                        "left_bound_score": 74,
                        "right_bound_score": 50,
                        "num_leads": 701,
                        "lift": 3.074274845458868,
                        "creation_timestamp": 1523337152898,
                        "last_modified_by_user": "dipei.he@lattice-engines.com"
                    }, {
                        "bucket_name": "D",
                        "left_bound_score": 49,
                        "right_bound_score": 5,
                        "num_leads": 3823,
                        "lift": 0.4831807481035836,
                        "creation_timestamp": 1523337152898,
                        "last_modified_by_user": "dipei.he@lattice-engines.com"
                    }]
                }
                return this.data;
            }
            this.getNormalData = function () {
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

            this.getBugConfig = function () {

                if (this.config === undefined) {
                    this.config = {
                        "data": {
                            "tosort": true,
                            "sortBy": "bucket_name",
                            "trim": true,
                            "top": 5
                        },
                        "chart": {
                            "header": "Value",
                            "emptymsg": "",
                            "usecolor": true,
                            "color": "#e8e8e8",
                            "mousehover": false,
                            "type": "decimal",
                            "showstatcount": false,
                            "maxVLines": 4,
                            "showVLines": true
                        },
                        "vlines": {
                            "suffix": "x"
                        },
                        "columns": [{
                            "field": "lift",
                            "label": "Lift",
                            "type": "string",
                            "suffix": "x",
                            "chart": true
                        }]
                    };
                }
                return this.config;
            }
            this.getBugData = function () {
                if (this.data === undefined) {
                    // this.data = [{
                    //         "bucket_name": "A",
                    //         "left_bound_score": 95,
                    //         "right_bound_score": 85,
                    //         "num_leads": 15,
                    //         "lift": "20.5",
                    //         "creation_timestamp": 1523337152898,
                    //         "last_modified_by_user": "dipei.he@lattice-engines.com",
                    //         "$$hashKey": "object:365"
                    //     },
                    //     {
                    //         "bucket_name": "B",
                    //         "left_bound_score": 84,
                    //         "right_bound_score": 75,
                    //         "num_leads": 79,
                    //         "lift": "3.9",
                    //         "creation_timestamp": 1523337152898,
                    //         "last_modified_by_user": "dipei.he@lattice-engines.com",
                    //         "$$hashKey": "object:366"
                    //     },
                    //     {
                    //         "bucket_name": "C",
                    //         "left_bound_score": 74,
                    //         "right_bound_score": 50,
                    //         "num_leads": 701,
                    //         "lift": "3.1",
                    //         "creation_timestamp": 1523337152898,
                    //         "last_modified_by_user": "dipei.he@lattice-engines.com",
                    //         "$$hashKey": "object:367"
                    //     },
                    //     {
                    //         "bucket_name": "D",
                    //         "left_bound_score": 49,
                    //         "right_bound_score": 5,
                    //         "num_leads": 3823,
                    //         "lift": "0.5",
                    //         "creation_timestamp": 1523337152898,
                    //         "last_modified_by_user": "dipei.he@lattice-engines.com",
                    //         "$$hashKey": "object:368"
                    //     }
                    // ]
                    this.data = [{
                        "bucket_name": "A",
                        "left_bound_score": 95,
                        "right_bound_score": 85,
                        "num_leads": 5,
                        "lift": 0,
                        "creation_timestamp": 1523414446993,
                        "last_modified_by_user": "dipei.he@lattice-engines.com"
                    },
                    {
                        "bucket_name": "B",
                        "left_bound_score": 84,
                        "right_bound_score": 75,
                        "num_leads": 166,
                        "lift": 1.6644011945216763,
                        "creation_timestamp": 1523414446993,
                        "last_modified_by_user": "dipei.he@lattice-engines.com"
                    },
                    {
                        "bucket_name": "C",
                        "left_bound_score": 74,
                        "right_bound_score": 50,
                        "num_leads": 855,
                        "lift": 0.4154745838956365,
                        "creation_timestamp": 1523414446993,
                        "last_modified_by_user": "dipei.he@lattice-engines.com"
                    },
                    {
                        "bucket_name": "D",
                        "left_bound_score": 49,
                        "right_bound_score": 5,
                        "num_leads": 3592,
                        "lift": 1.109821445567548,
                        "creation_timestamp": 1523414446993,
                        "last_modified_by_user": "dipei.he@lattice-engines.com"
                    }];
                }
                return this.data;
            }

            this.getFunnyConfig = function () {

                if (this.config === undefined) {
                    this.config = {
                        'data': {
                            'tosort': false,
                            'sortBy': 'Lbl',
                            'trim': false,
                            'top': 5,
                        },
                        'chart': {
                            'header': 'Attributes Value',
                            'emptymsg': '',
                            'color': '#2E6099',
                            'mousehover': true,
                            'hovercolor': '#77aae5',
                            'type': 'decimal',
                            'showstatcount': true,
                            'maxVLines': 4,
                            'showVLines': true
                        },
                        'vlines': {
                            'suffix': 'x'
                        },
                        'columns': [{
                            'field': 'lift',
                            'label': 'Lifts',
                            'type': 'string',
                            'sufix': 'x',
                            'chart': true
                        }]
                    };
                }
                return this.config;
            }
            this.getNormalConfig = function () {

                if (this.config === undefined) {
                    this.config = {
                        'data': {
                            'tosort': false,
                            'sortBy': 'Lbl',
                            'trim': false,
                            'top': 5,
                        },
                        'chart': {
                            'header': 'Attributes Value',
                            'emptymsg': '',
                            'color': '#2E6099',
                            'mousehover': true,
                            'hovercolor': '#77aae5',
                            'type': 'decimal',
                            'showstatcount': true,
                            'maxVLines': 4,
                            'showVLines': true
                        },
                        'vlines': {
                            'suffix': 'x'
                        },
                        'columns': [{
                                'field': 'Lift',
                                'label': 'Lifts',
                                'type': 'string',
                                'sufix': 'x',
                                'chart': true
                            },
                            {
                                'field': 'Cnt',
                                'label': 'Records',
                                'type': 'number',
                                'chart': false,
                            }
                        ]
                    };
                }
                return this.config;
            }

            this.clicked = function (stat) {
                this.statcount = this.statcount + 1;
            }

            this.getConfig = function () {
                if (this.type === 'normal') {
                    return this.getNormalConfig();
                } else if (this.type === 'long') {
                    return this.getFunnyConfig();
                } else {
                    return this.getBugConfig();
                }
            }

            this.getData = function () {
                if (this.type === 'normal') {
                    return this.getNormalData();
                } else if (this.type === 'long') {
                    return this.getFunnyData();
                }else {
                    return this.getBugData();
                }
            }

            this.reset = function () {
                // this.type = 'tmp';
                this.draw = false;
                this.data = undefined;
                this.config = undefined;
                var self = this;
                setTimeout(function () {
                    self.getConfig();
                    self.getData();
                    self.draw = true;
                    $scope.$apply(); //this triggers a $digest
                }, 100);
            }
        }

    });