angular.module('common.datacloud.query.builder.tree.service', [])
    .service('QueryTreeService', function (QueryTreeAccountEntityService, QueryTreePurchaseHistoryService) {
        // console.log('TEST service');
        var QueryTreeService = this;
        this.cmpMap = {
            "Yes": "is",
            "No": "is",
            "": "is",
            "is": "is",
            "empty": "is empty",
            "between": "is between",
            'IS_NULL': 'is empty',
            'IS_NOT_NULL': 'is present',
            'EQUAL': 'is equal to',
            'NOT_EQUAL': 'is not equal to',
            'GREATER_THAN': 'is greater than',
            'GREATER_OR_EQUAL': 'is greater than or equal to',
            'LESS_THAN': 'is less than',
            'LESS_OR_EQUAL': 'is less than or equal to',
            'GTE_AND_LTE': 'is greater or equal and lesser or equal',
            'GTE_AND_LT': 'is greater or equal and less than',
            'GT_AND_LTE': "is greater than and lesser or equal",
            'GT_AND_LT': "is greater than and less than",
            'IN_COLLECTION': 'in collection',
            'CONTAINS': 'contains',
            'NOT_CONTAINS': 'not contains',
            'STARTS_WITH': 'starts with'
        };
        this.numerical_operations = {
            'IS_NULL': 'Empty',
            'IS_NOT_NULL': 'Not Empty',
            'EQUAL': 'Equal',
            'NOT_EQUAL': 'Not Equal',
            'GREATER_THAN': 'Greater Than',
            'GREATER_OR_EQUAL': 'Greater or Equal',
            'LESS_THAN': 'Less Than',
            'LESS_OR_EQUAL': 'Lesser or Equal',
            'GTE_AND_LTE': '>= and <=',
            'GTE_AND_LT': '>= and <',
            'GT_AND_LTE': "> and <=",
            'GT_AND_LT': "> and <"
        },
            this.enum_operations = {
                'EQUAL': 'Is Equal To',
                'NOT_EQUAL': 'Does Not Equal'
            };
        this.no_inputs = [
            'IS_NULL',
            'IS_NOT_NULL'
        ];
        this.two_inputs = [
            'GTE_AND_LTE',
            'GTE_AND_LT',
            'GT_AND_LTE',
            'GT_AND_LT'
        ];

        /**
         * Return the service based on the Entity type
         * 'Account', 'Contact', 'PurchaseHistory'
         * @param {*} entity 
         */
        function getService(entity) {
            switch (entity) {
                case 'Account':
                case 'Contact': {
                    return QueryTreeAccountEntityService;
                };
                case 'PurchaseHistory': {
                    return QueryTreePurchaseHistoryService;
                };
                default: return null;
            }
        }

        /**
         * Retrives from attr the Entity
         * @param {*} bucketRestriction 
         */
        function getEntity(bucketRestriction) {
            if (bucketRestriction) {
                var attr = bucketRestriction.attr;
                if (attr) {
                    var dot = attr.indexOf('.');
                    var entity = attr.slice(0, dot);
                    return entity;
                } else {
                    return 'Unkown';
                }
            } else {
                console.warn('Bucket Restriction not existing', bucketRestriction);
                return 'Unkown';
            }
        }

        this.showEmptyOption = function (bucketRestriction) {
            if ('PurchaseHistory' === getEntity(bucketRestriction)) {
                return false;
            } else {
                return true;
            }
        }

        /**
         * Return if a type ('Boolean', 'Numerical', 'Enum') can be shown or not
         * @param {*} bucketRestriction 
         * @param {*} type 
         * @param {*} typeToShow 
         */
        this.showType = function (bucketRestriction, type, typeToShow) {
            var entity = getEntity(bucketRestriction);
            var service = getService(entity);
            if (service) {
                return service.showType(bucketRestriction, type, typeToShow);
            }
            else {
                console.warn('Service not implemented');
                return false;
            }
        }

        /**
         * Return if the second value (To) can be shown or not
         * @param {*} bucketRestriction 
         */
        this.showTo = function (bucketRestriction) {
            var entity = getEntity(bucketRestriction);
            var service = getService(entity);
            if (service) {
                return service.showTo(bucketRestriction, this.two_inputs);
            } else {
                console.warn('Service not implemented');
                return false;
            }
        }
        /**
         * Return the operation label (Ex. 'is equal to' 'is less than')
         * based on the Entity
         * @param {*} type 
         * @param {*} bucketRestriction 
         */
        this.getOperationLabel = function (type, bucketRestriction) {
            var entity = getEntity(bucketRestriction);
            var service = getService(entity);
            if (service) {
                return service.getOperationLabel(QueryTreeService.cmpMap, type, bucketRestriction);
            } else {
                console.warn('Service not implemented');
                return 'has a value of';
            }
        }
        /**
         * Return the value of the bucket restriction
         * @param {*} bucketRestriction 
         * @param {*} operatorType 
         * @param {*} position 
         */
        this.getOperationValue = function (bucketRestriction, operatorType, position) {
            var entity = getEntity(bucketRestriction);
            var service = getService(entity);
            if (service) {
                return service.getOperationValue(bucketRestriction, operatorType, position);
            } else {
                console.warn('Service not implemented');
                return 'Uknown';
            }
        }

        this.getAttributeRules = function (bucketRestriction, bkt, bucket, isSameAttribute) {
            
            var entity = getEntity(bucketRestriction);
            // console.log('ENTITY', entity);
            var service = getService(entity);
            return service.getAttributeRules(bkt, bucket, isSameAttribute);
        }
        
        //***************** Editing ************************************/


        /**
         * Return the var for ng-model in a boolean bucket restriction
         * @param {*} bucketRestriction 
         */
        this.getBooleanModel = function (bucketRestriction) {
            var entity = getEntity(bucketRestriction);
            var service = getService(entity);
            if (service) {
                return service.getBooleanModel(bucketRestriction);
            } else {
                console.warn(' changeBooleanValue() Service not implemented');
            }
        }

        this.getEnumCmpModel = function (bucketRestriction) {
            var entity = getEntity(bucketRestriction);
            var service = getService(entity);
            if (service) {
                return service.getEnumCmpModel(bucketRestriction);
            } else {
                console.warn(' changeBooleanValue() Service not implemented');
            }
        }

        this.getNumericalCmpModel = function (bucketRestriction) {
            var entity = getEntity(bucketRestriction);
            var service = getService(entity);
            if (service) {
                return service.getNumericalCmpModel(bucketRestriction);
            } else {
                console.warn(' getNumericalCmpModel() Service not implemented');
            }
        }

        /**
        * Change a boolean value for a Boolean restriction
        * @param {*} bucketRestriction 
        */
        this.changeBooleanValue = function (bucketRestriction, booleanValue) {
            var entity = getEntity(bucketRestriction);
            var service = getService(entity);
            if (service) {
                service.changeBooleanValue(bucketRestriction, booleanValue);
            } else {
                console.warn(' changeBooleanValue() Service not implemented');
            }
        }
        this.changeEnumCmpValue = function (bucketRestriction, value) {
            var entity = getEntity(bucketRestriction);
            var service = getService(entity);
            if (service) {
                return service.changeEnumCmpValue(bucketRestriction, value);
            } else {
                console.warn(' changeEnumCmpValue() Service not implemented');
            }
        }

        this.changeNumericalCmpValue = function (bucketRestriction, value) {
            var entity = getEntity(bucketRestriction);
            var service = getService(entity);
            if (service) {
                return service.changeNumericalCmpValue(bucketRestriction, value);
            } else {
                console.warn(' changeNumericalCmpValue() Service not implemented');
            }
        }

        this.changeBktValue = function (bucketRestriction, value, position) {
            var entity = getEntity(bucketRestriction);
            var service = getService(entity);
            if (service) {
                return service.changeBktValue(bucketRestriction, value, position);
            } else {
                console.warn(' changeNumericalCmpValue() Service not implemented');
            }
        }


        this.getBktValue = function (bucketRestriction, position) {
            var entity = getEntity(bucketRestriction);
            var service = getService(entity);
            if (service) {
                return service.getBktValue(bucketRestriction, position);
            } else {
                console.warn(' getBktValue() Service not implemented');
            }
        }

        this.getCubeBktList = function (bucketRestriction, cube) {
            var entity = getEntity(bucketRestriction);
            var service = getService(entity);
            if (service) {
                return service.getCubeBktList(cube);
            } else {
                console.warn(' getCubeBktList() Service not implemented');
            }
        }
        
    })
    .service('QueryTreeAccountEntityService', function () {

        this.showType = function (bucketRestriction, type, typeToShow) {
            switch (typeToShow) {
                case 'Numerical': {
                    if (typeToShow === type) {
                        return true;
                    } else {
                        return false;
                    }
                };
                case 'Boolean': {
                    if (type === typeToShow) {
                        return true;
                    } else {
                        return false;
                    }
                };
                case 'Enum': {
                    if (type === typeToShow) {
                        return true;
                    } else {
                        return false;
                    }
                };
                case 'Date': {
                    return false;
                };
                default: return false;
            }

        }

        this.showTo = function (bucketRestriction, two_inputs) {
            if (two_inputs.indexOf(bucketRestriction.bkt.Cmp) >= 0) {
                return true;
            } else {
                return false;
            }
        }
        /**
         * Return the operation label for and Account or Contacts Entity
         * @param {*} cmpMap 
         * @param {*} type 
         * @param {*} bucketRestriction 
         */
        this.getOperationLabel = function (cmpMap, type, bucketRestriction) {
            if (!bucketRestriction.bkt) {
                return;
            }

            switch (type) {
                case 'Boolean': return cmpMap[bucketRestriction.bkt.Vals[0] || ''];
                case 'Numerical': return cmpMap[bucketRestriction.bkt.Cmp];
                case 'Enum': return cmpMap[bucketRestriction.bkt.Cmp];
                default: return 'has a value of';
            }
        }

        function getBooleanValue(bucketRestriction) {
            if (bucketRestriction.bkt.Vals[0] === 'Yes') {
                return 'True';
            } if (bucketRestriction.bkt.Vals[0] === 'No') {
                return 'False';
            }
            return 'Empty';
        }

        function getNumericalValue(bucketRestriction, position) {
            return bucketRestriction.bkt.Vals[position];
        }

        function getEnumValues(bucketRestriction) {
            return bucketRestriction.bkt.Vals;
        }

        this.getOperationValue = function (bucketRestriction, operatorType, position) {
            // console.log('Operation Value', operatorType, position);
            switch (operatorType) {
                case 'Boolean': {
                    return getBooleanValue(bucketRestriction);
                };
                case 'Numerical': {
                    return getNumericalValue(bucketRestriction, position);
                };
                case 'Enum': {
                    return getEnumValues(bucketRestriction);
                };
                default: return 'Unknown';
            }
        }

        this.getAttributeRules = function (bkt, bucket, isSameAttribute) {
            // console.log('Account', bucket, bkt);
            var isSameBucket = true;
            if (bucket && bucket.Vals !== undefined && bucket.Vals != null && bkt.Vals !== undefined && bkt.Vals != null) {
                if(bucket.Vals.length == bkt.Vals.length){
                    var valsEquals = true;
                    for(var i = 0; i<bucket.Vals.length; i++){
                        if(bkt.Vals[i] !== bucket.Vals[i]){
                            valsEquals = false;
                            break;
                        }
                    }
                    isSameBucket = valsEquals && bkt.Cmp == bucket.Cmp;
                }
            }
            
            var r = isSameAttribute && isSameBucket;
            return r;
        }

        //******************** Editing mode *********************************/
        this.changeBooleanValue = function (bucketRestriction, booleanValue) {
            if (!bucketRestriction.bkt.Vals[0]) {
                bucketRestriction.bkt.Vals[0] = null;
            } else {
                bucketRestriction.bkt.Vals[0] = booleanValue;
            }
        }
        this.changeEnumCmpValue = function (bucketRestriction, value) {
            bucketRestriction.bkt.Cmp = value;
        }
        this.changeNumericalCmpValue = function (bucketRestriction, value) {
            bucketRestriction.bkt.Cmp = value;
        }

        this.changeBktValue = function (bucketRestriction, value, position) {
            bucketRestriction.bkt.Vals[position] = value;
        }

        this.getBooleanModel = function (bucketRestriction) {
            return bucketRestriction.bkt.Vals[0];
        }
        this.getEnumCmpModel = function (bucketRestriction) {
            return bucketRestriction.bkt.Cmp;
        }

        this.getNumericalCmpModel = function (bucketRestriction) {
            return bucketRestriction.bkt.Cmp;
        }

        this.getBktValue = function (bucketRestriction, position) {
            return bucketRestriction.bkt.Vals[position];
        }

        this.getCubeBktList = function (cube) {
            return cube.Bkts.List;
        }


        //*******************************************************************/

    })
    .service('QueryTreePurchaseHistoryService', function () {

        /**
         * type is 'TimeSeries'
         * How to identify 'Boolean':
         * bkt: {
         *      Txn:{
         *          Negate: false/true
         *          Time: {
         *              Cmp: "EVER"
         *              Period: "Day"
         *              Vals: []
         *          }
         *      }
         * }
         * 
         * ///////
         * bkt: {
         *      Txn:{
         *          Qty: {
         *              Cmp: "LESS_THAN"
         *              Vals:[1]
         *          }
         *          Time:{
         *              Cmp: ""
         *              Period: "EVER"
         *              Vals: []
         *          }
         *      }
         * }
         * ////
         * bkt: {
         *      Txn:{
         *          Amt: {
         *              Cmp: ""
         *              Vals:[2]
         *          }
         *          Time:{
         *              Cmp: ""
         *              Period: "EVER"
         *              Vals: []
         *          }
         *      }
         * }
         * How to identify 'Numerical'
         * 
         * How to identify 'Enum'
         * 
         * @param {*} bucketRestriction 
         * @param {*} type 
         * @param {*} typeToShow 
         */
        this.showType = function (bucketRestriction, type, typeToShow) {
            // console.log(bucketRestriction, ' - TO SHOW: ', typeToShow, ' - TYPE: ', type);
            if ('TimeSeries' === type) {

                switch (typeToShow) {
                    case 'Boolean': {
                        var txn = bucketRestriction.bkt.Txn;
                        if (txn.Negate != undefined) {
                            return true;
                        } else {
                            return false;
                        }

                    };
                    case 'Date':{
                        var time = bucketRestriction.bkt.Txn.Time;
                        if (time != undefined) {
                            return true;
                        } else {
                            return false;
                        }
                    }

                    case 'Numerical': {
                        return false;
                    };

                    case 'Enum': {
                        return false;
                    };
                    default: return false;
                }
            }
        }

        this.showTo = function (bucketRestriction, two_inputs) {
            if (two_inputs.indexOf(bucketRestriction.bkt.Cmp) >= 0) {
                return true;
            } else {
                return false;
            }
        }
        /**
         * Return the operation label for and Account or Contacts Entity
         * @param {*} cmpMap 
         * @param {*} type 
         * @param {*} bucketRestriction 
         */
        this.getOperationLabel = function (cmpMap, type, bucketRestriction) {
            if (!bucketRestriction.bkt) {
                return;
            }

            switch (type) {
                case 'TimeSeries': {
                    var txn = bucketRestriction.bkt.Txn;
                    var cmp = '';
                    if (txn.Negate !== undefined) {
                        cmp = 'is';
                    }
                    else if (txn.Amt) {
                        cmp = txn.Amt.Cmp;
                    } else {
                        cmp = txn.Time.Cmp;
                    }
                    // console.log('CMP', cmp);
                    var ret = cmpMap[cmp];
                    // console.log('RET', ret);
                    return ret;
                };
                default: return 'has a value of';
            }
        }

        function getBooleanValue(bucketRestriction) {
            if (bucketRestriction.bkt.Txn.Negate === true) {
                return 'False';
            } if (bucketRestriction.bkt.Txn.Negate === false) {
                return 'True';
            }
            return 'Empty';
        }

        function getDateValue(bucketRestriction){
            var time = bucketRestriction.bkt.Txn.Time;
            if(time !== undefined){
                return 'Ever';
                // return time.Period; This is the value that can be edited in the future
            }else{
                return 'Ever';
            }
        }

        function getNumericalValue(bucketRestriction, position) {
            return bucketRestriction.bkt.Vals[position];
        }

        function getEnumValues(bucketRestriction) {
            return bucketRestriction.bkt.Vals;
        }

        this.getOperationValue = function (bucketRestriction, operatorType, position) {
            // console.log('Operation Value', operatorType, position);
            switch (operatorType) {
                case 'Boolean': {
                    return getBooleanValue(bucketRestriction);
                };
                case 'Date': {
                    return getDateValue(bucketRestriction);
                };
                case 'Numerical': {
                    return getNumericalValue(bucketRestriction, position);
                };
                case 'Enum': {
                    return getEnumValues(bucketRestriction);
                };
                default: return 'Unknown';
            }
        }

        this.getAttributeRules = function (bkt, bucket, isSameAttribute) {
            // console.log('PurchaseHistory');
            var isSameBucket = true;
            if (bucket && bucket.Txn) {
                var qty1 = bucket.Txn.Qty;
                var amt1 = bucket.Txn.Amt;
                var qty2 = bkt.Txn.Qty;
                var amt2 = bkt.Txn.Amt;

                if (!qty1 && !amt1 && !qty2 && !qty2) {
                    var txn1 = bucket.Txn;
                    var txn2 = bkt.Txn;
                    var neg1 = txn1.Negate;
                    var neg2 = txn1.Negate;
                    var lbl1 = bucket.Lbl;
                    var lbl2 = bkt.Lbl;
                    isSameBucket = neg1 == neg2 && lbl1 == lbl2;
                }
            } 
            var r = isSameAttribute && isSameBucket;
            return r;
        }

        //******************** Editing mode *********************************/
        this.changeBooleanValue = function (bucketRestriction, booleanValue) {
            var txn = bucketRestriction.bkt.Txn;
            if (txn != undefined) {
                if ('Yes' === booleanValue) {
                    txn.Negate = false;
                    bucketRestriction.bkt.Lbl = 'Yes';
                } else if ('No' === booleanValue) {
                    txn.Negate = true;
                    bucketRestriction.bkt.Lbl = 'No';
                } else {
                    txn.Negate = null;
                    bucketRestriction.bkt.Lbl = 'Undefined';
                }

            }
        }
        this.changeEnumCmpValue = function (bucketRestriction, value) {
            bucketRestriction.bkt.Cmp = value;
        }
        this.changeNumericalCmpValue = function (bucketRestriction, value) {
            bucketRestriction.bkt.Cmp = value;
        }

        this.changeBktValue = function (bucketRestriction, value, position) {
            bucketRestriction.bkt.Vals[position] = value;
        }

        this.getBooleanModel = function (bucketRestriction) {
            var txn = bucketRestriction.bkt.Txn;
            if (txn.Negate != undefined) {
                if (txn.Negate === true) {
                    return 'No';
                } if (txn.Negate === false) {
                    return 'Yes';
                }
                return '';

            } else {
                console.warn('Buket restirction with Boolean Value not set');
                return '?';
            }
        }

        this.getEnumCmpModel = function (bucketRestriction) {
            return bucketRestriction.bkt.Cmp;
        }

        this.getNumericalCmpModel = function (bucketRestriction) {
            return bucketRestriction.bkt.Cmp;
        }

        this.getBktValue = function (bucketRestriction, position) {
            var txn = bucketRestriction.bkt.Txn;
            if (txn.Negate !== undefined) {
                return (txn.Negate === true) ? 'No' : 'Yes';
            }
            if (txn.Qty) {
                return txn.Qry.Vals[position];
            }
            if (txn.Amt) {
                return txn.Amt.Vals[position];
            }
            return '';
        }

        this.getCubeBktList = function (cube) {
            return cube.Bkts.List;
        }


    });