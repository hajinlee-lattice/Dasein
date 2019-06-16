angular.module('common.datacloud.query.builder.tree.purchasehistory.service', ['common.datacloud.query.builder.tree.edit.percent'])
    .service('QueryTreePurchaseHistoryService', function (PercentStore) {

        this.cmpMap = {
            'EQUAL': 'is equal to',
            'NOT_EQUAL': 'is not equal to',
            'IN_COLLECTION': 'is equal to',
            'NOT_IN_COLLECTION': 'is not',
            'GREATER_THAN': 'is greater than',
            'GREATER_OR_EQUAL': 'is greater than or equal to',
            'LESS_THAN': 'is less than',
            'LESS_OR_EQUAL': 'is less than or equal to',
            'GTE_AND_LTE': 'is greater than or equal and lesser than or equal',
            'GTE_AND_LT': 'is between',
            'GT_AND_LT': "is greater than and less than",
            'IS_NULL': 'is empty',
            'IS_NOT_NULL': 'is present'
        };

        function setValsBasedOnPosition(cmp, valsArray, position, value) {
            switch (cmp) {
                case 'GTE_AND_LT':
                case 'BETWEEN':
                case 'BETWEEN_DATE': {
                    valsArray[position] = value;
                    break;
                }
                default: {
                    valsArray[0] = value;
                }
            }
        }
        function getValsBasedOnPosition(cmp, valsArray, position) {
            switch (cmp) {
                case 'GTE_AND_LT':
                case 'BETWEEN':
                case 'BETWEEN_DATE':
                case 'EQUAL':
                case 'NOT_EQUAL': {
                    return valsArray[position];
                }
                case 'GREATER_THAN':
                case 'GREATER_OR_EQUAL':
                case 'AFTER': {
                    if (position == valsArray.length - 1 || valsArray.length == 0) {
                        return valsArray[valsArray.length - 1];
                    } else {
                        return null;
                    }
                }
                case 'LESS_THAN':
                case 'LESS_OR_EQUAL':
                case 'BEFORE':
                case 'WITHIN':
                case 'PRIOR_ONLY': {
                    if (position == valsArray.length - 1 || valsArray.length == 0) {
                        return null;
                    } else {
                        return valsArray[valsArray.length - 1];
                    }
                }
                default: {
                    return null;
                }
            }
        }
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
                    case 'Date':
                    case 'Numerical':
                    case 'Enum': {
                        return false;
                    }
                    case 'Transaction': {
                        return true;
                    }
                    default: return false;
                }
            } else if ('PercentChange' === type) { //TO DO: change this with back end value
                // console.log('IS AverageSeries');
                switch (typeToShow) {
                    case 'Percent': {
                        return true;
                    }
                    default: {
                        return false;
                    }
                }
            } else {
                switch (typeToShow) {
                    case 'Numerical': {
                        return true;
                    }
                    default: {
                        return false;
                    }
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
            // console.log('The op label ', type, '  ',cmpMap, '      ',bucketRestriction);
            if (!bucketRestriction.bkt) {
                return;
            }

            //console.log('[transaction-service] getOperationLabel()', type, bucketRestriction.bkt.Cmp, cmpMap[bucketRestriction.bkt.Cmp]);

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
                }
                case 'PercentChange': {
                    return PercentStore.getCmpRedable(bucketRestriction);
                }
                case 'Numerical': {
                    var numRet = this.cmpMap[bucketRestriction.bkt.Cmp];
                    return numRet;
                }

                default: return 'has a value of';
            }
        }

        function getBooleanValue(bucketRestriction) {
            if (bucketRestriction.bkt.Txn) {
                return !bucketRestriction.bkt.Txn.Negate;
            } else {
                return 'Empty';
            }
        }

        function getDateValue(bucketRestriction) {
            var time = bucketRestriction.bkt.Txn.Time;
            if (time !== undefined) {
                return 'Ever';
                // return time.Period; This is the value that can be edited in the future
            } else {
                return 'Ever';
            }
        }

        function getNumericalValue(bucketRestriction, position) {
            if (bucketRestriction.ignored === false) {
                return bucketRestriction.bkt.Vals[position];
            }
        }

        function getEnumValues(bucketRestriction) {
            if (bucketRestriction.ignored === false) {
                return bucketRestriction.bkt.Vals;
            }
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

        function sameValues(vals1, vals2) {
            var sameVals = true;
            if (vals1 && vals2 && vals1.length == vals2.length) {
                var index = 0;
                vals1.forEach(function (val) {
                    if (val != vals2[index]) {
                        sameVals = false;
                        return;
                    }
                    index++;
                });
            } else {
                sameVals = false;
            }
            return sameVals;
        }

        this.getAttributeRules = function (bkt, bucket, isSameAttribute) {
            // console.log('PurchaseHistory');
            var isSameBucket = true;
            if (bucket && bucket.Txn && bkt && bkt.Txn) {
                var qty1 = bucket.Txn.Qty;
                var qty2 = bkt.Txn.Qty;
                var amt1 = bucket.Txn.Amt;
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
            } else if (bucket && bucket.Chg && bkt && bkt.Chg) {
                var direction1 = bucket.Chg.Direction;
                var direction2 = bkt.Chg.Direction;
                var cmp1 = bucket.Chg.Cmp;
                var cmp2 = bkt.Chg.Cmp;

                isSameBucket = direction1 == direction2 && cmp1 == cmp2;
                if (isSameBucket) {
                    isSameBucket = sameValues(bucket.Chg.Vals, bkt.Chg.Vals);
                }
            } else {
                if (bucket && bucket.Vals !== undefined && bucket.Vals != null && bkt.Vals !== undefined && bkt.Vals != null) {
                    var tmp = bkt.Vals[0] == bucket.Vals[0] && bkt.Vals[1] == bucket.Vals[1] && bkt.Cmp == bucket.Cmp && bkt.Direction == bucket.Direction;
                    isSameBucket = tmp;
                }
            }
            var r = isSameAttribute && isSameBucket;
            return r;
        }

        this.isBucketUsed = function (bucket) {
            return typeof bucket.bkt.Id == "number";//typeof bucket.bkt.Id == "number" && bucket.bkt.Vals && bucket.bkt.Vals.length > 0;
        }

        /**
         * type: TimeSeries
         * @param {*} bucketRestriction 
         * @param {*} type 
         */
        this.getBktVals = function (bucketRestriction, type) {
            if (bucketRestriction.bkt.Txn) {
                var txn = bucketRestriction.bkt.Txn;
                switch (type) {
                    case 'TimeSeries': {
                        var val = getBooleanValue(bucketRestriction);
                        var vals = [val];
                        return vals;
                    }
                    default: {
                        return [];
                    }
                }
            } else if (bucketRestriction.bkt.Chg) {
                switch (type) {
                    case 'PercentChange': {
                        var values = bucketRestriction.bkt.Chg.Vals;
                        if (values) {
                            var listPct = values.toString();
                            listPct = listPct.replace(/,/g, ' - ');
                            return listPct;
                        } else {
                            return '';
                        }
                    }
                    default: {
                        return [];
                    }
                }
            } else {
                var vals = bucketRestriction.bkt;
                if (vals) {
                    var list = vals.toString();
                    list = list.replace(/,/g, ' - ');
                    return list;
                } else {
                    return '';
                }
            }
        }

        this.getValue = function (bucketRestriction, type, position, subType) {
            if (type === 'TimeSeries' && bucketRestriction.bkt.Txn) {
                var txn = bucketRestriction.bkt.Txn;
                switch (subType) {
                    case 'Time': {
                        var tsTime = txn.Time;
                        if (tsTime) {
                            return getValsBasedOnPosition(tsTime.Cmp, tsTime.Vals, position);
                        }
                        // if (tsTime && tsTime.Vals && position <= tsTime.Vals.length - 1) {
                        //     return tsTime.Vals[position];
                        // }
                    }
                    case 'Qty': {
                        var qty = txn.Qty;
                        if (qty) {
                            return getValsBasedOnPosition(qty.Cmp, qty.Vals, position);
                        }
                    }
                    case 'Amt': {
                        var amt = txn.Amt;
                        if (amt) {
                            return getValsBasedOnPosition(amt.Cmp, amt.Vals, position);
                        }
                    }

                    default: {
                        return 0;
                    }
                }
            } else if (type === 'PercentChange' && bucketRestriction.bkt.Chg) {
                return getValsBasedOnPosition(bucketRestriction.bkt.Chg.Cmp, bucketRestriction.bkt.Chg.Vals, position);
            } else {
                return getValsBasedOnPosition(bucketRestriction.bkt.Cmp, bucketRestriction.bkt.Vals, position);
            }
        }

        this.getValues = function (bucketRestriction, type, subType) {
            if (type === 'TimeSeries' && bucketRestriction.bkt.Txn) {
                var txn = bucketRestriction.bkt.Txn;
                switch (subType) {
                    case 'Time': {
                        var tsTime = txn.Time;
                        if (tsTime && tsTime.Vals) {
                            return tsTime.Vals;
                        }
                        return [];
                    }
                    case 'Qty': {
                        var qty = txn.Qty;
                        if (qty && qty.Vals) {
                            return qty.Vals;
                        }
                        return [];
                    }
                    case 'Amt': {
                        var amt = txn.Amt;
                        if (amt && amt.Vals) {
                            return amt.Vals;
                        }
                        return [];
                    }
                    default: {
                        return [];
                    }
                }
            } else if (type === 'PercentChange' && bucketRestriction.bkt.Chg) {
                return bucketRestriction.bkt.Chg.Vals;
            } else {
                return bucketRestriction.bkt.Vals;
            }
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
            // console.log('changeNumericalCmpValue', bucketRestriction, value, bucketRestriction.bkt.Vals);
            bucketRestriction.bkt.Cmp = value;
        }

        this.changeBktValue = function (bucketRestriction, value, position) {
            // var cmp = bucketRestriction.bkt.Cmp;
            // var vals = bucketRestriction.bkt.Vals;
            setValsBasedOnPosition(bucketRestriction.bkt.Cmp, bucketRestriction.bkt.Vals, position, value);
            // bucketRestriction.bkt.Vals[position] = value;
        }

        this.changeValue = function (bucketRestriction, type, value, position, subType) {
            // console.log(bucketRestriction, type, value, position, subType)
            if (type === 'TimeSeries') {
                var txn = bucketRestriction.bkt.Txn;
                if (txn) {
                    switch (subType) {
                        case 'Time': {
                            var tsTime = txn.Time;
                            if (tsTime && value !== undefined) {
                                setValsBasedOnPosition(tsTime.Cmp, tsTime.Vals, position, value);
                                // tsTime.Vals[position] = value;
                            }
                            break;
                        }
                        case 'Qty': {
                            var qty = txn.Qty;
                            if (qty && value !== undefined) {
                                setValsBasedOnPosition(qty.Cmp, qty.Vals, position, value);
                            }
                            // console.log('Changed ', bucketRestriction);
                            break;
                        }
                        case 'Amt': {
                            var amt = txn.Amt;
                            if (amt && value !== undefined) {
                                setValsBasedOnPosition(amt.Cmp, amt.Vals, position, value);
                                // amt.Vals[position] = value;
                            }
                            break;
                        }
                    }

                }
            } else if (type === 'PercentChange' && bucketRestriction.bkt.Chg) {
                setValsBasedOnPosition(bucketRestriction.bkt.Chg.Cmp, bucketRestriction.bkt.Chg.Vals, position, value);
            } else {
                setValsBasedOnPosition(bucketRestriction.bkt.Cmp, bucketRestriction.bkt.Vals, position, value);
            }
        }

        this.changeVals = function (bucketRestriction, value) {
            // console.log('[transaction-service] changeVals()', value, bucketRestriction);
            bucketRestriction.bkt.Vals = value;
        }

        this.changeTimeframePeriod = function (bucketRestriction, type, value) {
            switch (type) {
                case 'TimeSeries': {
                    var txn = bucketRestriction.bkt.Txn;
                    if (txn) {
                        var tsTime = txn.Time;
                        if (tsTime) {
                            tsTime.Period = value;
                        }
                    }
                };

                // default: return '';
            }
        }

        this.changeCmp = function (bucketRestriction, type, value, subType) {
            if (type === 'TimeSeries') {
                var txn = bucketRestriction.bkt.Txn;
                // console.log('Change Cmp', value, subType);
                if (txn) {
                    switch (subType) {
                        case 'Time': {
                            var tsTime = txn.Time;
                            if (tsTime) {
                                tsTime.Cmp = value;
                            } else {
                                txn[subType] = {
                                    Cmp: value,
                                    Period: "Month",
                                    Vals: []
                                };
                            }
                            break;
                        }
                        case 'Qty': {
                            var qty = txn.Qty;
                            if (qty) {
                                qty.Cmp = value;
                            } else {
                                txn[subType] = {
                                    Cmp: value,
                                    Vals: []
                                };
                            }
                            break;
                        }
                        case 'Amt': {
                            var amt = txn.Amt;
                            if (amt) {
                                amt.Cmp = value;
                            } else {
                                txn[subType] = {
                                    Cmp: value,
                                    Vals: []
                                };
                            }
                            break;
                        }
                    }

                }
            }
            else if (type === 'PercentChange' && bucketRestriction.bkt.Chg) {
                bucketRestriction.bkt.Chg.Cmp = value;
            } else {
                bucketRestriction.bkt.Cmp = value;
            }
        }

        this.changeCmpValue = this.changeCmp;

        this.removeKey = function (bucketRestriction, type, subType) {
            if (type === 'TimeSeries') {
                var txn = bucketRestriction.bkt.Txn;
                // console.log('Change Cmp', value, subType);
                if (txn) {
                    switch (subType) {
                        case 'Qty': {
                            delete txn.Qty;
                            break;
                        }
                        case 'Amt': {
                            delete txn.Amt;
                            break;
                        }
                    }

                }
            }
        }
        this.resetBktValues = function (bucketRestriction, type, subType) {
            if (type === 'TimeSeries') {
                var txn = bucketRestriction.bkt.Txn;
                // console.log('Change Cmp', value, subType);
                if (txn) {
                    switch (subType) {
                        case 'Time': {
                            var t = txn.Time;
                            if (t) {
                                t.Vals = [];
                            }
                            break;
                        }
                        case 'Qty': {
                            var qty = txn.Qty;
                            if (qty) {
                                qty.Vals = [];
                            }

                            break;
                        }
                        case 'Amt': {
                            var amt = txn.Amt;
                            if (amt) {
                                amt.Vals = [];
                            }
                            break;
                        }
                    }

                }
            } else if (type === 'PercentChange' && bucketRestriction.bkt.Chg) {
                bucketRestriction.bkt.Chg.Vals = [];
            } else {
                bucketRestriction.bkt.Vals = [];
            }
        }

        this.getBooleanModel = function (bucketRestriction) {
            var txn = bucketRestriction.bkt.Txn;
            if (txn && txn.Negate != undefined) {
                if (txn.Negate === true) {
                    return 'No';
                } if (txn.Negate === false) {
                    return 'Yes';
                }
                return '';

            } else {
                // console.warn('Buket restirction with Boolean Value not set');
                return '';
            }
        }

        this.getEnumCmpModel = function (bucketRestriction) {
            return bucketRestriction.bkt.Cmp;
        }

        this.getNumericalCmpModel = function (bucketRestriction) {
            return bucketRestriction.bkt.Cmp;
        }
        this.getStringCmpModel = function (bucketRestriction) {
            return bucketRestriction.bkt.Cmp;
        }

        this.getBktValue = function (bucketRestriction, position) {
            var txn = bucketRestriction.bkt.Txn;
            if (txn) {
                if (txn.Negate !== undefined) {
                    return (txn.Negate === true) ? 'No' : 'Yes';
                }
                if (txn.Qty) {
                    return txn.Qry.Vals[position];
                }
                if (txn.Amt) {
                    return txn.Amt.Vals[position];
                }
            } else {
                return getValsBasedOnPosition(bucketRestriction.bkt.Cmp, bucketRestriction.bkt.Vals, position);
            }
        }

        this.getCubeBktList = function (cube) {
            return cube.Bkts.List;
        }

        function getTransacionCmp(bucketRestriction) {
            if (bucketRestriction && bucketRestriction.bkt && bucketRestriction.bkt.Txn && bucketRestriction.bkt.Txn.Time) {
                return bucketRestriction.bkt.Txn.Time.Cmp;
            } else {
                console.warn('Transaction attribute does not contain Cmp')
                return '';
            }
        }

        this.getCmp = function (bucketRestriction, type) {
            // console.log('TYPE', type);
            switch (type) {
                case 'TimeSeries': {
                    return getTransacionCmp(bucketRestriction);
                };
                case 'PercentChange': {
                    return bucketRestriction.bkt.Chg;
                }

                default: return bucketRestriction.bkt.Cmp;
            }
        }

        this.getCmp = function (bucketRestriction, type, subType) {
            switch (type) {
                case 'TimeSeries': {
                    if (bucketRestriction.bkt.Txn !== undefined) {
                        var txn = bucketRestriction.bkt.Txn;
                        if (subType === 'Time' && txn !== undefined) {
                            return txn.Time.Cmp;
                        }
                        if (subType === 'Qty' && txn.Qty !== undefined) {
                            return txn.Qty.Cmp;
                        }
                        if (subType === 'Amt' && txn.Amt !== undefined) {
                            return txn.Amt.Cmp;
                        }
                        return '';
                    } else {
                        console.warn('TimeSeries does not have Txn object');
                    }
                };
                case 'PercentChange': {
                    return bucketRestriction.bkt.Chg.Cmp;
                }
                default: return bucketRestriction.bkt.Cmp;
            }
        }

        this.getPeriodValue = function (bucketRestriction, type, subType) {
            if (type === 'TimeSeries' && subType !== undefined) {
                var txn = bucketRestriction.bkt.Txn;
                switch (subType) {
                    case 'Time': {
                        return txn.Time.Period;
                    }
                    default:
                        return '';
                }
            } else {
                return '';
            }
        }
    });