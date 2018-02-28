angular.module('common.datacloud.query.builder.tree.purchasehistory.service', [])
    .service('QueryTreePurchaseHistoryService', function () {

        function setValsBasedOnPosition(cmp, valsArray, position, value) {
            switch (cmp) {
                case 'GTE_AND_LT': {
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
                case 'GTE_AND_LT': {
                    return valsArray[position];
                    break;
                }
                case 'GREATER_THAN':
                case 'GREATER_OR_EQUAL': {
                    if (position == valsArray.length - 1 || valsArray.length == 0) {
                        return valsArray[valsArray.length - 1];
                    } else {
                        return null;
                    }
                    break;
                }
                case 'LESS_THAN':
                case 'LESS_OR_EQUAL': {
                    if (position == valsArray.length - 1 || valsArray.length == 0) {
                        return null;
                    } else {
                        return valsArray[valsArray.length - 1];
                    }
                    break;
                    ;
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
            }
        }

        this.getValue = function (bucketRestriction, type, position, subType) {
            if (type === 'TimeSeries' && bucketRestriction.bkt.Txn) {
                var txn = bucketRestriction.bkt.Txn;
                switch (subType) {
                    case 'Time': {
                        var tsTime = txn.Time;
                        if (tsTime && tsTime.Vals && position <= tsTime.Vals.length - 1) {
                            return tsTime.Vals[position];
                        }
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
                    }
                    case 'Qty': {
                        var qty = txn.Qty;
                        if (qty && qty.Vals) {
                            return qty.Vals;
                        }
                    }
                    case 'Amt': {
                        var amt = txn.Amt;
                        if (amt && amt.Vals) {
                            return amt.Vals;
                        }
                    }
                    default: {
                        return [];
                    }
                }
            } else {
                return [];
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
            bucketRestriction.bkt.Cmp = value;
        }

        this.changeBktValue = function (bucketRestriction, value, position) {
            bucketRestriction.bkt.Vals[position] = value;
        }

        this.changeValue = function (bucketRestriction, type, value, position, subType) {
            if (type === 'TimeSeries') {
                var txn = bucketRestriction.bkt.Txn;
                if (txn) {
                    switch (subType) {
                        case 'Time': {
                            var tsTime = txn.Time;
                            if (tsTime && value !== undefined) {
                                tsTime.Vals[position] = value;
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
            }
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
        }

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
                            console.warn('Not implemented');
                            break;
                        }
                        case 'Qty': {
                            var qty = txn.Qty;
                            if (qty) {
                                qty.Vals = [];
                            }
                            console.log('RESET Vals ', bucketRestriction);

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
            }
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

                default: return '';
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
                default: return '';
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