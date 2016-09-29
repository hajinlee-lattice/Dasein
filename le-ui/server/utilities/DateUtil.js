'use strict';

module.exports.getTimeStamp = function (ts) {
    ts = ts || new Date();

    return (ts.getMonth() + 1) + '/' + ts.getDate() + '/' + ts.getFullYear() + ' ' +
          ts.getHours() + ':' + ts.getMinutes() + ':' + ts.getSeconds();
};
