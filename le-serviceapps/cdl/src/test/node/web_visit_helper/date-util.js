'use strict';

const moment = require('moment');

module.exports = {
    getDateId,
    toDateId,
    fromDateId
};

function getDateId(year, month, day) {
    return (year - 1900) * 13 * 32 + month * 32 + day;
}

function toDateId(timestamp) {
    let d = moment.utc(timestamp);
    let year = d.year();
    let month = d.month() + 1;
    let day = d.date();
    return getDateId(year, month, day);
}

function fromDateId(dateId) {
    if (typeof dateId === 'string') {
        dateId = parseInt(dateId);
    }

    let day = dateId % 32;
    let month = (dateId / 32) % 13;
    let year = 1900 + (dateId / 32 / 13);
    return `${year}-${month}-${day}`;
}
