'use strict';

const fs = require('fs-extra');

// json file path to two daily aggregate files that will be compared together
module.exports = (dailyPath, expectedDailyPath) => {
    console.log(`Result daily JSON path = ${dailyPath}, Expected result JSON path = ${expectedDailyPath}`);

    let dailyMap = fs.readJSONSync(dailyPath).dailyAggMap;
    let expectedDailyMap = fs.readJSONSync(expectedDailyPath).dailyAggMap;

    let hasDiff = false;
    for (let d in dailyMap) {
        if (!(d in expectedDailyMap)) {
            hasDiff = true;
            console.log(`DateId=${d} is in result but not in expected`);
        }

        let res = dailyMap[d];
        let exp = expectedDailyMap[d];
        for (let key in dailyMap[d]) {
            if (dailyMap[d][key] !== expectedDailyMap[d][key]) {
                hasDiff = true;
                console.log(`DateId=${d}, Key=${key}, Result=${dailyMap[d][key]}, Expected=${expectedDailyMap[d][key]}`)
            }
        }

        for (let key in dailyMap[d]) {
            if (!(key in dailyMap[d])) {
                hasDiff = true;
                console.log(`DateId=${d}, Key=${key}, Result=${dailyMap[d][key]}, Expected=${expectedDailyMap[d][key]}`)
            }
        }
    }

    for (let d in expectedDailyMap) {
        if (!(d in dailyMap)) {
            hasDiff = true;
            console.log(`DateId=${d} is in expected but not in result`);
        }
    }

    if (!hasDiff) {
        console.log(`daily aggregates are the same as expected, # keys (days) = ${Object.keys(dailyMap).length}`);
    }
}
