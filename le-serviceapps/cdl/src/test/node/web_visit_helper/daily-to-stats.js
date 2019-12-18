'use strict';

const fs = require('fs-extra');
const moment = require('moment');
const csv = require('json2csv');
const path = require('path');
const DateUtil = require('./date-util');

const BASE_STATS_DIR = './stats';
const BASE_RECORDS_DIR = './records';
const keyRegex = /PtnId=(.*),SM=(.*),AccId=(.*),UserId=(.*)/;

// dailyPath: daily json file path
// startDateStr: start date (inclusive), format = yyyy-MM-dd
// endDateStr: end date (inclusive), format = yyyy-MM-dd
// destDirname: dir name under records/stats directory
module.exports = (dailyPath, startDateStr, endDateStr, destDirname) => {
    const startDate = moment(startDateStr);
    const endDate = moment(endDateStr);
    if (startDate.isAfter(endDate)) {
        throw new Error(`Start date ${startDate.toString()} should not be later than end date ${endDate.toString()}`);
    }

    fs.ensureDirSync(BASE_STATS_DIR);
    fs.ensureDirSync(BASE_RECORDS_DIR);

    const startDateId = DateUtil.getDateId(...yearMonthDay(startDateStr));
    const endDateId = DateUtil.getDateId(...yearMonthDay(endDateStr));
    const dailyObj = fs.readJSONSync(dailyPath);

    let smStats = {};
    let ptnStats = {};
    let smPtnStats = {};
    let smRecords = {};
    let ptnRecords = {};
    let smPtnRecords = {};

    for (let dateId = startDateId; dateId <= endDateId; dateId++) {
        let d = dateId.toString();

        let agg = dailyObj.dailyAggMap[d];
        for (let key in agg) {
            let obj = parseKey(key);
            if (!obj) {
                continue;
            }
            let ptnId = obj.PtnId;
            let smId = obj.SmId;
            let accId = obj.AccId;
            let count = agg[key];

            let smPtnKey = `PtnId=${ptnId},SmId=${smId}`;
            ensureMap(smStats, smId);
            ensureMap(ptnStats, ptnId);
            ensureMap(smPtnStats, smPtnKey);
            incr(smStats[smId], accId, count);
            incr(ptnStats[ptnId], accId, count);
            incr(smPtnStats[smPtnKey], accId, count);
        }

        let records = dailyObj.records[d];
        for (let key in records) {
            let obj = parseKey(key);
            if (!obj) {
                continue;
            }
            let ptnId = obj.PtnId;
            let smId = obj.SmId;
            let rows = records[key];

            let smPtnKey = `PtnId=${ptnId},SmId=${smId}`;
            for (let row of rows) {
                addToArr(smRecords, smId, row);
                addToArr(ptnRecords, ptnId, row);
                addToArr(smPtnRecords, smPtnKey, row);
            }
        }
    }

    // output to csv/json
    let filename = path.basename(dailyPath, path.extname(dailyPath));
    destDirname = destDirname || filename;
    fs.emptyDirSync(path.join(BASE_STATS_DIR, destDirname));
    let smStatsFilePath = path.join(BASE_STATS_DIR, destDirname, `sm_stats.json`);
    let ptnStatsFilePath = path.join(BASE_STATS_DIR, destDirname, `ptn_stats.json`);
    let smPtnStatsFilePath = path.join(BASE_STATS_DIR, destDirname, `sm_ptn_stats.json`);

    console.log(`Write stats to directory ${path.join(BASE_STATS_DIR, destDirname)}`);
    fs.writeFileSync(smStatsFilePath, getStatsOutputStr(smStats, startDateStr, endDateStr, filename));
    fs.writeFileSync(ptnStatsFilePath, getStatsOutputStr(ptnStats, startDateStr, endDateStr, filename));
    fs.writeFileSync(smPtnStatsFilePath, getStatsOutputStr(smPtnStats, startDateStr, endDateStr, filename));

    fs.emptyDirSync(path.join(BASE_RECORDS_DIR, destDirname));
    console.log(`Write records to directory ${path.join(BASE_RECORDS_DIR, destDirname)}`);

    // record input info for records
    let inputMetadata = {
        dailyInputPath: `${dailyPath}`,
        filename: `${filename}.json`,
        startDate: startDateStr,
        endDate: endDateStr
    };
    let inputMetadataPath = path.join(BASE_RECORDS_DIR, destDirname, `input_metadata.json`);
    fs.writeFileSync(inputMetadataPath, JSON.stringify(inputMetadata));

    // write records to csv
    let csvParser = new csv.Parser();
    for (let smId in smRecords) {
        let filepath = path.join(BASE_RECORDS_DIR, destDirname, `sm_records_id_${smId}.csv`);
        let csvVal = csvParser.parse(smRecords[smId]);
        fs.writeFileSync(filepath, csvVal);
    }
    for (let ptnId in ptnRecords) {
        let filepath = path.join(BASE_RECORDS_DIR, destDirname, `ptn_records_id_${ptnId}.csv`);
        let csvVal = csvParser.parse(ptnRecords[ptnId]);
        fs.writeFileSync(filepath, csvVal);
    }
    for (let key in smPtnRecords) {
        let filepath = path.join(BASE_RECORDS_DIR, destDirname, `sm_ptn_records_${key}.csv`);
        let csvVal = csvParser.parse(smPtnRecords[key]);
        fs.writeFileSync(filepath, csvVal);
    }
}

/* helpers */

function getStatsOutputStr(statsObj, startDateStr, endDateStr, filename) {
    let obj = {
        stats: statsObj,
        startDate: startDateStr,
        endDate: endDateStr,
        filename
    };
    return JSON.stringify(obj);
}

function incr(obj, key, val) {
    obj[key] = (obj[key] || 0) + val;
}

function addToArr(obj, key, val) {
    if (!(key in obj)) {
        obj[key] = [];
    }
    obj[key].push(val);
}

function ensureMap(obj, key) {
    if (!(key in obj)) {
        obj[key] = {};
    }
}

function parseKey(key) {
    let grps = key.match(keyRegex);
    if (!grps) {
        return null;
    }
    return {
        PtnId: grps[1],
        SmId: grps[2],
        AccId: grps[3]
    };
}

function yearMonthDay(dateStr) {
    let tokens = dateStr.split('-');
    return tokens.map(v => parseInt(v));
}
