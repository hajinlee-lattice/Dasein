'use strict';

const fs = require('fs-extra');
const path = require('path');

const AvroReader = require('./avro-reader');
const DateUtil = require('./date-util');

const WILDCARD = /(?<!\.)\*/;

module.exports = async (request) => {
    let format = request.format;
    let src = request.src; // dir for avro, file for csv
    let isCsv = (format || '').toLowerCase() === 'csv';
    let dst = request.dst; // dst daily agg json file path
    let dimPath = request.dimPath; // dimension JSON file path

    // instantiation
    console.log(`Format=${format}, src=${src}, dst=${dst}, dimMetadataPath=${dimPath}`);

    fs.ensureDirSync(path.dirname(dst));
    let metadata = fs.readJSONSync(dimPath);
    let smMap = {};
    let ptnMap = {};
    for (let ptn of metadata.PathPatternId.dimension_values) {
        ptnMap[ptn.PathPattern.replace(WILDCARD, '.*')] = ptn.PathPatternId;
    }
    for (let sm of metadata.SourceMediumId.dimension_values) {
        smMap[sm.SourceMedium] = sm.SourceMediumId;
    }

    let reader = new AvroReader();
    let rawArr = isCsv ? await csv().fromFile(src) : await reader.readDir(src);

    let dailyAggMap = {};
    let records = {};

    for (let row of rawArr) {
        let d = DateUtil.toDateId(row.WebVisitDate);
        let url = row.WebVisitPageUrl;
        let src = row.SourceMedium;
        let accId = row.AccountId;
        let user = row.UserId;

        if (!(d in dailyAggMap)) {
            dailyAggMap[d] = {};
        }
        if (!(d in records)) {
            records[d] = {};
        }
        if (src in smMap) {
            src = smMap[src];
        } else {
            src = null;
        }

        let matched = false;
        for (let ptn in ptnMap) {
            let r = new RegExp('^' + ptn + '$');
            if (r.test(url)) {
                matched = true;
                let key = `PtnId=${ptnMap[ptn]},SM=${src},AccId=${accId},UserId=${user}`;

                if (!(key in records[d])) {
                    records[d][key] = [];
                }
                records[d][key].push(row);
                dailyAggMap[d][key] = (dailyAggMap[d][key] || 0) + 1;
            }
        }
        if (!matched) {
            let key = `PtnId=${null},SM=${src},AccId=${accId},UserId=${user}`;
            dailyAggMap[d][key] = (dailyAggMap[d][key] || 0) + 1;
            if (!(key in records[d])) {
                records[d][key] = [];
            }
            records[d][key].push(row);
        }
    }

    let output = {
        dailyAggMap,
        records
    };
    fs.writeFileSync(dst, JSON.stringify(output));
}
