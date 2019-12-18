'use strict';

const fs = require('fs-extra');
const AvroReader = require('./avro-reader');
const DateUtil = require('./date-util');

// avroDir: avro path of daily store avro files
// dst: destination daily agg json file path
module.exports = async (avroDir, dst) => {
    console.log(`Daily aggregates avro dir path = ${avroDir}, Dest JSON Path = ${dst}`);

    let reader = new AvroReader();
    let dailyArr = await reader.readDir(avroDir);

    let dailyAggMap = {};
    let records = {};

    for (let row of dailyArr) {
        let date = row['__StreamDate'];
        let tokens = date.split('-');
        let d = DateUtil.getDateId(parseInt(tokens[0]), parseInt(tokens[1]), parseInt(tokens[2]));

        let ptnId = row.PathPatternId || null;
        let src = row.SourceMediumId || null;
        let accId = row.AccountId;
        let user = row.UserId;

        if (!(d in dailyAggMap)) {
            dailyAggMap[d] = {};
        }
        let key = `PtnId=${ptnId},SM=${src},AccId=${accId},UserId=${user}`;
        dailyAggMap[d][key] = row['__Row_Count__'];
        if (!(d in records)) {
            records[d] = [];
        }
        records[d].push(row);
    }

    let output = {
        dailyAggMap,
        records
    };
    fs.writeFileSync(dst, JSON.stringify(output));
}
