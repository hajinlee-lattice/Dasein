'use strict';

const uuidv4 = require('uuid/v4');
const casual = require('casual');
const moment = require('moment');
const commander = require('commander');
const fs = require('fs-extra');
const createCsvWriter = require('csv-writer').createArrayCsvWriter;

const DATA_DIR = './data';
const DEFAULT_NUM_RECORDS = 1000000;
const DEFAULT_RECORD_OPP_RATIO = 5;
const DEFAULT_RECORD_ACC_RATIO = 100;
const DEFAULT_NUM_STAGES = 10;
const DEFAULT_FIRST_LOAD_END_DATE = '2017-07-01';
const DEFAULT_INCREMENTAL_END_DATE = '2017-08-01';
const DEFAULT_INTERVAL_DAYS = 90; // 90 days

commander
    .version('0.0.1')
    .description('Genereate test data for opportunity data')
    .arguments('<nRecords>')
    .option('-i, --incremental', 'Generate test data for incremental scenarios')
    .option('-d, --interval-days [intervalDays]', 'Number of days which test data will be generated within')
    .action((nRecords) => {
        generateTestData({
            nRecords,
            intervalDays: commander.intervalDays,
            isIncremental: commander.incremental,
        });
    });

commander.parse(process.argv);



/* private functions */

function generateTestData(options) {
    options = options || {};
    let isIncremental = !!options.isIncremental;
    let nRecords = parseInt(options.nRecords) || DEFAULT_NUM_RECORDS;
    let nDaysInInterval = parseInt(options.intervalDays) || DEFAULT_INTERVAL_DAYS;
    let numStages = DEFAULT_NUM_STAGES;
    let numAccounts = nRecords / DEFAULT_RECORD_ACC_RATIO;
    let numOpportunities = nRecords / DEFAULT_RECORD_OPP_RATIO;
    let endDateStr = isIncremental ? DEFAULT_INCREMENTAL_END_DATE : DEFAULT_FIRST_LOAD_END_DATE;

    const stages = stageNames(numStages);
    const oppIds = opportunityIds(numOpportunities);
    const accIds = accountIds(numAccounts);
    const oppToAccMap = oppAccMap(oppIds, accIds);
    const endDate = new Date(endDateStr);
    const startDate = new Date(endDate.getTime() - nDaysInInterval * 86400 * 1000);

    console.log(`Generating opportunity test data, numRecords=${nRecords}, numAccounts=${numAccounts}, startDate=${startDate}, endDate=${endDate}`)

    fs.emptyDirSync(DATA_DIR);

    let counter = 0;
    let filename = `opportunity_${uuidv4()}`;
    let lastStages = {}; // accId -> oppId -> last row
    let records = [];
    let startTime = Date.now();
    for (let i = 0; i < nRecords; i++) {
        let row = opportunityRow(counter++, startDate, endDate, oppIds, oppToAccMap, stages);
        records.push(row);

        let oppId = row[1];
        let accId = row[2];
        let t = new Date(row[3]).getTime();
        if (!lastStages[accId]) {
            lastStages[accId] = {};
        }
        if (!lastStages[accId][oppId] || lastStages[accId][oppId].t <= t) {
            lastStages[accId][oppId] = {
                t,
                stage: row[4]
            };
        }
    }
    let stats = {};
    for (let accId in lastStages) {
        for (let oppId in lastStages[accId]) {
            let obj = lastStages[accId][oppId];
            stats[obj.stage] = (stats[obj.stage] || 0) + 1;
        }
    }


    let csvPath = DATA_DIR + `/${filename}.csv`;
    console.log(`Finished generating test data, start writing to csv, path=${csvPath} time=${Date.now() - startTime}`);
    const csvWriter = createCsvWriter({
        header: [ 'RowId', 'Id', 'AccountId', 'LastModifiedDate', 'StageName' ],
        path: csvPath
    });
    csvWriter
        .writeRecords(records)
        .then(() => console.log(`Finish writing # of records = ${records.length}, time=${Date.now() - startTime}`));

    fs.writeFileSync(`${DATA_DIR}/${filename}.json`, JSON.stringify({
        stats,
        lastStages
    }));
}

function opportunityRow(rowId, startDate, endDate, oppIds, oppToAccMap, stages) {
    return [ rowId, ...oppAccPair(oppIds, oppToAccMap), date(startDate, endDate), casual.random_element(stages) ];
}

function oppAccPair(oppIds, oppToAccMap) {
    let oppId = casual.random_element(oppIds);
    return [ oppId, oppToAccMap[oppId] ];
}

function date(startDate, endDate) {
    let d = randomDate(startDate, endDate);
    return moment(d).toISOString();
}

function oppAccMap(oppIds, accIds) {
    let res = {};
    for (let opp of oppIds) {
        res[opp] = casual.random_element(accIds);
    }
    return res;
}

function stageNames(nStages) {
    let stages = [];
    for (let i = 0; i < nStages; i++) {
        stages.push(`Stage${i}`);
    }
    return stages;
}

function opportunityIds(nOpportunities) {
    let oppIds = [];
    for (let i = 0; i < nOpportunities; i++) {
        oppIds.push(uuidv4());
    }
    return oppIds;
}

function accountIds(nAccounts) {
    let accIds = [];
    for (let i = 0; i < nAccounts; i++) {
        accIds.push(`Acc_${i}`);
    }
    return accIds;
}

function randomDate(start, end) {
    return new Date(start.getTime() + Math.random() * (end.getTime() - start.getTime()));
}
