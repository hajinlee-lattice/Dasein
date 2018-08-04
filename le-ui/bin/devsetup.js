var path = require('path');
var argParser = require('argparse');


console.log('============= Setting up NODE_ENV =============');
console.log('-----------------<process.argv>----------------')
for (let j = 0; j < process.argv.length; j++) {  
    console.log(j + ' -> ' + (process.argv[j]));
}
console.log('-----------------------------------------------')
var envAPI = 'a';
var env = process.argv[2];
if(env && env.indexOf('=' > 0)){
    envAPI = env.substring(env.indexOf('=')+1, env.length);
}
console.log('Using env: ', envAPI);
var pathEnv = path.join(__dirname, ".env"+envAPI);
require('dotenv').config({path: pathEnv})
console.log('=========== NODE_ENV DONE =====================');
console.log('===============================================');
require('../app');