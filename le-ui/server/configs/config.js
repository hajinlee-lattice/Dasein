'use strict';

const path = require('path');
const DateUtil = require('../utilities/DateUtil');

const env_vars = {
    NODE_ENV:   process.env.NODE_ENV    || 'development',
    STACK_ENV:  process.env.STACK_ENV   || 'none',
    HTTPS_KEY:  process.env.HTTPS_KEY   || './server/certs/privatekey.key',
    HTTPS_CRT:  process.env.HTTPS_CRT   || './server/certs/certificate.crt',
    HTTPS_PASS: process.env.HTTPS_PASS  || false,
    WHITELIST:  process.env.WHITELIST   || false,
    COMPRESSED: process.env.COMPRESSED  || false,
    LOGGING:    process.env.LOGGING     || './server/log',
    LOG_LEVEL:  process.env.LOG_LEVEL   || 'verbose',
    TIMESTAMP:  DateUtil.getTimeStamp(),
    APP_ROOT:   path.join(__dirname, '../..'),
    SRC_PATH:   '/projects'
};

Object.keys(env_vars).forEach(key => {
    env_vars[key] === 'false' ? env_vars[key] = false : null;
    env_vars[key] === 'true'  ? env_vars[key] = true  : null;
});

module.exports = {
    leui: {
        name: 'leui',
        config: Object.assign({}, env_vars, require('./config_leui')),
        routes: require('../routes/routes_leui_' + (env_vars.COMPRESSED ? 'dist' : 'dev'))
    },
    leadmin: {
        name: 'leadmin',
        config: Object.assign({}, env_vars, require('./config_leadmin')),
        routes: require('../routes/routes_leadmin_' + (env_vars.COMPRESSED ? 'dist' : 'dev'))
    }
};
