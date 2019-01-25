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
    LOGGING:    process.env.LOGGING,
    LOG_LEVEL:  process.env.LOG_LEVEL   || 'verbose',
    TIMESTAMP:  DateUtil.getTimeStamp(),
    APP_ROOT:   path.join(__dirname, '../..'),
    SRC_PATH:   '/projects'
};

Object.keys(env_vars).forEach(key => {
    env_vars[key] === 'false' ? env_vars[key] = false : null;
    env_vars[key] === 'true'  ? env_vars[key] = true  : null;
});

function getRoutes(app){
    var routes = '../routes/routes_'+app+'_';
    if(env_vars.COMPRESSED){
        routes = routes.concat('dist');
    } else {
        routes = routes.concat('dev');
    }
   
    console.log('ROUTES for ', app, ' ---> ', routes);
    return routes;
}

module.exports = {
    leui: {
        name: 'leui',
        config: Object.assign({}, env_vars, require('./config_leui')),
        routes: require(getRoutes('leui'))
    },
    leadmin: {
        name: 'leadmin',
        config: Object.assign({}, env_vars, require('./config_leadmin')),
        routes: require(getRoutes('leadmin'))
    }
};
