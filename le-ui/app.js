'use strict';

/*
              Lattice Engines Express Server Application
           See /server/server.js for the actual server code
    See Gruntfile.js to define environment variables for local dev
    See /conf/env/* to define environment variables for QA/PROD/etc
*/
const express   = require('express');
const chalk     = require('chalk');

const DateUtil  = require('./server/utilities/DateUtil');
const Server    = require('./server/Server');

const configs   = require('./server/configs/config');
const apps      = process.env.NODE_APPS ? process.env.NODE_APPS.split(',') : [];

if (apps.length === 0) {
    throw new Error('NODE_APPS environment variable not set');
}

apps.forEach(app => {
    const config = configs[app];

    if (typeof config !== 'undefined') {
        new Server(express, express(), config).start(onStart);
    } else {
        console.log(chalk.red(DateUtil.getTimeStamp() + ':bootstrap> ') + 'Config missing for APP:' + app + ' in NODE_APPS:' + process.env.NODE_APPS);
    }
});

function onStart(err, meta) {
    if (err) {
        console.log(chalk.red(DateUtil.getTimeStamp() + ':bootstrap> ') + 'Error starting ' + meta.app + ' ' + meta.proto + ' server on port ' + meta.port +'\n', err);
    }
}

process.on('uncaughtException', err => {
    console.log(chalk.red(DateUtil.getTimeStamp() + ':uncaughtException>')+'\n', err);
});

process.on('SIGTERM', err => {
    console.log(chalk.red(DateUtil.getTimeStamp() + ':SIGTERM>'), err);
});

process.on('ECONNRESET', err => {
    console.log(chalk.red(DateUtil.getTimeStamp() + ':ECONNRESET>'), err);
});
