"use strict";

/*
              Lattice Engines Express Server Application
           See /server/server.js for the actual server code
    See Gruntfile.js to define environment variables for local dev
    See /conf/env/* to define environment variables for QA/PROD/etc
*/
const path      = require('path');
const fmt       = require('util').format;
const express   = require('express');
const log4js    = require('log4js');

const DateUtil  = require('./server/utilities/DateUtil');
const Server    = require('./server/Server');

const configs   = require('./server/configs/config');
const apps      = process.env.NODE_APPS.split(',');

const log       = log4js.getLogger('LE-UI');
const logPath   = process.env.LOGGING ? path.resolve(process.env.LOGGING + '/le-ui.log') : null;

if (logPath) {
    log4js.loadAppender('file');
    log4js.addAppender(log4js.appenders.file(logPath), 'LE-UI');
    log.info(fmt('Logging Node Server to %s', logPath));
}
log.info('Logging Node Server to stdout');

apps.forEach(app => {
    new Server(express, express(), configs[app]).start(onStart);
});

function onStart(err, meta) {
    if (err) {
        log.error(fmt('Error starting %s %s server on port %d \n %s', meta.app, meta.proto, meta.port, err));
        return;
    }

    log.info(fmt('Started %s server - LISTENING: %s://localhost:%d', meta.app, meta.proto, meta.port));
}

process.on('uncaughtException', err => {
    log.error(fmt(':uncaughtException> %s \n %s', err, err.stack));
});

process.on('SIGTERM', err => {
    log.error(fmt(':SIGTERM> %s \n %s', err, err.stack));
});

process.on('ECONNRESET', err => {
    log.error(fmt(':ECONNRESET> %s \n %s', err, err.stack));
});
