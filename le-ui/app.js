'use strict';

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

const Server    = require('./server/Server');

const configs   = require('./server/configs/config');
const apps      = process.env.NODE_APPS ? process.env.NODE_APPS.split(',') : [];
const logPath   = process.env.LOGGING ? path.resolve(process.env.LOGGING + '/le-ui.log') : null;

const log       = log4js.getLogger('LE-UI');
log.info('Logging Node Server to stdout');
if (logPath) {
    log4js.loadAppender('file');
    log4js.addAppender(log4js.appenders.file(logPath), 'LE-UI');
    log.info(fmt('Logging Node Server to %s', logPath));
}

if (apps.length === 0) {
    let msg = 'NODE_APPS environment variable not set';
    let e = new Error(msg);
    log.error(fmt('%s\n%s', e, e.stack));
    throw e;
}

apps.forEach(app => {
    const config = configs[app];

    if (typeof config !== undefined) {
        new Server(express, express(), config).start(onStart);
    } else {
        log.error(fmt('Config missing for APP:%s in NODE_APPS:%s', app, process.env.NODE_APPS));
    }
});

function onStart(err, meta) {
    if (err) {
        log.error(fmt('Error starting %s %s server on port %d\n%s', meta.app, meta.proto, meta.port, err));
        return;
    }

    log.info(fmt('Started %s server - LISTENING: %s://localhost:%d', meta.app, meta.proto, meta.port));
}

process.on('uncaughtException', err => {
    log.error(fmt(':uncaughtException> %s\n%s', err, err.stack));
});

process.on('SIGTERM', err => {
    log.error(fmt(':SIGTERM> %s\n%s', err, err.stack));
});

process.on('ECONNRESET', err => {
    log.error(fmt(':ECONNRESET> %s\n%s', err, err.stack));
});
