'use strict';

const avro = require('avsc');
const snappy = require('snappy');
const readdr = require("recursive-readdir");

const codecs = {
    snappy: function (buf, cb) {
      // Avro appends checksums to compressed blocks, which we skip here.
      return snappy.uncompress(buf.slice(0, buf.length - 4), cb);
    }
};

module.exports = Reader;

function Reader(options) {
    // TODO set params
}

Reader.prototype.readDir = async (dirPath) => {
    let files = await readAvrosInDir(dirPath);

    let rows = [];
    for (let file of files) {
        if (!file.endsWith('.avro')) {
            continue;
        }
        let list = await readAvroFile(file);
        if (Array.isArray(list)) {
            for (let obj of list) {
                rows.push(obj);
            }
        }
    }
    return rows;
}

async function readAvrosInDir(dir) {
    return readdr(dir);
}

async function readAvroFile(filepath) {
    let stream = avro.createFileDecoder(filepath, {codecs});
    let list = [];
    return new Promise((resolve, reject) => {
        stream.on('data', (data) => list.push(data));
        stream.on('end', () => resolve(list));
        stream.on('error', (err) => reject(err));
    });
}