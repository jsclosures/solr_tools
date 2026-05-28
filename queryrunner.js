#!/usr/bin/env node
/*
 * queryrunner.js
 *
 * Use a set of queries read from a csv file to run REST requests and capture the results
 * as json in a file (one line records).
 *
 * Configuration (env vars):
 *   SOLR_URL          Base URL to a Solr node, e.g. http://localhost:8983/solr/collection/select?q=
 *                     (default: http://localhost:8983/solr)
 *   SOLR_USER         Optional basic-auth username
 *   SOLR_PASS         Optional basic-auth password
 *   SOURCE_FILE       source list of queries
 *   DRY_RUN           If "true", report only — do not call ADDREPLICA
 *
 * Usage:
 *   node solr-tools/ensure-replicas.js
 *   SOLR_URL=http://solr1:8983/solr SOLR_HOST=host SOLR_USER=xxx SOLR_PASSWORD=xxx SOURCE_FILE=./queries.csv node solr-tools/queryrunner.js
 *   DRY_RUN=true node solr-tools/queryrunner.js
 */

'use strict';

const http = require('http');
const https = require('https');
const { URL } = require('url');
const fs = require('fs');
const readline = require('readline');

const CONTEXT = {lib: {http,https,URL,fs,readline}};
CONTEXT.SOLR_URL = (process.env.SOLR_URL || 'http://localhost:8983/solr').replace(/\/+$/, '');
CONTEXT.SOLR_USER = process.env.SOLR_USER || '';
CONTEXT.SOLR_PASS = process.env.SOLR_PASS || '';
CONTEXT.SOURCE_FILE = process.env.SOURCE_FILE || './queries.csv';
CONTEXT.DRY_RUN = String(process.env.DRY_RUN || '').toLowerCase() === 'true';

function authHeaders(ctx) {
  if (!ctx.SOLR_USER) return {};
  const token = Buffer.from(`${ctx.SOLR_USER}:${ctx.SOLR_PASS}`).toString('base64');
  return { Authorization: `Basic ${token}` };
}

/**
 * Issue a GET request to a Solr endpoint and parse the JSON body.
 * Node-style callback: cb(err, body).
 */
function solrGet(ctx,path, params, cb) {
  let url;
  try {
    url = new URL(`${ctx.SOLR_URL}${path}`);
  } catch (e) {
    return process.nextTick(cb, e);
  }
  
  if (params) {
    for (const k of Object.keys(params)) {
      url.searchParams.set(k, String(params[k]));
    }
  }

  const transport = url.protocol === 'https:' ? https : http;
  const options = {
    method: 'GET',
    headers: Object.assign({ Accept: 'application/json' }, authHeaders()),
  };

  const req = transport.request(url, options, (res) => {
    const chunks = [];
    res.on('data', (chunk) => chunks.push(chunk));
    res.on('end', () => {
      const text = Buffer.concat(chunks).toString('utf8');
      let body;
      try {
        body = JSON.parse(text);
      } catch (parseErr) {
        return cb(
          new Error(
            `Solr returned non-JSON response (HTTP ${res.statusCode}): ${text.slice(0, 500)}`,
          ),
        );
      }
      if (res.statusCode < 200 || res.statusCode >= 300) {
        const msg = body && body.error && body.error.msg ? body.error.msg : text;
        return cb(
          new Error(`Solr request failed (HTTP ${res.statusCode}) for ${url}: ${msg}`),
        );
      }
      cb(null, body);
    });
    res.on('error', (err) => cb(err));
  });

  req.on('error', (err) => cb(err));
  req.end();
}

function runQueries(ctx) {
    const rl = false;
    const instream = CONTEXT.lib.fs.createReadStream(ctx.SOURCE_FILE);
    instream.readable = true;
    
    rl = ctx.lib.readline.createInterface({
        input: instream,
        terminal: false
    });
    
    ctx rowCounter = 0;
    
    function readFunc(line) {
        let ctx = this.ctx;
        let solrPath = ctx.SOLR_URL + uriEncode(line);
        solrGet(ctx,solrPath, { action: 'CLUSTERSTATUS' }, (err, body) => {
            if (err) 
                console.log(`Error:         ${err}`);
            console.log(`Body:         ${body}`);
          });
    }

    function completeFunc() {
        console.log(`Rows Read:         ${ctx.rowCounter}`);
    }

    if( rl ) rl.on('line', readFunc.bind({ctx});
    if( rl ) rl.on('close', completeFunc.bind({ctx});
}

function main(ctx) {
  console.log(`Solr URL:         ${ctx.SOLR_URL}`);
  console.log(`SOURCE_FILE:      ${ctx.SOURCE_FILE} per shard`);
  console.log(`Dry run:          ${ctx.DRY_RUN ? 'yes' : 'no'}`);
  console.log('');

  runQueries(ctx);
}

main(CONTEXT);
