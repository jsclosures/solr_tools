#!/usr/bin/env node
/*
 * ensure-replicas.js
 *
 * Ensures every collection in a SolrCloud cluster has at least N replicas
 * (default 3) per shard. For any shard that has fewer active/healthy
 * replicas than the target, the missing replicas are added via the
 * Collections API ADDREPLICA action.
 *
 * Configuration (env vars):
 *   SOLR_URL          Base URL to a Solr node, e.g. http://localhost:8983/solr
 *                     (default: http://localhost:8983/solr)
 *   SOLR_USER         Optional basic-auth username
 *   SOLR_PASS         Optional basic-auth password
 *   TARGET_REPLICAS   Desired replicas per shard (default: 3)
 *   DRY_RUN           If "true", report only — do not call ADDREPLICA
 *
 * Usage:
 *   node solr-tools/ensure-replicas.js
 *   SOLR_URL=http://solr1:8983/solr TARGET_REPLICAS=3 node solr-tools/ensure-replicas.js
 *   DRY_RUN=true node solr-tools/ensure-replicas.js
 */

'use strict';

const http = require('http');
const https = require('https');
const { URL } = require('url');

const SOLR_URL = (process.env.SOLR_URL || 'http://localhost:8983/solr').replace(/\/+$/, '');
const SOLR_USER = process.env.SOLR_USER || '';
const SOLR_PASS = process.env.SOLR_PASS || '';
const TARGET_REPLICAS = parseInt(process.env.TARGET_REPLICAS || '3', 10);
const DRY_RUN = String(process.env.DRY_RUN || '').toLowerCase() === 'true';

if (!Number.isFinite(TARGET_REPLICAS) || TARGET_REPLICAS < 1) {
  console.error(`Invalid TARGET_REPLICAS value: ${process.env.TARGET_REPLICAS}`);
  process.exit(1);
}

function authHeaders() {
  if (!SOLR_USER) return {};
  const token = Buffer.from(`${SOLR_USER}:${SOLR_PASS}`).toString('base64');
  return { Authorization: `Basic ${token}` };
}

/**
 * Issue a GET request to a Solr endpoint and parse the JSON body.
 * Node-style callback: cb(err, body).
 */
function solrGet(path, params, cb) {
  let url;
  try {
    url = new URL(`${SOLR_URL}${path}`);
  } catch (e) {
    return process.nextTick(cb, e);
  }
  url.searchParams.set('wt', 'json');
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

function getClusterStatus(cb) {
  solrGet('/admin/collections', { action: 'CLUSTERSTATUS' }, (err, body) => {
    if (err) return cb(err);
    if (!body.cluster || !body.cluster.collections) {
      return cb(new Error('Unexpected CLUSTERSTATUS response: missing cluster.collections'));
    }
    cb(null, body.cluster);
  });
}

function countHealthyReplicas(replicas) {
  // Replicas live under shard.replicas as a map of replicaName -> replicaInfo.
  // A replica is considered healthy if it's marked active. Replicas that are
  // "down", "recovering", or "recovery_failed" are not counted toward the
  // target, since they aren't currently serving the desired role.
  let count = 0;
  if (!replicas) return 0;
  for (const r of Object.values(replicas)) {
    if (r && typeof r.state === 'string' && r.state.toLowerCase() === 'active') {
      count++;
    }
  }
  return count;
}

function addReplica(collection, shard, cb) {
  solrGet(
    '/admin/collections',
    { action: 'ADDREPLICA', collection, shard },
    cb,
  );
}

/**
 * Run a list of async tasks one after another. Each task is
 * `function(done)` where `done(err)` continues. Aborts on first error.
 */
function runSeries(tasks, cb) {
  let i = 0;
  function next(err) {
    if (err) return cb(err);
    if (i >= tasks.length) return cb(null);
    const task = tasks[i++];
    task(next);
  }
  next(null);
}

function processShard(collName, shardName, shard, counters, cb) {
  const healthy = countHealthyReplicas(shard.replicas);
  const missing = TARGET_REPLICAS - healthy;

  if (missing <= 0) {
    console.log(`  - ${shardName}: ${healthy} active replica(s) — OK`);
    counters.alreadyOk++;
    return cb(null);
  }

  console.log(`  - ${shardName}: ${healthy} active replica(s), need ${missing} more`);

  const tasks = [];
  for (let i = 0; i < missing; i++) {
    tasks.push(function (done) {
      if (DRY_RUN) {
        console.log(`      [dry-run] would ADDREPLICA to ${collName}/${shardName}`);
        return done(null);
      }
      addReplica(collName, shardName, (err) => {
        if (err) {
          console.error(
            `      FAILED to add replica to ${collName}/${shardName}: ${err.message}`,
          );
          counters.failed++;
          // Do not abort the whole run on a single ADDREPLICA failure.
          return done(null);
        }
        console.log(`      added replica to ${collName}/${shardName}`);
        counters.added++;
        done(null);
      });
    });
  }
  runSeries(tasks, cb);
}

function processCollection(collName, coll, counters, cb) {
  const shards = coll.shards || {};
  const shardNames = Object.keys(shards).sort();
  console.log(`Collection: ${collName} (${shardNames.length} shard(s))`);

  const tasks = shardNames.map((shardName) => {
    return function (done) {
      processShard(collName, shardName, shards[shardName], counters, done);
    };
  });
  runSeries(tasks, cb);
}

function main() {
  console.log(`Solr URL:         ${SOLR_URL}`);
  console.log(`Target replicas:  ${TARGET_REPLICAS} per shard`);
  console.log(`Dry run:          ${DRY_RUN ? 'yes' : 'no'}`);
  console.log('');

  getClusterStatus((err, cluster) => {
    if (err) {
      console.error(`Fatal error: ${err.message}`);
      process.exit(1);
    }

    const collections = cluster.collections;
    const collectionNames = Object.keys(collections).sort();

    if (collectionNames.length === 0) {
      console.log('No collections found.');
      return;
    }

    const counters = { added: 0, failed: 0, alreadyOk: 0 };

    const tasks = collectionNames.map((collName) => {
      return function (done) {
        processCollection(collName, collections[collName], counters, done);
      };
    });

    runSeries(tasks, (runErr) => {
      if (runErr) {
        console.error(`Fatal error: ${runErr.message}`);
        process.exit(1);
      }

      console.log('');
      console.log('Summary:');
      console.log(`  Shards already at target: ${counters.alreadyOk}`);
      console.log(`  Replicas added:           ${counters.added}`);
      if (counters.failed > 0) {
        console.log(`  Replica add failures:     ${counters.failed}`);
        process.exit(1);
      }
    });
  });
}

main();
