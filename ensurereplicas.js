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
 *
 * Requires Node.js 18+ (uses the built-in fetch API).
 */

'use strict';

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

async function solrGet(path, params) {
  const url = new URL(`${SOLR_URL}${path}`);
  url.searchParams.set('wt', 'json');
  if (params) {
    for (const [k, v] of Object.entries(params)) {
      url.searchParams.set(k, String(v));
    }
  }

  const res = await fetch(url, { headers: authHeaders() });
  const text = await res.text();
  let body;
  try {
    body = JSON.parse(text);
  } catch {
    throw new Error(`Solr returned non-JSON response (HTTP ${res.status}): ${text.slice(0, 500)}`);
  }
  if (!res.ok) {
    const msg = body && body.error && body.error.msg ? body.error.msg : text;
    throw new Error(`Solr request failed (HTTP ${res.status}) for ${url}: ${msg}`);
  }
  return body;
}

async function getClusterStatus() {
  const body = await solrGet('/admin/collections', { action: 'CLUSTERSTATUS' });
  if (!body.cluster || !body.cluster.collections) {
    throw new Error('Unexpected CLUSTERSTATUS response: missing cluster.collections');
  }
  return body.cluster;
}

function countHealthyReplicas(replicas) {
  // Replicas live under shard.replicas as a map of replicaName -> replicaInfo.
  // A replica is considered healthy if it's marked active. Replicas that are
  // "down", "recovering", or "recovery_failed" are not counted toward the
  // target, since they aren't currently serving the desired role.
  let count = 0;
  for (const r of Object.values(replicas || {})) {
    if (r && typeof r.state === 'string' && r.state.toLowerCase() === 'active') {
      count++;
    }
  }
  return count;
}

async function addReplica(collection, shard) {
  const params = {
    action: 'ADDREPLICA',
    collection,
    shard,
  };
  return solrGet('/admin/collections', params);
}

async function main() {
  console.log(`Solr URL:         ${SOLR_URL}`);
  console.log(`Target replicas:  ${TARGET_REPLICAS} per shard`);
  console.log(`Dry run:          ${DRY_RUN ? 'yes' : 'no'}`);
  console.log('');

  const cluster = await getClusterStatus();
  const collections = cluster.collections;
  const collectionNames = Object.keys(collections).sort();

  if (collectionNames.length === 0) {
    console.log('No collections found.');
    return;
  }

  let totalAdded = 0;
  let totalFailed = 0;
  let totalAlreadyOk = 0;

  for (const collName of collectionNames) {
    const coll = collections[collName];
    const shards = coll.shards || {};
    const shardNames = Object.keys(shards).sort();

    console.log(`Collection: ${collName} (${shardNames.length} shard(s))`);

    for (const shardName of shardNames) {
      const shard = shards[shardName];
      const healthy = countHealthyReplicas(shard.replicas);
      const missing = TARGET_REPLICAS - healthy;

      if (missing <= 0) {
        console.log(`  - ${shardName}: ${healthy} active replica(s) — OK`);
        totalAlreadyOk++;
        continue;
      }

      console.log(
        `  - ${shardName}: ${healthy} active replica(s), need ${missing} more`,
      );

      for (let i = 0; i < missing; i++) {
        if (DRY_RUN) {
          console.log(`      [dry-run] would ADDREPLICA to ${collName}/${shardName}`);
          continue;
        }
        try {
          await addReplica(collName, shardName);
          console.log(`      added replica to ${collName}/${shardName}`);
          totalAdded++;
        } catch (err) {
          console.error(
            `      FAILED to add replica to ${collName}/${shardName}: ${err.message}`,
          );
          totalFailed++;
        }
      }
    }
  }

  console.log('');
  console.log('Summary:');
  console.log(`  Shards already at target: ${totalAlreadyOk}`);
  console.log(`  Replicas added:           ${totalAdded}`);
  if (totalFailed > 0) {
    console.log(`  Replica add failures:     ${totalFailed}`);
    process.exit(1);
  }
}

main().catch((err) => {
  console.error(`Fatal error: ${err.message}`);
  process.exit(1);
});
