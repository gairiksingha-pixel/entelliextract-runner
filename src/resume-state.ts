/**
 * Resume state for sync-extract pipeline: tracks the file currently being
 * downloaded so that on resume we can delete the partial file and re-run from that file.
 */

import { readFileSync, writeFileSync, existsSync, unlinkSync, mkdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import type { Config } from './types.js';

export interface ResumeState {
  syncInProgressPath?: string;
  syncInProgressManifestKey?: string;
}

function getResumeStatePath(config: Config): string {
  const checkpointDir = dirname(config.run.checkpointPath);
  return join(checkpointDir, 'resume-state.json');
}

function getManifestPath(config: Config): string {
  return (
    config.s3.syncManifestPath ??
    join(dirname(config.run.checkpointPath), 'sync-manifest.json')
  );
}

export function loadResumeState(config: Config): ResumeState {
  const path = getResumeStatePath(config);
  if (!existsSync(path)) return {};
  try {
    const raw = readFileSync(path, 'utf-8');
    const data = JSON.parse(raw) as ResumeState;
    return typeof data === 'object' && data !== null ? data : {};
  } catch {
    return {};
  }
}

export function saveResumeState(config: Config, state: ResumeState): void {
  const path = getResumeStatePath(config);
  const dir = dirname(path);
  if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
  writeFileSync(path, JSON.stringify(state, null, 0), 'utf-8');
}

export function clearResumeState(config: Config): void {
  saveResumeState(config, {});
}

/**
 * Called when starting with --resume: delete the partial file (if any) from disk
 * and remove its entry from the sync manifest, then clear resume state so the
 * pipeline can run and re-download that file.
 */
export function clearPartialFileAndResumeState(config: Config): void {
  const state = loadResumeState(config);
  const path = state.syncInProgressPath;
  const manifestKey = state.syncInProgressManifestKey;

  if (path && existsSync(path)) {
    try {
      unlinkSync(path);
    } catch (_) {
      // ignore
    }
  }

  if (manifestKey) {
    const manifestPath = getManifestPath(config);
    if (existsSync(manifestPath)) {
      try {
        const raw = readFileSync(manifestPath, 'utf-8');
        const manifest = JSON.parse(raw) as Record<string, string>;
        if (typeof manifest === 'object' && manifest !== null && manifestKey in manifest) {
          delete manifest[manifestKey];
          writeFileSync(manifestPath, JSON.stringify(manifest, null, 0), 'utf-8');
        }
      } catch (_) {
        // ignore
      }
    }
  }

  clearResumeState(config);
}
