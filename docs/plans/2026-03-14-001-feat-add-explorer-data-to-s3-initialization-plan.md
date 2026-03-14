---
title: "feat: Add Explorer Data Files to S3 Initialization"
type: feat
status: active
date: 2026-03-14
origin: docs/brainstorms/2026-03-14-explorer-data-files-brainstorm.md
---

# feat: Add Explorer Data Files to S3 Initialization

Add four new explorer data files to the S3 bucket and data manifest so they are automatically downloaded into `/mnt/astrodash-data/explorer/` during container initialization.

## Acceptance Criteria

- [x] Four files uploaded to S3 bucket `astrodash` under `init/data/explorer/`
- [x] `astrodash-data.json` manifest regenerated and includes the four new entries
- [ ] Container initialization downloads the files to `/mnt/astrodash-data/explorer/`
- [ ] Etag verification passes for all four files
- [ ] Updated `astrodash-data.json` committed to the repository

## Context

A colleague provided four files that support a spectral twins/similarity explorer feature. They need to be available at `/mnt/astrodash-data/explorer/` in the running container (see brainstorm: `docs/brainstorms/2026-03-14-explorer-data-files-brainstorm.md`).

The project already has a robust manifest-based download system (`app/entrypoints/initialize_data.py`) that handles downloading, checksum verification, and retries. No code changes are needed — only S3 uploads and manifest regeneration.

### Files

| File | Size | Description |
|------|------|-------------|
| `dash_twins_embeddings.npy` | ~15 MB | Embeddings data |
| `dash_twins_payload.json` | ~73 MB | Payload data |
| `dash_twins_pca.pkl` | ~13 KB | PCA model |
| `dash_twins_umap.pkl` | ~16 MB | UMAP model |

Source location: `/home/skoranda/CAPS/SCiMMA/DASH/new_data/`

### S3 Configuration

- **Endpoint:** `https://js2.jetstream-cloud.org:8001`
- **Bucket:** `astrodash`
- **Upload path:** `init/data/explorer/<filename>`
- **Credentials:** User has write access via `ASTRODASH_S3_ACCESS_KEY_ID` and `ASTRODASH_S3_SECRET_ACCESS_KEY`

## MVP

### Step 1: Upload files to S3

Using the `ObjectStore.put_object()` method or MinIO client directly, upload each file:

```python
# upload_explorer_data.py (one-time script, not committed)
import os, sys
from pathlib import Path

sys.path.append(os.path.join(str(Path(__file__).resolve().parent.parent)))
from astrodash.shared.object_store import ObjectStore

conf = {
    'endpoint-url': os.getenv("ASTRODASH_S3_ENDPOINT_URL", 'https://js2.jetstream-cloud.org:8001'),
    'region-name': os.getenv("ASTRODASH_S3_REGION_NAME", ''),
    'aws_access_key_id': os.getenv("ASTRODASH_S3_ACCESS_KEY_ID"),
    'aws_secret_access_key': os.getenv("ASTRODASH_S3_SECRET_ACCESS_KEY"),
    'bucket': os.getenv("ASTRODASH_S3_BUCKET", 'astrodash'),
}

s3 = ObjectStore(conf=conf)
source_dir = '/home/skoranda/CAPS/SCiMMA/DASH/new_data'

for filename in [
    'dash_twins_embeddings.npy',
    'dash_twins_payload.json',
    'dash_twins_pca.pkl',
    'dash_twins_umap.pkl',
]:
    local_path = os.path.join(source_dir, filename)
    s3_path = f'init/data/explorer/{filename}'
    print(f'Uploading {filename} to {s3_path}...')
    s3.put_object(path=s3_path, file_path=local_path)
    print(f'  Done.')

print('All files uploaded.')
```

### Step 2: Regenerate manifest

```bash
cd app
python entrypoints/initialize_data.py manifest
```

This reads the S3 bucket contents and writes the updated `astrodash-data.json` with the four new `explorer/` entries.

### Step 3: Verify manifest

Confirm the manifest now contains entries with `"path": "explorer/..."` for all four files.

### Step 4: Commit

```bash
git add app/entrypoints/astrodash-data.json
git commit -m "feat: add explorer data files to initialization manifest"
```

### Step 5: Verify download

On next container start (or by running `python entrypoints/initialize_data.py download`), confirm all four files appear in `/mnt/astrodash-data/explorer/`.

## Sources

- **Origin brainstorm:** [docs/brainstorms/2026-03-14-explorer-data-files-brainstorm.md](docs/brainstorms/2026-03-14-explorer-data-files-brainstorm.md) — decided to use existing manifest approach; rejected separate init script and Django migration alternatives
- **Initialization script:** `app/entrypoints/initialize_data.py` — download, verify, and manifest commands
- **S3 client:** `app/astrodash/shared/object_store.py` — `ObjectStore` class with upload/download/checksum methods
- **Current manifest:** `app/entrypoints/astrodash-data.json` — 810 existing file entries
