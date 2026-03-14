# Brainstorm: Add Explorer Data Files to S3 Initialization

**Date:** 2026-03-14
**Status:** Ready for planning

## What We're Building

Adding four new explorer data files to the existing S3-based data initialization process so they are automatically downloaded into `/mnt/astrodash-data/explorer/` when the container starts.

The files are:
- `dash_twins_embeddings.npy` (~15 MB) - Embeddings data
- `dash_twins_payload.json` (~73 MB) - Payload data
- `dash_twins_pca.pkl` (~13 KB) - PCA model
- `dash_twins_umap.pkl` (~16 MB) - UMAP model

These support a spectral twins/similarity explorer feature. They are stable and rarely updated, similar to the existing pre-trained models.

## Why This Approach

**Chosen: Add files to the existing manifest (`astrodash-data.json`)**

The project already has a robust S3 download system (`initialize_data.py`) that:
- Downloads files listed in `astrodash-data.json` from the `astrodash` S3 bucket
- Verifies integrity via etag checksums
- Retries failed downloads up to 5 times
- Skips files that already exist and pass verification

Adding four entries to the manifest requires zero code changes. The existing infrastructure handles everything.

**Rejected alternatives:**
- **Separate init script:** Over-engineered for 4 stable files; duplicates existing infrastructure.
- **Django migration:** Anti-pattern; migrations shouldn't make network calls.

## Key Decisions

- **Destination path:** `/mnt/astrodash-data/explorer/` (new subdirectory alongside `pre_trained_models/`, `spectra/`, `user_models/`)
- **S3 path:** `init/data/explorer/<filename>` (follows existing convention)
- **Approach:** Upload files to S3, then regenerate the manifest using `python initialize_data.py manifest`
- **No code changes needed** - only manifest and S3 bucket updates

## Steps

1. Upload the 4 files to S3 bucket `astrodash` under path `init/data/explorer/`
2. Run `python initialize_data.py manifest` to regenerate `astrodash-data.json` with the new entries
3. Commit the updated `astrodash-data.json`
4. On next container start, the files will be automatically downloaded
