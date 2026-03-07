# CLA Setup Checklist

Use this checklist before making the repository public.

## 1. Install CLA Assistant / Contributor Assistant

- Install the CLA Assistant GitHub app (or contributor-assistant workflow equivalent) on this repository.
- Ensure pull requests receive a required CLA status check.

## 2. Ensure workflow is active

This repository includes `.github/workflows/cla.yml` with automated signature handling.

## 3. Branch protection

Add `CLA` (or the exact job name from workflow runs) as a required status check on the default branch.

## 4. Contributor UX

- Keep `docs/legal/ICLA.md` current.
- Keep `CONTRIBUTING.md` pointing to CLA requirements.

## 5. Audit

Periodically verify `.github/cla-signatures/version1/cla.json` exists and includes expected signers.
