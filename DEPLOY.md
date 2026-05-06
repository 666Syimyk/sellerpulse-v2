# Deployment

## GitHub Pages

The frontend is published from `master:/docs`.

After the backend gets a public URL, rebuild `docs` with:

```powershell
$env:VITE_API_URL="https://sellerpulse-api.onrender.com"
.\scripts\publish-pages.ps1
git add docs
git commit -m "Update Pages build with production API"
git push origin master
```

## Render

This repo includes a `render.yaml` Blueprint with:

- `sellerpulse-api` for FastAPI
- `sellerpulse-worker` for background sync jobs
- `sellerpulse-db` for Postgres

### Values you must set in Render during Blueprint creation

None are strictly required for the first deploy.

### Shared secrets

- `SECRET_KEY` is generated once on `sellerpulse-api`
- `ADMIN_SECRET` is generated once on `sellerpulse-api`
- `sellerpulse-worker` reads both from the API service

### Optional secret

- `TOKEN_ENCRYPTION_KEY`
  If you leave it unset, the app derives an encryption key from `SECRET_KEY`.

### Recommended Render flow

1. Create a new Blueprint from this repository.
2. Let Render provision the web service, worker, and database.
3. Open the `sellerpulse-api` service URL.
4. Rebuild GitHub Pages with that URL in `VITE_API_URL`.
