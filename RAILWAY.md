# Railway Deploy

This project can run on Railway as a single service plus Postgres.

## Recommended setup

1. Create a new Railway project.
2. Add a PostgreSQL database.
3. Add a service from this GitHub repository.
4. Railway should use the root `Dockerfile`.
5. Set these environment variables on the app service:

```text
DATABASE_URL=${{Postgres.DATABASE_URL}}
SECRET_KEY=generate-a-random-long-string
ADMIN_SECRET=generate-a-random-long-string
ENABLE_SCHEDULER=true
RUN_SYNC_WORKER_IN_WEB=true
```

## Notes

- The backend serves the built frontend from `frontend/dist`.
- In production the frontend uses same-origin API requests, so no separate frontend service is required.
