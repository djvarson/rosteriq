# release runs once before the new web dynos start, so DB migrations are
# applied on buildpack/Heroku-style deploys (the Dockerfile runs them in its
# CMD instead). Keep both in sync.
release: python -m rosteriq.migrations.run_migrations --run
web: uvicorn rosteriq.api:app --host 0.0.0.0 --port $PORT --workers 2 --access-log
