docker compose run --rm --entrypoint pipe dev segment_vessel \
    --source_segment_vessel_daily=scratch_matias_ttl7d.internal__segment_vessel_daily \
    --destination=scratch_matias_ttl7d.internal__segment_vessel \
    --labels=environment=production \
    --labels=resource_creator=matias \
    --labels=project=core_pipeline \
    --labels=version=v3 \
    --labels=step=segment \
    --labels=stage=productive \
    --project=world-fishing-827
