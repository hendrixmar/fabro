CREATE UNIQUE INDEX run_events_by_session_owner
ON run_events(session_id)
WHERE session_id IS NOT NULL
  AND event_name = 'run.session.created';
