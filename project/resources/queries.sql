call UPSERT_TO_PURGE_RUNTIME_AUDIT();

select count(*) from RUNTIME_AUDIT();