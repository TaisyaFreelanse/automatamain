-- Permanent dev ban for rug spikes (expires_at = 0). Upgrade existing SL CRASH rows.
UPDATE dev_blacklist
SET
    expires_at = 0,
    reason = CASE
        WHEN close_reason LIKE 'SL CRASH%' THEN 'dev_blacklist_permanent: SL CRASH rug'
        WHEN reason LIKE 'SL CRASH%' OR reason LIKE '%SL CRASH%' THEN 'dev_blacklist_permanent: SL CRASH rug'
        ELSE reason
    END
WHERE close_reason LIKE 'SL CRASH%'
   OR reason LIKE 'SL CRASH%';
