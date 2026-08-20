# Recovering stuck documents

An alert in the configured alerts index, or a non-zero
`flowstitch_dlq_records` metric, means OpenSearch permanently rejected one or
more documents.

1. Inspect the reason and index breakdown. The listing contains metadata only;
   fetching a producer payload is a separate, explicit operation.

   ```bash
   curl -H "Authorization: Bearer $FLOWSTITCH_ADMIN_TOKEN" \
     http://localhost:8080/v1/admin/dlq
   ```

2. Fix the rejection's cause before replaying. Replaying first achieves
   nothing: the same document will be rejected in the same way.

   For a mapping conflict, regenerate the template with
   `print-index-template` and reinstall it. A corrected template does not
   change an existing index's mapping. Roll over to a new index, or delete the
   affected index only when its existing data is expendable.

3. Preview a bounded replay. Filter by `output_id`, `reason_type`, `index`, or
   an RFC 3339 `older_than` timestamp. Requests without an exact output ID
   default to `dry_run: true`.

   ```bash
   curl -XPOST -H "Authorization: Bearer $FLOWSTITCH_ADMIN_TOKEN" \
     -H 'Content-Type: application/json' \
     http://localhost:8080/v1/admin/dlq/replay \
     -d '{"reason_type":"mapper_parsing_exception","limit":100,"dry_run":true}'
   ```

4. After confirming the preview and the repaired destination, submit the same
   bounded request with `"dry_run": false`. Watch `flowstitch_dlq_records` and
   `flowstitch_dlq_replayed_total`; repeated failures retain their replay count.

Administration is absent unless `server.admin_token_env` names an environment
variable containing a non-empty token. Never put the token itself in YAML.
