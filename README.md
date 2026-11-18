# Raw Classification Assurance — portable dashboard and monitoring

**One code package for DEV, ITG, STG and PRD. Each environment supplies its own configuration and establishes its own baseline. No environment-specific counts are embedded in the dashboard or monitor.**

## What changes operationally

- Existing missing classifications are **aggregate coverage backlog**, not automatically created incidents or individual emails.
- The detailed dashboard action queue focuses on P1/P2 corrections and evidence reviews. Full inventory remains available separately.
- New untagged columns are tracked from their first observation and get a configurable classification grace period.
- Removed tags, weaker assigned tiers, invalid/conflicting values and new mapped-requirement gaps create change incidents.
- Native notifications are **grouped digests**, by owner, table and finding. There is no one-email-per-column mode.
- A configurable daily summary reports current coverage. Reminders and recovery are tracked for announced incidents.
- Native job failure notifications and a separate scheduled watchdog report monitor failures and overdue runs.

## Deploy in DEV first

1. Import **`Deploy_Governance_Monitor.py`** into the Databricks Workspace as a Python notebook. Attach approved existing Spark compute.
2. Open **`configs/dev.example.json`**. Its source bindings reflect the DEV objects already inspected. Fill in your existing **`warehouse_id`**, **`existing_cluster_id`**, and **`notifications.emails`** or approved **`destination_ids`**. Use workspace user email addresses; distribution lists/channels should use configured notification destinations. No secrets or webhook URLs are required.
3. Paste that JSON into the notebook's **`environment_config_json`** widget. Set **`mode=install`** and **Run all**.
4. Open the dashboard URL printed by the notebook. It also prints the managed job and alert IDs. The schedule is activated only after the baseline, dashboard, job and alert resources exist.

The default **`mode=demo`** is an optional synthetic test. It demonstrates baseline, grace, new-change and recovery behavior without reading source data, writing tables or sending notifications. Switch to `install` for actual deployment.

**For another environment:** reuse the same notebook and dashboard definition. Copy `configs/environment.template.json`, then supply that environment's actual workspace ID, catalog scope, source tables/service identity, output catalog/schema, compute and recipients. Do not guess bindings by changing a DEV suffix.

## Proposed defaults — change them in the environment configuration

| Setting | Example default |
| --- | --- |
| Metadata check | Every 60 minutes; 15/30 minutes also supported |
| New-column classification grace | 30 minutes from first monitor observation |
| Unresolved-incident reminder | Every 24 hours after accepted alert evaluation |
| Daily coverage summary | Enabled, first scan at/after 13:00 UTC |
| Event batch | At most 1,000 events, grouped into a digest |
| Journal retention | 30 days, preserving the latest full state |
| Collate review freshness | 7 days |

These are configurable starting points, not established IFBI SLAs. With hourly scans and a grace period, notifications are not instantaneous: first discovery, grace expiry, the next scan, task execution and metadata lag all affect timing.

## What installation creates

Inside the **existing configured catalog**:

- The configured governance schema if it does not already exist.
- **One Delta metadata journal** for snapshots, change incidents, pending digest events and evaluation receipts.
- **12 logical SQL views** used by the portable dashboard.

In the workspace:

- A dashboard draft, bound to that environment's default dataset catalog/schema.
- One scheduled job on the existing approved Spark cluster.
- One native change-digest SQL alert, evaluated only when a batch is due.
- One independently scheduled monitor-health SQL alert.

No new catalog, cluster, warehouse, external notification service or customer-data scan is created. Installation requires the existing privileges to create these objects, read the configured metadata/references and use the approved compute. No broad grants or public sharing are added. Publishing and audience permissions follow the normal governance release process.

Use a dedicated output schema for each environment/deployment, especially when catalogs are shared across workspaces. Scope, taxonomy or execution-identity changes do not silently replace an existing baseline. Review such changes through change control. Existing source-view and journal access should be limited to the governance audience.

## Reuse and validation

- Set `dashboard_id` in the configuration to update an existing draft using its version guard; otherwise installation creates a draft and retains its ID for later installs.
- Re-running installation reuses managed job/alert IDs. Do not redeploy while a managed run is active. If an API operation has an uncertain outcome, inspect the workspace and retained installation receipts before repeating it.
- The dashboard JSON is portable because its SQL references stable logical view names. The installer supplies dataset catalog/schema context; there is no viewer control that switches deployment environments.
- The native SQL Alert task and alert parameter binding must be supported by the workspace. Installation checks that the API preserves the required parameters, and the alert query fails if its batch parameter is missing. This prevents a failed notification binding from looking like successful evaluation.
- Evaluation receipts mean **Databricks evaluated the alert successfully**. They do not prove recipient delivery or that someone read a message. Ambiguous failures can cause a repeated digest; stable event IDs support reconciliation.
- The package has been tested locally on synthetic metadata and mocked control-plane APIs. Live workspace import, API availability, permissions, compute behavior and notification delivery must be checked in DEV before promotion.

## Test without touching source classifications

Run the notebook with `mode=demo` for the logic smoke test. For end-to-end delivery, use an **approved in-scope DEV test table** after the real baseline: add a harmless new column without a sensitivity tag, allow the configured grace/check interval, inspect the digest and incident queue, then apply the appropriate reviewed test tag. Check that a recovery event appears on the following scan. Do not remove or weaken tags on business data to test notifications.

See `ARCHITECTURE.md` for state, isolation, deduplication and failure semantics. The remaining policy/UDF and enforcement stories stay separate from this classification monitor.
