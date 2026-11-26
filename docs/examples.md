# Example workflows

Paste these into **POST /dags** at http://localhost:8000/docs (Try it out), then
trigger them with **POST /dags/{dag_id}/runs** and watch **GET /runs/{run_id}**.

Triggering returns immediately (HTTP 202) with `"status": "running"`. Poll
`GET /runs/{run_id}` to watch tasks move through `pending -> running -> success`.
Add `?wait=true` to the trigger call to wait for the final result instead.

Built-in operations: `echo`, `add`, `multiply`, `subtract`, `divide`, plus three
for demos: `sleep` (wait `seconds`), `fail` (always fails with `message`),
`flaky` (fails the first `fail_times` attempts, then succeeds).

## 1. Retries with exponential backoff

`download` fails twice, then succeeds. With `retry_delay: 1` the waits are 1s then 2s.
While it waits, the task shows `"state": "retrying"`.

```json
{
  "dag_id": "flaky_demo",
  "description": "Retries with exponential backoff",
  "tasks": [
    {
      "task_id": "download",
      "operation": "flaky",
      "params": {"fail_times": 2, "message": "downloaded"},
      "retries": 3,
      "retry_delay": 1,
      "retry_backoff": 2
    },
    {"task_id": "report", "operation": "echo", "params": {"message": "report ready"}, "depends_on": ["download"]}
  ]
}
```

## 2. Pause and resume a run

Trigger `pausable`, then quickly call **POST /runs/{run_id}/pause**. The task that is
already running finishes; the next ones stay `pending` until you call **POST /runs/{run_id}/resume**.

```json
{
  "dag_id": "pausable",
  "description": "Three steps, slow enough to pause",
  "tasks": [
    {"task_id": "one", "operation": "sleep", "params": {"seconds": 5}},
    {"task_id": "two", "operation": "sleep", "params": {"seconds": 5}, "depends_on": ["one"]},
    {"task_id": "three", "operation": "echo", "params": {"message": "done"}, "depends_on": ["two"]}
  ]
}
```

To stop a whole workflow from being triggered, use **POST /dags/{dag_id}/pause**
(and `/resume`). Triggering a paused workflow returns HTTP 409.

## 3. Failure alert

When a run fails after all retries, PyFlow POSTs a JSON message to `alert_webhook`.
This one points at the built-in receiver, so the alert shows up in **GET /alerts/sink**.

```json
{
  "dag_id": "alert_demo",
  "description": "Fails on purpose and raises an alert",
  "alert_webhook": "http://localhost:8000/alerts/sink",
  "tasks": [
    {"task_id": "bad", "operation": "fail", "params": {"message": "disk full"}, "retries": 1, "retry_delay": 0.5},
    {"task_id": "after", "operation": "echo", "depends_on": ["bad"]}
  ]
}
```

Use any webhook URL instead (for example one from https://webhook.site) to see
a real external alert. Set `PYFLOW_ALERT_WEBHOOK` to alert for every DAG that has no webhook of its own.
