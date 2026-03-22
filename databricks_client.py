from __future__ import annotations
import time
from urllib.parse import quote
import requests

_MAX_POLL = 120
_POLL_INTERVAL = 2


def _base(workspace_url):
    return workspace_url.rstrip("/")


def _headers(token):
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def _is_json(r):
    return "json" in r.headers.get("content-type", "")


def _extract_error(r):
    if _is_json(r):
        body = r.json()
        return body.get("message") or body.get("error") or ""
    return ""


def test_connection(workspace_url, token, warehouse_id):
    url = f"{_base(workspace_url)}/api/2.0/sql/warehouses/{quote(warehouse_id)}"
    r = requests.get(url, headers=_headers(token), timeout=15)

    if r.status_code in (401, 403):
        raise RuntimeError("Invalid token or insufficient permissions")
    if r.status_code == 404:
        raise RuntimeError("Warehouse not found — check the Warehouse ID")
    if not r.ok:
        msg = r.json().get("message", "") if _is_json(r) else ""
        raise RuntimeError(msg or f"Connection failed: HTTP {r.status_code}")

    data = r.json()
    return {"name": data.get("name", ""), "state": data.get("state", "OK")}


def execute_sql(workspace_url, token, warehouse_id, sql):
    base = _base(workspace_url)
    hdrs = _headers(token)

    r = requests.post(
        f"{base}/api/2.0/sql/statements",
        headers=hdrs,
        json={
            "warehouse_id": warehouse_id,
            "statement": sql,
            "wait_timeout": "0s",
            "disposition": "INLINE",
            "format": "JSON_ARRAY",
        },
        timeout=30,
    )
    if not r.ok:
        msg = r.json().get("message", "") if _is_json(r) else ""
        raise RuntimeError(msg or f"Databricks API error: HTTP {r.status_code}")

    data = r.json()

    attempts = 0
    while data.get("status", {}).get("state") in ("PENDING", "RUNNING"):
        if attempts >= _MAX_POLL:
            raise RuntimeError("Query timed out after 120 poll attempts.")
        time.sleep(_POLL_INTERVAL)
        attempts += 1
        pr = requests.get(
            f"{base}/api/2.0/sql/statements/{data['statement_id']}",
            headers=hdrs,
            timeout=15,
        )
        if not pr.ok:
            raise RuntimeError(f"Poll error: HTTP {pr.status_code}")
        data = pr.json()

    state = data.get("status", {}).get("state", "")
    if state == "FAILED":
        err = data.get("status", {}).get("error", {}).get("message", "Query execution failed.")
        raise RuntimeError(err)

    columns = [c["name"] for c in data.get("manifest", {}).get("schema", {}).get("columns", [])]
    rows = data.get("result", {}).get("data_array", [])
    return {"columns": columns, "rows": rows, "row_count": len(rows)}


def execute_pyspark(workspace_url, token, cluster_id, code):
    base = _base(workspace_url)
    hdrs = _headers(token)

    # create execution context
    r = requests.post(
        f"{base}/api/1.2/contexts/create",
        headers=hdrs,
        json={"clusterId": cluster_id, "language": "python"},
        timeout=30,
    )
    if not r.ok:
        msg = _extract_error(r)
        raise RuntimeError(msg or f"Failed to create execution context: HTTP {r.status_code}")
    context_id = r.json().get("id")

    try:
        # run the command
        r = requests.post(
            f"{base}/api/1.2/commands/execute",
            headers=hdrs,
            json={
                "clusterId": cluster_id,
                "contextId": context_id,
                "language": "python",
                "command": code,
            },
            timeout=30,
        )
        if not r.ok:
            msg = _extract_error(r)
            raise RuntimeError(msg or f"Failed to execute command: HTTP {r.status_code}")

        command_id = r.json().get("id")
        status = r.json()

        # poll until done
        attempts = 0
        while status.get("status") not in ("Finished", "Error", "Cancelled"):
            if attempts >= _MAX_POLL:
                raise RuntimeError("PySpark execution timed out.")
            time.sleep(_POLL_INTERVAL)
            attempts += 1
            pr = requests.get(
                f"{base}/api/1.2/commands/status",
                headers=hdrs,
                params={
                    "clusterId": cluster_id,
                    "contextId": context_id,
                    "commandId": command_id,
                },
                timeout=15,
            )
            if not pr.ok:
                raise RuntimeError(f"Poll error: HTTP {pr.status_code}")
            status = pr.json()

        if status.get("status") in ("Error", "Cancelled"):
            results = status.get("results", {})
            cause = results.get("cause") or results.get("summary") or "PySpark execution failed."
            raise RuntimeError(cause)

        results = status.get("results", {})
        result_type = results.get("resultType")
        result_data = results.get("data")

        if result_type == "table":
            columns = [c.get("name", c) if isinstance(c, dict) else str(c) for c in (results.get("schema") or [])]
            rows = result_data or []
            return {"columns": columns, "rows": rows, "row_count": len(rows)}

        if result_type == "text" and result_data:
            return {"columns": ["output"], "rows": [[result_data]], "row_count": 1}

        return {
            "columns": ["result"],
            "rows": [[result_data or "Command executed successfully."]],
            "row_count": 1,
        }

    finally:
        # cleanup context
        try:
            requests.post(
                f"{base}/api/1.2/contexts/destroy",
                headers=hdrs,
                json={"clusterId": cluster_id, "contextId": context_id},
                timeout=10,
            )
        except Exception:
            pass


def load_schema(workspace_url, token):
    base = _base(workspace_url)
    hdrs = _headers(token)
    all_tables = []

    r = requests.get(f"{base}/api/2.1/unity-catalog/catalogs", headers=hdrs, timeout=15)
    if not r.ok:
        raise RuntimeError(
            "Unable to access Unity Catalog. "
            "Check permissions or query INFORMATION_SCHEMA manually."
        )

    catalogs = r.json().get("catalogs", [])

    for catalog in catalogs:
        cat_name = catalog.get("name", "")
        sr = requests.get(
            f"{base}/api/2.1/unity-catalog/schemas",
            headers=hdrs,
            params={"catalog_name": cat_name},
            timeout=15,
        )
        if not sr.ok:
            continue

        for schema in sr.json().get("schemas", []):
            sch_name = schema.get("name", "")
            if sch_name == "information_schema":
                continue

            tr = requests.get(
                f"{base}/api/2.1/unity-catalog/tables",
                headers=hdrs,
                params={"catalog_name": cat_name, "schema_name": sch_name},
                timeout=15,
            )
            if not tr.ok:
                continue

            for tbl in tr.json().get("tables", []):
                all_tables.append({
                    "full_name": tbl.get("full_name", f"{cat_name}.{sch_name}.{tbl.get('name', '')}"),
                    "name": tbl.get("name", ""),
                    "catalog": cat_name,
                    "schema": sch_name,
                    "columns": [
                        {
                            "name": c.get("name", ""),
                            "type_name": c.get("type_name") or c.get("type_text") or "unknown",
                        }
                        for c in tbl.get("columns", [])
                    ],
                })

    return all_tables
