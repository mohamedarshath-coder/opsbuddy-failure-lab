"""CI job selection (.github/scripts/select_qa_jobs.py): the running order of the
affected jobs, so a medallion chain runs bronze -> silver -> gold in QA and prod."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / ".github" / "scripts"))

import select_qa_jobs as sel  # noqa: E402


def _notebook(root: Path, name: str, body: str) -> str:
    path = root / "notebooks" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("# Databricks notebook source\n" + body, encoding="utf-8")
    return f"./notebooks/{name}"


def _job(path: str, **tags) -> dict:
    job = {"tasks": [{"task_key": "t", "notebook_task": {"notebook_path": path}}]}
    if tags:
        job["tags"] = tags
    return job


@pytest.fixture
def medallion(tmp_path):
    bronze = _notebook(
        tmp_path,
        "bronze.py",
        'df = spark.table("dev.raw.sf_accounts")\n'
        'df.write.mode("overwrite").saveAsTable(f"{target_schema}.accounts_bronze")\n',
    )
    silver = _notebook(
        tmp_path,
        "silver.py",
        'df = spark.table(f"{target_schema}.accounts_bronze")\n'
        "df.write.saveAsTable(\n    f\"{target_schema}.accounts_silver\"\n)\n",
    )
    gold = _notebook(
        tmp_path,
        "gold.py",
        'spark.table(f"{target_schema}.accounts_silver").write.saveAsTable("dev.gold.account_summary")\n',
    )
    spec = {
        "resources": {
            "jobs": {
                "a_gold": _job(gold),
                "b_silver": _job(silver),
                "c_bronze": _job(bronze),
            }
        }
    }
    return tmp_path, spec


def test_a_job_s_tables_are_read_from_its_notebooks(medallion):
    root, spec = medallion
    writes, reads = sel.job_tables(spec["resources"]["jobs"]["b_silver"], str(root))
    assert writes == {"accounts_silver"} and reads == {"accounts_bronze"}


def test_a_medallion_chain_runs_bronze_then_silver_then_gold(medallion):
    root, spec = medallion
    order = sel.run_order(["a_gold", "b_silver", "c_bronze"], spec, str(root))
    assert order == ["c_bronze", "b_silver", "a_gold"]


def test_only_selected_jobs_are_ordered(medallion):
    root, spec = medallion
    assert sel.run_order(["a_gold", "c_bronze"], spec, str(root)) == ["a_gold", "c_bronze"]


def test_an_explicit_tag_orders_what_the_code_does_not_show(tmp_path):
    feed = _notebook(tmp_path, "feed.py", "spark.table(feed_table)\n")  # a variable: not seen
    land = _notebook(tmp_path, "land.py", "pass\n")
    spec = {
        "resources": {
            "jobs": {
                "a_report": _job(feed, datasakshi_after="z_landing"),
                "z_landing": _job(land),
            }
        }
    }
    assert sel.run_order(["a_report", "z_landing"], spec, str(tmp_path)) == ["z_landing", "a_report"]


def test_a_cycle_falls_back_to_alphabetical(tmp_path, capsys):
    one = _notebook(tmp_path, "one.py", 'spark.table("x.y.t2")\nd.saveAsTable("x.y.t1")\n')
    two = _notebook(tmp_path, "two.py", 'spark.table("x.y.t1")\nd.saveAsTable("x.y.t2")\n')
    spec = {"resources": {"jobs": {"j1": _job(one), "j2": _job(two)}}}
    assert sel.run_order(["j2", "j1"], spec, str(tmp_path)) == ["j1", "j2"]
    assert "dependency cycle" in capsys.readouterr().err


# --- DLT / Lakeflow Declarative Pipelines ---


def _pipeline(*paths: str) -> dict:
    return {"libraries": [{"notebook": {"path": p}} for p in paths], "development": True}


def test_a_pipeline_is_selected_by_its_library_files(tmp_path):
    lib = _notebook(tmp_path, "dlt_accounts.py", "import dlt\n")
    yml = tmp_path / "databricks.yml"
    yml.write_text(
        "resources:\n"
        "  jobs:\n"
        "    some_job:\n"
        "      tasks:\n"
        "        - task_key: t\n"
        "          notebook_task: {notebook_path: ./notebooks/other.py}\n"
        "  pipelines:\n"
        f"    accounts_dlt:\n      libraries:\n        - notebook: {{path: {lib}}}\n",
        encoding="utf-8",
    )
    deps = sel.load_jobs(str(yml), str(tmp_path))
    assert deps["accounts_dlt"] == {"notebooks/dlt_accounts.py"}
    assert set(deps) == {"some_job", "accounts_dlt"}


def test_dlt_tables_order_a_pipeline_and_the_job_reading_it(tmp_path):
    dlt_lib = _notebook(
        tmp_path,
        "dlt.py",
        "import dlt\n"
        '@dlt.table(name="accounts_silver")\n'
        "def silver():\n"
        '    return dlt.read("accounts_bronze")\n',
    )
    report = _notebook(
        tmp_path, "report.py", 'spark.table("dev.sales.accounts_silver").write.saveAsTable("dev.gold.r")\n'
    )
    spec = {
        "resources": {
            "jobs": {"a_report": _job(report)},
            "pipelines": {"z_accounts": _pipeline(dlt_lib)},
        }
    }
    writes, reads = sel.job_tables(spec["resources"]["pipelines"]["z_accounts"], str(tmp_path))
    assert writes == {"accounts_silver"} and reads == {"accounts_bronze"}
    assert sel.run_order(["a_report", "z_accounts"], spec, str(tmp_path)) == ["z_accounts", "a_report"]


def test_bundle_resources_keeps_jobs_and_pipelines():
    spec = {"resources": {"jobs": {"j": {}}, "pipelines": {"p": {}}, "schemas": {"s": {}}}}
    assert set(sel.bundle_resources(spec)) == {"j", "p"}


# --- pre-deploy backups copy only what the release writes ---

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / ".github" / "scripts"))
import backup_tables  # noqa: E402


def test_backups_cover_the_tables_the_selected_jobs_write(medallion, monkeypatch):
    root, spec = medallion
    import yaml

    (root / "databricks.yml").write_text(yaml.safe_dump(spec), encoding="utf-8")
    monkeypatch.chdir(root)
    assert backup_tables.written_by(["b_silver"]) == {"accounts_silver"}
    assert backup_tables.written_by(["c_bronze", "a_gold"]) == {"accounts_bronze", "account_summary"}


def test_backups_fall_back_to_everything_when_writes_are_not_visible(tmp_path, monkeypatch):
    import yaml

    hidden = _notebook(tmp_path, "hidden.py", "df.write.saveAsTable(target_table)\n")
    (tmp_path / "databricks.yml").write_text(
        yaml.safe_dump({"resources": {"jobs": {"hidden_job": _job(hidden)}}}), encoding="utf-8"
    )
    monkeypatch.chdir(tmp_path)
    assert backup_tables.written_by(["hidden_job"]) is None
