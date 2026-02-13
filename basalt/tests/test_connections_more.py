from __future__ import annotations

from types import ModuleType, SimpleNamespace
import sys

import pytest

import basalt.connections as c


def test_env_flag_defaults(monkeypatch):
    monkeypatch.delenv("XFLAG", raising=False)
    assert c._env_flag("XFLAG", default=True) is True
    monkeypatch.setenv("XFLAG", "0")
    assert c._env_flag("XFLAG", default=True) is False


def test_secret_get_set_best_effort(monkeypatch):
    monkeypatch.setitem(sys.modules, "secretstorage", ModuleType("secretstorage"))
    assert c._secret_get("a", "b") is None
    c._secret_set("a", "b", "s")


def test_snowflake_connect_kwargs_secret_and_persist(monkeypatch):
    monkeypatch.delenv("SNOWFLAKE_PASSWORD", raising=False)
    monkeypatch.setattr(c, "_secret_get", lambda *_: "pw")
    saved = {}
    monkeypatch.setattr(c, "_secret_set", lambda n, s, sec: saved.setdefault("v", (n, s, sec)))
    out = c.snowflake_connect_kwargs(
        user="u",
        sso=False,
        use_secret_storage=True,
        persist_password=True,
    )
    assert out["password"] == "pw"
    assert saved["v"][2] == "pw"


def test_databricks_kwargs_errors(monkeypatch):
    monkeypatch.delenv("DATABRICKS_SERVER_HOSTNAME", raising=False)
    monkeypatch.delenv("DATABRICKS_HTTP_PATH", raising=False)
    with pytest.raises(ValueError, match="Missing Databricks connection fields"):
        c.databricks_connect_kwargs(sso=True)
    with pytest.raises(ValueError, match="Databricks token is required"):
        c.databricks_connect_kwargs(
            sso=False, server_hostname="h", http_path="p", access_token=None
        )


def test_connection_wrappers(monkeypatch):
    fake_sf = ModuleType("snowflake")
    fake_sf_connector = ModuleType("snowflake.connector")
    fake_sf_connector.connect = lambda **kwargs: {"sf": kwargs}
    fake_sf.connector = fake_sf_connector
    monkeypatch.setitem(sys.modules, "snowflake", fake_sf)
    monkeypatch.setitem(sys.modules, "snowflake.connector", fake_sf_connector)
    out = c.snowflake_connection(user="u", password="p", sso=False, use_secret_storage=False)
    assert "sf" in out

    fake_dbsql = SimpleNamespace(connect=lambda **kwargs: {"db": kwargs})
    fake_db = ModuleType("databricks")
    fake_db.sql = fake_dbsql
    monkeypatch.setitem(sys.modules, "databricks", fake_db)
    out2 = c.databricks_connection(
        server_hostname="h", http_path="p", access_token="t", sso=False
    )
    assert "db" in out2
