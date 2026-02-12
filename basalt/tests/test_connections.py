from __future__ import annotations

import os

import pytest

from basalt.connections import (
    databricks_connect_kwargs,
    s3_connection_options,
    snowflake_connect_kwargs,
)


def test_snowflake_kwargs_uses_sso_without_password(monkeypatch):
    monkeypatch.delenv("SNOWFLAKE_USER", raising=False)
    monkeypatch.setenv("SNOWFLAKE_USE_SSO", "1")
    kwargs = snowflake_connect_kwargs(user="alice", use_secret_storage=False)
    assert kwargs["user"] == "alice"
    assert kwargs["authenticator"] == "externalbrowser"
    assert "password" not in kwargs


def test_snowflake_kwargs_requires_password_without_sso(monkeypatch):
    monkeypatch.delenv("SNOWFLAKE_USE_SSO", raising=False)
    monkeypatch.delenv("SNOWFLAKE_PASSWORD", raising=False)
    with pytest.raises(ValueError, match="password is required"):
        snowflake_connect_kwargs(user="alice", sso=False, use_secret_storage=False)


def test_snowflake_kwargs_uses_env_password(monkeypatch):
    monkeypatch.delenv("SNOWFLAKE_USER", raising=False)
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "pw")
    kwargs = snowflake_connect_kwargs(user="alice", sso=False, use_secret_storage=False)
    assert kwargs["password"] == "pw"
    assert kwargs["user"] == "alice"


def test_databricks_kwargs_default_sso(monkeypatch):
    monkeypatch.setenv("DATABRICKS_SERVER_HOSTNAME", "adb.example.com")
    monkeypatch.setenv("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/abc")
    kwargs = databricks_connect_kwargs()
    assert kwargs["auth_type"] == "databricks-oauth"
    assert "access_token" not in kwargs


def test_databricks_kwargs_token_when_sso_disabled(monkeypatch):
    monkeypatch.setenv("DATABRICKS_SERVER_HOSTNAME", "adb.example.com")
    monkeypatch.setenv("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/abc")
    kwargs = databricks_connect_kwargs(sso=False, access_token="tok")
    assert kwargs["access_token"] == "tok"
    assert "auth_type" not in kwargs


def test_s3_connection_options_from_env(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-west-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "k")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "s")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "t")
    opts = s3_connection_options()
    assert opts == {"region": "eu-west-1", "key": "k", "secret": "s", "token": "t"}


def test_s3_connection_options_prefers_args(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-west-1")
    opts = s3_connection_options(region="us-east-1")
    assert opts["region"] == "us-east-1"
