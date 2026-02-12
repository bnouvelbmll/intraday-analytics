from __future__ import annotations

from functools import lru_cache
import os
from typing import Any, Optional


def _env_flag(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return str(value).strip().lower() not in {"", "0", "false", "no", "off"}


def _derive_user_from_bmll2_storage() -> Optional[str]:
    try:
        import bmll2  # type: ignore[import-not-found]

        storage_paths = getattr(bmll2, "storage_paths", None)
        if not callable(storage_paths):
            return None
        user_storage = storage_paths().get("user", {})
        prefix = user_storage.get("prefix")
        if not isinstance(prefix, str):
            return None
        bits = [x for x in prefix.split("/") if x]
        if not bits:
            return None
        if bits[0].startswith("s3:") and len(bits) >= 2:
            return bits[1]
        return bits[0]
    except Exception:
        return None


def _secret_get(name: str, system: str) -> Optional[str]:
    try:
        import secretstorage  # type: ignore[import-not-found]

        connection = secretstorage.dbus_init()
        collection = secretstorage.get_default_collection(connection)
        for item in collection.search_items({"name": name, "system": system}):
            secret = item.get_secret()
            if isinstance(secret, bytes):
                secret = secret.decode("utf-8")
            if isinstance(secret, str) and secret:
                return secret
    except Exception:
        return None
    return None


def _secret_set(name: str, system: str, secret: str) -> None:
    try:
        import secretstorage  # type: ignore[import-not-found]

        connection = secretstorage.dbus_init()
        collection = secretstorage.get_default_collection(connection)
        if collection.is_locked():
            collection.unlock()
        collection.create_item(
            label=name,
            attributes={"name": name, "system": system},
            secret=secret,
            replace=True,
        )
    except Exception:
        # best-effort only
        return


def snowflake_connect_kwargs(
    *,
    database: str = "PROD_DERIVED_DATA_DB",
    schema: str = "PUBLIC",
    warehouse: str = "PROD_DERIVED_DEFAULT_WH",
    role: str = "PROD_SALES_RO_ROLE",
    user: Optional[str] = None,
    password: Optional[str] = None,
    account: str = "tib96089",
    region: str = "us-east-1",
    use_secret_storage: bool = True,
    secret_name: str = "default_snowflake",
    secret_system: str = "bmll",
    persist_password: bool = False,
    sso: Optional[bool] = None,
) -> dict[str, Any]:
    resolved_user = os.environ.get("SNOWFLAKE_USER") or user or _derive_user_from_bmll2_storage()
    resolved_sso = _env_flag("SNOWFLAKE_USE_SSO", default=False) if sso is None else bool(sso)

    resolved_password = os.environ.get("SNOWFLAKE_PASSWORD") or password
    if not resolved_password and use_secret_storage and not resolved_sso:
        resolved_password = _secret_get(secret_name, secret_system)

    if persist_password and resolved_password and use_secret_storage:
        _secret_set(secret_name, secret_system, resolved_password)

    kwargs: dict[str, Any] = {
        "user": resolved_user,
        "account": os.environ.get("SNOWFLAKE_ACCOUNT", account),
        "region": os.environ.get("SNOWFLAKE_REGION", region),
        "role": role,
        "database": database,
        "schema": schema,
        "warehouse": warehouse,
    }
    if resolved_sso:
        kwargs["authenticator"] = "externalbrowser"
    else:
        if not resolved_password:
            raise ValueError(
                "Snowflake password is required when SSO is disabled. "
                "Set SNOWFLAKE_PASSWORD, pass password=..., or enable SSO."
            )
        kwargs["password"] = resolved_password
    return kwargs


@lru_cache(maxsize=5)
def snowflake_connection(
    *,
    database: str = "PROD_DERIVED_DATA_DB",
    schema: str = "PUBLIC",
    warehouse: str = "PROD_DERIVED_DEFAULT_WH",
    role: str = "PROD_SALES_RO_ROLE",
    user: Optional[str] = None,
    password: Optional[str] = None,
    account: str = "tib96089",
    region: str = "us-east-1",
    use_secret_storage: bool = True,
    secret_name: str = "default_snowflake",
    secret_system: str = "bmll",
    persist_password: bool = False,
    sso: Optional[bool] = None,
):
    import snowflake.connector  # type: ignore[import-not-found]

    kwargs = snowflake_connect_kwargs(
        database=database,
        schema=schema,
        warehouse=warehouse,
        role=role,
        user=user,
        password=password,
        account=account,
        region=region,
        use_secret_storage=use_secret_storage,
        secret_name=secret_name,
        secret_system=secret_system,
        persist_password=persist_password,
        sso=sso,
    )
    return snowflake.connector.connect(**kwargs)


def databricks_connect_kwargs(
    *,
    server_hostname: Optional[str] = None,
    http_path: Optional[str] = None,
    access_token: Optional[str] = None,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    sso: Optional[bool] = None,
) -> dict[str, Any]:
    resolved_sso = _env_flag("DATABRICKS_USE_SSO", default=True) if sso is None else bool(sso)
    kwargs: dict[str, Any] = {
        "server_hostname": os.environ.get("DATABRICKS_SERVER_HOSTNAME", server_hostname),
        "http_path": os.environ.get("DATABRICKS_HTTP_PATH", http_path),
    }
    if catalog:
        kwargs["catalog"] = catalog
    if schema:
        kwargs["schema"] = schema
    if resolved_sso:
        kwargs["auth_type"] = "databricks-oauth"
    else:
        token = os.environ.get("DATABRICKS_TOKEN") or access_token
        if not token:
            raise ValueError(
                "Databricks token is required when SSO is disabled. "
                "Set DATABRICKS_TOKEN or pass access_token=...."
            )
        kwargs["access_token"] = token
    missing = [k for k in ("server_hostname", "http_path") if not kwargs.get(k)]
    if missing:
        raise ValueError(
            f"Missing Databricks connection fields: {', '.join(missing)}."
        )
    return kwargs


@lru_cache(maxsize=5)
def databricks_connection(
    *,
    server_hostname: Optional[str] = None,
    http_path: Optional[str] = None,
    access_token: Optional[str] = None,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    sso: Optional[bool] = None,
):
    from databricks import sql as dbsql  # type: ignore[import-not-found]

    kwargs = databricks_connect_kwargs(
        server_hostname=server_hostname,
        http_path=http_path,
        access_token=access_token,
        catalog=catalog,
        schema=schema,
        sso=sso,
    )
    return dbsql.connect(**kwargs)


def s3_connection_options(
    *,
    region: Optional[str] = None,
    key: Optional[str] = None,
    secret: Optional[str] = None,
    token: Optional[str] = None,
) -> dict[str, str]:
    out = {
        "region": region or os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION", ""),
        "key": key or os.environ.get("AWS_ACCESS_KEY_ID", ""),
        "secret": secret or os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
        "token": token or os.environ.get("AWS_SESSION_TOKEN", ""),
    }
    return {k: v for k, v in out.items() if v}
