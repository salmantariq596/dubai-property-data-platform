"""Guards on the Terraform code: the security and design choices must not drift.

Reads the .tf files as text; `terraform validate` in CI checks the syntax.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

INFRA = Path(__file__).parents[2] / "infra"
STACKS = ["bootstrap", "platform", "unity-catalog", "orchestration", "cicd"]


def tf(stack: str) -> str:
    return "\n".join(p.read_text(encoding="utf-8") for p in sorted((INFRA / stack).glob("*.tf")))


def blocks(text: str, resource_type: str) -> list[str]:
    """Bodies of every `resource "<type>" "<name>" { ... }` block (top-level braces)."""
    bodies = []
    for match in re.finditer(rf'resource "{resource_type}" "\w+" \{{', text):
        depth, i = 1, match.end()
        while depth:
            depth += {"{": 1, "}": -1}.get(text[i], 0)
            i += 1
        bodies.append(text[match.end() : i - 1])
    return bodies


@pytest.mark.parametrize("stack", STACKS)
def test_each_stack_exists_and_pins_terraform(stack):
    assert 'required_version = ">= 1.6"' in tf(stack)


@pytest.mark.parametrize("stack", ["bootstrap", "platform"])
def test_storage_accounts_have_no_keys_and_no_public_access(stack):
    accounts = blocks(tf(stack), "azurerm_storage_account")
    assert accounts
    for body in accounts:
        assert re.search(r"shared_access_key_enabled\s*=\s*false", body)
        assert re.search(r"allow_nested_items_to_be_public\s*=\s*false", body)
        assert re.search(r'min_tls_version\s*=\s*"TLS1_2"', body)


def test_lake_is_adls_gen2_with_the_four_layers():
    text = tf("platform")
    [lake] = blocks(text, "azurerm_storage_account")
    assert re.search(r"is_hns_enabled\s*=\s*true", lake)
    assert 'layers = toset(["raw", "bronze", "silver", "gold"])' in text


def test_unity_catalog_reaches_storage_through_a_managed_identity():
    platform, uc = tf("platform"), tf("unity-catalog")
    [connector] = blocks(platform, "azurerm_databricks_access_connector")
    assert 'type = "SystemAssigned"' in connector
    assert "azure_managed_identity" in uc
    assert not re.search(r"azure_service_principal|client_secret|account_key", uc)


def test_databricks_workspace_is_premium_for_unity_catalog():
    [workspace] = blocks(tf("platform"), "azurerm_databricks_workspace")
    assert re.search(r'sku\s*=\s*"premium"', workspace)


def test_key_vault_uses_rbac():
    [vault] = blocks(tf("platform"), "azurerm_key_vault")
    assert re.search(r"rbac_authorization_enabled\s*=\s*true", vault)


def test_budget_alert_comes_first():
    assert blocks(tf("bootstrap"), "azurerm_consumption_budget_subscription")


@pytest.mark.parametrize("stack", ["platform", "unity-catalog", "orchestration", "cicd"])
def test_state_lives_in_the_remote_backend(stack):
    assert 'backend "azurerm" {}' in tf(stack)
    example = (INFRA / stack / "backend.example.hcl").read_text(encoding="utf-8")
    assert "use_azuread_auth     = true" in example


def test_no_secrets_or_real_ids_in_examples():
    for path in INFRA.rglob("*example*"):
        text = path.read_text(encoding="utf-8")
        ids = re.findall(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", text)
        assert all(i == "00000000-0000-0000-0000-000000000000" for i in ids), path
        assert "@example.com" in text or "@" not in text, path


def test_unity_catalog_matches_the_notebooks():
    """The catalog, schemas and volumes the notebooks expect."""
    text = tf("unity-catalog")
    assert 'default     = "dubai"' in (INFRA / "unity-catalog" / "variables.tf").read_text()
    for schema in ["raw", "bronze", "silver", "gold", "quality"]:
        assert re.search(rf"^\s+{schema}\s+= \{{ storage", text, re.M), schema
    for volume in ["landing", "checkpoints", "exports"]:
        assert f'name             = "{volume}"' in text or f'name         = "{volume}"' in text
