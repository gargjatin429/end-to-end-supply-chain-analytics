import pytest
import os
import sys

# To test infrastructure config securely, we patch os.environ
def test_production_config_fails_without_vars(monkeypatch):
    monkeypatch.setenv("TEST_MODE", "false")
    monkeypatch.delenv("S3_ACCESS_KEY", raising=False)
    monkeypatch.delenv("SQL_SERVER_NAME", raising=False)

    # Reloading config should raise ConfigurationError
    with pytest.raises(Exception) as exc_info:
        import config
        import importlib
        importlib.reload(config)

    assert "Missing required production environment variables" in str(exc_info.value)

def test_test_mode_config_passes(monkeypatch):
    monkeypatch.setenv("TEST_MODE", "true")
    import config
    import importlib
    importlib.reload(config)
    assert config.S3_ACCESS_KEY == "admin"
