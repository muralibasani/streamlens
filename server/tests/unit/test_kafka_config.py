"""Unit tests for server/src/kafka/config.py."""
import subprocess
from unittest.mock import MagicMock, patch

import pytest

import src.kafka.config as config_mod
from src.kafka.config import (
    _apply_ssl_certs,
    _apply_ssl_endpoint_id,
    _apply_ssl_verification,
    _export_keystore,
    _export_truststore,
    _parse_truststore_aliases,
    _parse_truststore_aliases_short,
    _ssl_java_to_pem,
    client_config,
)


@pytest.fixture(autouse=True)
def clear_ssl_cache():
    """Clear the SSL PEM cache between tests to avoid cross-test pollution."""
    config_mod._ssl_pem_cache.clear()
    yield
    config_mod._ssl_pem_cache.clear()


class TestClientConfig:
    """Tests for client_config(cluster)."""

    @patch("src.kafka.config._ssl_java_to_pem")
    def test_plaintext_cluster_bootstrap_only(self, mock_ssl_java):
        mock_ssl_java.return_value = (None, None, None)
        cluster = {"bootstrapServers": "localhost:9092"}
        cfg = client_config(cluster)
        assert cfg["bootstrap.servers"] == "localhost:9092"
        assert "ssl.ca.location" not in cfg
        assert "security.protocol" not in cfg

    @patch("src.kafka.config._ssl_java_to_pem")
    def test_ssl_cluster_with_ssl_ca_location(self, mock_ssl_java):
        mock_ssl_java.return_value = (None, None, None)
        cluster = {
            "bootstrapServers": "broker:9093",
            "securityProtocol": "SSL",
            "sslCaLocation": "/path/to/ca.pem",
        }
        cfg = client_config(cluster)
        assert cfg["bootstrap.servers"] == "broker:9093"
        assert cfg["ssl.ca.location"] == "/path/to/ca.pem"
        assert cfg["security.protocol"] == "SSL"

    @patch("src.kafka.config._ssl_java_to_pem")
    def test_ssl_with_enable_ssl_certificate_verification_false(self, mock_ssl_java):
        mock_ssl_java.return_value = (None, None, None)
        cluster = {
            "bootstrapServers": "broker:9093",
            "securityProtocol": "SSL",
            "enableSslCertificateVerification": False,
        }
        cfg = client_config(cluster)
        assert cfg["enable.ssl.certificate.verification"] == "false"

    @patch("src.kafka.config._ssl_java_to_pem")
    def test_ssl_with_enable_ssl_certificate_verification_snake_case(self, mock_ssl_java):
        mock_ssl_java.return_value = (None, None, None)
        cluster = {
            "bootstrapServers": "broker:9093",
            "securityProtocol": "SSL",
            "enable_ssl_certificate_verification": False,
        }
        cfg = client_config(cluster)
        assert cfg["enable.ssl.certificate.verification"] == "false"

    @patch("src.kafka.config._ssl_java_to_pem")
    def test_ssl_with_ssl_endpoint_identification_algorithm_empty(self, mock_ssl_java):
        mock_ssl_java.return_value = (None, None, None)
        cluster = {
            "bootstrapServers": "broker:9093",
            "sslEndpointIdentificationAlgorithm": "",
        }
        cfg = client_config(cluster)
        assert cfg["ssl.endpoint.identification.algorithm"] == "none"

    @patch("src.kafka.config._ssl_java_to_pem")
    def test_ssl_with_ssl_endpoint_identification_algorithm_none(self, mock_ssl_java):
        mock_ssl_java.return_value = (None, None, None)
        cluster = {
            "bootstrapServers": "broker:9093",
            "ssl_endpoint_identification_algorithm": "none",
        }
        cfg = client_config(cluster)
        assert cfg["ssl.endpoint.identification.algorithm"] == "none"

    @patch("src.kafka.config._ssl_java_to_pem")
    def test_multiple_bootstrap_servers_comma_separated(self, mock_ssl_java):
        mock_ssl_java.return_value = (None, None, None)
        cluster = {"bootstrapServers": "broker1:9092, broker2:9092 , broker3:9092"}
        cfg = client_config(cluster)
        assert cfg["bootstrap.servers"] == "broker1:9092,broker2:9092,broker3:9092"

    @patch("src.kafka.config._ssl_java_to_pem")
    def test_bootstrap_servers_snake_case(self, mock_ssl_java):
        mock_ssl_java.return_value = (None, None, None)
        cluster = {"bootstrap_servers": "localhost:9092"}
        cfg = client_config(cluster)
        assert cfg["bootstrap.servers"] == "localhost:9092"


class TestParseTruststoreAliases:
    """Tests for _parse_truststore_aliases(stdout)."""

    def test_parse_single_alias(self):
        stdout = """
Keystore type: JKS
Keystore provider: SUN
Alias name: myalias
Creation date: ...
"""
        result = _parse_truststore_aliases(stdout)
        assert result == ["myalias"]

    def test_parse_empty_string(self):
        result = _parse_truststore_aliases("")
        assert result == []

    def test_parse_multiple_aliases(self):
        stdout = """
Alias name: alias1
Creation date: ...
Alias name: alias2
Creation date: ...
Alias name: alias1
"""
        result = _parse_truststore_aliases(stdout)
        assert result == ["alias1", "alias2"]  # Deduplicated, order preserved

    def test_parse_case_insensitive_alias_name(self):
        stdout = "ALIAS NAME: testalias"
        result = _parse_truststore_aliases(stdout)
        assert result == ["testalias"]

    def test_parse_no_aliases(self):
        stdout = "Some other output without alias lines"
        result = _parse_truststore_aliases(stdout)
        assert result == []


class TestApplySslEndpointId:
    """Tests for _apply_ssl_endpoint_id(cfg, cluster)."""

    def test_empty_string_sets_none(self):
        cfg = {}
        cluster = {"sslEndpointIdentificationAlgorithm": ""}
        _apply_ssl_endpoint_id(cfg, cluster)
        assert cfg["ssl.endpoint.identification.algorithm"] == "none"

    def test_none_value_sets_none(self):
        cfg = {}
        cluster = {"ssl_endpoint_identification_algorithm": "none"}
        _apply_ssl_endpoint_id(cfg, cluster)
        assert cfg["ssl.endpoint.identification.algorithm"] == "none"

    def test_custom_value_preserved(self):
        cfg = {}
        cluster = {"sslEndpointIdentificationAlgorithm": "https"}
        _apply_ssl_endpoint_id(cfg, cluster)
        assert cfg["ssl.endpoint.identification.algorithm"] == "https"

    def test_not_in_cluster_no_change(self):
        cfg = {"bootstrap.servers": "x"}
        cluster = {}
        _apply_ssl_endpoint_id(cfg, cluster)
        assert "ssl.endpoint.identification.algorithm" not in cfg


class TestApplySslVerification:
    """Tests for _apply_ssl_verification(cfg, cluster)."""

    def test_enable_false_sets_verification_false(self):
        cfg = {}
        cluster = {"enableSslCertificateVerification": False}
        skip = _apply_ssl_verification(cfg, cluster)
        assert cfg["enable.ssl.certificate.verification"] == "false"
        assert skip is True

    def test_enable_true_no_change(self):
        cfg = {}
        cluster = {"enableSslCertificateVerification": True}
        skip = _apply_ssl_verification(cfg, cluster)
        assert "enable.ssl.certificate.verification" not in cfg
        assert skip is False

    def test_snake_case_enable_false(self):
        cfg = {}
        cluster = {"enable_ssl_certificate_verification": False}
        skip = _apply_ssl_verification(cfg, cluster)
        assert cfg["enable.ssl.certificate.verification"] == "false"
        assert skip is True

    def test_key_not_present_no_change(self):
        cfg = {}
        cluster = {}
        skip = _apply_ssl_verification(cfg, cluster)
        assert "enable.ssl.certificate.verification" not in cfg
        assert skip is False


class TestApplySslCerts:
    """Tests for _apply_ssl_certs(cfg, cluster, skip_verify)."""

    @patch("src.kafka.config._ssl_java_to_pem")
    def test_explicit_ca_location_used(self, mock_ssl_java):
        mock_ssl_java.return_value = (None, None, None)
        cfg = {"security.protocol": "SSL"}
        cluster = {"sslCaLocation": "/path/to/ca.pem"}
        _apply_ssl_certs(cfg, cluster, skip_verify=False)
        assert cfg["ssl.ca.location"] == "/path/to/ca.pem"

    @patch("src.kafka.config._ssl_java_to_pem")
    def test_truststore_converted_to_ca(self, mock_ssl_java):
        mock_ssl_java.return_value = ("/tmp/ca.pem", None, None)
        cfg = {"security.protocol": "SSL"}
        cluster = {}
        _apply_ssl_certs(cfg, cluster, skip_verify=False)
        assert cfg["ssl.ca.location"] == "/tmp/ca.pem"

    @patch("src.kafka.config._ssl_java_to_pem")
    def test_keystore_adds_cert_and_key(self, mock_ssl_java):
        mock_ssl_java.return_value = (None, "/tmp/cert.pem", "/tmp/key.pem")
        cfg = {"security.protocol": "SSL"}
        cluster = {}
        _apply_ssl_certs(cfg, cluster, skip_verify=False)
        assert cfg["ssl.certificate.location"] == "/tmp/cert.pem"
        assert cfg["ssl.key.location"] == "/tmp/key.pem"

    @patch("src.kafka.config._ssl_java_to_pem")
    def test_ssl_ca_location_precedence_over_snake_case(self, mock_ssl_java):
        mock_ssl_java.return_value = (None, None, None)
        cfg = {}
        cluster = {"ssl_ca_location": "/snake/ca.pem"}
        _apply_ssl_certs(cfg, cluster, skip_verify=False)
        assert cfg["ssl.ca.location"] == "/snake/ca.pem"


class TestSslJavaToPem:
    """Tests for _ssl_java_to_pem with mocked subprocess."""

    @patch("src.kafka.config._export_truststore")
    @patch("src.kafka.config.Path")
    def test_successful_truststore_conversion(self, mock_path_cls, mock_export_truststore):
        from src.kafka.config import _ssl_java_to_pem

        mock_path_cls.return_value.exists.return_value = True
        mock_export_truststore.return_value = "/tmp/test_ca.pem"

        cluster = {
            "sslTruststoreLocation": "/keystore.jks",
            "sslTruststorePassword": "changeit",
        }
        ca, cert, key = _ssl_java_to_pem(cluster)
        assert ca == "/tmp/test_ca.pem"
        assert cert is None
        assert key is None

    @patch("src.kafka.config.subprocess.run")
    @patch("src.kafka.config.Path")
    def test_keytool_not_found_returns_none(self, mock_path, mock_run):
        from src.kafka.config import _export_truststore

        mock_path.return_value.exists.return_value = True
        mock_run.side_effect = FileNotFoundError("keytool not found")

        result = _export_truststore("/tmp", "/keystore.jks", "changeit")
        assert result is None

    @patch("src.kafka.config.subprocess.run")
    @patch("src.kafka.config.Path")
    def test_keystore_export_keytool_not_found(self, mock_path, mock_run):
        from src.kafka.config import _export_keystore

        mock_path.return_value.exists.return_value = True
        mock_run.side_effect = FileNotFoundError("openssl not found")

        cert, key = _export_keystore("/tmp", "/keystore.p12", "changeit", None)
        assert cert is None
        assert key is None

    def test_no_truststore_or_keystore_returns_none(self):
        cluster = {"bootstrapServers": "localhost:9092"}
        ca, cert, key = _ssl_java_to_pem(cluster)
        assert ca is None
        assert cert is None
        assert key is None

    @patch("src.kafka.config._export_keystore")
    @patch("src.kafka.config._export_truststore")
    @patch("src.kafka.config.Path")
    def test_ssl_java_to_pem_cache_hit_second_call_uses_cache(
        self, mock_path_cls, mock_export_truststore, mock_export_keystore
    ):
        mock_path_cls.return_value.exists.return_value = True
        mock_export_truststore.return_value = "/tmp/ca.pem"
        mock_export_keystore.return_value = ("/tmp/cert.pem", "/tmp/key.pem")

        cluster = {
            "sslTruststoreLocation": "/trust.jks",
            "sslTruststorePassword": "pw",
            "sslKeystoreLocation": "/key.p12",
            "sslKeystorePassword": "pw",
        }
        ca1, cert1, key1 = _ssl_java_to_pem(cluster)
        ca2, cert2, key2 = _ssl_java_to_pem(cluster)

        assert ca1 == ca2 == "/tmp/ca.pem"
        assert cert1 == cert2 == "/tmp/cert.pem"
        assert key1 == key2 == "/tmp/key.pem"
        mock_export_truststore.assert_called_once()
        mock_export_keystore.assert_called_once()

    @patch("src.kafka.config._export_keystore")
    @patch("src.kafka.config._export_truststore")
    @patch("src.kafka.config.Path")
    def test_ssl_java_to_pem_exception_returns_none(
        self, mock_path_cls, mock_export_truststore, mock_export_keystore
    ):
        mock_path_cls.return_value.exists.return_value = True
        mock_export_truststore.side_effect = Exception("keytool failed")

        cluster = {"sslTruststoreLocation": "/trust.jks", "sslTruststorePassword": "pw"}
        ca, cert, key = _ssl_java_to_pem(cluster)
        assert ca is None
        assert cert is None
        assert key is None

    @patch("src.kafka.config._export_keystore")
    @patch("src.kafka.config._export_truststore")
    @patch("src.kafka.config.Path")
    def test_ssl_java_to_pem_trust_and_key_both_converted(
        self, mock_path_cls, mock_export_truststore, mock_export_keystore
    ):
        mock_path_cls.return_value.exists.return_value = True
        mock_export_truststore.return_value = "/tmp/ca.pem"
        mock_export_keystore.return_value = ("/tmp/cert.pem", "/tmp/key.pem")

        cluster = {
            "sslTruststoreLocation": "/trust.jks",
            "sslTruststorePassword": "pw",
            "sslKeystoreLocation": "/key.p12",
            "sslKeystoreType": "pkcs12",
            "sslKeystorePassword": "pw",
        }
        ca, cert, key = _ssl_java_to_pem(cluster)
        assert ca == "/tmp/ca.pem"
        assert cert == "/tmp/cert.pem"
        assert key == "/tmp/key.pem"
        mock_export_truststore.assert_called_once()
        assert mock_export_truststore.call_args[0][1] == "/trust.jks"
        assert mock_export_truststore.call_args[0][2] == "pw"
        mock_export_keystore.assert_called_once()


class TestExportTruststore:
    """Tests for _export_truststore."""

    @patch("src.kafka.config._parse_truststore_aliases_short")
    @patch("src.kafka.config._parse_truststore_aliases")
    @patch("src.kafka.config.subprocess.run")
    def test_aliases_exported_per_alias(self, mock_run, mock_parse, mock_parse_short):
        mock_parse.return_value = ["ca1", "ca2"]
        mock_parse_short.return_value = []
        mock_run.side_effect = [
            MagicMock(returncode=0),  # -exportcert for ca1
            MagicMock(returncode=0),  # -exportcert for ca2
        ]

        def fake_exists(path):
            return path.endswith("ca_0.pem") or path.endswith("ca_1.pem") or path == "/tmp/ca.pem"

        with patch("os.path.exists", side_effect=fake_exists):
            with patch("os.path.getsize", return_value=5):
                with patch("builtins.open", create=True) as mopen:
                    mopen.return_value.__enter__ = MagicMock(return_value=MagicMock())
                    mopen.return_value.__exit__ = MagicMock(return_value=False)
                    mopen.return_value.__enter__.return_value.read.return_value = "-----BEGIN CERT-----\n"
                    result = _export_truststore("/tmp", "/trust.jks", "pw")

        assert result == "/tmp/ca.pem"

    @patch("src.kafka.config._parse_truststore_aliases_short")
    @patch("src.kafka.config._parse_truststore_aliases")
    @patch("src.kafka.config.subprocess.run")
    def test_empty_aliases_fallback_direct_export(self, mock_run, mock_parse, mock_parse_short):
        mock_parse.return_value = []
        mock_parse_short.return_value = []
        mock_run.return_value = MagicMock(returncode=0)
        with patch("os.path.exists", return_value=True):
            result = _export_truststore("/tmp", "/trust.jks", "pw")
        assert result == "/tmp/ca.pem"

    @patch("src.kafka.config._parse_truststore_aliases_short")
    @patch("src.kafka.config._parse_truststore_aliases")
    @patch("src.kafka.config.subprocess.run")
    def test_empty_ca_pem_after_export_returns_none(self, mock_run, mock_parse, mock_parse_short):
        mock_parse.return_value = ["ca1"]
        mock_parse_short.return_value = []
        mock_run.return_value = MagicMock(returncode=1)  # export fails for alias

        with patch("os.path.exists", return_value=False):
            with patch("os.path.getsize", return_value=0):
                result = _export_truststore("/tmp", "/trust.jks", "pw")
        assert result is None

    @patch("src.kafka.config.subprocess.run")
    def test_keytool_list_fails_returns_none(self, mock_run):
        mock_run.return_value = MagicMock(returncode=1, stderr="bad password")
        result = _export_truststore("/tmp", "/trust.jks", "wrong")
        assert result is None


class TestExportKeystore:
    """Tests for _export_keystore."""

    @patch("src.kafka.config.subprocess.run")
    def test_openssl_success_returns_cert_and_key(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0)
        cert, key = _export_keystore("/tmp", "/key.p12", "changeit", None)
        assert cert == "/tmp/cert.pem"
        assert key == "/tmp/key.pem"

    @patch("src.kafka.config.subprocess.run")
    def test_openssl_failure_returns_none_none(self, mock_run):
        mock_run.side_effect = subprocess.CalledProcessError(1, "openssl")

        result = _export_keystore("/tmp", "/key.p12", "wrong", None)
        assert result == (None, None)


class TestParseTruststoreAliasesShort:
    """Tests for _parse_truststore_aliases_short."""

    @patch("src.kafka.config.subprocess.run")
    def test_successful_parse_returns_aliases(self, mock_run):
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="alias1, Jan 1, 2025, trustedCertEntry,\n"
            "alias2, Feb 2, 2025, trustedCertEntry,\n",
        )
        result = _parse_truststore_aliases_short("/trust.jks", "pw")
        assert "alias1" in result
        assert "alias2" in result

    @patch("src.kafka.config.subprocess.run")
    def test_keytool_failure_returns_empty(self, mock_run):
        mock_run.return_value = MagicMock(returncode=1)
        result = _parse_truststore_aliases_short("/trust.jks", "wrong")
        assert result == []


class TestApplySslCertsAdditional:
    """Additional tests for _apply_ssl_certs."""

    @patch("src.kafka.config._ssl_java_to_pem")
    def test_ssl_protocol_no_ca_logs_warning(self, mock_ssl_java, caplog):
        import logging

        caplog.set_level(logging.WARNING)
        mock_ssl_java.return_value = (None, None, None)
        cfg = {"security.protocol": "SSL"}
        cluster = {"bootstrapServers": "broker:9093", "securityProtocol": "SSL"}
        _apply_ssl_certs(cfg, cluster, skip_verify=False)
        assert "no CA is configured" in caplog.text or "ssl.ca.location" in caplog.text

    @patch("src.kafka.config._ssl_java_to_pem")
    def test_explicit_pem_cert_key_paths_used(self, mock_ssl_java):
        mock_ssl_java.return_value = (None, None, None)
        cfg = {"security.protocol": "SSL"}
        cluster = {
            "sslCertificateLocation": "/cert.pem",
            "sslKeyLocation": "/key.pem",
            "sslKeyPassword": "secret",
        }
        _apply_ssl_certs(cfg, cluster, skip_verify=False)
        assert cfg["ssl.certificate.location"] == "/cert.pem"
        assert cfg["ssl.key.location"] == "/key.pem"
        assert cfg["ssl.key.password"] == "secret"
