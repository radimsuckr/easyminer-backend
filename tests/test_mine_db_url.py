"""Unit tests for database URL construction from PMML extensions.

Note: Validation of missing/invalid extensions is handled by MinerTaskValidator,
which is called before _build_db_url_from_pmml_extensions in the mine task.
These tests assume valid, pre-validated input.
"""

from easyminer.parsers.pmml.miner import Extension
from easyminer.tasks.mine import _build_db_url_from_pmml_extensions, _mask_password


def test_build_db_url_with_mysql_protocol():
    """Build db_url from extensions with mysql:// protocol in server"""
    extensions = [
        Extension(name="database-server", value="mysql://localhost:3306"),
        Extension(name="database-name", value="testdb"),
        Extension(name="database-user", value="testuser"),
        Extension(name="database-password", value="testpass"),
    ]

    result = _build_db_url_from_pmml_extensions(extensions)
    assert result == "mysql+pymysql://testuser:testpass@localhost:3306/testdb"


def test_build_db_url_without_protocol():
    """Build db_url from extensions with plain host:port format"""
    extensions = [
        Extension(name="database-server", value="localhost:3306"),
        Extension(name="database-name", value="mydb"),
        Extension(name="database-user", value="admin"),
        Extension(name="database-password", value="secret123"),
    ]

    result = _build_db_url_from_pmml_extensions(extensions)
    assert result == "mysql+pymysql://admin:secret123@localhost:3306/mydb"


def test_build_db_url_case_insensitive():
    """Extension names are case-insensitive"""
    extensions = [
        Extension(name="Database-Server", value="localhost:3306"),
        Extension(name="Database-Name", value="mydb"),
        Extension(name="Database-User", value="admin"),
        Extension(name="Database-Password", value="secret123"),
    ]

    result = _build_db_url_from_pmml_extensions(extensions)
    assert result == "mysql+pymysql://admin:secret123@localhost:3306/mydb"


def test_build_db_url_with_remote_host():
    """Build db_url from extensions with remote host"""
    extensions = [
        Extension(name="database-server", value="db.example.com:3306"),
        Extension(name="database-name", value="production"),
        Extension(name="database-user", value="produser"),
        Extension(name="database-password", value="prodpass123"),
    ]

    result = _build_db_url_from_pmml_extensions(extensions)
    assert result == "mysql+pymysql://produser:prodpass123@db.example.com:3306/production"


def test_build_db_url_with_custom_port():
    """Build db_url from extensions with non-standard port"""
    extensions = [
        Extension(name="database-server", value="localhost:3307"),
        Extension(name="database-name", value="mydb"),
        Extension(name="database-user", value="admin"),
        Extension(name="database-password", value="secret"),
    ]

    result = _build_db_url_from_pmml_extensions(extensions)
    assert result == "mysql+pymysql://admin:secret@localhost:3307/mydb"


def test_mask_password_standard_url():
    """Password is masked in standard mysql URL"""
    url = "mysql+pymysql://testuser:testpass@localhost:3306/testdb"
    masked = _mask_password(url)
    assert masked == "mysql+pymysql://testuser:***@localhost:3306/testdb"
    assert "testpass" not in masked


def test_mask_password_no_password():
    """URL without password is returned unchanged"""
    url = "mysql+pymysql://testuser@localhost:3306/testdb"
    masked = _mask_password(url)
    assert masked == url


def test_mask_password_no_protocol():
    """URL without protocol is returned unchanged"""
    url = "localhost:3306/testdb"
    masked = _mask_password(url)
    assert masked == url


def test_mask_password_complex_password():
    """Complex password with special chars is masked"""
    url = "mysql+pymysql://user:p@ss:w0rd!@localhost:3306/testdb"
    masked = _mask_password(url)
    assert masked == "mysql+pymysql://user:***@localhost:3306/testdb"
    assert "p@ss:w0rd!" not in masked
