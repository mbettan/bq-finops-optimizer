import pytest
from unittest.mock import MagicMock, patch
from src import utils


def test_get_adc_credentials_caches_result():
    with patch("src.utils.google.auth.default") as mock_auth:
        mock_auth.return_value = (MagicMock(), "test-adc-project")
        
        c1, p1 = utils.get_adc_credentials()
        c2, p2 = utils.get_adc_credentials()
        
        assert c1 is c2
        assert p1 == p2 == "test-adc-project"
        assert mock_auth.call_count == 1


def test_get_bq_client_returns_cached_instance():
    with patch("src.utils.google.auth.default", return_value=(MagicMock(), "adc-project")), \
         patch("src.utils.bigquery.Client") as mock_client_cls:
        m1 = MagicMock()
        m2 = MagicMock()
        mock_client_cls.side_effect = [m1, m2]

        cli1 = utils.get_bq_client("project-a")
        cli2 = utils.get_bq_client("project-a")

        assert cli1 is cli2
        assert mock_client_cls.call_count == 1

        cli_b = utils.get_bq_client("project-b")
        assert cli_b is m2
        assert mock_client_cls.call_count == 2


def test_get_bq_client_lru_eviction():
    with patch("src.utils.google.auth.default", return_value=(MagicMock(), "adc-project")), \
         patch("src.utils.bigquery.Client") as mock_client_cls, \
         patch("src.utils._BQ_CLIENT_CACHE_SIZE", 2):
        
        mock_client_cls.side_effect = lambda project, credentials: MagicMock(project=project)

        cli_a = utils.get_bq_client("proj-1")
        cli_b = utils.get_bq_client("proj-2")
        cli_c = utils.get_bq_client("proj-3")  # evicts proj-1

        with utils._bq_clients_lock:
            assert "proj-1" not in utils._bq_clients
            assert "proj-2" in utils._bq_clients
            assert "proj-3" in utils._bq_clients


def test_close_bq_clients_closes_all_and_clears():
    with patch("src.utils.google.auth.default", return_value=(MagicMock(), "adc-project")), \
         patch("src.utils.bigquery.Client") as mock_client_cls:
        m1 = MagicMock()
        m2 = MagicMock()
        mock_client_cls.side_effect = [m1, m2]

        utils.get_bq_client("proj-1")
        utils.get_bq_client("proj-2")

        utils.close_bq_clients()

        m1.close.assert_called_once()
        m2.close.assert_called_once()
        with utils._bq_clients_lock:
            assert len(utils._bq_clients) == 0
