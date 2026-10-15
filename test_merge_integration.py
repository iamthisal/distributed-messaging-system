"""Isolated checks for fault tolerance combined with time sync and Raft."""
from unittest.mock import Mock

import httpx
import pytest
from fastapi.testclient import TestClient

import database
import fault_tolerance as ft
import node_manager as nm
import replication as replication
import server


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setattr(ft, "WAL_FILE", str(tmp_path / "wal.log"))
    monkeypatch.setattr(server, "logical_clock", 0)
    monkeypatch.setattr(server, "raft_commit_index", 0)
    monkeypatch.setattr(server, "raft_current_term", 1)
    monkeypatch.setattr(server, "raft_voted_for", None)
    monkeypatch.setattr(server, "CURRENT_PRIMARY_URL", server.OWN_URL)
    monkeypatch.setattr(server, "KNOWN_NODES", [server.OWN_URL])
    monkeypatch.setattr(server, "replicate_to_all", Mock(return_value=True))
    monkeypatch.setattr(replication, "REPLICAS", [])
    monkeypatch.setattr(replication, "REPLICA_STATUS", {})
    monkeypatch.setattr(replication, "REPLICA_LAST_FAIL", {})
    database.clear_all()
    # No lifespan: these checks must not start network probes or recovery threads.
    yield TestClient(server.app)
    database.clear_all()


def send(client, content="hello"):
    response = client.post("/send", json={"sender": "A", "receiver": "B", "content": content})
    assert response.status_code == 200, response.text
    return response.json()


def test_send_combines_wal_time_sync_and_replication(client):
    message = send(client)
    assert message["logical_timestamp"] == 1
    assert message["corrected_timestamp"]
    server.replicate_to_all.assert_called_once_with(message, skip_url=None)
    assert ft.wal_get_stats()["COMMITTED"] == 1
    assert ft.wal_get_pending() == []
    assert client.get("/messages").json() == [message]


def test_failed_write_records_wal_failure(client, monkeypatch):
    monkeypatch.setattr(server, "add_message", Mock(side_effect=OSError("disk failure")))
    assert client.post("/send", json={"sender": "A", "receiver": "B", "content": "x"}).status_code == 500
    assert ft.wal_get_stats()["FAILED"] == 1


def test_legacy_replication_preserves_deduplication(client):
    message = {"id": "old", "sender": "A", "receiver": "B", "content": "x",
               "timestamp": "2026-01-01T00:00:00"}
    first = client.post("/replicate", json=message)
    assert first.status_code == 200
    assert first.json()["status"] == "replicated"
    assert first.json()["corrected_timestamp"] == message["timestamp"]
    assert client.post("/replicate", json=message).json()["status"] == "duplicate skipped"
    assert len(client.get("/messages").json()) == 1
    assert ft.wal_get_stats()["COMMITTED"] == 1


def test_pending_wal_recovery_preserves_message_order(client):
    message = send(client)
    database.clear_all()
    ft.wal_write(message)
    assert ft.recover_from_wal(database.add_message) == 1
    assert send(client, "after restart")["logical_timestamp"] > message["logical_timestamp"]
    assert len(client.get("/messages").json()) == 2


@pytest.mark.asyncio
async def test_startup_recovers_pending_replica_write_to_current_primary(client, monkeypatch):
    message = {"id": "pending-replica", "sender": "A", "receiver": "B", "content": "x",
               "timestamp": "2026-01-01T00:00:00", "corrected_timestamp": "2026-01-01T00:00:00",
               "logical_timestamp": 12}
    ft.wal_write(message)
    primary = "http://localhost:8001"
    monkeypatch.setattr(server, "initialize_node", lambda: server.update_cluster_state(primary_url=primary))
    monkeypatch.setattr(server, "start_heartbeat", Mock())
    monkeypatch.setattr(server, "stop_heartbeat", Mock())
    monkeypatch.setattr(server, "start_recovery_manager", Mock())
    monkeypatch.setattr(server, "stop_recovery_manager", Mock())
    monkeypatch.setattr(server, "heartbeat_loop", Mock())
    monkeypatch.setattr(nm, "_on_node_down_callbacks", [])
    monkeypatch.setattr(nm, "_on_node_recover_callbacks", [])
    remote = Mock()
    monkeypatch.setattr(ft.httpx, "Client", Mock(return_value=Mock(
        __enter__=Mock(return_value=remote), __exit__=Mock(return_value=False))))
    async with server.lifespan(server.app):
        assert database.get_all_messages() == [message]
        assert server.get_state_snapshot()["logical_clock"] >= 12
    remote.post.assert_called_once_with(f"{primary}/replicate", json=message, timeout=2.0)
    assert ft.wal_get_pending() == []


def test_delta_sync_and_rejoin_check_http_failures(client, monkeypatch):
    first = send(client, "first")
    second = send(client, "second")
    # Give the messages distinct physical times to make the delta boundary explicit.
    database.clear_all()
    first["timestamp"] = "2026-01-01T00:00:00"
    second["timestamp"] = "2026-01-01T00:00:01"
    database.add_message(first)
    database.add_message(second)
    assert client.get("/sync", params={"since": first["timestamp"]}).json() == [second]
    remote = Mock()
    remote.post.return_value = httpx.Response(503, request=httpx.Request("POST", "http://node/replicate"))
    monkeypatch.setattr(server.httpx, "Client", Mock(return_value=Mock(
        __enter__=Mock(return_value=remote), __exit__=Mock(return_value=False))))
    response = client.post("/rejoin", params={"url": "http://localhost:8001", "since": first["timestamp"]})
    assert response.json()["messages_failed"] == 1
    assert response.json()["messages_sent"] == 0
    assert remote.post.call_args.kwargs["json"] == second


def test_recovery_manager_retries_incomplete_recovery(client, monkeypatch):
    remote = Mock()
    remote.post.return_value.json.return_value = {"messages_failed": 1}
    monkeypatch.setattr(ft.httpx, "Client", Mock(return_value=Mock(
        __enter__=Mock(return_value=remote), __exit__=Mock(return_value=False))))
    assert ft._do_recovery("http://localhost:8001", None, server.OWN_URL) is False


def test_health_monitor_uses_existing_raft_primary(client, monkeypatch):
    leader = "http://localhost:8004"
    monkeypatch.setattr(nm, "_primary_provider", lambda: leader)
    assert nm.get_primary() == leader
    assert len(nm.ALL_SERVERS) == 5


def test_follower_forwards_without_writing_second_wal(client, monkeypatch):
    monkeypatch.setattr(server, "CURRENT_PRIMARY_URL", "http://localhost:8001")
    message = {"id": "forwarded", "sender": "A", "receiver": "B", "content": "x",
               "timestamp": "2026-01-01T00:00:00", "corrected_timestamp": "2026-01-01T00:00:00",
               "logical_timestamp": 9}
    monkeypatch.setattr(server, "forward_send_to_primary", Mock(return_value=message))
    assert send(client) == message
    assert ft.wal_get_stats()["total"] == 0


def test_restored_raft_and_time_handlers(client):
    vote = client.post("/raft/request-vote", json={"candidate_id": 8001,
        "candidate_url": "http://localhost:8001", "term": 2, "last_logical_timestamp": 0})
    assert vote.status_code == 200
    assert vote.json()["vote_granted"] is True
    append = client.post("/raft/append-entries", json={"leader_id": 8001,
        "leader_url": "http://localhost:8001", "term": 2, "commit_index": 1,
        "known_nodes": [server.OWN_URL, "http://localhost:8001"]})
    assert append.json()["success"] is True
    assert client.get("/role").json()["role"] == "replica"
    assert client.post("/time-sync", json={"client_send_time_ms": 0}).status_code == 200


def test_replication_retries_and_reports_recovery(client, monkeypatch):
    remote = Mock()
    remote.post.side_effect = [OSError("offline"), Mock()]
    monkeypatch.setattr(replication.httpx, "Client", Mock(return_value=Mock(
        __enter__=Mock(return_value=remote), __exit__=Mock(return_value=False))))
    monkeypatch.setattr(replication.time, "sleep", Mock())
    assert replication._push_to_replica("http://localhost:8001", {"id": "retry"})
    assert remote.post.call_count == 2
    assert replication.REPLICA_STATUS["http://localhost:8001"] is True


def test_ui_and_fault_tolerance_status_coexist(client):
    assert client.get("/", headers={"Accept": "text/html"}).status_code == 200
    assert client.get("/health").json()["status"] == "healthy"
    assert "time" in client.get("/ui/status").json()
    assert "wal" in client.get("/ft-status").json()
    assert client.get("/status").json()["message_count"] == 0
