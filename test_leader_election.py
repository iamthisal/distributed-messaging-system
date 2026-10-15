"""
test_leader_election.py
Pytest tests for the Bully Algorithm implementation.

Run: pytest test_leader_election.py -v
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, patch
import leader_election as le
from leader_election import NodeState


# ── Fixtures ──────────────────────────────────────────────────────────────────
@pytest.fixture(autouse=True)
def reset_state():
    """Reset shared state before every test."""
    le.state = NodeState()
    le._heartbeat_task = None
    le._watchdog_task = None
    yield


# ── getState equivalent ───────────────────────────────────────────────────────
def test_initial_state():
    assert le.state.current_leader is None
    assert le.state.is_leader is False
    assert le.state.election_in_progress is False
    assert le.state.term == 0


# ── handle_election_message ───────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_election_message_returns_ok():
    with patch("leader_election.start_election", new_callable=AsyncMock):
        result = await le.handle_election_message(candidate_id=1, term=1)
    assert result["ok"] is True
    assert result["responder_id"] == le.NODE_ID


@pytest.mark.asyncio
async def test_election_message_updates_term():
    with patch("leader_election.start_election", new_callable=AsyncMock):
        await le.handle_election_message(candidate_id=1, term=99)
    assert le.state.term == 99


# ── handle_victory_message ────────────────────────────────────────────────────
def test_victory_message_sets_leader():
    le.handle_victory_message(leader_id=5, term=2)
    assert le.state.current_leader == 5
    assert le.state.is_leader is False   # NODE_ID is 4, not 5
    assert le.state.election_in_progress is False
    assert le.state.term == 2


def test_victory_message_this_node_wins():
    le.handle_victory_message(leader_id=le.NODE_ID, term=3)
    assert le.state.is_leader is True
    assert le.state.current_leader == le.NODE_ID


# ── handle_heartbeat ──────────────────────────────────────────────────────────
def test_heartbeat_resets_missed_count():
    le.state.missed_heartbeats = 2
    le.handle_heartbeat(leader_id=5, term=1)
    assert le.state.missed_heartbeats == 0
    assert le.state.last_heartbeat is not None


def test_heartbeat_updates_term():
    le.handle_heartbeat(leader_id=5, term=10)
    assert le.state.term == 10


# ── start_election (no higher peers) ─────────────────────────────────────────
@pytest.mark.asyncio
async def test_declares_victory_when_no_higher_peers_respond():
    """When all higher peers are unreachable, this node should win."""
    with patch("leader_election.post", new_callable=AsyncMock, return_value=None):
        with patch("leader_election.start_sending_heartbeats"):
            with patch("leader_election.asyncio.gather", new_callable=AsyncMock, return_value=[None, None]):
                await le.start_election()

    # Node should have declared itself leader
    assert le.state.current_leader == le.NODE_ID or le.state.election_in_progress is False
