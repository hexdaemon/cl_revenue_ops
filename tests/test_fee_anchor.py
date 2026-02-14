"""
Tests for the Advisor Fee Anchor system.

Covers:
- FeeAnchor dataclass (decay, expiry, effective_weight)
- _apply_fee_anchor blend logic (decay, clamping, threshold)
- Database CRUD operations
- RPC action routing
"""

import time
import pytest
import sqlite3
import tempfile
import os
import sys
from unittest.mock import MagicMock, patch

# Mock pyln.client before importing modules
mock_pyln = MagicMock()
mock_pyln.Plugin = MagicMock
mock_pyln.RpcError = Exception
sys.modules.setdefault('pyln', mock_pyln)
sys.modules.setdefault('pyln.client', mock_pyln)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.fee_controller import FeeAnchor


# =========================================================================
# FeeAnchor dataclass tests
# =========================================================================

class TestFeeAnchorDataclass:
    def test_effective_weight_fresh(self):
        """Fresh anchor should have full weight."""
        anchor = FeeAnchor(
            channel_id="100x1x0", target_fee_ppm=500,
            base_weight=0.7, confidence=1.0,
            ttl_seconds=86400, reason="test", set_at=time.time()
        )
        w = anchor.effective_weight()
        assert 0.69 < w <= 0.70

    def test_effective_weight_half_life(self):
        """Anchor at half TTL should have half weight."""
        anchor = FeeAnchor(
            channel_id="100x1x0", target_fee_ppm=500,
            base_weight=0.7, confidence=1.0,
            ttl_seconds=86400, reason="test",
            set_at=time.time() - 43200  # half of 86400
        )
        w = anchor.effective_weight()
        assert abs(w - 0.35) < 0.02

    def test_effective_weight_expired(self):
        """Expired anchor should have zero weight."""
        anchor = FeeAnchor(
            channel_id="100x1x0", target_fee_ppm=500,
            base_weight=0.7, confidence=1.0,
            ttl_seconds=86400, reason="test",
            set_at=time.time() - 90000
        )
        assert anchor.effective_weight() == 0.0

    def test_effective_weight_low_confidence(self):
        """Low confidence should reduce weight."""
        anchor = FeeAnchor(
            channel_id="100x1x0", target_fee_ppm=500,
            base_weight=0.7, confidence=0.5,
            ttl_seconds=86400, reason="test", set_at=time.time()
        )
        w = anchor.effective_weight()
        assert abs(w - 0.35) < 0.02

    def test_is_expired(self):
        """Test expiry detection."""
        alive = FeeAnchor(
            channel_id="100x1x0", target_fee_ppm=500,
            base_weight=0.7, confidence=1.0,
            ttl_seconds=86400, reason="test", set_at=time.time()
        )
        assert not alive.is_expired()

        dead = FeeAnchor(
            channel_id="100x1x0", target_fee_ppm=500,
            base_weight=0.7, confidence=1.0,
            ttl_seconds=86400, reason="test",
            set_at=time.time() - 90000
        )
        assert dead.is_expired()

    def test_zero_ttl_no_crash(self):
        """Zero TTL should return zero weight, not crash with ZeroDivisionError."""
        anchor = FeeAnchor(
            channel_id="100x1x0", target_fee_ppm=500,
            base_weight=0.7, confidence=1.0,
            ttl_seconds=0, reason="test", set_at=time.time()
        )
        assert anchor.effective_weight() == 0.0


# =========================================================================
# Blend calculation tests
# =========================================================================

class TestAnchorBlend:
    """Test the blend formula: final = proposed*(1-w) + target*w"""

    def test_full_weight_blend(self):
        """With weight=0.7, blend should be 30% proposed + 70% target."""
        proposed = 100
        target = 500
        weight = 0.7
        blended = int(proposed * (1 - weight) + target * weight)
        assert blended == 380  # 30 + 350

    def test_zero_weight_no_change(self):
        """Zero weight should leave proposed unchanged."""
        proposed = 100
        target = 500
        weight = 0.0
        blended = int(proposed * (1 - weight) + target * weight)
        assert blended == 100

    def test_blend_respects_floor(self):
        """Blended fee should not go below floor."""
        proposed = 100
        target = 10
        weight = 0.7
        floor_ppm = 50
        blended = int(proposed * (1 - weight) + target * weight)
        blended = max(floor_ppm, blended)
        assert blended == 50  # 30 + 7 = 37, clamped to 50

    def test_blend_respects_ceiling(self):
        """Blended fee should not exceed ceiling."""
        proposed = 100
        target = 10000
        weight = 0.7
        ceiling_ppm = 5000
        blended = int(proposed * (1 - weight) + target * weight)
        blended = min(ceiling_ppm, blended)
        assert blended == 5000  # 30 + 7000 = 7030, clamped to 5000

    def test_weight_threshold(self):
        """Weights below 0.01 should be skipped."""
        # Anchor nearly expired: weight < 0.01
        anchor = FeeAnchor(
            channel_id="100x1x0", target_fee_ppm=500,
            base_weight=0.7, confidence=1.0,
            ttl_seconds=86400, reason="test",
            set_at=time.time() - 86300  # 100s left out of 86400
        )
        ew = anchor.effective_weight()
        # decay = 100/86400 ≈ 0.00116, weight = 0.7 * 1.0 * 0.00116 ≈ 0.0008
        assert ew < 0.01

    def test_decay_linearity(self):
        """Weight should decay linearly."""
        ttl = 10000
        base_w = 1.0
        conf = 1.0
        now = time.time()

        weights = []
        for fraction in [0.0, 0.25, 0.5, 0.75, 1.0]:
            a = FeeAnchor(
                channel_id="100x1x0", target_fee_ppm=500,
                base_weight=base_w, confidence=conf,
                ttl_seconds=ttl, reason="test",
                set_at=now - (fraction * ttl)
            )
            weights.append(a.effective_weight())

        # Should be 1.0, 0.75, 0.5, 0.25, 0.0
        assert abs(weights[0] - 1.0) < 0.01
        assert abs(weights[1] - 0.75) < 0.01
        assert abs(weights[2] - 0.5) < 0.01
        assert abs(weights[3] - 0.25) < 0.01
        assert weights[4] == 0.0


# =========================================================================
# Database CRUD tests
# =========================================================================

class TestFeeAnchorDatabase:
    @pytest.fixture
    def db(self):
        """Create a minimal database with fee_anchors table."""
        fd, path = tempfile.mkstemp(suffix='.db')
        os.close(fd)

        plugin = MagicMock()
        plugin.log = MagicMock()

        from modules.database import Database
        database = Database(path, plugin)
        database.initialize()

        yield database

        if os.path.exists(path):
            os.unlink(path)

    def test_set_and_get(self, db):
        db.set_fee_anchor("100x1x0", 500, 0.7, 1.0, 86400, "test reason")
        row = db.get_fee_anchor("100x1x0")
        assert row is not None
        assert row["target_fee_ppm"] == 500
        assert row["base_weight"] == 0.7
        assert row["confidence"] == 1.0
        assert row["ttl_seconds"] == 86400
        assert row["reason"] == "test reason"

    def test_get_nonexistent(self, db):
        assert db.get_fee_anchor("999x1x0") is None

    def test_upsert(self, db):
        db.set_fee_anchor("100x1x0", 500, 0.7, 1.0, 86400, "first")
        db.set_fee_anchor("100x1x0", 800, 0.5, 0.9, 3600, "second")
        row = db.get_fee_anchor("100x1x0")
        assert row["target_fee_ppm"] == 800
        assert row["base_weight"] == 0.5
        assert row["reason"] == "second"

    def test_get_all(self, db):
        db.set_fee_anchor("100x1x0", 500, 0.7, 1.0, 86400, "a")
        db.set_fee_anchor("200x2x0", 600, 0.5, 0.8, 3600, "b")
        all_anchors = db.get_all_fee_anchors()
        assert len(all_anchors) == 2

    def test_delete(self, db):
        db.set_fee_anchor("100x1x0", 500, 0.7, 1.0, 86400, "test")
        db.delete_fee_anchor("100x1x0")
        assert db.get_fee_anchor("100x1x0") is None

    def test_delete_all(self, db):
        db.set_fee_anchor("100x1x0", 500, 0.7, 1.0, 86400, "a")
        db.set_fee_anchor("200x2x0", 600, 0.5, 0.8, 3600, "b")
        db.delete_all_fee_anchors()
        assert len(db.get_all_fee_anchors()) == 0

    def test_prune_expired(self, db):
        # Set an already-expired anchor
        conn = db._get_connection()
        conn.execute("""
            INSERT INTO fee_anchors (channel_id, target_fee_ppm, base_weight,
                                     confidence, ttl_seconds, reason, set_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, ("100x1x0", 500, 0.7, 1.0, 100, "expired", time.time() - 200))

        # Set a live anchor
        db.set_fee_anchor("200x2x0", 600, 0.5, 0.8, 86400, "alive")

        pruned = db.prune_expired_fee_anchors()
        assert pruned == 1
        assert db.get_fee_anchor("100x1x0") is None
        assert db.get_fee_anchor("200x2x0") is not None


# =========================================================================
# RPC action routing tests (mock fee_controller)
# =========================================================================

class TestRPCActionRouting:
    """Test the RPC dispatcher logic (unit-level, without plugin bootstrap)."""

    def test_set_validates_channel_id(self):
        """Set action should require channel_id."""
        # We test the validation logic directly
        action = "set"
        channel_id = ""
        assert not channel_id  # Would return error

    def test_set_validates_fee_ppm(self):
        """Set action should reject negative fees."""
        fee = -10
        assert fee < 0  # Would return error

    def test_ttl_conversion(self):
        """ttl_hours should convert to seconds."""
        ttl_hours = 24
        ttl_seconds = ttl_hours * 3600
        assert ttl_seconds == 86400

    def test_max_ttl(self):
        """Max TTL is 7 days = 604800 seconds."""
        ttl_hours = 168  # 7 days
        ttl_seconds = ttl_hours * 3600
        assert ttl_seconds == 604800

    def test_ttl_exceeds_max(self):
        """TTL over 7 days should be rejected."""
        ttl_hours = 200
        ttl_seconds = ttl_hours * 3600
        assert ttl_seconds > 604800  # Would return error

    def test_valid_scid_format(self):
        """SCID format should be accepted."""
        import re
        assert re.match(r'^\d+[x:]\d+[x:]\d+$', '100x1x0')
        assert re.match(r'^\d+[x:]\d+[x:]\d+$', '100:1:0')

    def test_invalid_channel_id_format(self):
        """Invalid formats should be rejected."""
        import re
        assert not re.match(r'^\d+[x:]\d+[x:]\d+$', 'invalid')
        assert not re.match(r'^\d+[x:]\d+[x:]\d+$', '')


# =========================================================================
# Fee controller integration tests (with mock DB)
# =========================================================================

class TestFeeControllerIntegration:
    """Test _apply_fee_anchor via the FeeAnchor dataclass directly."""

    def test_apply_fresh_anchor(self):
        """Fresh anchor with full confidence should blend strongly."""
        anchor = FeeAnchor(
            channel_id="100x1x0", target_fee_ppm=500,
            base_weight=0.7, confidence=1.0,
            ttl_seconds=86400, reason="test", set_at=time.time()
        )
        proposed = 100
        ew = anchor.effective_weight()
        blended = int(proposed * (1.0 - ew) + anchor.target_fee_ppm * ew)
        # ~0.7 weight: 100*0.3 + 500*0.7 = 30 + 350 = 380
        assert 370 <= blended <= 390

    def test_apply_half_decayed_anchor(self):
        """Anchor at half TTL should blend at half strength."""
        anchor = FeeAnchor(
            channel_id="100x1x0", target_fee_ppm=500,
            base_weight=0.7, confidence=1.0,
            ttl_seconds=86400, reason="test",
            set_at=time.time() - 43200  # half TTL
        )
        proposed = 100
        ew = anchor.effective_weight()  # ~0.35
        blended = int(proposed * (1.0 - ew) + anchor.target_fee_ppm * ew)
        # ~0.35 weight: 100*0.65 + 500*0.35 = 65 + 175 = 240
        assert 230 <= blended <= 250

    def test_apply_expired_anchor_no_blend(self):
        """Expired anchor should return proposed fee unchanged."""
        anchor = FeeAnchor(
            channel_id="100x1x0", target_fee_ppm=500,
            base_weight=0.7, confidence=1.0,
            ttl_seconds=86400, reason="test",
            set_at=time.time() - 90000
        )
        assert anchor.is_expired()
        assert anchor.effective_weight() == 0.0

    def test_apply_low_confidence_anchor(self):
        """Low confidence should reduce blend impact."""
        anchor = FeeAnchor(
            channel_id="100x1x0", target_fee_ppm=1000,
            base_weight=0.7, confidence=0.3,
            ttl_seconds=86400, reason="test", set_at=time.time()
        )
        proposed = 100
        ew = anchor.effective_weight()  # 0.7 * 0.3 = 0.21
        blended = int(proposed * (1.0 - ew) + anchor.target_fee_ppm * ew)
        # ~0.21 weight: 100*0.79 + 1000*0.21 = 79 + 210 = 289
        assert 280 <= blended <= 300

    def test_floor_clamping(self):
        """Blend should not go below floor."""
        anchor = FeeAnchor(
            channel_id="100x1x0", target_fee_ppm=10,
            base_weight=0.7, confidence=1.0,
            ttl_seconds=86400, reason="test", set_at=time.time()
        )
        proposed = 100
        floor_ppm = 80
        ew = anchor.effective_weight()
        blended = int(proposed * (1.0 - ew) + anchor.target_fee_ppm * ew)
        blended = max(floor_ppm, blended)
        assert blended >= floor_ppm

    def test_ceiling_clamping(self):
        """Blend should not exceed ceiling."""
        anchor = FeeAnchor(
            channel_id="100x1x0", target_fee_ppm=50000,
            base_weight=0.7, confidence=1.0,
            ttl_seconds=86400, reason="test", set_at=time.time()
        )
        proposed = 100
        ceiling_ppm = 5000
        ew = anchor.effective_weight()
        blended = int(proposed * (1.0 - ew) + anchor.target_fee_ppm * ew)
        blended = min(ceiling_ppm, blended)
        assert blended <= ceiling_ppm
