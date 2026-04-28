"""
Production health monitoring with Prometheus-compatible metrics export.

Provides:
- MetricsCollector: Singleton for collecting and exporting metrics
- Request, DB, cache, WebSocket, queue, and system metrics
- Prometheus text exposition format export
- Alerting based on configurable thresholds

Usage:
    from rosteriq.services.monitoring import MetricsCollector

    collector = MetricsCollector()
    collector.record_request("GET", "/api/v1/rosters", 200, 45.2)
    prometheus_text = collector.to_prometheus()
"""

import time
import logging
import json
import os
import resource
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from enum import Enum
from collections import deque

logger = logging.getLogger(__name__)


class AlertLevel(Enum):
    """Alert severity levels."""
    CRITICAL = "critical"
    WARNING = "warning"
    INFO = "info"


@dataclass
class Alert:
    """Represents an active alert."""
    level: AlertLevel
    name: str
    value: float
    threshold: float
    message: str
    timestamp: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "level": self.level.value,
            "name": self.name,
            "value": self.value,
            "threshold": self.threshold,
            "message": self.message,
            "timestamp": self.timestamp.isoformat(),
        }


class ReservoirSample:
    """Reservoir sampling for percentile calculation without storing all values."""

    def __init__(self, max_size: int = 1000):
        self.max_size = max_size
        self.samples = deque(maxlen=max_size)
        self.count = 0

    def add(self, value: float):
        """Add a value using reservoir sampling."""
        self.count += 1
        if len(self.samples) < self.max_size:
            self.samples.append(value)
        else:
            # Random replacement with decreasing probability
            import random
            j = random.randint(0, self.count - 1)
            if j < self.max_size:
                self.samples[j] = value

    def get_percentile(self, percentile: float) -> Optional[float]:
        """Get approximate percentile (0-100)."""
        if not self.samples:
            return None
        sorted_samples = sorted(self.samples)
        idx = int(len(sorted_samples) * percentile / 100)
        idx = min(idx, len(sorted_samples) - 1)
        return float(sorted_samples[idx])

    def get_min(self) -> Optional[float]:
        """Get minimum value."""
        return min(self.samples) if self.samples else None

    def get_max(self) -> Optional[float]:
        """Get maximum value."""
        return max(self.samples) if self.samples else None

    def get_mean(self) -> Optional[float]:
        """Get mean value."""
        if not self.samples:
            return None
        return sum(self.samples) / len(self.samples)


class MetricsCollector:
    """Singleton metrics collector for production monitoring."""

    _instance: Optional["MetricsCollector"] = None

    def __new__(cls) -> "MetricsCollector":
        """Ensure singleton."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        """Initialize metrics collector."""
        if self._initialized:
            return

        # Request metrics
        self.request_count: int = 0
        self.error_count: int = 0
        self.request_durations: Dict[str, ReservoirSample] = {}
        self.request_status_codes: Dict[int, int] = {}
        self.requests_by_endpoint: Dict[str, int] = {}

        # DB metrics
        self.db_query_count: int = 0
        self.db_query_durations: ReservoirSample = ReservoirSample()
        self.db_pool_active: int = 0
        self.db_pool_idle: int = 0
        self.db_pool_max: int = 20  # Default, updated from config
        self.db_queries_by_type: Dict[str, int] = {}

        # Cache metrics
        self.cache_hits: int = 0
        self.cache_misses: int = 0
        self.cache_sets: int = 0
        self.cache_evictions: int = 0

        # WebSocket metrics
        self.ws_connections_current: int = 0
        self.ws_connections_total: int = 0
        self.ws_messages_sent: int = 0
        self.ws_messages_received: int = 0

        # Queue metrics
        self.queue_depths: Dict[str, int] = {}
        self.queue_processed: Dict[str, int] = {}
        self.queue_errors: Dict[str, int] = {}

        # System metrics
        self.start_time: float = time.time()
        self.last_memory_check: float = time.time()
        self.peak_memory_bytes: int = 0
        self.current_memory_bytes: int = 0

        # Alert configuration (threshold defaults)
        self.alert_config = {
            "error_rate_threshold": 5.0,  # %
            "error_rate_window": 300,  # seconds (5 min)
            "p99_latency_threshold": 2000,  # ms
            "db_pool_utilization_threshold": 90,  # %
            "queue_depth_threshold": 100,
            "memory_usage_threshold": 512 * 1024 * 1024,  # 512 MB
            "cache_hit_rate_threshold": 50,  # %
        }

        # Active alerts
        self.active_alerts: Dict[str, Alert] = {}

        # Recent request tracking for error rate calculation
        self.recent_requests: deque = deque(maxlen=1000)

        self._initialized = True
        logger.info("MetricsCollector initialized")

    def load_config(self, config_path: str = "monitoring_config.json"):
        """Load alert thresholds from config file."""
        if not os.path.exists(config_path):
            logger.warning(f"Config file not found: {config_path}")
            return

        try:
            with open(config_path, "r") as f:
                config = json.load(f)
            self.alert_config.update(config.get("thresholds", {}))
            logger.info(f"Loaded monitoring config from {config_path}")
        except Exception as e:
            logger.error(f"Failed to load config: {e}")

    def record_request(
        self,
        method: str,
        path: str,
        status_code: int,
        duration_ms: float,
    ):
        """Record an HTTP request."""
        self.request_count += 1
        self.request_status_codes[status_code] = self.request_status_codes.get(status_code, 0) + 1
        self.requests_by_endpoint[path] = self.requests_by_endpoint.get(path, 0) + 1

        # Track error count
        if status_code >= 400:
            self.error_count += 1

        # Track duration by endpoint
        if path not in self.request_durations:
            self.request_durations[path] = ReservoirSample()
        self.request_durations[path].add(duration_ms)

        # Track recent requests for error rate calculation
        self.recent_requests.append({
            "timestamp": time.time(),
            "status_code": status_code,
        })

    def record_db_query(self, query_type: str, duration_ms: float):
        """Record a database query."""
        self.db_query_count += 1
        self.db_query_durations.add(duration_ms)
        self.db_queries_by_type[query_type] = self.db_queries_by_type.get(query_type, 0) + 1

    def set_db_pool_stats(self, active: int, idle: int, max_size: int):
        """Update database pool statistics."""
        self.db_pool_active = active
        self.db_pool_idle = idle
        self.db_pool_max = max_size

    def record_cache_hit(self):
        """Record a cache hit."""
        self.cache_hits += 1

    def record_cache_miss(self):
        """Record a cache miss."""
        self.cache_misses += 1

    def record_cache_set(self):
        """Record a cache set operation."""
        self.cache_sets += 1

    def record_cache_eviction(self):
        """Record a cache eviction."""
        self.cache_evictions += 1

    def track_ws_connect(self):
        """Track WebSocket connection."""
        self.ws_connections_current += 1
        self.ws_connections_total += 1

    def track_ws_disconnect(self):
        """Track WebSocket disconnection."""
        self.ws_connections_current = max(0, self.ws_connections_current - 1)

    def record_ws_message_sent(self, count: int = 1):
        """Record WebSocket messages sent."""
        self.ws_messages_sent += count

    def record_ws_message_received(self, count: int = 1):
        """Record WebSocket messages received."""
        self.ws_messages_received += count

    def record_queue_depth(self, queue_name: str, depth: int):
        """Record queue depth."""
        self.queue_depths[queue_name] = depth

    def record_queue_processed(self, queue_name: str, count: int = 1):
        """Record queue items processed."""
        self.queue_processed[queue_name] = self.queue_processed.get(queue_name, 0) + count

    def record_queue_error(self, queue_name: str, count: int = 1):
        """Record queue processing error."""
        self.queue_errors[queue_name] = self.queue_errors.get(queue_name, 0) + count

    def update_memory_usage(self):
        """Update memory usage metrics."""
        try:
            usage = resource.getrusage(resource.RUSAGE_SELF)
            # RSS in bytes (maxrss is in KB on some systems)
            rss_bytes = usage.ru_maxrss * 1024
            self.current_memory_bytes = rss_bytes
            self.peak_memory_bytes = max(self.peak_memory_bytes, rss_bytes)
            self.last_memory_check = time.time()
        except Exception as e:
            logger.warning(f"Failed to get memory usage: {e}")

    def get_uptime_seconds(self) -> float:
        """Get server uptime in seconds."""
        return time.time() - self.start_time

    def get_cache_hit_rate(self) -> Optional[float]:
        """Get cache hit rate as percentage."""
        total = self.cache_hits + self.cache_misses
        if total == 0:
            return None
        return (self.cache_hits / total) * 100

    def get_error_rate(self, window_seconds: Optional[int] = None) -> float:
        """Get error rate as percentage over a time window."""
        if window_seconds is None:
            window_seconds = self.alert_config["error_rate_window"]

        cutoff_time = time.time() - window_seconds
        errors_in_window = sum(
            1 for req in self.recent_requests
            if req["timestamp"] >= cutoff_time and req["status_code"] >= 400
        )
        requests_in_window = sum(
            1 for req in self.recent_requests
            if req["timestamp"] >= cutoff_time
        )

        if requests_in_window == 0:
            return 0.0
        return (errors_in_window / requests_in_window) * 100

    def get_db_pool_utilization(self) -> float:
        """Get DB pool utilization as percentage."""
        if self.db_pool_max == 0:
            return 0.0
        return (self.db_pool_active / self.db_pool_max) * 100

    def get_alerts(self) -> List[Alert]:
        """Check thresholds and return active alerts."""
        self.active_alerts.clear()

        # Error rate check
        error_rate = self.get_error_rate()
        if error_rate > self.alert_config["error_rate_threshold"]:
            alert = Alert(
                level=AlertLevel.WARNING,
                name="high_error_rate",
                value=error_rate,
                threshold=self.alert_config["error_rate_threshold"],
                message=f"Error rate {error_rate:.2f}% exceeds threshold {self.alert_config['error_rate_threshold']}%",
            )
            self.active_alerts["high_error_rate"] = alert

        # P99 latency check
        p99_latency = self._get_p99_latency()
        if p99_latency and p99_latency > self.alert_config["p99_latency_threshold"]:
            alert = Alert(
                level=AlertLevel.WARNING,
                name="high_p99_latency",
                value=p99_latency,
                threshold=self.alert_config["p99_latency_threshold"],
                message=f"P99 latency {p99_latency:.0f}ms exceeds threshold {self.alert_config['p99_latency_threshold']}ms",
            )
            self.active_alerts["high_p99_latency"] = alert

        # DB pool utilization check
        db_util = self.get_db_pool_utilization()
        if db_util > self.alert_config["db_pool_utilization_threshold"]:
            alert = Alert(
                level=AlertLevel.CRITICAL,
                name="high_db_pool_utilization",
                value=db_util,
                threshold=self.alert_config["db_pool_utilization_threshold"],
                message=f"DB pool utilization {db_util:.1f}% exceeds threshold {self.alert_config['db_pool_utilization_threshold']}%",
            )
            self.active_alerts["high_db_pool_utilization"] = alert

        # Queue depth check
        for queue_name, depth in self.queue_depths.items():
            if depth > self.alert_config["queue_depth_threshold"]:
                alert = Alert(
                    level=AlertLevel.WARNING,
                    name=f"queue_depth_{queue_name}",
                    value=float(depth),
                    threshold=self.alert_config["queue_depth_threshold"],
                    message=f"Queue '{queue_name}' depth {depth} exceeds threshold {self.alert_config['queue_depth_threshold']}",
                )
                self.active_alerts[f"queue_depth_{queue_name}"] = alert

        # Memory usage check
        self.update_memory_usage()
        if self.current_memory_bytes > self.alert_config["memory_usage_threshold"]:
            alert = Alert(
                level=AlertLevel.WARNING,
                name="high_memory_usage",
                value=float(self.current_memory_bytes),
                threshold=self.alert_config["memory_usage_threshold"],
                message=f"Memory usage {self.current_memory_bytes / 1024 / 1024:.1f}MB exceeds threshold {self.alert_config['memory_usage_threshold'] / 1024 / 1024:.1f}MB",
            )
            self.active_alerts["high_memory_usage"] = alert

        # Cache hit rate check
        cache_hit_rate = self.get_cache_hit_rate()
        if cache_hit_rate is not None and cache_hit_rate < self.alert_config["cache_hit_rate_threshold"]:
            alert = Alert(
                level=AlertLevel.INFO,
                name="low_cache_hit_rate",
                value=cache_hit_rate,
                threshold=self.alert_config["cache_hit_rate_threshold"],
                message=f"Cache hit rate {cache_hit_rate:.1f}% below threshold {self.alert_config['cache_hit_rate_threshold']}%",
            )
            self.active_alerts["low_cache_hit_rate"] = alert

        return list(self.active_alerts.values())

    def _get_p99_latency(self) -> Optional[float]:
        """Get P99 latency across all endpoints."""
        all_durations = []
        for sample in self.request_durations.values():
            all_durations.extend(sample.samples)

        if not all_durations:
            return None

        all_durations.sort()
        idx = int(len(all_durations) * 0.99)
        idx = min(idx, len(all_durations) - 1)
        return float(all_durations[idx])

    def to_prometheus(self) -> str:
        """Export metrics in Prometheus text exposition format."""
        self.update_memory_usage()
        lines = []

        # Request metrics
        lines.append("# HELP rosteriq_requests_total Total HTTP requests")
        lines.append("# TYPE rosteriq_requests_total counter")
        lines.append(f"rosteriq_requests_total {self.request_count}")

        lines.append("# HELP rosteriq_requests_errors_total Total HTTP errors")
        lines.append("# TYPE rosteriq_requests_errors_total counter")
        lines.append(f"rosteriq_requests_errors_total {self.error_count}")

        lines.append("# HELP rosteriq_request_duration_ms HTTP request duration in milliseconds")
        lines.append("# TYPE rosteriq_request_duration_ms histogram")
        for path, sample in self.request_durations.items():
            for percentile, label in [(50, "p50"), (95, "p95"), (99, "p99")]:
                val = sample.get_percentile(percentile)
                if val is not None:
                    lines.append(f'rosteriq_request_duration_ms{{path="{path}",percentile="{label}"}} {val:.2f}')

        # Request by status code
        lines.append("# HELP rosteriq_request_status_codes HTTP response status codes")
        lines.append("# TYPE rosteriq_request_status_codes gauge")
        for status_code, count in sorted(self.request_status_codes.items()):
            lines.append(f'rosteriq_request_status_codes{{status="{status_code}"}} {count}')

        # DB metrics
        lines.append("# HELP rosteriq_db_queries_total Total database queries")
        lines.append("# TYPE rosteriq_db_queries_total counter")
        lines.append(f"rosteriq_db_queries_total {self.db_query_count}")

        if self.db_query_durations.samples:
            lines.append("# HELP rosteriq_db_query_duration_ms Database query duration")
            lines.append("# TYPE rosteriq_db_query_duration_ms histogram")
            for percentile, label in [(50, "p50"), (95, "p95"), (99, "p99")]:
                val = self.db_query_durations.get_percentile(percentile)
                if val is not None:
                    lines.append(f"rosteriq_db_query_duration_ms{{percentile=\"{label}\"}} {val:.2f}")

        lines.append("# HELP rosteriq_db_pool_active Active database connections")
        lines.append("# TYPE rosteriq_db_pool_active gauge")
        lines.append(f"rosteriq_db_pool_active {self.db_pool_active}")

        lines.append("# HELP rosteriq_db_pool_idle Idle database connections")
        lines.append("# TYPE rosteriq_db_pool_idle gauge")
        lines.append(f"rosteriq_db_pool_idle {self.db_pool_idle}")

        lines.append("# HELP rosteriq_db_pool_max Maximum database connections")
        lines.append("# TYPE rosteriq_db_pool_max gauge")
        lines.append(f"rosteriq_db_pool_max {self.db_pool_max}")

        lines.append("# HELP rosteriq_db_pool_utilization Database pool utilization percentage")
        lines.append("# TYPE rosteriq_db_pool_utilization gauge")
        lines.append(f"rosteriq_db_pool_utilization {self.get_db_pool_utilization():.2f}")

        # Cache metrics
        lines.append("# HELP rosteriq_cache_hits_total Cache hits")
        lines.append("# TYPE rosteriq_cache_hits_total counter")
        lines.append(f"rosteriq_cache_hits_total {self.cache_hits}")

        lines.append("# HELP rosteriq_cache_misses_total Cache misses")
        lines.append("# TYPE rosteriq_cache_misses_total counter")
        lines.append(f"rosteriq_cache_misses_total {self.cache_misses}")

        cache_hit_rate = self.get_cache_hit_rate()
        if cache_hit_rate is not None:
            lines.append("# HELP rosteriq_cache_hit_rate Cache hit rate percentage")
            lines.append("# TYPE rosteriq_cache_hit_rate gauge")
            lines.append(f"rosteriq_cache_hit_rate {cache_hit_rate:.2f}")

        lines.append("# HELP rosteriq_cache_evictions_total Cache evictions")
        lines.append("# TYPE rosteriq_cache_evictions_total counter")
        lines.append(f"rosteriq_cache_evictions_total {self.cache_evictions}")

        # WebSocket metrics
        lines.append("# HELP rosteriq_ws_connections_active Active WebSocket connections")
        lines.append("# TYPE rosteriq_ws_connections_active gauge")
        lines.append(f"rosteriq_ws_connections_active {self.ws_connections_current}")

        lines.append("# HELP rosteriq_ws_connections_total Total WebSocket connections")
        lines.append("# TYPE rosteriq_ws_connections_total counter")
        lines.append(f"rosteriq_ws_connections_total {self.ws_connections_total}")

        lines.append("# HELP rosteriq_ws_messages_sent WebSocket messages sent")
        lines.append("# TYPE rosteriq_ws_messages_sent counter")
        lines.append(f"rosteriq_ws_messages_sent {self.ws_messages_sent}")

        lines.append("# HELP rosteriq_ws_messages_received WebSocket messages received")
        lines.append("# TYPE rosteriq_ws_messages_received counter")
        lines.append(f"rosteriq_ws_messages_received {self.ws_messages_received}")

        # Queue metrics
        lines.append("# HELP rosteriq_queue_depth Queue depth")
        lines.append("# TYPE rosteriq_queue_depth gauge")
        for queue_name, depth in self.queue_depths.items():
            lines.append(f'rosteriq_queue_depth{{queue="{queue_name}"}} {depth}')

        lines.append("# HELP rosteriq_queue_processed_total Queue items processed")
        lines.append("# TYPE rosteriq_queue_processed_total counter")
        for queue_name, count in self.queue_processed.items():
            lines.append(f'rosteriq_queue_processed_total{{queue="{queue_name}"}} {count}')

        # System metrics
        lines.append("# HELP rosteriq_memory_usage_bytes Current memory usage")
        lines.append("# TYPE rosteriq_memory_usage_bytes gauge")
        lines.append(f"rosteriq_memory_usage_bytes {self.current_memory_bytes}")

        lines.append("# HELP rosteriq_memory_usage_peak_bytes Peak memory usage")
        lines.append("# TYPE rosteriq_memory_usage_peak_bytes gauge")
        lines.append(f"rosteriq_memory_usage_peak_bytes {self.peak_memory_bytes}")

        lines.append("# HELP rosteriq_uptime_seconds Server uptime")
        lines.append("# TYPE rosteriq_uptime_seconds gauge")
        lines.append(f"rosteriq_uptime_seconds {self.get_uptime_seconds():.0f}")

        lines.append("# HELP rosteriq_error_rate_percent Error rate over 5-minute window")
        lines.append("# TYPE rosteriq_error_rate_percent gauge")
        lines.append(f"rosteriq_error_rate_percent {self.get_error_rate():.2f}")

        return "\n".join(lines) + "\n"

    def to_json(self) -> dict:
        """Export metrics in JSON format."""
        self.update_memory_usage()

        return {
            "timestamp": datetime.now().isoformat(),
            "uptime_seconds": self.get_uptime_seconds(),
            "requests": {
                "total": self.request_count,
                "errors": self.error_count,
                "error_rate_percent": self.get_error_rate(),
                "by_status": dict(self.request_status_codes),
                "by_endpoint": dict(self.requests_by_endpoint),
                "latency_ms": {
                    endpoint: {
                        "p50": sample.get_percentile(50),
                        "p95": sample.get_percentile(95),
                        "p99": sample.get_percentile(99),
                        "min": sample.get_min(),
                        "max": sample.get_max(),
                        "mean": sample.get_mean(),
                    }
                    for endpoint, sample in self.request_durations.items()
                },
            },
            "database": {
                "queries_total": self.db_query_count,
                "queries_by_type": dict(self.db_queries_by_type),
                "query_duration_ms": {
                    "p50": self.db_query_durations.get_percentile(50),
                    "p95": self.db_query_durations.get_percentile(95),
                    "p99": self.db_query_durations.get_percentile(99),
                    "min": self.db_query_durations.get_min(),
                    "max": self.db_query_durations.get_max(),
                    "mean": self.db_query_durations.get_mean(),
                },
                "pool": {
                    "active": self.db_pool_active,
                    "idle": self.db_pool_idle,
                    "max": self.db_pool_max,
                    "utilization_percent": self.get_db_pool_utilization(),
                },
            },
            "cache": {
                "hits": self.cache_hits,
                "misses": self.cache_misses,
                "hit_rate_percent": self.get_cache_hit_rate(),
                "sets": self.cache_sets,
                "evictions": self.cache_evictions,
            },
            "websocket": {
                "active_connections": self.ws_connections_current,
                "total_connections": self.ws_connections_total,
                "messages_sent": self.ws_messages_sent,
                "messages_received": self.ws_messages_received,
            },
            "queues": {
                "depths": dict(self.queue_depths),
                "processed": dict(self.queue_processed),
                "errors": dict(self.queue_errors),
            },
            "system": {
                "memory_usage_bytes": self.current_memory_bytes,
                "memory_usage_mb": self.current_memory_bytes / 1024 / 1024,
                "memory_peak_bytes": self.peak_memory_bytes,
                "memory_peak_mb": self.peak_memory_bytes / 1024 / 1024,
            },
        }


def get_metrics_collector() -> MetricsCollector:
    """Get the singleton metrics collector instance."""
    return MetricsCollector()
