# Barnacles Communication Protocol

This document defines the HTTP Batch Ingestion API and the WebSocket Real-Time Streaming Protocol.

---

## 1. HTTP Ingestion API

Agents deliver batches to the central server via HTTP POST.

### Endpoint
```http
POST /api/v1/ingest
Content-Type: application/json
Authorization: Bearer <token>
```

### Request Payload (`IngestRequest`)
```json
{
  "agent_id": "agent-us-east-01",
  "events": [
    {
      "id": "c1f76092-23c7-43cf-bc01-e28e08d66141",
      "timestamp": "2026-08-14T12:00:00Z",
      "host": "srv-prod-01",
      "source": "nginx-access",
      "level": "INFO",
      "message": "GET /api/v1/users 200 45ms",
      "fields": {
        "status": "200",
        "ip": "10.0.1.42",
        "latency_ms": "45"
      }
    }
  ]
}
```

### Response Payload (`IngestResponse`)
```json
{
  "status": "ok",
  "accepted": 1,
  "duplicates": 0,
  "errors": []
}
```

### HTTP Status Codes
- `200 OK`: Batch accepted and processed successfully.
- `400 Bad Request`: Payload validation failed, malformed JSON, or batch exceeded size limits. (Non-retryable)
- `401 Unauthorized`: Missing or invalid Bearer token. (Non-retryable)
- `429 Too Many Requests`: Server rate limit reached. (Retryable with exponential backoff)
- `500 Internal Server Error`: Server storage or persistence error. (Retryable with exponential backoff)
- `503 Service Unavailable`: Server is in maintenance or shutting down. (Retryable with exponential backoff)

---

## 2. WebSocket Streaming Protocol

Browsers and real-time clients stream logs over WebSockets.

### Endpoint
```http
GET /ws
GET /api/v1/stream
```

### Authentication
Can be supplied via HTTP header `Authorization: Bearer <token>` or URL query parameter `/ws?token=<token>`.

### Envelope Formats

#### 1. Initial State Batch (`recent_batch`)
Sent immediately upon connection to populate the client dashboard with recent in-memory logs:
```json
{
  "type": "recent_batch",
  "data": [
    {
      "id": "e-100",
      "timestamp": "2026-08-14T11:59:58Z",
      "host": "srv-prod-01",
      "source": "api",
      "level": "INFO",
      "message": "service healthy"
    }
  ]
}
```

#### 2. Live Log Event (`log`)
Broadcast to connected clients in real-time:
```json
{
  "type": "log",
  "data": {
    "id": "c1f76092-23c7-43cf-bc01-e28e08d66141",
    "timestamp": "2026-08-14T12:00:00Z",
    "host": "srv-prod-01",
    "source": "nginx-access",
    "level": "INFO",
    "message": "GET /api/v1/users 200 45ms",
    "fields": {
      "status": "200"
    }
  }
}
```

#### 3. Heartbeats
WebSocket Ping frames are sent by the server every `ping_interval` (default 30s). Standard Pong responses are validated to keep connections alive and detect stale sockets.

---

## 3. REST Query API

### Endpoint
```http
GET /api/v1/logs?host=srv01&source=nginx&level=ERROR&search=timeout&limit=100
```

### Parameters
- `host`: (string) Filter by host identifier
- `source`: (string) Filter by source name
- `level`: (string) Filter by log level (e.g. `INFO`, `WARN`, `ERROR`)
- `start_time`: (RFC3339 timestamp) Start of time window
- `end_time`: (RFC3339 timestamp) End of time window
- `search`: (string) Case-insensitive substring query matching message or fields
- `limit`: (integer) Maximum number of entries (default: 100, max: 10000)

### Response
```json
{
  "total": 1,
  "logs": [
    {
      "id": "...",
      "timestamp": "2026-08-14T12:00:00Z",
      "host": "srv01",
      "source": "nginx",
      "level": "ERROR",
      "message": "upstream connection timeout"
    }
  ]
}
```
