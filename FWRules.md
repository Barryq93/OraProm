# Firewall Rules for Monitoring Stack

## Overview
- **VLAN A (Monitoring):** Prometheus, Grafana, Loki
- **VLAN B (Workload):** Grafana Alloy agents
- **Admin IP:** `<YOUR_ADMIN_IP>` (replace with your actual IP/CIDR)

---

## Cross-VLAN Rules (Alloy → Monitoring Stack)

| Rule # | Source | Destination | Protocol | Port | Direction | Purpose |
|--------|--------|-------------|----------|------|-----------|---------|
| 1 | VLAN B (Alloy) | VLAN A (Prometheus) | TCP | 9090 | One-way → | Alloy remote-writes metrics to Prometheus |
| 2 | VLAN B (Alloy) | VLAN A (Loki) | TCP | 3100 | One-way → | Alloy pushes logs to Loki (HTTP/gRPC) |

---

## Admin Access Rules (Admin IP → Monitoring Stack)

| Rule # | Source | Destination | Protocol | Port | Direction | Purpose |
|--------|--------|-------------|----------|------|-----------|---------|
| 3 | Admin IP | VLAN A (Grafana) | TCP | 3000 | One-way → | Admin browser access to Grafana UI |
| 4 | Admin IP | VLAN A (Prometheus) | TCP | 9090 | One-way → | Admin browser access to Prometheus UI |
| 5 | Admin IP | VLAN A (Loki) | TCP | 3100 | One-way → | Admin API/browser access to Loki (if using directly) |

---

## Admin Access Rules (Admin IP → Alloy)

| Rule # | Source | Destination | Protocol | Port | Direction | Purpose |
|--------|--------|-------------|----------|------|-----------|---------|
| 6 | Admin IP | VLAN B (Alloy) | TCP | 12345 | One-way → | Alloy built-in UI (health, components, config, debug) |

> **Note:** Port 12345 is the default. Check your Alloy config for `--server.http.listen-addr` if customised. Apply per-agent IP or the entire VLAN B subnet depending on your setup.

---

## Intra-VLAN Rules (Monitoring Stack Internal)

> **Note:** Only required if intra-VLAN traffic is restricted by firewall. If components are on the same subnet with no internal filtering, these can be ignored.

| Rule # | Source | Destination | Protocol | Port | Direction | Purpose |
|--------|--------|-------------|----------|------|-----------|---------|
| 7 | Grafana | Prometheus | TCP | 9090 | One-way → | Grafana queries Prometheus datasource |
| 8 | Grafana | Loki | TCP | 3100 | One-way → | Grafana queries Loki datasource |
| 9 | Prometheus | Loki | TCP | 3100 | One-way → | Prometheus scrapes Loki metrics (optional, if Loki metrics endpoint enabled) |

---

## Summary Visual

                    ┌─────────────────────────────────┐
                    │        VLAN A (Monitoring)       │
                    │                                  │
  Admin IP ────────►│  Grafana  :3000                  │
  Admin IP ────────►│  Prometheus :9090                │
  Admin IP ────────►│  Loki      :3100                 │
                    │                                  │
                    │  Grafana ──► Prometheus :9090    │
                    │  Grafana ──► Loki      :3100     │
                    │  Prometheus ──► Loki   :3100     │
                    └────────────▲─────────────────────┘
                                 │
                    ┌────────────┴────────────────────┐
                    │      VLAN B (Workload)          │
                    │                                 │
  Admin IP ────────►│  Alloy  :12345                  │
                    │                                 │
                    │  Alloy ────► Prometheus :9090   │
                    │  Alloy ────► Loki      :3100    │
                    └─────────────────────────────────┘

---

## Port Summary

| Port | Service | Required Rules |
|------|---------|----------------|
| 3000 | Grafana UI | Admin IP → VLAN A (Rule 3) |
| 9090 | Prometheus UI & Remote Write | Admin IP → VLAN A (Rule 4), VLAN B → VLAN A (Rule 1) |
| 3100 | Loki Ingestion & Query | Admin IP → VLAN A (Rule 5), VLAN B → VLAN A (Rule 2) |
| 12345 | Alloy UI & API | Admin IP → VLAN B (Rule 6) |

---

## Implementation Checklist

- [ ] Replace `<YOUR_ADMIN_IP>` with actual IP/CIDR
- [ ] Apply Rules 1-2 for Alloy data flow
- [ ] Apply Rules 3-5 for admin UI access to monitoring stack
- [ ] Apply Rule 6 for admin access to Alloy agents
- [ ] Apply Rules 7-9 only if intra-VLAN filtering is active
- [ ] Test metrics flow: Alloy → Prometheus
- [ ] Test logs flow: Alloy → Loki
- [ ] Test admin access to all UIs (Grafana, Prometheus, Loki, Alloy)
