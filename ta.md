# Tungsten TotalAgility – mTLS Support Summary
**Date:** 2026-09-09  
**Context:** Need to call an external Apigee gateway (SaaS) that requires mutual TLS (mTLS) + JWKS validation of the client certificate. Also evaluating the same requirement for on-prem endpoints from the Integration Server.

---

## 1. Original Question
Does Tungsten TotalAgility support mTLS:
- On the Integration Server, **or**
- As part of the OAuth server setup?

---

## 2. Findings

### A. Inbound mTLS (callers → TotalAgility REST APIs)
**Supported**

- When configuring **TotalAgility OAuth clients** (Integration → TotalAgility OAuth clients), you can optionally supply up to **three client certificate thumbprints**.
- TotalAgility validates the presented client certificate against these thumbprints during token generation and subsequent REST API calls.
- Documented as “extra authentication using client certificates (mTLS)”.

This is the only clearly documented mTLS capability.

### B. Outbound mTLS (TotalAgility / Integration Server → external systems)
**Not natively supported**

| Scenario                                      | Native Support? | Notes |
|-----------------------------------------------|-----------------|-------|
| Outbound mTLS to Apigee (or any external system) | No              | No configuration available in Designer or Integration Server to attach a client certificate + private key to web service / REST calls. |
| Outbound mTLS from Integration Server (on-prem) | No              | Same limitation applies. |
| mTLS between TotalAgility instances (Linked Servers) | Limited Yes     | Certificate-based two-way TLS is possible, but only for TotalAgility-to-TotalAgility communication. |
| One-way TLS / HTTPS                           | Yes             | Fully supported. |
| Private Key JWT (outbound OAuth)              | Yes             | Certificate can be used for JWT client assertion – **not** the same as a full mTLS handshake. |

### C. Apigee-Specific Requirements (from partner documentation)
Apigee requires **two layers**:

1. **Network-level mTLS** – Client must present a valid leaf certificate during the TLS handshake.  
   - Apigee rejects self-signed certificates or certificates signed by self-signed CAs.
2. **Application-level validation** – After the handshake, Apigee’s custom SSA module validates the leaf certificate against a **JWKS endpoint that the partner (us) must publish and maintain**.  
   - The JWKS must contain the client’s public certificate with `"use": "tls"` or `"use": "enc"`.

TotalAgility has **no built-in capability** to:
- Present a client certificate on outbound calls, **or**
- Host / publish a JWKS endpoint containing its client certificates.

---

## 3. Conclusion

| Requirement                                      | Supported in TotalAgility? |
|--------------------------------------------------|----------------------------|
| Inbound mTLS (protect TotalAgility APIs)         | Yes                        |
| Outbound mTLS to Apigee (SaaS)                   | No                         |
| Outbound mTLS from Integration Server (on-prem)  | No                         |
| Ability to publish a JWKS for client cert validation | No                      |

The mTLS support that exists is primarily designed to **protect TotalAgility itself**, not to enable TotalAgility to act as an mTLS client toward external systems that enforce mutual authentication + JWKS validation.

---

## 4. Recommended Next Steps / Workarounds

1. **Custom .NET assembly / activity**  
   Implement an `HttpClient` call that loads a client certificate and performs the mTLS request. Expose it to TotalAgility processes (works on both main servers and Integration Server).

2. **Intermediate proxy / sidecar**  
   Place a lightweight proxy (NGINX, Envoy, Azure API Management, custom service, etc.) that handles the mTLS + JWKS requirements. TotalAgility calls the proxy with simpler authentication.

3. **Formal request to Tungsten Automation**  
   - Confirm whether outbound client-certificate / mTLS support exists in any unpublished configuration or is on the roadmap for future releases (especially for Integration Server and web service references).  
   - Request native support for outbound mTLS (client certificate selection) and optionally a simple JWKS publishing mechanism.

---

**Request to Tungsten Support:**  
Please confirm the current and planned support for **outbound mutual TLS (client certificate presentation)** from TotalAgility and the Integration Server when calling external REST endpoints (both SaaS and on-prem). Additionally, is there any guidance for scenarios that also require the calling party to publish a JWKS containing the client certificate?

Thank you.
