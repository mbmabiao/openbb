# Financial System AI Instructions (Strict Mode)

## Core Principle

This is a financial system. Correctness, traceability, and explicit failure are mandatory.
Silent fallbacks, assumptions, or hidden corrections are strictly forbidden.

---

## 1. No Silent Fallbacks (Critical)

* NEVER introduce fallback logic unless explicitly requested.
* NEVER auto-retry with modified inputs.
* NEVER substitute missing or invalid data with defaults.
* NEVER "best guess" values.

If data is missing, invalid, or inconsistent:
→ FAIL explicitly and surface the issue.

---

## Summary

In this project:

* No silent fallback
* No guessing
* No hidden fixes

Only explicit, traceable, deterministic behaviour is allowed.
