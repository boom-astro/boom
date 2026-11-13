"""A Babamul account for the benchmark's API clients.

Babamul routes need a signed-in user. Signing up goes through email
activation, so the harness writes an activated user straight into the run
database and signs a JWT for it with the stack's secret (src/api/auth.rs).
"""

from __future__ import annotations

import time

import jwt

BENCH_USER_ID = "benchmark-user"


def ensure_bench_user(db) -> str:
    now = int(time.time())
    db["babamul_users"].update_one(
        {"_id": BENCH_USER_ID},
        {"$setOnInsert": {
            "username": "benchmark",
            "email": "benchmark@example.org",
            "password_hash": "!",  # matches no password: the account signs in by JWT only
            "activation_code": None,
            "is_activated": True,
            "created_at": now,
            "kafka_credentials": [],
            "tokens": [],
            "password_reset_token_hash": None,
            "password_reset_token_expires_at": None,
            "password_last_changed_at": None,
            "identities": [],
            "orcid_id": None,
            "name": "Benchmark",
            # Admin sees every program, so query results do not depend on ACLs.
            "is_admin": True,
            "acls": [],
        }},
        upsert=True,
    )
    return BENCH_USER_ID


def bench_token(secret: str, user_id: str = BENCH_USER_ID, ttl_s: int = 7 * 86400) -> str:
    now = int(time.time())
    return jwt.encode({"sub": f"babamul:{user_id}", "iat": now, "exp": now + ttl_s}, secret, algorithm="HS256")
