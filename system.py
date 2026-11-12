"""Regras e autenticação das mensagens do protótipo."""
import hashlib
import hmac
import json

ROOT = "enchentes/v1/"
NODES = ("c1", "c2", "c3", "c4")
MODES = ("AUTO", "MANUAL_ON", "MANUAL_OFF")

def canonical(data):
    return json.dumps(data, sort_keys=True, separators=(",", ":"), allow_nan=False)

def signed(key, data):
    tag = hmac.new(key.encode(), canonical(data).encode(), hashlib.sha256).hexdigest()
    return {**data, "mac": tag}

def valid(key, message):
    data = {field: value for field, value in message.items() if field != "mac"}
    tag = message.get("mac")
    return isinstance(tag, str) and hmac.compare_digest(tag, signed(key, data)["mac"])

def decision(mode, sample, previous):
    if not sample["gate_open"] or mode == "MANUAL_OFF":
        return False
    if mode == "MANUAL_ON":
        return True
    level = sample["level_cm"]
    return level >= 100 or (level >= 80 and sample["rain_mm_h"] >= 35) or (previous and level > 60)

def quorum(votes):
    for value in (True, False):
        if sum(vote is value for vote in votes.values()) >= 3:
            return value
    return None
