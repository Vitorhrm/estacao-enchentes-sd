"""Regras e autenticação das mensagens do protótipo."""
import hashlib
import hmac
import json

ROOT = "enchentes/v1/"
NODES = ("c1", "c2", "c3", "c4")
MODES = ("AUTO", "MANUAL_ON", "MANUAL_OFF")

def canonical_json(data):
    """Serializa os dados de forma estável para persistência e cálculo do HMAC."""
    return json.dumps(data, sort_keys=True, separators=(",", ":"), allow_nan=False)

def sign_message(key, data):
    """Acrescenta um HMAC-SHA256 que autentica o conteúdo da mensagem."""
    tag = hmac.new(key.encode(), canonical_json(data).encode(), hashlib.sha256).hexdigest()
    return {**data, "mac": tag}

def verify_message(key, message):
    """Verifica a autenticação da mensagem sem comparar o HMAC byte a byte com igualdade comum."""
    data = {field: value for field, value in message.items() if field != "mac"}
    tag = message.get("mac")
    return isinstance(tag, str) and hmac.compare_digest(tag, sign_message(key, data)["mac"])

def decide_pump_state(mode, sample, previous):
    """Calcula o estado da bomba com intertravamento, modo de operação e histerese."""
    if not sample["gate_open"] or mode == "MANUAL_OFF":
        return False
    if mode == "MANUAL_ON":
        return True
    level = sample["level_cm"]
    return level >= 100 or (level >= 80 and sample["rain_mm_h"] >= 35) or (previous and level > 60)

def find_quorum(votes):
    """Retorna a decisão apoiada por três nós distintos ou None se faltar quórum."""
    for value in (True, False):
        if sum(vote is value for vote in votes.values()) >= 3:
            return value
    return None
