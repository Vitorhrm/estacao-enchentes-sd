"""Processos MQTT, persistência local e middleware de objetos remotos."""
import argparse
import asyncio
import json
import logging
import os
from pathlib import Path
import queue
import sqlite3
import threading
import time
import uuid
from xmlrpc.server import SimpleXMLRPCServer

from amqtt.broker import Broker
import paho.mqtt.client as mqtt
from system import MODES, NODES, ROOT, canonical_json, decide_pump_state, find_quorum, sign_message, verify_message

LOG = logging.getLogger("enchentes")

def key(name):
    """Lê a chave de autenticação provisionada para o componente."""
    return os.environ["SD_KEY_" + name.upper()]

class MqttBus:
    def __init__(self, port, topics, handler):
        """Conecta ao broker e aguarda a confirmação das assinaturas."""
        self.ready = threading.Event()
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.client.on_connect = lambda client, user, flags, rc, props: (
            client.subscribe([(ROOT + topic, 1) for topic in topics])
            if topics else self.ready.set()
        )
        self.client.on_subscribe = lambda client, user, mid, codes, props: self.ready.set()

        def receive(client, user, packet):
            """Decodifica mensagens MQTT e encaminha objetos válidos ao componente."""
            try:
                data = json.loads(packet.payload)
                if isinstance(data, dict):
                    handler(packet.topic[len(ROOT):], data)
            except (ValueError, KeyError, TypeError, OverflowError):
                LOG.warning("Mensagem inválida em %s", packet.topic)

        self.client.on_message = receive
        self.client.connect("127.0.0.1", port)
        self.client.loop_start()
        if not self.ready.wait(5):
            self.close()
            raise TimeoutError("Broker MQTT indisponível")

    def send(self, topic, data):
        """Publica uma mensagem JSON no prefixo da estação com QoS 1."""
        return self.client.publish(ROOT + topic, canonical_json(data), qos=1)

    def close(self):
        """Encerra a conexão MQTT e aguarda a thread de rede."""
        self.client.disconnect()
        self.client.loop_stop()

def open_journal(path):
    """Abre um diário SQLite local com gravação durável."""
    db = sqlite3.connect(path, check_same_thread=False)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=FULL")
    db.execute("CREATE TABLE IF NOT EXISTS journal (id TEXT PRIMARY KEY, record TEXT NOT NULL)")
    return db

def read_record(db, identifier):
    """Busca o registro de uma rodada ou de um comando pelo identificador."""
    row = db.execute("SELECT record FROM journal WHERE id=?", (identifier,)).fetchone()
    return json.loads(row[0]) if row else None

def append_record(db, identifier, record):
    """Persiste o registro antes de devolver o controle ao chamador."""
    with db:
        db.execute("INSERT INTO journal VALUES (?, ?)", (identifier, canonical_json(record)))

async def broker(port):
    """Executa o broker MQTT local até o encerramento do processo."""
    server = Broker({
        "listeners": {"default": {"type": "tcp", "bind": f"127.0.0.1:{port}"}},
        "plugins": {"amqtt.plugins.authentication.AnonymousAuthPlugin": {"allow_anonymous": True}},
    })
    await server.start()
    try:
        await asyncio.Future()
    finally:
        await server.shutdown()

def controller(args):
    """Inicializa uma réplica com diário próprio e chave individual."""
    db = open_journal(args.data / f"{args.node}.sqlite")
    bus = None

    def handle(topic, request):
        """Autentica a rodada, persiste o voto e aplica a falha de teste configurada."""
        if not verify_message(key(args.node), request):
            return
        round_id = request["round"]
        record = read_record(db, round_id)
        if record is None:
            pump = decide_pump_state(request["mode"], request["sample"], request["previous"])
            if args.fault == "wrong":
                pump = not pump
            vote = sign_message(key(args.node), {"round": round_id, "node": args.node, "pump": pump})
            record = {"request": request, "vote": vote}
            append_record(db, round_id, record)
        if record["request"] != request:
            return
        if args.fault == "crash":
            os._exit(17)
        if args.fault == "forge":
            bus.send("vote", {**record["vote"], "node": "c2"})
            return
        bus.send("vote", record["vote"])

    bus = MqttBus(args.mqtt, [f"round/{args.node}"], handle)
    wait_for_shutdown(bus)

def actuator(args):
    """Inicializa o atuador simulado com persistência entre reinicializações."""
    db = open_journal(args.data / "actuator.sqlite")
    bus = None

    def handle(topic, command):
        """Responde a consultas e confirma comandos depois de registrar sua aplicação."""
        if not verify_message(key("command"), command):
            return
        round_id = command["round"]
        if topic == "probe":
            row = db.execute("SELECT record FROM journal ORDER BY rowid DESC LIMIT 1").fetchone()
            current = json.loads(row[0]) if row else {"pump": False, "mode": "AUTO"}
            lookup_id = command.get("lookup")
            receipt = read_record(db, lookup_id) if lookup_id else None
            state = {
                "round": round_id,
                "pump": current["pump"],
                "mode": current["mode"],
                "receipt": receipt,
            }
            bus.send("state", sign_message(key("ack"), state))
            return
        identifier = command["id"]
        previous = read_record(db, identifier)
        if previous:
            status = "applied" if (previous["mode"], previous["pump"]) == (
                command["mode"], command["pump"]) else "conflict"
        else:
            previous = {"pump": command["pump"], "mode": command["mode"]}
            append_record(db, identifier, previous)
            status = "applied"
        bus.send("ack", sign_message(key("ack"), {"round": round_id, "id": identifier,
                                           "status": status, **previous}))

    bus = MqttBus(args.mqtt, ["command", "probe"], handle)
    wait_for_shutdown(bus)

def wait_for_shutdown(bus):
    """Mantém o componente disponível para receber mensagens MQTT."""
    try:
        print("READY", flush=True)
        threading.Event().wait()
    finally:
        bus.close()

class Middleware:
    def __init__(self, port, vote_timeout):
        """Inicializa a caixa de entrada do RPC e as assinaturas MQTT."""
        self.samples = {}
        self.inbox = queue.Queue()
        self.timeout = vote_timeout
        self.bus = MqttBus(port, ["sensor/+", "vote", "ack", "state"], self.receive)

    def receive(self, topic, message):
        """Atualiza sensores autenticados ou enfileira respostas de uma rodada."""
        if topic.startswith("sensor/"):
            field = topic.split("/")[1]
            value = message.get("value")
            limits = {"level_cm": 500, "rain_mm_h": 400}
            is_level = field in limits and type(value) is int and 0 <= value <= limits[field]
            if (is_level or (field == "gate_open" and type(value) is bool)) and verify_message(key("sensor"), message):
                if message.get("field") == field and abs(time.time() - message["time"]) < 5:
                    self.samples[field] = (time.monotonic(), value)
            return
        self.inbox.put((topic, message))

    def receive_round_messages(self, round_id, topic, timeout):
        """Consome apenas respostas da rodada e do tópico esperados até o prazo."""
        deadline = time.monotonic() + timeout
        while True:
            try:
                channel, data = self.inbox.get(timeout=max(0, deadline - time.monotonic()))
            except queue.Empty:
                return
            if channel == topic and data.get("round") == round_id:
                yield data
            if time.monotonic() >= deadline:
                return

    def state(self, identifier=None):
        """Consulta o estado do atuador e, opcionalmente, o recibo de um comando."""
        round_id = uuid.uuid4().hex
        self.bus.send("probe", sign_message(key("command"), {"round": round_id, "lookup": identifier}))
        return next((data for data in self.receive_round_messages(round_id, "state", 1)
                     if verify_message(key("ack"), data)), None)

    def sample(self):
        """Retorna apenas os valores de sensores recebidos nos últimos cinco segundos."""
        return {field: value for field, (received, value) in self.samples.copy().items()
                if time.monotonic() - received <= 5}

    def status(self):
        """Combina a telemetria recente com o estado confirmado da bomba."""
        state = self.state()
        if state is None:
            return {"status": "ACTUATOR_OFFLINE", "sample": self.sample()}
        return {"status": "OK", "sample": self.sample(), "pump": state["pump"], "mode": state["mode"]}

    def set_mode(self, mode, identifier):
        """Valida o pedido, coleta quórum e aguarda a confirmação do atuador."""
        if mode not in MODES or not isinstance(identifier, str) or not 1 <= len(identifier) <= 80:
            return {"status": "INVALID_REQUEST"}
        sample = self.sample()
        # Telemetria ausente ou vencida força o intertravamento para desligado.
        if len(sample) != 3:
            sample = {"level_cm": 0, "rain_mm_h": 0, "gate_open": False}
        previous = self.state(identifier)
        if previous is None:
            return {"status": "ACTUATOR_OFFLINE"}
        receipt = previous.get("receipt")
        if receipt is not None:
            status = "OK" if receipt["mode"] == mode else "ID_CONFLICT"
            return {"status": status, "id": identifier, "pump": receipt["pump"], "replayed": True}
        round_id = uuid.uuid4().hex
        request = {"round": round_id, "mode": mode, "sample": sample, "previous": previous["pump"]}
        for node in NODES:
            self.bus.send(f"round/{node}", sign_message(key(node), request))
        votes = {}
        # A janela limitada também coleta votos tardios para mostrar divergências.
        for vote in self.receive_round_messages(round_id, "vote", self.timeout):
            node = vote.get("node")
            if node in NODES and type(vote.get("pump")) is bool and verify_message(key(node), vote):
                votes.setdefault(node, vote["pump"])
        pump = find_quorum(votes)
        if pump is None:
            return {"status": "NO_QUORUM", "votes": votes}
        command = {"round": round_id, "id": identifier, "mode": mode, "pump": pump}
        self.bus.send("command", sign_message(key("command"), command))
        for ack in self.receive_round_messages(round_id, "ack", 1.5):
            if ack.get("id") == identifier and verify_message(key("ack"), ack):
                status = "OK" if ack["status"] == "applied" else "ID_CONFLICT"
                return {"status": status, "id": identifier, "pump": ack["pump"], "votes": votes}
        return {"status": "UNKNOWN_OUTCOME", "id": identifier, "votes": votes}

def serve(args):
    """Atende as chamadas XML-RPC e verifica periodicamente a política automática."""
    middleware = Middleware(args.mqtt, args.vote_timeout)
    with SimpleXMLRPCServer(("127.0.0.1", args.rpc), logRequests=False) as server:
        server.register_function(middleware.status)
        server.register_function(middleware.set_mode)
        server.timeout = 0.25
        next_tick = time.monotonic() + 1
        print("READY", flush=True)
        try:
            while True:
                server.handle_request()
                if not args.manual and time.monotonic() >= next_tick:
                    status = middleware.status()
                    if status.get("mode") in MODES:
                        sample = status["sample"]
                        safe = sample if len(sample) == 3 else {"gate_open": False}
                        mode = status["mode"]
                        if decide_pump_state(mode, safe, status["pump"]) != status["pump"]:
                            middleware.set_mode(mode, uuid.uuid4().hex)
                    next_tick = time.monotonic() + 1
        finally:
            middleware.bus.close()

def sensor(args):
    """Publica as três medições simuladas a cada meio segundo."""
    bus = MqttBus(args.mqtt, [], lambda *ignored: None)
    try:
        print("READY", flush=True)
        while True:
            for field, value in (("level_cm", args.level), ("rain_mm_h", args.rain), ("gate_open", not args.closed)):
                sample = sign_message(key("sensor"), {"field": field, "value": value, "time": time.time()})
                bus.send(f"sensor/{field}", sample).wait_for_publish(2)
            time.sleep(0.5)
    finally:
        bus.close()

def main():
    """Interpreta os argumentos e inicia o componente solicitado."""
    parser = argparse.ArgumentParser()
    parser.add_argument("role", choices=["broker", "controller", "actuator", "middleware", "sensor"])
    parser.add_argument("--mqtt", type=int, default=18883)
    parser.add_argument("--rpc", type=int, default=18080)
    parser.add_argument("--data", type=Path, default=Path("data"))
    parser.add_argument("--node", choices=NODES)
    parser.add_argument("--fault", choices=["crash", "wrong", "forge"])
    parser.add_argument("--manual", action="store_true", help="Executa decisões apenas sob demanda")
    parser.add_argument("--vote-timeout", type=float, default=0.8)
    parser.add_argument("--level", type=int, default=125)
    parser.add_argument("--rain", type=int, default=42)
    parser.add_argument("--closed", action="store_true")
    args = parser.parse_args()
    if args.role == "broker":
        asyncio.run(broker(args.mqtt))
        return
    args.data.mkdir(parents=True, exist_ok=True)
    {"controller": controller, "actuator": actuator, "middleware": serve, "sensor": sensor}[args.role](args)

if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    main()
