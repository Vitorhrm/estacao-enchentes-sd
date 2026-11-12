"""Inicia processos separados e executa cenários verificáveis."""
import argparse
import json
import os
from pathlib import Path
import secrets
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
from contextlib import closing, contextmanager
from uuid import uuid4

import paho.mqtt.client as mqtt_client

from client import FloodStation
from system import MODES, NODES

HERE = Path(__file__).resolve().parent

def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]

@contextmanager
def station(directory, faults=None, manual=True):
    mqtt, rpc = free_port(), free_port()
    processes = {}
    env = dict(os.environ, PYTHONUNBUFFERED="1")
    for name in (*NODES, "sensor", "command", "ack"):
        env["SD_KEY_" + name.upper()] = secrets.token_hex(32)

    def start(name, role, *flags):
        # Uma réplica recebe apenas a sua própria chave.
        credentials = {
            "controller": (name,), "sensor": ("sensor",),
            "actuator": ("command", "ack"), "broker": (),
            "middleware": (*NODES, "sensor", "command", "ack"),
        }[role]
        child_env = {k: v for k, v in env.items() if not k.startswith("SD_KEY_")}
        child_env.update({f"SD_KEY_{k.upper()}": env[f"SD_KEY_{k.upper()}"] for k in credentials})
        proc = subprocess.Popen(
            [sys.executable, str(HERE / "runtime.py"), role, "--mqtt", str(mqtt),
             "--rpc", str(rpc), "--data", str(directory), *flags],
            env=child_env, stdout=subprocess.DEVNULL,
        )
        processes[name] = proc
        return proc

    def await_condition(predicate, detail):
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            try:
                result = predicate()
                if result:
                    return result
            except OSError:
                pass
            time.sleep(0.08)
        raise TimeoutError(detail)

    def broker_ready():
        probe = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION2)
        try:
            probe.connect("127.0.0.1", mqtt, keepalive=5)
            return True
        finally:
            probe.disconnect()

    try:
        start("broker", "broker")
        await_condition(broker_ready, "O broker não iniciou")
        for node in NODES:
            fault = (faults or {}).get(node)
            start(node, "controller", "--node", node, *(["--fault", fault] if fault else []))
        start("actuator", "actuator")
        start("middleware", "middleware", *(["--manual"] if manual else []))
        start("sensor", "sensor")
        client = FloodStation(rpc)
        await_condition(lambda: len(client.status().get("sample", {})) == 3
                        and client.status()["status"] == "OK", "Telemetria ou atuador indisponível")
        yield client, start, processes, await_condition
    finally:
        for proc in processes.values():
            if proc.poll() is None:
                proc.terminate()
        for proc in processes.values():
            try:
                proc.wait(3)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()

def rows(directory, name):
    with closing(sqlite3.connect(directory / f"{name}.sqlite")) as db:
        return db.execute("SELECT COUNT(*) FROM journal").fetchone()[0]

def verify():
    scenarios = (
        ("normal", {}, "OK"),
        ("queda em plena rodada", {"c1": "crash"}, "OK"),
        ("voto divergente autenticado", {"c1": "wrong"}, "OK"),
        ("identidade falsificada", {"c1": "forge"}, "OK"),
        ("perda de quorum", {"c1": "crash", "c2": "wrong"}, "NO_QUORUM"),
    )
    for name, faults, expected in scenarios:
        with tempfile.TemporaryDirectory() as folder:
            directory = Path(folder)
            with station(directory, faults) as (client, start, procs, wait):
                request_id = uuid4().hex
                result = client.set_mode("MANUAL_ON", request_id)
                assert result["status"] == expected, result
                assert client.status()["pump"] is (expected == "OK")
                assert rows(directory, "actuator") == (1 if expected == "OK" else 0)
                if faults.get("c1") == "crash":
                    assert procs["c1"].wait(2) == 17
                if faults.get("c1") == "wrong":
                    assert result["votes"]["c1"] is False
                if faults.get("c1") == "forge":
                    assert set(result["votes"]) == {"c2", "c3", "c4"}
                assert all(rows(directory, node) == 1 for node in NODES)
                if not faults:
                    assert client.set_mode("MANUAL_ON", request_id)["replayed"] is True
                    assert client.set_mode("MANUAL_OFF", request_id)["status"] == "ID_CONFLICT"
                    assert rows(directory, "actuator") == 1
                    procs["actuator"].terminate()
                    procs["actuator"].wait(3)
                    start("actuator", "actuator")
                    assert wait(lambda: client.status().get("pump"), "Estado persistido não recuperado")
                    procs["sensor"].terminate()
                    procs["sensor"].wait(3)
                    start("sensor", "sensor", "--closed")
                    wait(lambda: client.status()["sample"].get("gate_open") is False, "Comporta")
                    assert client.set_mode("MANUAL_ON")["pump"] is False
                print(json.dumps({"scenario": name, **result}, ensure_ascii=False), flush=True)

    with tempfile.TemporaryDirectory() as folder:
        with station(Path(folder), manual=False) as (client, start, procs, wait):
            assert wait(lambda: client.status().get("pump"), "AUTO não ligou a bomba")
            procs["sensor"].terminate()
            procs["sensor"].wait(3)
            start("sensor", "sensor", "--level", "55", "--rain", "0")
            assert wait(lambda: client.status().get("pump") is False, "AUTO não desligou a bomba")
            print("AUTO: bomba ligou no nível alto e desligou no nível baixo.", flush=True)

def interactive():
    with tempfile.TemporaryDirectory() as folder:
        with station(Path(folder), manual=False) as (client, start, procs, wait):
            print("Comandos: status, acompanhar, auto, ligar, desligar, chuva, seco, fechar, sair")
            modes = dict(zip(("auto", "ligar", "desligar"), MODES))
            scenarios = {"chuva": (), "seco": ("--level", "55", "--rain", "0"), "fechar": ("--closed",)}
            while True:
                action = input("> ").strip().lower()
                if action == "sair":
                    return
                if action == "status":
                    print(client.status())
                elif action == "acompanhar":
                    try:
                        while True:
                            print(client.status())
                            time.sleep(1)
                    except KeyboardInterrupt:
                        print()
                elif action in modes:
                    print(client.set_mode(modes[action]))
                elif action in scenarios:
                    procs["sensor"].terminate()
                    procs["sensor"].wait(3)
                    start("sensor", "sensor", *scenarios[action])
                else:
                    print("Comando desconhecido.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--verify", action="store_true")
    args = parser.parse_args()
    if args.verify:
        verify()
    else:
        try:
            interactive()
        except (KeyboardInterrupt, EOFError):
            pass
