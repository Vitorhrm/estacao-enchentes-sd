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
from system import NODES

HERE = Path(__file__).resolve().parent

def find_free_port():
    """Encontra uma porta TCP local disponível para o laboratório."""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]

@contextmanager
def station(directory, faults=None, manual=True, open_publisher_terminal=False):
    """Inicia os processos da estação e os encerra ao sair do contexto."""
    mqtt, rpc = find_free_port(), find_free_port()
    processes = {}
    env = dict(os.environ, PYTHONUNBUFFERED="1")
    for name in (*NODES, "sensor", "command", "ack"):
        env["SD_KEY_" + name.upper()] = secrets.token_hex(32)

    def start(name, role, *flags):
        # Uma réplica recebe apenas a sua própria chave.
        """Inicia um componente com acesso apenas às suas credenciais."""
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

    def wait_until(predicate, detail):
        """Espera uma condição observável com prazo limitado."""
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

    def broker_accepts_mqtt():
        """Verifica se o broker aceita uma conexão MQTT local."""
        probe = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION2)
        try:
            probe.connect("127.0.0.1", mqtt, keepalive=5)
            return True
        finally:
            probe.disconnect()

    try:
        start("broker", "broker")
        wait_until(broker_accepts_mqtt, "O broker não iniciou")
        for node in NODES:
            fault = (faults or {}).get(node)
            start(node, "controller", "--node", node, *(["--fault", fault] if fault else []))
        start("actuator", "actuator")
        start("middleware", "middleware", *(["--manual"] if manual else []))
        if open_publisher_terminal:
            publisher_env = dict(os.environ, SD_KEY_SENSOR=env["SD_KEY_SENSOR"])
            processes["publisher"] = subprocess.Popen(
                [sys.executable, str(HERE / "demo.py"), "--publisher", str(mqtt), str(rpc)],
                env=publisher_env, creationflags=subprocess.CREATE_NEW_CONSOLE,
            )
        else:
            start("sensor", "sensor")
        client = FloodStation(rpc)
        wait_until(lambda: len(client.status().get("sample", {})) == 3
                        and client.status()["status"] == "OK", "Telemetria ou atuador indisponível")
        yield client, start, processes, wait_until
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

def count_records(directory, name):
    """Conta os registros persistidos no diário de um componente."""
    with closing(sqlite3.connect(directory / f"{name}.sqlite")) as db:
        return db.execute("SELECT COUNT(*) FROM journal").fetchone()[0]

def run_verification():
    """Executa cenários com processos reais e verifica estado, recibos e votos."""
    scenarios = (
        ("normal", {}, "OK",
         "Os quatro controladores concordaram. O atuador confirmou o comando, devolveu o mesmo recibo na repetição e recuperou o estado após reiniciar."),
        ("queda em plena rodada", {"c1": "crash"}, "OK",
         "C1 gravou o voto e caiu antes de responder. C2, C3 e C4 formaram quórum e o cliente recebeu OK após a confirmação do atuador."),
        ("voto divergente autenticado", {"c1": "wrong"}, "OK",
         "C1 enviou um voto válido, mas contrário. Os três votos iguais dos demais controladores formaram quórum."),
        ("identidade falsificada", {"c1": "forge"}, "OK",
         "C1 tentou se passar por C2. O HMAC rejeitou a mensagem falsificada e os três votos legítimos formaram quórum."),
        ("perda de quorum", {"c1": "crash", "c2": "wrong"}, "NO_QUORUM",
         "C1 caiu e C2 votou contra. Com apenas dois votos a favor, o middleware retornou NO_QUORUM e o atuador registrou zero aplicações. Resultado esperado."),
    )
    for name, faults, expected, explanation in scenarios:
        with tempfile.TemporaryDirectory() as folder:
            directory = Path(folder)
            with station(directory, faults) as (client, start, procs, wait):
                request_id = uuid4().hex
                result = client.set_mode("MANUAL_ON", request_id)
                assert result["status"] == expected, result
                assert client.status()["pump"] is (expected == "OK")
                assert count_records(directory, "actuator") == (1 if expected == "OK" else 0)
                if faults.get("c1") == "crash":
                    assert procs["c1"].wait(2) == 17
                if faults.get("c1") == "wrong":
                    assert result["votes"]["c1"] is False
                if faults.get("c1") == "forge":
                    assert set(result["votes"]) == {"c2", "c3", "c4"}
                assert all(count_records(directory, node) == 1 for node in NODES)
                if not faults:
                    assert client.set_mode("MANUAL_ON", request_id)["replayed"] is True
                    assert client.set_mode("MANUAL_OFF", request_id)["status"] == "ID_CONFLICT"
                    assert count_records(directory, "actuator") == 1
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
                print(f"  {explanation}\n", flush=True)

    with tempfile.TemporaryDirectory() as folder:
        with station(Path(folder), manual=False) as (client, start, procs, wait):
            assert wait(lambda: client.status().get("pump"), "AUTO não ligou a bomba")
            procs["sensor"].terminate()
            procs["sensor"].wait(3)
            start("sensor", "sensor", "--level", "55", "--rain", "0")
            assert wait(lambda: client.status().get("pump") is False, "AUTO não desligou a bomba")
            print("AUTO: bomba ligou no nível alto e desligou no nível baixo.", flush=True)
            print("  O teste mudou os sensores e confirmou as duas transições no estado da bomba.", flush=True)

def run_publisher_terminal(port, rpc_port):
    """Publica cenários MQTT e envia comandos do operador por XML-RPC."""
    os.system("title Publisher - Estacao de Enchentes")
    scenarios = {
        "chuva": (),
        "seco": ("--level", "55", "--rain", "0"),
        "fechar": ("--closed",),
    }
    modes = {"auto": "AUTO", "ligar": "MANUAL_ON", "desligar": "MANUAL_OFF"}
    client = FloodStation(rpc_port)
    print("Sensores MQTT: chuva, seco, fechar")
    print("Controle XML-RPC: auto, ligar, desligar")
    print("Encerrar: sair")
    sensor = None
    try:
        action = "chuva"
        while action != "sair":
            if action in scenarios:
                if sensor is not None:
                    sensor.terminate()
                    sensor.wait(3)
                sensor = subprocess.Popen(
                    [sys.executable, str(HERE / "runtime.py"), "sensor",
                     "--mqtt", str(port), *scenarios[action]],
                    cwd=tempfile.gettempdir(), stdout=subprocess.DEVNULL,
                )
                print(f"Cenário MQTT: {action}")
            elif action in modes:
                print(client.set_mode(modes[action]))
            else:
                print("Use chuva, seco, fechar, auto, ligar, desligar ou sair.")
            action = input("publisher> ").strip().lower()
    except (KeyboardInterrupt, EOFError):
        pass
    finally:
        if sensor is not None and sensor.poll() is None:
            sensor.terminate()
            sensor.wait(3)

def run_split_demo():
    """Abre um terminal de publicação e exibe a telemetria neste terminal."""
    if os.name != "nt":
        raise SystemExit("A demonstração em dois terminais requer Windows. Use --verify para rodar os testes neste sistema.")
    os.system("title Monitor - Estacao de Enchentes")
    with tempfile.TemporaryDirectory() as folder:
        with station(Path(folder), manual=False, open_publisher_terminal=True) as (client, _, processes, _):
            print("Monitor XML-RPC | altere o cenário no Publisher. Ctrl+C encerra.")
            while processes["publisher"].poll() is None:
                state = client.status()
                sample = state["sample"]
                pump = "ligada" if state.get("pump") else "desligada"
                gate = {True: "aberta", False: "fechada"}.get(sample.get("gate_open"), "?")
                print(f"{time.strftime('%H:%M:%S')} | nível {sample.get('level_cm', '?')} cm | "
                      f"chuva {sample.get('rain_mm_h', '?')} mm/h | comporta {gate} | "
                      f"bomba {pump} | {state.get('mode', state['status'])}", flush=True)
                time.sleep(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--verify", action="store_true")
    parser.add_argument("--split", action="store_true", help="Abre Publisher e Monitor (modo padrão no Windows)")
    parser.add_argument("--publisher", type=int, nargs=2, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.verify:
        run_verification()
    else:
        try:
            if args.publisher is not None:
                run_publisher_terminal(*args.publisher)
            else:
                run_split_demo()
        except (KeyboardInterrupt, EOFError):
            pass
