# Estação de contenção de enchentes urbanas

Trabalho prático de Sistemas Distribuídos. Protótipo local com sensores e bomba simulados, broker MQTT real, quatro controladores em processos independentes e um objeto remoto XML-RPC.

## Executar

Requer Python 3.12. Na pasta do projeto:

```sh
python -m pip install -r requirements.txt
python demo.py
python -m unittest -v
python demo.py --verify
```

Use um ambiente virtual para instalar as dependências. O demonstrador abre portas livres em 127.0.0.1, cria um banco SQLite por controlador, gera as chaves em memória e encerra os processos ao sair. Para execução direta, `python runtime.py --help` descreve as opções por processo. O código do cliente está em `client.py`.

## Demonstração em dois terminais no Windows

Execute `python demo.py --split`. A janela Monitor atualiza nível, chuva, comporta e bomba a cada segundo. Na janela Publisher, use `seco`, `chuva` ou `fechar` para trocar o cenário MQTT. Para encerrar, digite `sair` no Publisher.

## Demonstrar em aula

1. Digite `acompanhar` para visualizar chuva, nível, comporta, modo e bomba. Use Ctrl+C para sair do acompanhamento.
2. Em `AUTO`, use `seco` e aguarde a bomba desligar. Use `chuva` para reativá-la.
3. Digite `fechar` e depois `ligar`. O controlador mantém a bomba desligada pelo intertravamento.
4. Execute `python demo.py --verify` para mostrar a queda em plena chamada, o voto divergente, a falsificação de identidade e a recusa por falta de quórum.

## Arquitetura

Os sensores publicam em `enchentes/v1/sensor/{field}`. O cliente chama `status()` e `set_mode(modo, id)` via XML-RPC. O middleware captura a telemetria, consulta a bomba e publica a rodada em `round/{node}`. Cada réplica grava a rodada e seu voto antes de publicar em `vote`. Com três votos válidos iguais, o middleware envia `command`; o atuador registra a mudança e publica `ack`. Os tópicos `probe` e `state` consultam a posição atual e recibos anteriores. Todos os tópicos usam o mesmo prefixo e QoS 1.

`OK` em `set_mode` significa que a operação foi registrada pelo atuador, incluindo decisões válidas de manter a bomba desligada. Um ID repetido retorna o recibo original. Reusar esse ID com outro modo retorna `ID_CONFLICT`. `NO_QUORUM` significa que o middleware não enviou comando. `UNKNOWN_OUTCOME` significa que houve envio, mas o ACK não foi recebido no prazo. Nesse caso, repita com o mesmo ID.

O modo AUTO liga com comporta aberta e nível a partir de 100 cm, ou a partir de 80 cm com chuva de pelo menos 35 mm/h. Se a bomba já estiver ligada, mantém acima de 60 cm. Comporta fechada ou amostra incompleta força a decisão para desligado. A decisão só chega ao atuador se houver quórum.

## Plano executado

1. **Modelo do processo.** Definir telemetria, intertravamento, histerese e três modos. Verificar os limites por teste unitário.
2. **Integração.** Subir broker e processos locais, implementar XML-RPC e a confirmação do atuador. Verificar a chamada ponta a ponta.
3. **Replicação e segurança.** Fixar o estado de entrada por rodada, persistir nos controladores, autenticar cada voto e agrupar três respostas iguais. Verificar perda, divergência e falsificação.
4. **Demonstração e entrega.** Executar os testes, documentar as garantias e publicar o código-fonte.

## Limites

O modelo cobre uma réplica silenciosa ou uma réplica divergente entre quatro. O middleware é o sequenciador confiável e processa uma operação por vez. O broker, o middleware e o atuador são únicos. A replicação cobre os registros das rodadas recebidas, sem sincronização histórica automática para réplicas que ficaram offline. HMAC autentica mensagens no laboratório, sem confidencialidade ou controle de acesso ao broker. Processos no mesmo host compartilham o domínio de falha e pressupõem que o sistema operacional protege suas chaves. Este protótipo simula o acionamento em SQLite. A disponibilidade para atuação física exigiria proteção local independente, inclusive quando o quórum estiver indisponível.

## Referências

- [OASIS MQTT 3.1.1, seções 3.3 e 4.3](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html)
- [Eclipse Paho MQTT Python Client](https://eclipse.dev/paho/clients/python/docs/)

[Relatório e roteiro de apresentação](Relatorio-Estacao-de-Enchentes.pdf)
