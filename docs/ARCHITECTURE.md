# Architettura

## Vista logica

```text
                         +-------------------------+
                         | migration / demo control|
                         +------------+------------+
                                      |
             +------------------------+------------------------+
             |                         |                        |
      order-simulator           payment-simulator       fulfillment-simulator
        kafka|nats|dual           kafka|nats|dual          kafka|nats|dual
             |                         |                        |
      +------+-------------------------+------------------------+------+
      |                                                                 |
+-----v---------------- Kafka plane -------------+   +------------------v--- NATS plane --------+
| Kafka KRaft -> Schema Registry                 |   | NATS JetStream -> Apicurio -> PostgreSQL  |
|      |        Kafka Streams        ksqlDB       |   | durable consumers / replay / DLQ / HA    |
|      +--------------+---------------------------+   +------------------+-----------------------+
|                     |                                              |
|             flink-kafka cluster                           flink-nats cluster
+---------------------+----------------------------------------------+---------------------------+
                      |                                              |
                      +---------------- parity ----------------------+
                                             |
                        Prometheus / Grafana / Loki / Alloy / OTel
```

I due cluster Flink condividono soltanto contratti e funzione logica; runtime, checkpoint, dipendenze e reti sono distinti.

## Reti Compose

| Rete | Membri principali | Scopo |
|---|---|---|
| `kafka-net` | Kafka, Confluent Registry, ksqlDB, flink-kafka | traffico del data plane Kafka |
| `nats-net` | NATS, Apicurio, PostgreSQL, flink-nats | traffico del data plane NATS |
| `migration-net` | simulatori, bridge/controller, parity verifier, ingressi controllati dei due plane | dual run e confronto |
| `observability-net` | Prometheus, Grafana, Loki, Alloy, OTel, exporter | telemetria |

Un servizio entra solo nelle reti necessarie. Kafka e NATS non sono collegati direttamente: bridge e simulatori sono i punti espliciti di attraversamento.

## Event flow

```text
OrderCreated -> PaymentAuthorized|PaymentRejected -> FulfillmentStarted -> FulfillmentCompleted
```

La chiave dell'aggregato preserva ordering dove il trasporto lo consente. `eventId`, `correlationId` e checksum normalizzato permettono di confrontare i percorsi. Gli outcome, non i timestamp di trasporto, determinano la parità funzionale.

## Persistenza

- Kafka log e metadata KRaft su volume nominato;
- JetStream file store su volume nominato;
- PostgreSQL/Apicurio su volume nominato;
- checkpoint/savepoint separati per ciascun Flink;
- Grafana/Loki/Prometheus con retention corta da laboratorio;
- comando documentato per reset recuperabile/esplicito, mai eseguito implicitamente.

## Flink e NATS

L'integrazione è una decisione con spike obbligatoria. Il confine deve essere un modulo dedicato per evitare che una libreria immatura contamini dominio e simulatori. Ack NATS deve avvenire soltanto quando la semantica di checkpoint scelta rende il record recuperabile. La documentazione deve nominare chiaramente la garanzia effettiva: at-most-once, at-least-once o exactly-once provata.

## Porte host

Per default esporre soltanto UI/API necessarie all'operatore, legate a `127.0.0.1` quando supportato. I data plane comunicano tramite DNS Compose. Una tabella definitiva delle porte sarà introdotta con T02 e validata contro collisioni su Docker Desktop.

## Compatibilità architetturale

- immagini multi-arch preferite;
- nessun bind mount dipendente da `/var/run` nel percorso standard macOS/Windows;
- eventuale Docker socket solo nel profilo test, read-only e con rischio documentato;
- file e script LF, path relativi e quoting testato in PowerShell e shell POSIX.
