# Budget risorse

## Host di riferimento

Rilevazione del 31 agosto 2026:

- CPU host: AMD Ryzen 7 9800X3D, 8 core e 16 thread;
- RAM host: circa 61,6 GiB;
- memoria esposta da Docker Desktop: circa 30,2 GiB;
- CPU esposte da Docker Desktop: 16;
- spazio libero sul volume di lavoro: oltre 570 GiB.

Il computer deve restare comodo per editor, browser, terminali Herdr e review. Il budget non sfrutta tutta la RAM disponibile alla VM Docker.

## Budget iniziale del profilo completo

| Gruppo | Servizi | RAM target | RAM max | CPU max aggregata |
|---|---|---:|---:|---:|
| Kafka | broker, Schema Registry, ksqlDB, Kafka Streams | 4,5 GiB | 6,0 GiB | 2,5 |
| NATS | 1 nodo, Apicurio, PostgreSQL | 2,5 GiB | 3,5 GiB | 1,5 |
| Flink Kafka | JobManager, TaskManager | 2,0 GiB | 3,0 GiB | 1,5 |
| Flink NATS | JobManager, TaskManager | 2,0 GiB | 3,0 GiB | 1,5 |
| Simulatori/control | 3 simulatori, bridge/controller, parity | 2,0 GiB | 3,0 GiB | 2,0 |
| Osservabilità | Prometheus, Grafana, Loki, Alloy, OTel, exporter | 3,0 GiB | 4,0 GiB | 1,5 |
| Margine runtime | init, page cache, spike transitori | 2,0 GiB | 2,0 GiB | 1,0 |
| **Totale** | profilo completo | **18,0 GiB** | **24,5 GiB** | **11,5** |

I valori sono ipotesi iniziali, da trasformare in limiti per servizio e correggere con misura. Il target ordinario è 18-20 GiB; il picco deve restare sotto 24 GiB. Il profilo `ha` aggiunge due nodi NATS e può richiedere 1-2 GiB ulteriori: non va combinato automaticamente con `load` e `tracing`.

## Heap iniziali

- Kafka: 1 GiB;
- Schema Registry: 384-512 MiB;
- ksqlDB: 1 GiB;
- Flink JobManager: 512-768 MiB ciascuno;
- Flink TaskManager: 1-1,5 GiB ciascuno, slot limitati;
- simulatori Spring Boot: 256-384 MiB ciascuno;
- Apicurio: 512-768 MiB;
- Grafana/Prometheus/Loki: retention e cache ridotte.

Questi valori non sono prescrizioni finali: T02 deve verificarli con startup, idle e carico baseline.

## Regole di capacità

- ogni servizio Compose deve avere healthcheck e limite memoria;
- evitare più di un TaskManager per cluster nella baseline; aumentare solo nel profilo load;
- retention di log/metriche e segmenti Kafka/JetStream breve ma sufficiente al replay;
- test `load`, `ha` e `tracing` non partono insieme salvo scenario esplicito;
- se Docker supera 24 GiB o l'host presenta memory pressure, spegnere prima Tempo/UI opzionali, poi ksqlDB quando lo scenario non lo richiede; non unire i due Flink;
- misurare startup, idle 5 minuti, burst e soak con `docker stats --no-stream` e metriche runtime;
- archiviare in `reports/sizing/` i risultati CI, senza committare output voluminosi.

## Profili di lavoro suggeriti

| Attività | Profili | RAM stimata |
|---|---|---:|
| sviluppo contratti | `bootstrap` | 2-4 GiB |
| Kafka | `kafka` | 8-11 GiB |
| NATS | `nats` | 7-10 GiB |
| migrazione | `migration` | 14-18 GiB |
| demo osservabile | `full` | 18-20 GiB |
| HA NATS | `migration,ha` senza tracing/load | 18-22 GiB |

## Gate

Una PR che introduce o modifica container fallisce la review se non specifica limiti, healthcheck e delta del budget. Prima della release, il profilo completo deve girare per almeno 30 minuti con carico baseline senza OOM, restart imprevisti o degradazione significativa dell'host.
