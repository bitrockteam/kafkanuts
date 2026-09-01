# Budget risorse

## Classi di macchina target

Il budget definisce requisiti di esecuzione portabili per ciascun profilo della demo.

| Classe target | CPU logiche | RAM host | RAM disponibile a Docker | Disco libero | Scope supportato |
|---|---:|---:|---:|---:|---|
| profili ridotti | 8 | 16 GiB | 10-12 GiB | 60 GiB | `bootstrap`, `kafka` oppure `nats`; non entrambi i Flink con osservabilità completa |
| full minimo | 12 | 32 GiB | almeno 24 GiB | 120 GiB | `full` baseline; niente combinazione automatica di `ha`, `load` e `tracing` |
| full raccomandato | 16 | almeno 48 GiB | 30-32 GiB | 150 GiB | `full`, osservabilità, failure test e margine per strumenti host |

Su Windows e macOS la memoria disponibile a Docker deve essere verificata separatamente dalla RAM installata. Su Linux vale il limite effettivo imposto al daemon o ai container. La classe minima da 32 GiB può eseguire il profilo completo solo con limiti rigorosi e margine host ridotto; HA e carico appartengono alla classe raccomandata.

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

I valori sono ipotesi iniziali, da trasformare in limiti per servizio e correggere con misura. Il target ordinario è 18-20 GiB; il picco deve restare sotto 24 GiB. La somma dei massimi per servizio non rappresenta un consumo simultaneo garantito e serve come guardrail di configurazione. Il profilo `ha` aggiunge due nodi NATS e può richiedere 1-2 GiB ulteriori: non va combinato automaticamente con `load` e `tracing`.

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

Una PR che introduce o modifica container fallisce la review se non specifica limiti, healthcheck e delta del budget. Prima della release, il profilo completo deve girare per almeno 30 minuti sulla classe `full minimo` con carico baseline senza OOM o restart imprevisti. La stessa prova sulla classe `full raccomandato` deve lasciare margine sufficiente per editor, browser e strumenti di osservazione.

## T05 — Delta Kafka baseline

T05 introduce un singolo profilo Kafka con tre servizi runtime e un init usa-e-getta. I limiti Compose sono il budget dichiarato e sono configurabili via `.env`:

| Servizio | Limite CPU | Limite memoria | Delta T05 |
|---|---:|---:|---|
| `kafka` | 1,0 | 1 GiB | +1 broker KRaft, heap 256-512 MiB |
| `schema-registry` | 0,5 | 512 MiB | +Registry Confluent |
| `ksqldb` | 0,5 | 1 GiB | +server ksqlDB, heap gestito dall'immagine |
| `kafka-init` | nessun limite esplicito | nessun limite esplicito | container breve per topic/gate |
| **Delta dichiarato** | **2,0 CPU** | **2,5 GiB max** | profilo `kafka` isolato |

Il delta è un limite configurato, non una misura di consumo reale: il gate T05 registra correttezza funzionale, non sizing produttivo. Su una macchina della classe “profili ridotti” il profilo Kafka va eseguito senza gli altri data plane; il limite massimo configurato dei tre runtime è 2,5 GiB, oltre a page cache e container transitori. `kafka-init` deve essere terminato e rimosso dal cleanup prima che il report dichiari PASS.

## T02 — Tooling Compose misurato

T02 introduce soltanto il builder/test Maven containerizzato. I limiti sono configurabili tramite `.env` e hanno questi default:

| Servizio | Limite CPU | Limite memoria | Osservazione reale |
|---|---:|---:|---|
| `builder` | 1,0 | 1 GiB | 98,80% CPU; 136,2 MiB / 1 GiB (13,30%) |
| `test` | 1,0 | 1 GiB | limite configurato; non misurato separatamente |

La misura del builder è una singola osservazione durante uno smoke run e non costituisce sizing produttivo. Il volume Maven nominato è sviluppo locale e può essere rimosso con il comando `reset`; i task successivi devono aggiungere misure proprie prima di dichiarare completato il relativo budget.
