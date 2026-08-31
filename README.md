# kafkanuts

Laboratorio riproducibile per progettare, osservare e collaudare la migrazione progressiva di un sistema event-driven da Apache Kafka a NATS JetStream.

Il progetto mette a confronto i due ecosistemi su un flusso applicativo realistico, mantenendo contratti Avro, schema governance, stream processing, osservabilità e rollback verificabile. Non cerca una sostituzione meccanica topic-per-subject: rende esplicite le differenze di semantica, operatività e failure handling.

> **Stato:** pianificazione e bootstrap completati. L'implementazione parte da [T01](https://github.com/bitrockteam/kafkanuts/issues/1). Nessun componente runtime è ancora dichiarato pronto.

## Obiettivi

La demo deve permettere a un operatore di:

1. avviare tutto tramite Docker Compose da Windows, Linux o macOS;
2. generare un flusso coerente di ordini, pagamenti ed evasione tramite tre microservizi Spring Boot indipendenti;
3. eseguire lo stesso scenario su Kafka, su NATS JetStream o in modalità dual run;
4. serializzare e deserializzare gli eventi in Avro su entrambi i trasporti;
5. mostrare in funzione Confluent Schema Registry e Apicurio Registry;
6. confrontare elaborazioni streaming equivalenti su due cluster Flink separati;
7. esercitare Kafka Streams e ksqlDB nel data plane Kafka;
8. verificare ack, redelivery, deduplicazione, replay, DLQ e persistenza JetStream;
9. migrare un servizio alla volta con shadow traffic, parity check, cutover e rollback;
10. osservare metriche, log e trace correlati;
11. produrre test, immagini, JAR, SBOM e report di sicurezza versionati.

Il risultato atteso non è soltanto uno stack che si avvia, ma una migrazione dimostrabile attraverso test e report ripetibili.

## Architettura

```text
                                  +---------------------------+
                                  | migration / demo control  |
                                  +-------------+-------------+
                                                |
                 +------------------------------+------------------------------+
                 |                              |                              |
        order-simulator                payment-simulator              fulfillment-simulator
         kafka | nats | dual            kafka | nats | dual             kafka | nats | dual
                 |                              |                              |
       +---------+------------------------------+------------------------------+---------+
       |                                                                                 |
+------v-------------------- Kafka data plane --------+      +----------------------------v------+
| Kafka OSS, single broker KRaft                     |      | NATS JetStream data plane          |
| Confluent Schema Registry                          |      | single node / optional 3-node HA   |
| Kafka Streams                                      |      | durable consumers, replay and DLQ  |
| ksqlDB                                             |      | Apicurio Registry + PostgreSQL      |
|                                                    |      |                                  |
|  +----------------------------------------------+  |      |  +----------------------------+  |
|  | flink-kafka: JobManager + TaskManager        |  |      |  | flink-nats: JM + TM         |  |
|  +--------------------------+-------------------+  |      |  +--------------+-------------+  |
+-----------------------------+----------------------+      +-----------------+----------------+
                              |                                           |
                              +--------------+----------------------------+
                                             |
                                      parity-verifier
                                             |
                    +------------------------+-------------------------+
                    | Prometheus | Grafana | Loki | Alloy | OTel      |
                    | optional Tempo tracing                          |
                    +-------------------------------------------------+
```

I due cluster Flink sono intenzionalmente indipendenti. Condividono contratti e funzione logica, ma non runtime, classpath, checkpoint o failure domain. Questo consente di confrontare i risultati senza nascondere problemi di un connettore dentro un cluster condiviso.

## Flusso applicativo

La demo usa tre simulatori Java 21/Spring Boot, ciascuno in un container distinto:

| Servizio | Responsabilità | Eventi principali |
|---|---|---|
| `order-simulator` | genera ordini deterministici e burst configurabili | `OrderCreated` |
| `payment-simulator` | elabora l'ordine e simula autorizzazione o rifiuto | `PaymentAuthorized`, `PaymentRejected` |
| `fulfillment-simulator` | avvia e completa l'evasione degli ordini pagati | `FulfillmentStarted`, `FulfillmentCompleted` |

Ogni servizio dispone di adapter `kafka`, `nats` e `dual`, selezionabili tramite configurazione senza ricostruire l'immagine. L'envelope evento conserva:

- `eventId` stabile tra i trasporti;
- tipo e versione dell'evento;
- aggregate/partition key;
- timestamp UTC;
- producer, correlation ID e causation ID;
- payload Avro tipizzato;
- riferimento portabile allo schema.

`eventId`, correlation ID e checksum normalizzato consentono al parity verifier di confrontare cardinalità, outcome, duplicati e latenza senza trattare i timestamp di trasporto come risultato funzionale.

## Stack previsto

### Data plane Kafka

- Apache Kafka OSS, singolo broker in modalità KRaft;
- topic, partizioni e retention dichiarati come configurazione versionata;
- Confluent Schema Registry;
- serializer/deserializer Avro Confluent;
- una topologia Kafka Streams nel dominio pagamenti o in un modulo isolato;
- query ksqlDB versionate;
- cluster `flink-kafka` con checkpoint e output dedicati.

Il single broker è una scelta da laboratorio: riduce il consumo e rende leggibile la baseline, ma non simula l'alta affidabilità di un cluster Kafka di produzione.

### Data plane NATS

- NATS con JetStream e file storage persistente;
- nodo singolo nella baseline;
- profilo `ha` opzionale con tre nodi per leader loss e recovery;
- durable pull consumer con ack esplicito;
- Apicurio Registry con PostgreSQL;
- cluster `flink-nats` dedicato;
- subject, stream e consumer modellati sulle semantiche di dominio.

Il progetto usa JetStream, non il precedente NATS Streaming/STAN.

### Stream processing

Kafka Streams e ksqlDB mostrano le capacità native dell'ecosistema Kafka. I due cluster Flink applicano una trasformazione logica equivalente ai flussi Kafka e NATS e pubblicano risultati confrontabili.

L'integrazione Flink/NATS inizia con una spike con criteri go/no-go:

- compatibilità con la versione Flink selezionata;
- ack coerente con checkpoint e recovery;
- parallelismo e backpressure verificati;
- serializzazione Avro provata;
- licenza e manutenzione accettabili.

Se nessun connettore esistente soddisfa i criteri, verrà realizzato un adapter minimo e confinato sulle API Source/Sink supportate. La documentazione dichiarerà la garanzia effettivamente provata, senza attribuire exactly-once a un percorso che offre soltanto at-least-once.

## Avro e schema registry

Gli schema `.avsc` sono la fonte canonica dei contratti. La generazione Java e tutti i test di compatibilità vengono eseguiti in container. La baseline prevista è `BACKWARD_TRANSITIVE`, con test sia positivi sia negativi.

La migrazione separa il cambio di trasporto dal cambio di registry:

1. **Kafka + Confluent:** baseline con wire format Confluent;
2. **NATS + Confluent:** NATS trasporta payload Avro con lo stesso framing, riducendo le variabili del primo dual run;
3. **Confluent + Apicurio:** export/import e verifica di compatibilità;
4. **NATS + Apicurio:** uso delle API ccompat quando i test ne confermano il comportamento;
5. **Schema ID in header:** variante esplicita con ID/fingerprint negli header NATS;
6. **Rollback:** prova di lettura dei messaggi storici tornando al registry precedente.

Gli ID numerici non vengono considerati portabili tra registry. La correlazione usa subject, versione e fingerprint dello schema; il bridge mantiene una mappa degli ID oppure decodifica e re-serializza in modo controllato.

## Funzionalità JetStream dimostrate

La suite funzionale deve coprire:

- stream persistenti con limiti e retention espliciti;
- `LimitsPolicy` e uno scenario separato `WorkQueuePolicy`;
- durable pull consumer;
- ack, `AckWait`, `MaxDeliver` e backoff;
- redelivery dopo crash prima e dopo l'ack;
- deduplicazione tramite `Nats-Msg-Id`;
- DLQ e advisory verificabili;
- replay da sequence e timestamp;
- persistenza dei messaggi e consumer state dopo restart;
- pending e ack-pending confrontati con il consumer lag Kafka;
- wildcard subject e consumer paralleli;
- leader loss e recovery nel profilo NATS a tre nodi.

Mirror e source di stream verranno aggiunti soltanto se producono una dimostrazione utile e misurabile dopo la baseline.

## Migrazione progressiva

Il passaggio non avviene come big bang. Ogni servizio attraversa fasi controllate:

| Fase | Produzione | Consumo primario | Shadow | Registry NATS | Gate |
|---|---|---|---|---|---|
| M0 | Kafka | Kafka | — | — | baseline Kafka verde |
| M1 | Kafka + NATS | Kafka | NATS audit | Confluent | nessuna perdita e parità |
| M2 | Kafka + NATS | Kafka | consumer NATS | Confluent | latenze e duplicati entro soglia |
| M3 | Kafka + NATS | Kafka | NATS | Apicurio in validazione | mapping schema verificato |
| M4 | Kafka + NATS | NATS per un canary | Kafka | Apicurio | SLO e failure test verdi |
| M5 | NATS + safety feed Kafka | NATS progressivo | Kafka audit | Apicurio | tutti i domini validati |
| M6 | NATS | NATS | Kafka spento dopo soak | Apicurio | replay e rollback provati |

Ogni fase definisce precondizioni, report, soglia di promozione e rollback. Il controller può fermare la demo a qualunque fase. Il passaggio viene ripetuto separatamente per ordini, pagamenti ed evasione.

## Docker Compose e portabilità

Tutto il runtime e tutti i test d'integrazione devono essere avviabili con Docker Compose. Sull'host sono richiesti soltanto Git, Docker Engine/Desktop e Docker Compose v2; Java, Maven, Kafka, NATS e Flink restano nei container.

Un unico progetto Compose usa quattro reti logiche:

| Rete | Scopo |
|---|---|
| `kafka-net` | Kafka, Schema Registry, ksqlDB e `flink-kafka` |
| `nats-net` | NATS, Apicurio, PostgreSQL e `flink-nats` |
| `migration-net` | simulatori, controller/bridge e parity verifier |
| `observability-net` | metriche, log, trace ed exporter |

Kafka e NATS non comunicano direttamente. Simulatori e componenti di migrazione sono punti di attraversamento espliciti.

I vincoli di portabilità includono:

- immagini multi-arch `linux/amd64` e `linux/arm64`, oppure limitazioni documentate;
- volumi nominati per i dati e path relativi al repository;
- line ending LF imposti da Git;
- healthcheck reali, senza sleep arbitrari;
- configurazione non sensibile in `.env.example`;
- equivalenti PowerShell e POSIX soltanto quando un comando Compose non basta;
- nessuna dipendenza da path host specifici di Windows, Linux o macOS.

### Profili pianificati

| Profilo | Utilizzo |
|---|---|
| `bootstrap` | contratti e servizi minimi di verifica |
| `kafka` | data plane Kafka e simulatori Kafka |
| `nats` | data plane NATS e simulatori NATS |
| `migration` | entrambi i plane, dual run e parity |
| `full` | demo completa con osservabilità essenziale |
| `ha` | cluster NATS a tre nodi |
| `observability` | Prometheus, Grafana, Loki, Alloy e OTel |
| `tracing` | Tempo e trace sampling |
| `test` | suite integration/functional |
| `load` | carico controllato e soak |
| `security` | scansioni riproducibili containerizzate |

Gli script portabili tradurranno i profili compositi, poiché un profilo Compose non funziona come macro automatica di altri profili.

## Osservabilità

La baseline prevista comprende:

- Prometheus per metriche;
- Grafana con dashboard provisionate dal repository;
- Loki per log centralizzati;
- Grafana Alloy per discovery e forwarding dei log;
- OpenTelemetry Collector per ricezione, processing ed export;
- exporter JMX/servizio per Kafka, registry, ksqlDB, Flink e NATS;
- Tempo nel profilo opzionale `tracing`.

Event ID, correlation ID, transport e fase di migrazione sono campi strutturati. Le dashboard devono mostrare throughput, errori, latenza, consumer lag/pending, redelivery, DLQ, checkpoint Flink, errori dei registry e consumo CPU/RAM.

## Budget della macchina di riferimento

Rilevazione iniziale:

- AMD Ryzen 7 9800X3D, 8 core e 16 thread;
- circa 61,6 GiB di RAM host;
- Docker Desktop espone 16 CPU e circa 30,2 GiB di RAM;
- oltre 570 GiB liberi sul disco di lavoro al momento della misura.

| Scenario | RAM Docker prevista |
|---|---:|
| sviluppo contratti | 2-4 GiB |
| solo Kafka | 8-11 GiB |
| solo NATS | 7-10 GiB |
| migrazione senza osservabilità completa | 14-18 GiB |
| demo completa | 18-20 GiB |
| picco massimo consentito | 22-24 GiB |

L'uso ordinario deve restare entro circa 10 CPU e i test entro 12. `ha`, `load` e `tracing` non vengono attivati insieme per default. Ogni PR che aggiunge un container deve dichiarare healthcheck, limiti, porte e delta misurato con `docker stats`.

## Strategia di test

La qualità viene verificata a più livelli:

- unit test per dominio, codec, mapping e adapter;
- contract test Avro e cross-registry;
- integration test dei due data plane;
- functional test delle fasi M0-M6;
- test di restart, rete interrotta, retry, replay e DLQ;
- checkpoint/recovery per entrambi i Flink;
- parity test su conteggi, stati e checksum;
- test di schema evolution e incompatibilità;
- smoke, burst e soak con soglie dichiarate;
- smoke di portabilità Windows, Linux e macOS.

Le suite producono report JUnit e JSON/Markdown archiviabili da CI. I test di carico servono a confrontare e trovare limiti della demo, non a formulare benchmark commerciali universali.

## CI/CD, sicurezza e artefatti

Le pull request introdurranno progressivamente:

- build Maven e test in container;
- validazione e smoke Docker Compose;
- Checkstyle, formatter e SpotBugs;
- CodeQL;
- Trivy per filesystem, configurazione e immagini;
- Gitleaks;
- GitHub Dependency Review e Dependabot;
- CycloneDX SBOM;
- immagini non-root e privilegi/capability minimi;
- GitHub Actions pinnate a commit SHA con permessi minimali;
- JAR e immagini OCI versionate in GHCR;
- provenance, attestazioni e firma quando l'identità OIDC è pronta.

Vulnerabilità High o Critical bloccano merge e release, salvo eccezione temporanea con owner, mitigazione e scadenza documentati.

## Contratto operativo Git

Git e GitHub sono la fonte durevole di verità. Lo stato di finestre, chat e terminali è soltanto diagnostico.

- **Luna** (`gpt-5.6-luna`, reasoning `medium`) implementa un task alla volta, esegue i gate, committa, pusha e apre la PR; non fa merge.
- **Watcher Herdr/PowerShell** mantiene liveness, osserva GitHub e può inviare continuazioni; non modifica codice, commit, branch o architettura.
- **Codex** concentra i token su architettura, sicurezza, review e merge; legge diff, check e commenti GitHub invece delle finestre terminale.
- **GitHub** conserva backlog, issue, branch, PR, check, review, decisioni e release.

`main` richiede pull request, squash merge, cronologia lineare e conversazioni risolte; force push e cancellazione sono bloccati. I required checks vengono aggiunti al ruleset quando i relativi workflow esistono, evitando una protezione impossibile da soddisfare durante il bootstrap.

## Roadmap

Il lavoro è diviso in task piccoli. La numerazione conserva il collegamento con le issue; T10 viene deliberatamente anticipato per ritirare il rischio Flink/NATS prima dei simulatori:

1. **T01-T03:** toolchain, Compose e contratti Avro;
2. **T10 anticipato:** gate e cluster Flink NATS, con decisione Table/SQL, DataStream-only o esclusione dallo scope commerciale iniziale;
3. **T04:** tre simulatori e adapter;
4. **T05-T07:** baseline Kafka, Flink Kafka e suite JetStream;
5. **T08-T09:** Apicurio, Avro NATS e migrazione registry;
6. **T11:** dual run, cutover e rollback;
7. **T12-T13:** osservabilità, failure, replay, evolution e performance;
8. **T14-T15:** hardening, SBOM, attestazioni, documentazione e release `v0.1.0`.

Il backlog canonico, con dipendenze e gate, è in [TASKS.md](TASKS.md). Le issue [T01-T15](https://github.com/bitrockteam/kafkanuts/issues) ne costituiscono l'indice operativo su GitHub.

## Documentazione

- [Piano esecutivo completo](docs/PLAN.md)
- [Architettura e reti](docs/ARCHITECTURE.md)
- [Budget risorse](docs/RESOURCE-BUDGET.md)
- [Handoff per Luna](docs/EXECUTION-HANDOFF.md)
- [Contratto del watcher Herdr](docs/WATCHER-CONTRACT.md)
- [Regole per agenti e contributori](AGENTS.md)
- [Linee guida di contribuzione](CONTRIBUTING.md)
- [Security policy](SECURITY.md)
- [Decisioni architetturali](docs/adr/)

## Limiti dichiarati

`kafkanuts` è un laboratorio tecnico, non un sizing di produzione. La prima release non include Kubernetes, servizi cloud gestiti, multi-region, disaster recovery geografico o garanzie prestazionali generalizzabili. Le differenze tra Kafka e NATS vengono riportate dai test senza forzare equivalenze non dimostrate.

## Licenza

Apache License 2.0. Vedi [LICENSE](LICENSE).
