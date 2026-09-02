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
11. produrre test, immagini e JAR versionati; SBOM e report di sicurezza automatici sono rimandati a `v0.2.0`;
12. esportare misure tecniche ripetibili utilizzabili da un modello di costo esterno, senza incorporare prezzi o dati cliente nel repository.

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
| M6 | NATS | NATS | Kafka spento dopo periodo di osservazione | Apicurio | replay e rollback provati |

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

## Requisiti della macchina target

Le soglie iniziali della macchina target, da confermare con T02 e T13, sono:

| Classe target | CPU logiche | RAM host | RAM disponibile a Docker | Disco libero | Utilizzo previsto |
|---|---:|---:|---:|---:|---|
| profili ridotti | 8 | 16 GiB | 10-12 GiB | 60 GiB | `bootstrap` oppure un solo data plane |
| full minimo | 12 | 32 GiB | almeno 24 GiB | 120 GiB | demo completa senza combinare `ha`, `load` e `tracing` |
| full raccomandato | 16 | almeno 48 GiB | 30-32 GiB | 150 GiB | sviluppo confortevole, osservabilità e test di failure |

Il profilo `full` su una macchina da 32 GiB è una configurazione minima: richiede limiti rigorosi e margine ridotto per editor e altri processi. Per HA o prove di carico è raccomandata la classe superiore.

| Scenario | RAM Docker prevista |
|---|---:|
| sviluppo contratti | 2-4 GiB |
| solo Kafka | 8-11 GiB |
| solo NATS | 7-10 GiB |
| migrazione senza osservabilità completa | 14-18 GiB |
| demo completa | 18-20 GiB |
| picco massimo consentito | 22-24 GiB |

L'uso ordinario dello stack deve restare entro circa 10 CPU e i test entro 12. `ha`, `load` e `tracing` non vengono attivati insieme per default. Ogni PR che aggiunge un container deve dichiarare healthcheck, limiti, porte e delta misurato con `docker stats`.

## Strategia di test

Il perimetro di verifica della release `v0.1.0` è ridotto e normato da [docs/QA-SCOPE.md](docs/QA-SCOPE.md), approvato in [ADR 0005](docs/adr/0005-perimetro-qa-ridotto.md). La release dimostra **correttezza della migrazione**, non idoneità alla produzione.

Nel perimetro:

- unit test per dominio, codec, mapping e adapter;
- contract test Avro e cross-registry, compatibilità positiva e negativa;
- integration test dei due data plane;
- functional test delle fasi M0-M6 sui criteri di correttezza;
- tre scenari di fallimento deterministici, più replay e DLQ;
- checkpoint/recovery per entrambi i Flink;
- parity test su conteggi, stati terminali e checksum normalizzati.

Fuori perimetro, stato `NOT_TESTED` con motivazione nel report: percentili di latenza, throughput, burst, soak, dataset di capacità, RTO/RPO misurati, profilo NATS HA, interruzione di rete, component test Testcontainers e CI su runner Windows e macOS.

I gate producono un report JSON archiviabile. **Nessun dato di prestazione, capacità, disponibilità o costo può essere derivato da questa release**: i requisiti corrispondenti non sono stati esercitati e non sono presentati come soddisfatti.

## CI/CD, sicurezza e artefatti

Nel perimetro di `v0.1.0`:

- build Maven e test in container;
- validazione e smoke Docker Compose;
- Checkstyle, formatter e SpotBugs;
- ricerca di segreti nel repository;
- Dependabot;
- immagini non-root e privilegi/capability minimi;
- GitHub Actions pinnate a commit SHA con permessi minimali;
- JAR e immagini OCI versionate.

Rimandati a `v0.2.0`, stato `NOT_TESTED`: CodeQL, Trivy, GitHub Dependency Review, CycloneDX SBOM, GHCR, provenance, attestazioni e firma.

Un segreto committato blocca merge e release senza eccezioni. Per le vulnerabilità note, la release dichiara che l'analisi automatica non è stata eseguita, invece di dichiarare l'assenza di vulnerabilità.

## Contratto operativo Git

Git e GitHub sono la fonte durevole di verità. Lo stato di finestre, chat e terminali è soltanto diagnostico.

- **Esecutore singolo**: implementa un task alla volta, esegue i gate, committa, pusha il branch, apre la PR, svolge la review scritta e fa squash merge.
- **GitHub** conserva backlog, issue, branch, PR, check, review, decisioni e release.

Dal 2026-09-02 il ciclo a tre attori (esecutore Luna su Pi, watcher PowerShell, reviewer Codex/Sol) è ritirato. Il contratto Git resta invariato: cambia chi esegue i passi, non i passi.

`main` richiede pull request, squash merge, cronologia lineare e conversazioni risolte; force push e cancellazione sono bloccati. I required checks vengono aggiunti al ruleset quando i relativi workflow esistono, evitando una protezione impossibile da soddisfare durante il bootstrap.

### Mandato umano e autonomia agentica

Lo sponsor umano ha definito l'architettura, gli obiettivi, i vincoli e il piano di alto livello e li ha approvati esplicitamente nel bootstrap T00. Entro quel perimetro, il ciclo di sviluppo è completamente agentico: selezione del task, implementazione, test, handoff, apertura PR, code review, correzioni, merge e avanzamento del backlog non richiedono approvazioni umane intermedie.

L'approvazione del piano non autorizza cambiamenti impliciti al suo perimetro. Gli agenti tornano allo sponsor soltanto quando serve:

- cambiare architettura, scope, garanzia semantica o budget approvato;
- accettare un rischio di sicurezza o una vulnerabilità High/Critical;
- ottenere credenziali, permessi, UAC o accesso a risorse non già autorizzate;
- eseguire un'azione distruttiva o irreversibile non prevista dal piano;
- risolvere un blocco per cui le alternative lecite producono conseguenze materialmente diverse.

Una richiesta di chiarimento ordinaria non interrompe il flusso: gli agenti applicano le decisioni già registrate in piano, ADR, issue e criteri di accettazione. Le richieste di approvazione imposte dalla piattaforma o dall'ambiente restano comunque necessarie quando compaiono.

## Review a esecutore singolo

Autore e reviewer coincidono, quindi la review non può restare implicita: è scritta nel corpo della pull request e copre scope, correttezza architetturale, migrazione, sicurezza, concorrenza, failure mode, limiti risorse e coerenza fra i `PASS` dichiarati e ciò che il gate ha davvero eseguito.

Vale la **regola di astensione**: se la review individua un problema di architettura, sicurezza, semantica o budget, il task diventa `BLOCKED` nell'issue e torna allo sponsor umano. L'esecutore non si auto-assolve su una decisione che non gli compete.

L'efficienza deriva dalle stesse regole operative di prima:

- Git e GitHub conservano lo stato durevole; le chat sono contesto transitorio;
- un task e una PR alla volta evitano lavoro duplicato e context switching;
- log completi non vengono riversati nel prompt: PR e report conservano sintesi, comandi, risultati e link agli artefatti;
- non si approvano claim che i test non dimostrano;
- ambiguità che cambiano architettura, sicurezza, semantica o costo risorse diventano decisioni tracciate, non tentativi ripetuti.

### Perché la PR resta il confine di review

- **La review guarda un artefatto stabile, non un terminale.** Diff, commit, report e check evitano scraping di finestre, output troncati e ricostruzioni fragili di sessione.
- **Lo scope viene controllato prima della review semantica.** L'elenco dei file modificati va confrontato con task e prerequisiti autorizzati; una PR che tocca file fuori scope senza motivazione approvata si ferma lì.
- **Il branch isola il lavoro non approvato.** Finché la PR non viene unita, `main` non riceve il cambiamento.
- **GitHub conserva il verbale.** Issue, commit, check, commenti, decisioni e merge consentono di ricostruire quali prove sono passate e perché il cambiamento è stato accettato.
- **I gate meccanici precedono il giudizio.** I test richiesti girano prima della PR; CI ripete i controlli disponibili. La review semantica parte quando risultati e failure sono visibili.

## Roadmap

Il lavoro è diviso in task piccoli. La numerazione conserva il collegamento con le issue; T10 viene deliberatamente anticipato per ritirare il rischio Flink/NATS prima dei simulatori:

1. **T01-T03:** toolchain, Compose e contratti Avro;
2. **T10 anticipato:** gate e cluster Flink NATS, con decisione Table/SQL, DataStream-only o esclusione dallo scope commerciale iniziale;
3. **T04:** tre simulatori e adapter;
4. **T05-T07:** baseline Kafka, Flink Kafka e suite JetStream;
5. **T08-T09:** Apicurio, Avro NATS e migrazione registry;
6. **T11:** dual run, cutover e rollback;
7. **T12-T13:** osservabilità Prometheus/Grafana, failure e replay nel perimetro ridotto;
8. **T14-T15:** hardening essenziale, documentazione e release `v0.1.0`.

Il backlog canonico, con dipendenze e gate, è in [TASKS.md](TASKS.md). Le issue [T01-T15](https://github.com/bitrockteam/kafkanuts/issues) ne costituiscono l'indice operativo su GitHub.

## Documentazione

- [Piano esecutivo completo](docs/PLAN.md)
- [Architettura e reti](docs/ARCHITECTURE.md)
- [Budget risorse](docs/RESOURCE-BUDGET.md)
- [Perimetro di verifica v0.1.0](docs/QA-SCOPE.md)
- [Handoff esecutivo](docs/EXECUTION-HANDOFF.md)
- [Contratto del watcher Herdr, storico](docs/WATCHER-CONTRACT.md)
- [Regole per agenti e contributori](AGENTS.md)
- [Linee guida di contribuzione](CONTRIBUTING.md)
- [Security policy](SECURITY.md)
- [Decisioni architetturali](docs/adr/)

## Limiti dichiarati

`kafkanuts` è un laboratorio tecnico, non un sizing di produzione. La prima release non include Kubernetes, servizi cloud gestiti, multi-region, disaster recovery geografico o garanzie prestazionali generalizzabili. Le differenze tra Kafka e NATS vengono riportate dai test senza forzare equivalenze non dimostrate.

## Licenza

Apache License 2.0. Vedi [LICENSE](LICENSE).
