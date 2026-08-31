# Piano esecutivo di kafkanuts

> **Stato del piano:** baseline architetturale approvata; implementazione non avviata. Le decisioni accettate sono vincoli, mentre versioni, prestazioni e garanzie semantiche restano ipotesi finché i gate indicati sotto non producono evidenze ripetibili.

## 1. Scopo e criteri di successo

Il progetto realizza un ambiente dimostrativo completo, ripetibile e misurabile per migrare tre flussi applicativi da Kafka a NATS JetStream. Non è un benchmark universale né un blueprint di produzione: deve evidenziare capacità, differenze semantiche, rischi e tecniche di cutover con prove automatizzate.

Il progetto è riuscito quando un operatore può, su Windows, Linux o macOS:

1. clonare il repository e avviare un profilo documentato con Docker Compose;
2. produrre eventi Avro dai tre simulatori attraverso Kafka, NATS o entrambi;
3. osservare elaborazioni Kafka Streams, ksqlDB e i due cluster Flink;
4. usare Confluent Schema Registry e Apicurio Registry nei percorsi previsti;
5. eseguire una migrazione graduale, confrontare gli output e fare rollback;
6. provocare failure e verificare ack, redelivery, deduplica, replay e DLQ;
7. consultare metriche, log e trace correlati;
8. eseguire test, scansioni di sicurezza e build di artefatti senza toolchain locale.

## 2. Perimetro

### Incluso

- Kafka OSS single broker in KRaft;
- Confluent Schema Registry, ksqlDB e una topologia Kafka Streams;
- NATS con JetStream persistente; profilo opzionale a tre nodi per prove HA;
- Apicurio Registry con PostgreSQL;
- due cluster Flink indipendenti, ognuno con JobManager e TaskManager;
- tre simulatori Java 21/Spring Boot in container distinti;
- contratti Avro condivisi, compatibilità ed evoluzione schema;
- bridge/migration controller e parity verifier;
- osservabilità con Prometheus, Grafana, Loki, Grafana Alloy e OpenTelemetry Collector;
- Tempo nel profilo opzionale `tracing`;
- CI/CD, test, SAST/scansioni, SBOM e artefatti versionati;
- documentazione e script/target portabili.

### Non incluso nella prima release

- sizing o garanzie di produzione;
- Kubernetes, operator o servizi cloud gestiti;
- multi-region, disaster recovery geografico o benchmark certificato;
- connettori proprietari obbligatori;
- identità enterprise completa. TLS/auth NATS e Kafka possono essere dimostrati in un profilo dedicato dopo la baseline funzionale.

## 3. Principi di progetto

- **Separazione logica, compose unico:** un solo progetto Compose evita fragilità cross-project; reti e profili mantengono i domini separati.
- **Semantiche prima delle API:** l'equivalenza è valutata su ordering, ack, retry, replay, retention e failure, non sul nome delle primitive.
- **Avro end-to-end:** lo stesso record logico attraversa entrambi i trasporti, con strategia esplicita per registry e schema ID.
- **Cutover reversibile:** ogni fase ha precondizioni, osservazioni, criterio di promozione e rollback.
- **Risorse finite:** ogni servizio ha limit e reservation; i profili impediscono di avviare componenti inutili.
- **Riproducibilità:** immagini versionate, healthcheck, fixture deterministiche e comandi Docker Compose.
- **Git come stato:** decisioni, avanzamento, evidenze e handoff vivono in commit, issue e PR.

## 4. Architettura target

### Domini applicativi simulati

1. `order-simulator`: crea ordini e produce il flusso iniziale;
2. `payment-simulator`: reagisce agli ordini, produce esiti di pagamento e ospita una topologia Kafka Streams dimostrativa;
3. `fulfillment-simulator`: reagisce ai pagamenti e produce stati di evasione.

Ogni servizio espone health/metrics, usa un event envelope comune e dispone di adapter `kafka`, `nats` e `dual`. La modalità viene scelta via configurazione, non tramite rebuild.

### Data plane Kafka

- Kafka OSS, un broker, KRaft combinato;
- topic con partizioni e retention esplicite;
- Confluent Schema Registry;
- Kafka Streams nel simulatore 2 o in modulo dedicato se il ciclo di vita richiede isolamento;
- ksqlDB con query materializzate dimostrative;
- cluster `flink-kafka` che legge/scrive Kafka;
- UI amministrative solo in profili opzionali e se il budget lo consente.

### Data plane NATS

- NATS JetStream, singolo nodo nella baseline e tre nodi nel profilo `ha`;
- stream e subject coerenti con i domini, non una copia meccanica dei topic;
- durable pull consumers con ack esplicito;
- Apicurio Registry con PostgreSQL;
- cluster `flink-nats`, isolato dal cluster Kafka;
- adapter/connettore NATS per Flink validato con una spike prima di consolidarlo;
- DLQ/advisory stream e parity output dedicati.

### Control e comparison plane

- `migration-controller` o configurazione equivalente definisce le fasi;
- `parity-verifier` correla event ID e confronta outcome, cardinalità, latenze e duplicati;
- OpenTelemetry propaga trace/correlation ID nei record e negli header;
- Prometheus/Grafana mostrano lag/pending, throughput, failure e risorse;
- Alloy raccoglie log Docker verso Loki.

Per dettagli di reti e flussi: [ARCHITECTURE.md](ARCHITECTURE.md).

## 5. Contratti evento e Avro

### Envelope canonico

Ogni evento deve includere almeno:

- `eventId` UUID stabile tra trasporti;
- `eventType` e `eventVersion`;
- `aggregateId` e chiave di partizionamento;
- `occurredAt` UTC;
- `producer`, `correlationId`, `causationId`;
- payload tipizzato Avro;
- metadati di schema/registry disponibili anche negli header NATS.

I file `.avsc` sono la fonte del contratto. La generazione Java avviene in build containerizzata. Compatibility baseline: `BACKWARD_TRANSITIVE`, con test positivi e negativi.

### Kafka e Confluent Schema Registry

- serializer/deserializer Confluent;
- wire format Confluent con magic byte e schema ID;
- subject naming strategy documentata e stabile;
- test di registrazione, lookup, cache, incompatibilità e registry indisponibile.

### NATS e registry

Il modulo condiviso `schema-codec-avro` deve supportare:

1. **NATS + Confluent Registry:** payload nello stesso framing Confluent usato da Kafka, per il primo passaggio con un solo registry;
2. **NATS + Apicurio ccompat:** serializzazione/deserializzazione tramite API compatibile Confluent dove appropriato;
3. **NATS + schema header:** variante esplicita con schema/global ID negli header, mantenendo payload Avro binario.

L'adozione primaria sarà determinata dalla spike e dai test. Non assumere che gli ID numerici coincidano tra registry: mantenere una mappa `subject/version/fingerprint -> source ID -> target ID`, oppure decodificare e re-serializzare nel bridge. Il fingerprint dello schema e la versione logica sono l'identità portabile.

### Sequenza di migrazione registry

1. Kafka + Confluent come baseline;
2. NATS + Confluent durante il dual run, riducendo variabili simultanee;
3. export/import degli schema e verifica delle compatibilità in Apicurio;
4. NATS + Apicurio, con mapping o re-encoding controllato;
5. test di rollback verso Confluent senza perdita di leggibilità dei messaggi conservati.

## 6. Funzionalità JetStream da dimostrare

- stream con storage file e limiti espliciti;
- retention `LimitsPolicy` e, in scenario separato, `WorkQueuePolicy`;
- durable pull consumer, ack esplicito, `AckWait`, `MaxDeliver` e backoff;
- redelivery dopo crash prima/dopo ack;
- deduplicazione del publish tramite `Nats-Msg-Id`;
- DLQ tramite advisory/gestione applicativa verificabile;
- replay da sequence e da timestamp;
- restart del server con persistenza dei messaggi e consumer state;
- pending/ack pending come misura confrontabile con il lag Kafka;
- wildcard subjects e parallelismo consumer controllato;
- profilo HA a tre nodi con leader loss e recovery;
- mirror/source soltanto dopo la baseline, se aggiunge valore dimostrativo misurabile.

## 7. Flink: due cluster

La demo mantiene due installazioni indipendenti:

- `flink-kafka`: connettore Kafka supportato, job e checkpoint dedicati;
- `flink-nats`: integrazione NATS isolata, job e checkpoint dedicati.

Questo evita classpath e failure domain condivisi e rende il confronto credibile. I job applicano la stessa funzione logica e scrivono output confrontabili. Il parity verifier confronta risultati, latenze e duplicati usando `eventId`.

Per NATS, la spike è un gate anticipato: viene eseguita dopo fondazioni Compose/toolchain e contratti Avro, ma prima dei simulatori. Il numero `T10` resta associato all'issue esistente e non ne indica l'ordine cronologico.

La spike deve scegliere uno solo dei seguenti percorsi:

- **A — Table/SQL minimo:** DDL JetStream realmente eseguibile tramite un adapter sottile, confinato e con budget di manutenzione esplicito;
- **B — sola DataStream API:** supporto limitato ai job JVM la cui logica resta separata da source/sink;
- **C — nessun Flink/NATS nel displacement iniziale:** il laboratorio può continuare su transport, schema e migrazione, ma non può fare claim di portabilità Flink verso NATS.

I criteri go/no-go sono:

- API/connettore compatibile con la versione Flink selezionata;
- se viene scelto A, Table Factory, DDL, discovery e packaging SQL realmente provati;
- ack legato correttamente a checkpoint e recovery;
- bounded/unbounded source semantics documentate;
- parallelismo, backpressure e serializzazione Avro provati;
- licenza e manutenzione accettabili.

Se nessun connettore maturo soddisfa i criteri, l'eventuale adapter resta minimo e confinato sulle API supportate. Un componente general-purpose richiede una nuova decisione e non è implicito nel piano. Non fingere equivalenza exactly-once se non provata.

Il gate commerciale della prima settimana corrisponde a T10. Se toolchain, Compose e contratti non sono già riutilizzabili, T01-T03 sono prerequisiti tecnici precedenti all'avvio di quella settimana; non vengono nascosti dentro il time-box commerciale.

## 8. Migrazione dimostrata

| Fase | Producer | Consumer primario | Shadow | Registry | Gate di promozione | Rollback |
|---|---|---|---|---|---|---|
| M0 baseline | Kafka | Kafka | nessuno | Confluent | test Kafka verdi | n/a |
| M1 dual publish | Kafka+NATS | Kafka | NATS | Confluent | parità e zero perdita | disabilita NATS |
| M2 shadow consume | Kafka+NATS | Kafka | consumer NATS | Confluent | latenze/duplicati entro soglia | ferma shadow |
| M3 registry target | Kafka+NATS | Kafka | NATS | Confluent+Apicurio | schema mapping verificato | NATS torna a Confluent |
| M4 canary | Kafka+NATS | NATS per un servizio | Kafka | Apicurio su NATS | SLO e failure test | riattiva consumer Kafka |
| M5 cutover progressivo | NATS, Kafka safety feed | NATS | Kafka audit | Apicurio | tutti i domini validati | per-servizio |
| M6 target | NATS | NATS | Kafka spento dopo soak | Apicurio | soak e replay completi | restore da checkpoint/feed |

La transizione viene ripetuta per simulatori 1, 2 e 3, non eseguita come big bang. Il demo runner deve poter fermarsi a ogni fase e produrre un report machine-readable.

## 9. Docker Compose e portabilità

Un solo `compose.yaml` principale, eventualmente esteso da file override documentati, usa:

- reti `kafka-net`, `nats-net`, `migration-net`, `observability-net`;
- volumi nominati, mai path host dipendenti per i dati;
- path relativi al repository con slash portabili;
- healthcheck e `depends_on` con condizioni reali, senza sleep arbitrari;
- variabili in `.env.example` con default non sensibili;
- `platform` solo se una dipendenza non è multi-arch e la decisione è documentata;
- immagini compatibili `linux/amd64` e `linux/arm64` oppure build multi-arch;
- line ending LF e script equivalenti `scripts/*.ps1` e `scripts/*.sh` solo quando Compose non basta.

### Profili previsti

- `bootstrap`: registry/contratti e servizi minimi di verifica;
- `kafka`: data plane Kafka e simulatori in modalità Kafka;
- `nats`: data plane NATS e simulatori in modalità NATS;
- `migration`: entrambi i data plane, bridge/parity e simulatori dual;
- `full`: migrazione completa inclusa osservabilità essenziale;
- `ha`: cluster NATS a tre nodi e failure test;
- `observability`: Prometheus, Grafana, Loki, Alloy, OTel Collector;
- `tracing`: Tempo e campionamento trace;
- `test`: runner per integration/functional tests;
- `load`: generatori e profili di carico controllato;
- `security`: scan containerizzati riproducibili.

Compose non consente di attivare un profilo implicito chiamato `full` come macro: script portabili o un file override tradurranno il comando in una lista stabile di profili. Questo dettaglio deve essere testato su tutti i sistemi operativi.

## 10. Osservabilità

Baseline raccomandata, sostenibile sull'host misurato:

- Prometheus per metriche;
- Grafana per dashboard e provisioning Git-based;
- Loki per log;
- Grafana Alloy per discovery e forwarding, evitando una nuova dipendenza da Promtail;
- OpenTelemetry Collector per ricezione, processing ed export;
- JMX exporter per Kafka/ksqlDB/Schema Registry/Flink dove necessario;
- NATS Prometheus exporter/endpoint monitor;
- cAdvisor opzionale solo se compatibile con Docker Desktop; in alternativa metriche Docker/servizio.

Tempo è nel profilo `tracing`, non obbligatorio nel normale sviluppo. Correlation ID, event ID e transport devono essere campi strutturati nei log. Le dashboard minime mostrano throughput, error rate, latency, consumer lag/pending, redelivery, DLQ, checkpoint Flink, registry errors e uso risorse.

## 11. Sizing e guardrail

Misura host del 31 agosto 2026:

- AMD Ryzen 7 9800X3D, 8 core/16 thread;
- circa 61,6 GiB RAM host;
- Docker Desktop espone 16 CPU e circa 30,2 GiB RAM;
- oltre 570 GiB liberi sul disco di lavoro al momento della misura.

Obiettivo del profilo completo: 18-20 GiB stabili, massimo 22-24 GiB durante test; non oltre 10 CPU in uso ordinario e 12 CPU nei test. In questo modo restano circa 6-10 GiB nella VM Docker e oltre 30 GiB all'host per editor, browser e attività parallele.

I limiti dettagliati e i criteri di autotuning sono in [RESOURCE-BUDGET.md](RESOURCE-BUDGET.md). Ogni PR che aggiunge un servizio aggiorna il budget e fornisce un'osservazione reale con `docker stats`.

## 12. Strategia di test

### Livelli

- unit test per dominio, codec Avro, mapping e adapter;
- component test con Testcontainers dove utile, eseguito dentro container con accesso controllato al daemon;
- integration test Compose per ciascun data plane;
- contract test per schema compatibility e serializer cross-registry;
- functional test delle fasi M0-M6;
- chaos/failure test deterministici;
- performance smoke e soak breve con soglie relative, non marketing benchmark;
- cross-platform smoke su runner Linux, Windows e macOS per lint/config/client commands; lo stack Linux-container completo può essere validato su Linux CI e Docker Desktop locale.

### Matrice minima

- publish/consume valido e schema incompatibile;
- duplicate publish e duplicate delivery;
- producer/consumer/server/registry restart;
- network interruption e recovery;
- replay da offset/sequence/time;
- Kafka lag vs NATS pending;
- Flink checkpoint/restart per entrambi i cluster;
- parity su conteggi, stati finali e checksum normalizzati;
- cutover e rollback di ciascun simulatore;
- volume baseline, burst e soak;
- NATS single-node e profilo HA.

Tutti i test devono produrre report JUnit e, per le demo, JSON/Markdown archiviabili in CI.

## 13. CI/CD e artefatti

### Pull request

1. lint/format e validazione Compose;
2. build Maven e unit test;
3. contract test Avro;
4. integration test selettivi;
5. Gitleaks;
6. Trivy filesystem e misconfiguration;
7. CodeQL e Dependency Review;
8. SpotBugs/security plugin;
9. controllo licenze e SBOM.

### Main e release

- suite completa e profilo migration smoke;
- build JAR riproducibili;
- immagini OCI versionate in GHCR;
- tag SemVer, iniziando da `v0.1.0`;
- CycloneDX SBOM e provenance/attestation;
- firma immagini quando l'identità OIDC del repository è configurata;
- release notes generate dai PR, senza pubblicare se i gate falliscono.

Le GitHub Actions devono avere `permissions` minimi, concurrency cancellation e timeout. Le azioni sono pinnate a SHA. Dependabot aggiorna Maven, Docker e Actions con PR separate.

## 14. Sicurezza

- threat model leggero per porte esposte, Docker socket, registry, supply chain e deserializzazione;
- nessun `latest`, container non-root ove possibile, filesystem read-only e capability drop;
- reti isolate e porte host esposte solo quando servono all'operatore;
- segreti demo generati e montati, mai nel repository;
- input limit, validation e gestione sicura degli errori nei simulatori;
- scanning di Dockerfile, Compose, dipendenze, immagini e segreti;
- policy di blocco High/Critical definita in `SECURITY.md`;
- branch/ruleset protetto e workflow non modificabili senza review appropriata.

## 15. Sequenza di delivery

Il dettaglio operativo è in [TASKS.md](../TASKS.md). Le macro-fasi sono:

1. bootstrap/governance;
2. scaffold/toolchain containerizzata, Compose, reti e resource guardrails;
3. contratti Avro;
4. gate anticipato e cluster Flink NATS, con decisione A/B/C;
5. simulatori e adapter;
6. stack Kafka, Streams e ksqlDB;
7. JetStream e test delle sue primitive;
8. cluster Flink Kafka;
9. Apicurio e codec Avro per NATS;
10. migrazione registry e schema ID mapping;
11. bridge, shadow, cutover e rollback;
12. osservabilità;
13. failure, replay, evolution e performance;
14. hardening, SBOM e attestazioni;
15. documentazione finale e release `v0.1.0`.

Ogni fase è una o più PR, mai un unico mega-branch.

## 16. Gate e criteri di accettazione misurabili

### Gate G0 — fondazioni riproducibili

Prima della spike Flink/NATS devono essere fissati e verificati:

- matrice di versioni e licenze per Java, Spring Boot, Maven, Avro, Kafka, NATS, Flink e registry;
- immagini container disponibili sulle architetture dichiarate;
- build, test e `docker compose config` senza Java o Maven installati sull'host;
- budget Docker rilevato e soglie di arresto prima di avviare il profilo completo.

### Gate G1 — portabilità Flink/NATS

T10 produce un report con percorso A, B o C, commit, versioni, profilo Compose, fixture, risultati e limiti. Un esito A richiede DDL SQL reale; la sola presenza di source/sink DataStream non è prova di compatibilità SQL.

### Gate G2 — contratto evento e registry

Prima del dual run devono passare round-trip Kafka/NATS, compatibilità positiva e negativa, lettura di fixture storiche, cache/outage e mapping o re-encoding Confluent/Apicurio. Gli ID numerici dei registry non sono mai usati come identità portabile.

### Gate G3 — parità e cutover

Per ogni run il report deve distinguere almeno:

- eventi pubblicati con ack dal producer;
- consegne grezze, incluse redelivery;
- eventi logici unici dopo idempotenza;
- outcome terminali e messaggi in DLQ;
- record fuori ordine rispetto alla sequenza dell'aggregato;
- latenza end-to-end p50, p95 e p99;
- checkpoint/restart e durata del rollback.

I criteri minimi sono:

- ogni evento accettato deve avere un outcome terminale atteso oppure una DLQ spiegabile; nessuna perdita silenziosa;
- fixture deterministiche con parità del 100% su stato finale e checksum normalizzato;
- duplicati di consegna sempre visibili nel report e un solo effetto logico quando il flusso dichiara idempotenza;
- ordinamento per aggregato verificato soltanto per i flussi che lo richiedono e lo dichiarano;
- messaggi conservati leggibili prima e dopo cambio registry e rollback;
- latenza, throughput e RTO confrontati con soglie fissate prima del run sulla baseline M0, non scelte dopo aver visto il risultato.

RPO, RTO e soglie prestazionali non vengono inventati come valori universali del laboratorio: il profilo baseline propone valori iniziali, ma ogni scenario registra il contratto usato per promuovere o bloccare la fase.

### Formato dell'evidenza

Ogni gate genera JUnit per CI e JSON per analisi. Il report contiene almeno commit, timestamp UTC, host/architettura, versioni immagini, profili Compose, seed delle fixture, configurazione non sensibile, conteggi, percentili, risultato e motivazione di eventuali esclusioni.

## 17. Registro dei rischi e decisioni aperte

| Rischio/ipotesi | Decisione richiesta | Task che produce evidenza | Effetto se fallisce |
|---|---|---|---|
| Il connector NATS non supporta Flink SQL/Table API | scegliere A, B o C | T10 | nessun claim SQL; possibile esclusione del processing |
| Ack JetStream non è coordinabile con checkpoint | dichiarare garanzia effettiva e strategia di deduplica | T10,T13 | percorso solo at-least-once o blocco |
| Subject/split non preservano ordering e parallelismo Kafka | classificare i flussi che richiedono per-key ordering | T07,T10,T13 | flusso giallo/rosso, redesign esplicito |
| Apicurio ccompat differisce dalle API/feature usate | scegliere ccompat, API native o re-encoding | T08,T09 | registry resta Confluent nel primo cutover |
| Bridge introduce loop o duplicati | fissare hop metadata, idempotenza e ownership | T11 | nessun cutover finché il test fallisce |
| Profilo completo supera 24 GiB Docker | ridurre componenti opzionali, heap o concorrenza senza fondere i Flink | T02,T12,T13 | profilo full non promuovibile |
| Immagini o script non sono portabili | sostituire dipendenza o documentare piattaforma esclusa | T02,T14,T15 | release multipiattaforma bloccata |
| Docker socket nei test espande il rischio | preferire integration test Compose; confinare eventuale socket | T14 | test ridisegnato o profilo isolato |

Una decisione che cambia scope, semantica, sicurezza o budget richiede ADR prima dell'implementazione dipendente.

## 18. Tracciabilità requisito → evidenza

| Capacità | Task principali | Evidenza obbligatoria |
|---|---|---|
| Build portabile e Compose | T01,T02 | build/test containerizzati, config e smoke cross-platform |
| Contratti Avro | T03,T08,T09 | compatibility suite, fixture storiche e report cross-registry |
| Portabilità Flink/NATS | T10 | report A/B/C, checkpoint/recovery e matrice versioni |
| Baseline Kafka | T04,T05,T06 | report M0, restart e output Flink Kafka |
| Semantiche JetStream | T07 | ack/redelivery/dedup/replay/DLQ/restart/HA machine-readable |
| Migrazione reversibile | T11 | report M1-M6 per servizio e prova di rollback |
| Osservabilità | T12 | dashboard provisionate e ricerca end-to-end per correlation ID |
| Resilienza e capacità | T13 | failure matrix, burst, soak e sizing reale |
| Supply chain e release | T14,T15 | scan, SBOM, attestazioni, artefatti e runbook verificato |

## 19. Governance delle decisioni

- gli ADR accettati descrivono decisioni durevoli;
- le issue descrivono il lavoro;
- le PR contengono implementazione ed evidenze;
- `TASKS.md` è l'indice ordinato e viene aggiornato dal branch che completa il task;
- una modifica architetturale non concordata blocca il merge;
- stato watcher/terminale non è prova di completamento.

## 20. Definition of Done della release

- tutte le fasi M0-M6 riproducibili da una guida unica;
- tre simulatori migrati individualmente e rollback verificato;
- Avro validato con Confluent e Apicurio;
- due cluster Flink operativi con confronto degli output, oppure scope formalmente modificato da un ADR dopo esito C di T10; in quel caso la release non dichiara portabilità Flink/NATS;
- funzionalità JetStream elencate al paragrafo 6 coperte da test;
- dashboard e log disponibili nel profilo completo;
- profilo completo rispetta il budget sull'host di riferimento;
- smoke cross-platform documentati;
- CI e security gate verdi;
- SBOM, JAR, immagini e release `v0.1.0` pubblicati;
- nessun task critico aperto o rischio non esplicitamente accettato.
