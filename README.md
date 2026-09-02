# kafkanuts

Laboratorio riproducibile per progettare, osservare e collaudare la migrazione progressiva di un sistema event-driven da Apache Kafka a NATS JetStream.

Il progetto mette a confronto i due ecosistemi su un flusso applicativo realistico, mantenendo contratti Avro, schema governance, stream processing, osservabilità e rollback verificabile. Non cerca una sostituzione meccanica topic-per-subject: rende esplicite le differenze di semantica, operatività e failure handling.

> **Stato:** `v0.2.0`. Lo stack solo NATS JetStream è in piedi, le feature dichiarate sono esercitate da test funzionali contro il sistema reale, i dati persistono su un percorso dell'host e la dashboard mostra il flusso degli eventi dal vivo.

## Come si prova

```bash
docker compose up -d --wait
```

Poi apri <http://localhost:8090>. In cima alla pagina scorre il **flusso live**: una riga per ordine, che cambia stato sul posto mentre attraversa pagamento ed evasione, con il numero di sequence dello stream e la verifica della catena causale. Sotto, la topologia JetStream letta dal monitoring e la tabella di corrispondenza con Confluent.

Il percorso completo, con troubleshooting e cleanup, è nel [runbook](docs/RUNBOOK.md).

## Obiettivi

La demo deve permettere a un operatore di:

1. avviare tutto tramite Docker Compose da Windows, Linux o macOS, senza Java o Maven sull'host;
2. generare un flusso coerente di ordini, pagamenti ed evasione tramite tre microservizi Spring Boot indipendenti;
3. eseguire lo scenario interamente su NATS JetStream;
4. serializzare e deserializzare gli eventi in Avro, con Apicurio Registry via API `ccompat`;
5. esercitare ack, redelivery oltre `MaxDeliver`, DLQ, deduplica, replay e persistenza allo restart;
6. leggere in una dashboard quali feature JetStream sono realmente in uso e a quale costrutto Confluent Platform o Kafka corrisponderebbero;
7. produrre test e report riproducibili, senza incorporare prezzi o dati cliente nel repository.

Il risultato atteso è uno stack che si avvia e un insieme di capacità JetStream dimostrate da test funzionali, più una corrispondenza leggibile e onestamente etichettata verso il mondo Confluent.

## Architettura

Un solo data plane. Nessun secondo trasporto, nessuna migrazione eseguita.

```text
order-simulator ──┐
payment-simulator ├──> NATS + JetStream (persistente) ──> dashboard di corrispondenza
fulfillment-sim. ──┘            │
                                ├──> console (consumer effimeri) ──> SSE ──> dashboard
                                └──> Apicurio Registry (ccompat) + PostgreSQL
```

| Componente | Ruolo |
|---|---|
| NATS con JetStream | unico trasporto e store persistente degli eventi |
| Apicurio Registry | registry degli schemi Avro, esposto via API `ccompat` |
| PostgreSQL | storage di Apicurio |
| tre simulatori Spring Boot | ciclo di vita ordine, pagamento e fulfillment |
| console | osserva il flusso con consumer effimeri e lo ripubblica in SSE, senza toccare i durable |
| dashboard | flusso live, feature JetStream in uso e corrispondenza dichiarata con Confluent Platform e Kafka |

I dati persistono **su un percorso del filesystem dell'host**, non in un volume gestito da Docker: `${KAFKANUTS_DATA_DIR:-./data}/nats` per lo store JetStream e `${KAFKANUTS_DATA_DIR:-./data}/registry-db` per PostgreSQL. Restano visibili, salvabili e spostabili, e sopravvivono anche a `docker compose down -v`.

Kafka, Confluent Schema Registry, ksqlDB e i cluster Flink sono stati **rimossi fisicamente** dal repository con l'[ADR 0006](docs/adr/0006-solo-nats-jetstream.md). Lo stato precedente resta recuperabile dal tag `archive/kafka-flink-baseline-v0`.

## Flusso applicativo

La demo usa tre simulatori Java 21/Spring Boot, ciascuno in un container distinto:

| Servizio | Responsabilità | Eventi principali |
|---|---|---|
| `order-simulator` | genera ordini deterministici e burst configurabili | `OrderCreated` |
| `payment-simulator` | autorizza l'ordine; una quota deterministica fallisce sempre | `PaymentAuthorized` |
| `fulfillment-simulator` | completa l'evasione degli ordini pagati | `FulfillmentCompleted` |

Gli eventi sono **concatenati**: stesso `aggregateId`, stesso `correlationId`, e il `causationId` di ogni evento punta all'evento che lo ha causato. La proprietà è asserita da un test della suite funzionale e verificata dal vivo nella colonna *Catena causale* della dashboard.

`PaymentRejected` e `FulfillmentStarted` **non esistono**: oggi il rifiuto di business non è distinto dal fallimento tecnico, e l'evasione ha un solo evento terminale. È un limite del dominio simulato, non un difetto del trasporto.

L'envelope evento conserva:

- `eventId` stabile tra i trasporti;
- tipo e versione dell'evento;
- aggregate/partition key;
- timestamp UTC;
- producer, correlation ID e causation ID;
- payload Avro tipizzato;
- riferimento portabile allo schema.

`eventId`, correlation ID e checksum normalizzato rendono verificabili cardinalità, outcome terminali e duplicati senza trattare i timestamp di trasporto come risultato funzionale.

## Stack in piedi

| Servizio | Immagine | Note |
|---|---|---|
| `nats` | `nats:2.11.8-alpine` | JetStream con store persistente, monitoring su `8222` |
| `registry` | `apicurio/apicurio-registry:3.0.6` | storage SQL, API `ccompat` su `/apis/ccompat/v7` |
| `registry-db` | `postgres:16.4-alpine` | credenziali di laboratorio, non riusabili fuori dalla demo |
| `order-simulator`, `payment-simulator`, `fulfillment-simulator` | build locale | un ordine ogni due secondi, ack esplicito, DLQ |
| `console` | build locale | osservatore passivo di JetStream, decodifica Avro e serve il flusso in SSE |
| `dashboard` | `nginx:1.27-alpine` | pagina su `8090`, proxy di sola lettura verso il monitoring NATS e verso `console` |

Tutte le immagini sono pinnate a versione esplicita; ogni servizio dichiara healthcheck e limiti di CPU e memoria.

## Funzionalità JetStream dimostrate

Esercitate sul ciclo di vita ordine, pagamento e fulfillment:

- stream e consumer con provisioning idempotente;
- ack esplicito e pending;
- redelivery oltre `MaxDeliver` con backoff e instradamento in DLQ;
- deduplica tramite `Nats-Msg-Id`;
- replay per sequence e per tempo;
- restart del server con persistenza dello stream.

Avro resta il formato canonico degli eventi, con Apicurio come registry e compatibilità verificata in positivo e in negativo.

## Corrispondenza con Confluent e Kafka

Con lo stack avviato la dashboard è su <http://localhost:8090>. Mostra, per ogni feature JetStream realmente in uso, il costrutto Confluent Platform o Kafka corrispondente.

La colonna di sinistra è viva: stream, consumer, ack policy, `MaxDeliver`, riconsegne, pending, finestra di deduplica e conteggio della DLQ sono letti dagli endpoint `/varz` e `/jsz` del server NATS, esposti sulla stessa origine da un proxy di sola lettura senza credenziali. La colonna di destra è documentale.

> **La corrispondenza è dichiarata e citata, non misurata.** Con Kafka assente dallo stack non esiste alcun confronto sperimentale. Ogni riga porta la propria fonte documentale e l'etichetta esplicita. Le feature senza equivalente diretto sono dichiarate come tali, in modo conservativo.

Presentare questa tabella come una misura sarebbe esattamente il tipo di affermazione non supportata che questo repository vieta.

## Docker Compose e portabilità

Lo stack si avvia con `docker compose up -d` e non richiede Java o Maven sull'host. Build e test girano in container tramite il profilo `test`.

Il budget risorse dello stack completo è di circa tre CPU: NATS, PostgreSQL, Apicurio e tre simulatori.

## Requisiti della macchina target

Con un solo data plane il fabbisogno è modesto. Lo stack completo dichiara circa **3,75 CPU** e resta sotto i **3,5 GiB** di memoria assegnata:

| Servizio | CPU | Memoria |
|---|---:|---:|
| `nats` | 0,5 | 512 MiB |
| `registry-db` | 0,5 | 512 MiB |
| `registry` | 1,0 | 1 GiB |
| tre simulatori | 0,5 ciascuno | 512 MiB ciascuno |
| `console` | 0,5 | 512 MiB |
| `dashboard` | 0,25 | 128 MiB |

A questi va aggiunto il disco occupato da `./data`: lo store JetStream e il database del registry crescono sull'host, con `max_age` di ventiquattro ore sugli stream.

Una macchina con 8 CPU logiche, 16 GiB di RAM host e 40 GiB di disco libero esegue lo stack, la build in container e la suite funzionale senza stringere. I profili `test` e `suite` aggiungono un container Maven da 1 CPU e 1 GiB, non concorrente con lo stack.

Ogni PR che aggiunge un container deve dichiarare healthcheck, limiti e porte.

## Strategia di test

Il perimetro di verifica è ridotto e normato da [docs/QA-SCOPE.md](docs/QA-SCOPE.md), approvato in [ADR 0005](docs/adr/0005-perimetro-qa-ridotto.md), e la direzione è fissata dall'[ADR 0006](docs/adr/0006-solo-nats-jetstream.md).

Nel perimetro:

- unit test per dominio, codec, mapping e adapter;
- contract test Avro con compatibilità positiva e negativa;
- test funzionali delle feature JetStream eseguiti in container contro lo stack reale;
- modi di fallimento dichiarati: redelivery oltre `MaxDeliver`, DLQ, deduplica, replay, restart con persistenza.

Fuori perimetro, stato `NOT_TESTED` o `NOT_EXERCISED` con motivazione nel report: percentili di latenza, throughput, burst, soak, dataset di capacità, RTO e RPO misurati, profilo NATS HA, interruzione di rete, osservabilità completa, e ogni verifica che richiedeva Kafka o Flink in piedi.

I gate producono un report JSON archiviabile. **Nessun dato di prestazione, capacità, disponibilità, parità o costo può essere derivato da questa release.**

## CI minima

Con un solo esecutore che apre, revisiona e unisce le proprie pull request, la CI è l'unico controllo indipendente su `main`. Su ogni PR:

- validazione della configurazione Compose;
- build e test in container;
- ricerca di segreti sull'intera cronologia.

Un segreto committato blocca merge e release senza eccezioni. Analisi statica, scanner immagini, SBOM, provenance e firma restano `NOT_EXERCISED` e rimandati a `v0.2.0`: la release dichiara che l'analisi automatica non è stata eseguita, invece di dichiarare l'assenza di vulnerabilità.

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

Il backlog canonico, con stati e gate, è in [TASKS.md](TASKS.md).

Chiusi di recente:

- **T17** — rimozione fisica di Kafka e Flink, stack solo NATS JetStream con Apicurio, CI minima;
- **T18** — ciclo di vita su JetStream, suite funzionale contro lo stack reale e dashboard di corrispondenza.

Residuo per `v0.1.0`:

1. **T15** — runbook, sezione *Limitations* e release `v0.1.0`.

I task da T05 a T14 sono chiusi, superati o non esercitati per effetto dell'ADR 0006.

## Documentazione

- [Runbook della demo](docs/RUNBOOK.md)
- [Piano esecutivo completo](docs/PLAN.md)
- [Architettura e reti](docs/ARCHITECTURE.md)
- [Budget risorse](docs/RESOURCE-BUDGET.md)
- [Perimetro di verifica v0.1.0](docs/QA-SCOPE.md)
- [ADR 0006, solo NATS JetStream](docs/adr/0006-solo-nats-jetstream.md)
- [Brief di deployment](docs/DEPLOYMENT.md)
- [Handoff esecutivo](docs/EXECUTION-HANDOFF.md)
- [Contratto del watcher Herdr, storico](docs/WATCHER-CONTRACT.md)
- [Regole per agenti e contributori](AGENTS.md)
- [Linee guida di contribuzione](CONTRIBUTING.md)
- [Security policy](SECURITY.md)
- [Decisioni architetturali](docs/adr/)

## Limiti dichiarati

`kafkanuts` è un laboratorio tecnico, non un sizing di produzione. Questa sezione è normativa: elenca per intero ciò che la release **non** dimostra. I gate in [docs/gates/](docs/gates/) ne sono la fonte machine-readable.

### Migrazione e confronto

| Voce | Stato | Motivo |
|---|---|---|
| migrazione da Kafka a NATS | `NOT_EXERCISED` | nessuna migrazione viene eseguita; Kafka è assente dallo stack per [ADR 0006](docs/adr/0006-solo-nats-jetstream.md) |
| parità, cutover e rollback fra due trasporti | `NOT_EXERCISED` | richiedevano due data plane in piedi (T11) |
| migrazione registry e rimappatura degli schema ID | `NOT_EXERCISED` | richiedeva Confluent Schema Registry accanto ad Apicurio (T09) |
| equivalenza misurata fra costrutti Confluent e JetStream | `NOT_EXERCISED` | la corrispondenza mostrata dalla dashboard è **dichiarata e citata**, mai misurata |

### Feature senza controparte dimostrata

| Voce | Stato | Motivo |
|---|---|---|
| compacted topic e compattazione per chiave | `NOT_EXERCISED` | fuori perimetro v0.1.0 |
| transazioni ed exactly once processing | `NOT_EXERCISED` | fuori perimetro v0.1.0 |
| stream processing, Kafka Streams, ksqlDB, Flink | `NOT_EXERCISED` | escluso dall'esito C del gate T10 e dall'ADR 0006 |

### Prestazioni, capacità, disponibilità

| Voce | Stato | Motivo |
|---|---|---|
| percentili di latenza, throughput, burst, soak | `NOT_TESTED` | fuori perimetro per [ADR 0005](docs/adr/0005-perimetro-qa-ridotto.md) |
| dataset di capacità, RTO e RPO misurati | `NOT_TESTED` | idem |
| profilo NATS in alta disponibilità, cluster, partizioni di rete, failover | `NOT_EXERCISED` | verificato solo il riavvio di un singolo nodo |
| costi, risparmio, sizing economico | `NOT_EXERCISED` | nessun dato di costo è prodotto o derivabile da questa release |

I contatori esposti dai simulatori e dalla dashboard sono **conteggi funzionali, non benchmark**. Nessun numero di questa release può essere usato come misura di prestazione o come base di un dimensionamento.

### Sicurezza e operatività

| Voce | Stato | Motivo |
|---|---|---|
| TLS e autenticazione su NATS | `NOT_EXERCISED` | assetto di laboratorio |
| autenticazione su Apicurio e sulla dashboard | `NOT_EXERCISED` | assetto di laboratorio |
| autenticazione e controllo d'accesso su `console` e sul suo endpoint SSE | `NOT_EXERCISED` | assetto di laboratorio; `/api/events` è leggibile da chiunque raggiunga la porta `8090` |
| analisi statica, scanner immagini, SBOM, provenance, firma | `NOT_EXERCISED` | rimandati a `v0.3.0`; la release dichiara che l'analisi automatica non è stata eseguita, non l'assenza di vulnerabilità |
| osservabilità con Prometheus, Grafana, Loki, OpenTelemetry | `NOT_EXERCISED` | rimandata a `v0.3.0`; la dashboard legge direttamente il monitoring NATS e il flusso dalla console |
| render della dashboard su una matrice di browser | `NOT_EXERCISED` | verificato eseguendo lo script della pagina contro lo stack vivo, non aprendola in browser diversi |
| backup e ripristino di `./data` come procedura provata | `NOT_EXERCISED` | è verificata la sopravvivenza dei dati a `docker compose down -v`, non un ciclo completo di backup e restore |
| bind mount su filesystem diversi da Docker Desktop per Windows | `NOT_EXERCISED` | la persistenza su percorso host è provata solo su questa piattaforma |
| deployment su cloud, Kubernetes, servizi gestiti, multi-region, disaster recovery geografico | `NOT_EXERCISED` | [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md) descrive cosa servirebbe; non implementa nulla di ciò che prescrive |

Le credenziali PostgreSQL nel Compose sono valori di laboratorio con default espliciti, dichiarati come tali e non riusabili fuori dalla demo.

## Licenza

Apache License 2.0. Vedi [LICENSE](LICENSE).
