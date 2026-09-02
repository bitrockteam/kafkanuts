# Backlog esecutivo

## Uso

Gli stati ammessi sono `READY`, `IN_PROGRESS`, `PR_OPEN`, `BLOCKED`, `DONE`, `NOT_EXERCISED`, `SUPERSEDED` e `CANCELLED`. Ogni task attivo ha una issue GitHub omonima. Un solo task può essere `IN_PROGRESS`.

Il completamento reale richiede il merge in `main`; una PR aperta non equivale a `DONE`.

I gate sono limitati dal perimetro di `docs/QA-SCOPE.md` (ADR 0005). La direzione del progetto è fissata da `docs/adr/0006-solo-nats-jetstream.md` (ADR 0006). Ogni requisito fuori perimetro è `NOT_TESTED` o `NOT_EXERCISED` con motivazione nel report, mai un `PASS`.

## Cosa dimostra la release v0.1.0

Le capacità del **solo stack NATS JetStream**, con Apicurio come registry e tre simulatori sul ciclo di vita ordine, pagamento e fulfillment.

Nessuna migrazione viene eseguita. La corrispondenza con Confluent Platform e Kafka è mostrata da una dashboard come **mappatura dichiarata e citata, mai misurata**: senza Kafka in piedi non esiste confronto sperimentale.

## Bootstrap e fondazioni

- [x] **T00 — Piano, governance e handoff** (`DONE`)
- [x] **T01 — Scaffold multi-module Java e toolchain containerizzata** (`DONE`)
- [x] **T02 — Compose, reti, profili e resource guardrails** (`DONE`)
- [x] **T03 — Contratti Avro canonici** (`DONE`)
  - Resta il contratto evento del progetto: envelope, fingerprint e compatibility policy.
- [x] **T04 — Tre simulatori e adapter transport** (`DONE`)
  - I tre simulatori restano il cuore applicativo, ora sul solo trasporto NATS.

## Rimossi dal prodotto

- [x] **T05 — Kafka OSS, Confluent Registry, Kafka Streams e ksqlDB** (`DONE`, artefatti rimossi da T17)
  - Completato e unito, poi rimosso fisicamente per ADR 0006. Recuperabile dal tag `archive/kafka-flink-baseline-v0`.
- [ ] **T06 — Cluster Flink Kafka** (`CANCELLED`)
  - Implementato sul branch `feat/T06-flink-kafka` con gate `PASS`, mai unito. Flink esce dal perimetro per ADR 0006.
- [x] **T10 — Gate anticipato e cluster Flink NATS** (`DONE`, esito C, artefatti rimossi da T17)
  - L'esito C resta la decisione che esclude il processing Flink/NATS. Il codice della spike è rimosso.

## Superati da ADR 0006

- [ ] **T07 — JetStream baseline e feature suite** (`SUPERSEDED` da T18)
- [ ] **T08 — Apicurio e Avro su NATS** (`SUPERSEDED` da T17 e T18)
- [ ] **T09 — Migrazione registry e mapping schema ID** (`NOT_EXERCISED`)
  - Richiedeva due registry in piedi. Senza Confluent Schema Registry non è dimostrabile e non viene simulato.
- [ ] **T11 — Dual run, shadow, cutover e rollback** (`NOT_EXERCISED`)
  - Richiedeva due data plane. Nessuna affermazione su parità, cutover o rollback è prodotta da questa release.
- [ ] **T12 — Osservabilità Grafana/Loki/Prometheus/OTel** (`NOT_EXERCISED`, rimandato a v0.2.0)
  - Sostituita in v0.1.0 dalla dashboard di T18, alimentata dal monitoring NATS.
- [ ] **T13 — Failure, replay, schema evolution e performance** (`SUPERSEDED` da T18)
  - I modi di fallimento restano dimostrati; le misure di prestazione e capacità restano `NOT_TESTED`.

## Wave corrente

- [x] **T17 — Stack solo NATS JetStream e CI minima** (`DONE`, PR #28, gate `docs/gates/t17-gate.json`)
  - Rimuovere fisicamente Kafka, Confluent Schema Registry, ksqlDB, init topic e i moduli `kafka-baseline` e `flink-nats-spike`.
  - Compose con NATS JetStream persistente, Apicurio Registry e PostgreSQL, i tre simulatori sul trasporto `nats`.
  - Immagini pinnate a versione esplicita, limiti CPU e memoria, healthcheck per ogni servizio.
  - CI minima su ogni PR verso `main`: validazione Compose, build e test in container, ricerca segreti.
  - Gate: `docker compose config` valido, build e test verdi in container, stack che raggiunge lo stato healthy.

- [x] **T18 — Feature suite JetStream e dashboard di corrispondenza** (`DONE`, PR #29, gate `docs/gates/t18-gate.json`)
  - Esercitare sul ciclo di vita ordine, pagamento e fulfillment: stream e consumer idempotenti, ack, redelivery oltre `MaxDeliver` con backoff, DLQ, deduplica via `Nats-Msg-Id`, replay per sequence e per tempo, pending, restart con persistenza.
  - Avro canonico su NATS con Apicurio via `ccompat`, compatibilità positiva e negativa.
  - Test funzionali eseguiti in container contro lo stack reale, non simulato.
  - Dashboard: feature JetStream realmente in uso, lette dagli endpoint di monitoring NATS, con per ciascuna il costrutto Confluent Platform o Kafka corrispondente, la fonte documentale e l'etichetta esplicita di corrispondenza dichiarata, non misurata.
  - Le righe senza equivalente diretto vanno dichiarate come tali, in modo conservativo.
  - Gate: `docs/gates/t18-gate.json` machine-readable più la dashboard raggiungibile con dati reali.

- [ ] **T15 — Documentazione finale e release v0.1.0** (`READY` al merge di T18)
  - Runbook del solo percorso dimostrativo, troubleshooting essenziale e cleanup.
  - Sezione *Limitations* obbligatoria in `README.md` e nelle release notes, con l'elenco completo delle voci `NOT_TESTED` e `NOT_EXERCISED`.
  - Gate: clone pulito e riproduzione completa da un secondo ambiente.

## Ordine

Storico: `T01 || T02` → `T03` → `T10` → `T04` → `T05`.

Residuo per `v0.1.0`: `T15`.

I task in `SUPERSEDED`, `NOT_EXERCISED` e `CANCELLED` restano nel backlog come perimetro riattivabile, non come lavoro pianificato.
