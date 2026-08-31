# Backlog esecutivo

## Uso

Gli stati ammessi sono `READY`, `IN_PROGRESS`, `PR_OPEN`, `BLOCKED`, `DONE`. Dopo il bootstrap, ogni task deve avere una issue GitHub omonima. Un solo task può essere `IN_PROGRESS` per ogni esecutore. Luna seleziona il primo `READY` i cui prerequisiti sono `DONE`.

Il completamento reale richiede il merge in `main`; una PR aperta non equivale a `DONE`.

## Bootstrap

- [x] **T00 — Piano, governance e handoff** (`DONE`)
  - Repository, licenza, piano, ADR, contratto watcher e regole di collaborazione.
  - Evidenza: commit iniziale di bootstrap in `main`.

## Wave 1 — Fondazioni

- [ ] **T01 — Scaffold multi-module Java e toolchain containerizzata** (`READY`)
  - Creare Maven wrapper/multi-module, parent BOM, Java 21, formatter, Checkstyle, SpotBugs e unit test smoke.
  - Aggiungere container builder/test; nessun requisito Maven/Java sull'host.
  - Gate: build e test identici da Windows/Linux/macOS tramite Compose.

- [ ] **T02 — Compose, reti, profili e resource guardrails** (`READY`)
  - Creare struttura Compose, reti, volumi, healthcheck, `.env.example` e limiti CPU/RAM.
  - Implementare comandi portabili `doctor`, `config`, `up`, `down`, `status`, `reset` con PowerShell/POSIX solo se necessari.
  - Gate: `docker compose config` e smoke su runner/host; aggiornare budget con misure reali.

- [ ] **T03 — Contratti Avro canonici** (`BLOCKED` da T01)
  - Modulo `event-contracts`, envelope e schema Order/Payment/Fulfillment.
  - Generazione Java, fingerprint, compatibility policy e fixture deterministiche.
  - Gate: unit/contract test positivi e schema incompatibile rifiutato.

## Wave 2 — Applicazioni e Kafka baseline

- [ ] **T04 — Tre simulatori e adapter transport** (`BLOCKED` da T01,T03)
  - Tre immagini/container Spring Boot distinti.
  - Porte applicative `kafka`, `nats`, `dual`, idempotenza e telemetry context.
  - Gate: unit test dominio/adapter e smoke senza data plane reale.

- [ ] **T05 — Kafka OSS, Confluent Registry, Kafka Streams e ksqlDB** (`BLOCKED` da T02,T03,T04)
  - Kafka single broker KRaft, topic init idempotente, Schema Registry e Avro wire format.
  - Topologia Kafka Streams e query ksqlDB versionate.
  - Gate: end-to-end M0, restart broker e schema compatibility.

- [ ] **T06 — Cluster Flink Kafka** (`BLOCKED` da T02,T03,T05)
  - JobManager/TaskManager dedicati, connector Kafka, checkpoint e output di parità.
  - Gate: processing, checkpoint/restart, duplicate/recovery test.

## Wave 3 — NATS target

- [ ] **T07 — JetStream baseline e feature suite** (`BLOCKED` da T02,T04)
  - Stream/consumer provisioning idempotente e persistenza.
  - Coprire ack, redelivery, MaxDeliver/backoff, dedup, DLQ, replay, pending e restart.
  - Profilo HA a tre nodi come estensione separabile.
  - Gate: functional suite machine-readable.

- [ ] **T08 — Apicurio e Avro su NATS** (`BLOCKED` da T03,T07)
  - Apicurio/PostgreSQL, codec condiviso, ccompat e variante schema header.
  - Gate: round-trip con Confluent e Apicurio, incompatibilità e registry outage.

- [ ] **T09 — Migrazione registry e mapping schema ID** (`BLOCKED` da T05,T08)
  - Export/import, fingerprint mapping, decode/re-encode e rollback.
  - Gate: messaggi storici leggibili prima/dopo cambio registry, nessuna assunzione di ID uguali.

- [ ] **T10 — Spike e cluster Flink NATS** (`BLOCKED` da T06,T07,T08)
  - Valutare connector; registrare ADR go/no-go e implementare adapter confinato se necessario.
  - JobManager/TaskManager indipendenti da `flink-kafka`.
  - Gate: ack/checkpoint/recovery, backpressure, Avro e confronto funzionale.

## Wave 4 — Migrazione e osservabilità

- [ ] **T11 — Dual run, shadow, cutover e rollback** (`BLOCKED` da T06,T09,T10)
  - Migration controller, bridge dove necessario e parity verifier.
  - Automatizzare M1-M6 per ogni simulatore.
  - Gate: report per fase, soglie dichiarate e rollback senza perdita logica.

- [ ] **T12 — Osservabilità Grafana/Loki/Prometheus/OTel** (`BLOCKED` da T02,T05,T07)
  - Provisioning Git-based, Alloy, dashboard e alert di laboratorio.
  - Tempo nel profilo `tracing`.
  - Gate: correlation end-to-end e budget completo rispettato.

- [ ] **T13 — Failure, replay, schema evolution e performance** (`BLOCKED` da T11,T12)
  - Matrice failure deterministica, replay, burst e soak.
  - Gate: risultati JUnit/JSON, nessun claim non supportato dai dati.

## Wave 5 — Supply chain e release

- [ ] **T14 — CI/CD, security hardening, SBOM e attestazioni** (`BLOCKED` da T01,T02; avviabile incrementalmente)
  - Actions pin a SHA, CodeQL, Trivy, Gitleaks, Dependency Review, Dependabot, CycloneDX.
  - Immagini non-root/minimi privilegi, GHCR, provenance e firma quando configurabile.
  - Gate: nessun High/Critical non accettato, required checks pronti per ruleset.

- [ ] **T15 — Documentazione finale e release v0.1.0** (`BLOCKED` da T11-T14)
  - Runbook Windows/Linux/macOS, demo script, troubleshooting, limitations e cleanup.
  - Pubblicare JAR, immagini, SBOM e release notes.
  - Gate: clone pulito e riproduzione completa da un secondo ambiente.

## Ordine PR suggerito

`T01 || T02` → `T03` → `T04` → `T05` → `T06` → `T07` → `T08` → `T09` → `T10` → `T11` → `T12` → `T13` → `T14` → `T15`.

T01 e T02 possono procedere in parallelo solo con branch/worktree distinti. T12 e T14 possono iniziare incrementalmente, ma diventano `DONE` soltanto dopo l'integrazione finale. Il watcher, per semplicità, assegna a Luna un solo task per volta salvo decisione esplicita registrata in issue.
