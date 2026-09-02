# kafkanuts v0.1.0

Prima release. Mostra che cosa sa fare uno stack **NATS JetStream** con Apicurio Registry su un ciclo di vita applicativo realistico, e affianca a ogni feature il costrutto Confluent Platform o Kafka che le corrisponderebbe.

Nessuna migrazione viene eseguita. La migrazione è soltanto **evocata** dalla dashboard di corrispondenza.

## Cosa contiene

**Stack.** Sette container: NATS con JetStream persistente, Apicurio Registry su PostgreSQL, tre simulatori Spring Boot per ordine, pagamento ed evasione, e una dashboard nginx. Budget dichiarato circa 3,25 CPU e meno di 3 GiB. Si avvia con `docker compose up -d --wait` e non richiede Java o Maven sull'host.

**Feature JetStream esercitate** contro lo stack reale, non simulato:

- provisioning idempotente di stream e consumer;
- ack esplicito, pending e consumer durabili;
- redelivery oltre `MaxDeliver` con backoff;
- dead letter costruita sull'advisory `MAX_DELIVERIES`;
- deduplica lato server tramite `Nats-Msg-Id`;
- replay per sequence e per tempo;
- persistenza dello stream al riavvio del server.

**Contratti.** Avro canonico serializzato nel wire format Confluent, magic byte 0 più id dello schema big endian a 4 byte, con lo schema del writer risolto da Apicurio via API `ccompat`. Compatibilità verificata in positivo e in negativo.

**Dashboard** su <http://localhost:8090>. Legge `/varz` e `/jsz` dal monitoring NATS attraverso un proxy di sola lettura sulla stessa origine e mostra quali feature sono realmente in uso, affiancate al costrutto Kafka o Confluent corrispondente, con la fonte documentale riga per riga.

**CI minima.** Su ogni pull request verso `main`: validazione della configurazione Compose, build e test in container, ricerca di segreti sull'intera cronologia con gitleaks. Un segreto committato blocca merge e release senza eccezioni.

## Un ritrovamento che vale la pena leggere

Interrogato via API `ccompat`, **Apicurio Registry 3.0.6 parte da `compatibilityLevel NONE`**, sia globale sia per subject, e accetta quindi anche uno schema palesemente incompatibile. Confluent Schema Registry parte invece da `BACKWARD`.

È emerso da un test di compatibilità che è fallito, e il fallimento era corretto. La correzione è in codice di produzione: `AvroEventCodec` impone esplicitamente `BACKWARD` sul subject alla costruzione.

Chi sostituisce il registry senza impostare il livello perde in silenzio una garanzia che credeva di avere. Vale la pena saperlo prima, non dopo.

## Limitations

Questa sezione è normativa. Ciò che segue **non** è dimostrato da questa release, e nessuna affermazione contraria può esserne derivata.

### Migrazione e confronto

- **migrazione da Kafka a NATS** — `NOT_EXERCISED`. Nessuna migrazione viene eseguita; Kafka, Confluent Schema Registry, ksqlDB e Flink sono stati rimossi fisicamente dal repository con l'ADR 0006.
- **parità, cutover e rollback fra due trasporti** — `NOT_EXERCISED`. Richiedevano due data plane in piedi.
- **migrazione registry e rimappatura degli schema ID** — `NOT_EXERCISED`. Richiedeva Confluent Schema Registry accanto ad Apicurio.
- **equivalenza misurata fra costrutti Confluent e JetStream** — `NOT_EXERCISED`. La corrispondenza mostrata dalla dashboard è dichiarata e citata, mai misurata. Con Kafka assente dallo stack non esiste alcun confronto sperimentale.

### Feature senza controparte dimostrata

- **compacted topic e compattazione per chiave** — `NOT_EXERCISED`.
- **transazioni ed exactly once processing** — `NOT_EXERCISED`.
- **stream processing, Kafka Streams, ksqlDB, Flink** — `NOT_EXERCISED`, escluso dall'esito C del gate T10 e dall'ADR 0006.

### Prestazioni, capacità, disponibilità, costi

- **percentili di latenza, throughput, burst, soak** — `NOT_TESTED` per ADR 0005.
- **dataset di capacità, RTO e RPO misurati** — `NOT_TESTED`.
- **alta disponibilità, cluster, partizioni di rete, failover, perdita di disco** — `NOT_EXERCISED`. È verificato soltanto il riavvio di un singolo nodo NATS.
- **costi, risparmio, sizing economico** — `NOT_EXERCISED`. Nessun dato di costo è prodotto o derivabile.

I contatori esposti dai simulatori e dalla dashboard sono conteggi funzionali, non benchmark.

### Sicurezza e operatività

- **TLS e autenticazione su NATS** — `NOT_EXERCISED`. Assetto di laboratorio.
- **autenticazione su Apicurio e sulla dashboard** — `NOT_EXERCISED`. Assetto di laboratorio.
- **analisi statica, scanner immagini, SBOM, provenance, firma** — `NOT_EXERCISED`, rimandati a `v0.2.0`. La release dichiara che l'analisi automatica non è stata eseguita, non l'assenza di vulnerabilità.
- **osservabilità con Prometheus, Grafana, Loki, OpenTelemetry** — `NOT_EXERCISED`, rimandata a `v0.2.0`.
- **render della dashboard su una matrice di browser reali** — `NOT_EXERCISED`.
- **Kubernetes, servizi cloud gestiti, multi-region, disaster recovery geografico** — fuori perimetro.

Le credenziali PostgreSQL nel Compose sono valori di laboratorio con default espliciti, dichiarati come tali e non riusabili fuori dalla demo.

## Verifica

Riproduzione completa da clone pulito, documentata in `docs/gates/t15-gate.json`. I gate delle singole fasi sono in `docs/gates/`.

```
docker compose config --quiet
docker compose --profile test run --rm --no-deps --build test
docker compose up -d --wait
docker compose --profile suite run --rm feature-suite
```

## Come si parte

```bash
git clone https://github.com/bitrockteam/kafkanuts.git
cd kafkanuts
docker compose up -d --wait
```

Poi <http://localhost:8090>. Il percorso completo, con troubleshooting e cleanup, è in [docs/RUNBOOK.md](RUNBOOK.md).
