# kafkanuts

`kafkanuts` è un laboratorio riproducibile per osservare e collaudare la migrazione progressiva di tre microservizi Spring Boot da Apache Kafka a NATS JetStream, mantenendo Avro, schema governance, stream processing, osservabilità e rollback verificabile.

Il repository parte deliberatamente dal piano esecutivo: l'implementazione verrà prodotta tramite pull request piccole e verificabili, con Git e GitHub come unica fonte durevole di verità.

## Obiettivo della demo

La demo deve rendere visibile l'intero percorso:

1. baseline su Kafka con Schema Registry, Kafka Streams, ksqlDB e Flink;
2. dual write e shadow read verso NATS JetStream;
3. uso di Avro su NATS, prima con Confluent Schema Registry e poi con Apicurio Registry;
4. confronto dei risultati dei due cluster Flink indipendenti;
5. cutover graduale dei tre simulatori e rollback provato;
6. test di replay, redelivery, deduplicazione, compatibilità schema, resilienza e prestazioni;
7. osservazione tramite Prometheus, Grafana, Loki, Grafana Alloy e OpenTelemetry.

## Vincoli principali

- esecuzione interamente in container tramite Docker Compose;
- supporto host Windows, Linux e macOS, senza dipendenze runtime installate localmente oltre a Git, Docker Desktop/Engine e Docker Compose;
- Kafka OSS in modalità KRaft, singolo broker per la baseline dimostrativa;
- due cluster Flink distinti: uno per Kafka e uno per NATS;
- tre container Java 21/Spring Boot indipendenti, ciascuno capace di parlare con Kafka e NATS;
- Avro e test di compatibilità/migrazione per entrambi i registry;
- risorse limitate per lasciare il computer utilizzabile durante la demo;
- sicurezza e supply chain integrate nel flusso di pull request.

## Documenti vincolanti

- [Piano esecutivo](docs/PLAN.md)
- [Architettura](docs/ARCHITECTURE.md)
- [Budget risorse](docs/RESOURCE-BUDGET.md)
- [Handoff dell'esecutore](docs/EXECUTION-HANDOFF.md)
- [Contratto del watcher](docs/WATCHER-CONTRACT.md)
- [Backlog ordinato](TASKS.md)
- [Regole per agenti e contributori](AGENTS.md)

Le decisioni architetturali sono registrate in `docs/adr/`. Se una pull request modifica una decisione, deve aggiornare il relativo ADR o aggiungerne uno nuovo.

## Modello operativo

- **Luna** implementa un solo task alla volta su un branch dedicato, esegue i controlli previsti e apre una pull request.
- **Watcher Herdr/PowerShell** controlla soltanto avanzamento e liveness; può rilanciare Luna sul prossimo task non completato, ma non modifica codice o decisioni.
- **Codex** interviene per architettura, sicurezza, review ad alto valore e merge; usa diff, commit, check e commenti GitHub invece dello stato delle finestre terminale.
- **GitHub** conserva issue, branch, pull request, check, review e decisioni. Una sessione può essere interrotta senza perdere il punto di ripresa.

Il modello previsto per l'esecuzione è `gpt-5.6-luna` con reasoning `medium`. Le istruzioni sono state progettate per ridurre context switching e consumo di token: task piccoli, criteri di accettazione espliciti e handoff scritto nel repository.

## Stato

Fase di bootstrap e pianificazione. Nessun componente applicativo è ancora dichiarato pronto. Lo stato eseguibile è determinato esclusivamente dalle caselle in [TASKS.md](TASKS.md) e dalle pull request unite in `main`.

## Licenza

Apache License 2.0. Vedi [LICENSE](LICENSE).
