# Perimetro di verifica della release v0.1.0

Documento normativo. Prevale su `docs/PLAN.md` sezione 12 e sui criteri prestazionali della sezione 16. Motivazione e autorità in `docs/adr/0005-perimetro-qa-ridotto.md`.

**Aggiornato dall'ADR 0006**: Kafka e Flink sono rimossi dal repository. Ogni verifica che li richiedeva in piedi — end-to-end Kafka, round-trip cross-registry, mapping schema ID, parità, cutover, rollback, checkpoint Flink — passa a `NOT_EXERCISED`. Restano nel perimetro unit e contract test Avro, la feature suite JetStream contro lo stack reale, e i modi di fallimento dichiarati.

## Regola generale

Tre stati ammessi per un requisito di verifica:

- `PASS` / `FAIL` — il requisito è stato esercitato con evidenza riproducibile;
- `NOT_TESTED` — il requisito è fuori perimetro v0.1.0; il report ne registra il motivo.

Un requisito `NOT_TESTED` non può essere presentato come soddisfatto, né usato come base per un claim di prestazione, capacità, disponibilità o costo.

## Cosa resta nel perimetro

| Verifica | Dove | Motivo |
|---|---|---|
| Unit test dominio, codec, mapping, adapter | tutti i moduli | costo marginale, protegge il refactor |
| Contract test Avro, compatibilità positiva e negativa | `event-contracts` | è il contratto dichiarato al lettore |
| End-to-end M0 Kafka | T05, `reports/t05-gate.json` | già `PASS` |
| End-to-end M0 NATS JetStream | T07 | baseline del trasporto target |
| Feature JetStream: ack, redelivery, MaxDeliver/backoff, dedup, DLQ, replay, restart | T07 | sono le garanzie che il progetto afferma di sostituire |
| Round-trip Confluent e Apicurio | T08 | contratto evento cross-registry |
| Mapping fingerprint e schema ID, decode/re-encode, rollback registry | T09 | rischio differenziante del progetto |
| Parità su conteggi, outcome terminali e checksum normalizzato | T11 | criterio di successo della migrazione |
| Cutover e rollback per simulatore | T11 | garanzia operativa dichiarata |
| Checkpoint e restart Flink, cluster `flink-kafka` | T06, `reports/t06-gate.json` | già `PASS` |
| Tre scenari di fallimento: restart broker, redelivery JetStream, checkpoint/restart Flink | T07, T11, T13 ridotto | copre i modi di fallimento dichiarati |
| Replay da offset, sequence o time | T13 ridotto | funzionalità dimostrata, non misurata |
| Gate JSON machine-readable per task | `reports/` | tracciabilità requisito to evidenza |
| Build e test containerizzati, `docker compose config` | T01, T02 | già `PASS`, portabilità |
| Ricerca di segreti nel repository | T14 ridotto | vincolo non negoziabile di `AGENTS.md` |

## Cosa esce dal perimetro

| Verifica esclusa | Stato | Motivo |
|---|---|---|
| Latenza p50, p95, p99 | `NOT_TESTED` | misura di dimensionamento, non di correttezza |
| Throughput, volume baseline, burst | `NOT_TESTED` | idem |
| Soak breve | `NOT_TESTED` | richiede tempo macchina sproporzionato al laboratorio |
| Dataset di capacità CPU, RAM, storage | `NOT_TESTED` | input per modello economico, rimandato a campagna dedicata |
| RTO e RPO confrontati con soglie | `NOT_TESTED` | soglie di produzione non applicabili a un laboratorio single-node |
| Profilo NATS HA a tre nodi | `NOT_TESTED` | estensione già dichiarata separabile in `TASKS.md` |
| Matrice failure esaustiva e chaos test deterministici | ridotta a tre scenari | copertura marginale decrescente |
| Network interruption e recovery | `NOT_TESTED` | non riproducibile in modo deterministico entro il perimetro |
| Kafka lag vs NATS pending come metrica confrontata | `NOT_TESTED` | resta visibile in dashboard, non è un gate |
| Layer component test con Testcontainers | rimosso | ridondante rispetto ai test di integrazione Compose |
| CI su runner Windows e macOS | rimosso | resta CI Linux e il percorso locale Docker Desktop documentato |
| Report JUnit per le demo | rimosso | resta il report JSON per gate |
| CodeQL, Trivy, Dependency Review | `NOT_TESTED` | rimandati a v0.2.0 |
| SBOM CycloneDX, provenance, firma immagini | `NOT_TESTED` | rimandati a v0.2.0 |
| Correlazione tracing end-to-end, Loki, Tempo, Alloy | `NOT_TESTED` | osservabilità ridotta a Prometheus e una dashboard Grafana |

## Effetto sui task aperti

- **T07** — suite funzionale mantenuta senza profilo HA.
- **T08, T09** — invariati: sono il nucleo di evidenza del progetto.
- **T11** — gate G3 ridotto ai criteri di correttezza; sezione prestazioni del report compilata con `NOT_TESTED`.
- **T12** — Prometheus, Grafana e una dashboard di parità. Fuori: Loki, Tempo, Alloy, profilo `tracing`, correlazione end-to-end.
- **T13** — ridotto a tre scenari di fallimento più un replay. Fuori: burst, soak, percentili, dataset di capacità.
- **T14** — build, test e ricerca segreti in CI, più Dependabot. Fuori: CodeQL, Trivy, SBOM, provenance, firma.
- **T15** — le release notes e `README.md` devono contenere una sezione *Limitations* che elenca ogni voce `NOT_TESTED` di questo documento.
