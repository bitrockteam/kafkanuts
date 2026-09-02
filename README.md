# kafkanuts

Laboratorio locale e riproducibile per vedere **NATS JetStream** al lavoro su un flusso event-driven completo.

Il progetto avvia un unico stack NATS e mostra in una dashboard:

- il flusso live `ordine -> pagamento -> fulfillment`;
- stream, consumer, ack, redelivery, pending, deduplica, replay e DLQ;
- la persistenza degli eventi su disco;
- una mappatura documentale tra le funzionalità JetStream usate nella demo e i costrutti Kafka/Confluent più vicini.

> **Stato: `v0.2.1`.** Oggi il progetto non avvia Kafka, Confluent Schema Registry, ksqlDB o Flink e non esegue una migrazione. Il confronto con Kafka/Confluent mostrato nella dashboard è dichiarato e citato, non misurato.

## Avvio rapido e URL della dashboard

Servono Docker Engine 24 o successivo e Docker Compose v2. Java e Maven **non** devono essere installati sull'host: build e test girano nei container.

Le porte `4222`, `8222`, `8081` e `8090` devono essere libere.

1. Apri un terminale nella cartella del repository. Se devi ancora clonarlo:

   ```bash
   git clone https://github.com/bitrockteam/kafkanuts.git
   cd kafkanuts
   ```

2. Avvia l'intero stack e attendi che tutti i servizi siano pronti:

   ```bash
   docker compose up -d --wait
   ```

   Al primo avvio Docker scarica le immagini e compila i moduli Java, quindi il comando può richiedere alcuni minuti. Quando termina senza errori, gli otto servizi sono pronti.

3. Apri un browser e digita esattamente questo indirizzo nella barra degli URL:

   **[http://localhost:8090](http://localhost:8090)**

   La dashboard **non si apre automaticamente**. `localhost` indica il computer sul quale hai eseguito Docker Compose; `8090` è la porta pubblicata dal container `dashboard`. Se Docker gira su un altro computer, sostituisci `localhost` con il nome host o l'indirizzo IP di quella macchina, per esempio `http://192.168.1.20:8090`.

4. Se la pagina non risponde, controlla lo stato dei servizi:

   ```bash
   docker compose ps
   ```

   Devono risultare `healthy`: `nats`, `registry-db`, `registry`, `order-simulator`, `payment-simulator`, `fulfillment-simulator`, `console` e `dashboard`.

5. Quando hai finito, arresta i container:

   ```bash
   docker compose down
   ```

   Questo comando conserva i dati della demo nella cartella `./data`. Il percorso completo, con troubleshooting, test e pulizia dei dati, è nel [runbook](docs/RUNBOOK.md).

## Cosa mostra la dashboard

La pagina su [http://localhost:8090](http://localhost:8090) riunisce quattro viste:

1. **Flusso live**: ogni ordine cambia stato mentre attraversa pagamento e fulfillment. Sono visibili le sequence reali dello stream e la verifica della catena causale.
2. **Stato dello stack**: versione NATS, connessioni, stream, consumer, messaggi e spazio occupato.
3. **Topologia JetStream**: ack policy, `MaxDeliver`, redelivery, pending e DLQ letti dal monitoring NATS.
4. **Corrispondenza Kafka/Confluent**: una spiegazione documentale del costrutto più vicino per ogni feature JetStream usata.

I dati NATS e il flusso applicativo sono reali e vengono aggiornati dallo stack in esecuzione. La colonna Kafka/Confluent è invece una guida concettuale: Kafka non viene avviato e non viene eseguito alcun benchmark comparativo.

## Cosa viene eseguito oggi

```text
order-simulator ------ OrderCreated --------> NATS JetStream
NATS JetStream ------- OrderCreated --------> payment-simulator
payment-simulator ---- PaymentAuthorized ---> NATS JetStream
NATS JetStream ------- PaymentAuthorized ---> fulfillment-simulator
fulfillment-simulator - FulfillmentCompleted -> NATS JetStream

NATS JetStream -> console (consumer effimeri) -> SSE -> dashboard :8090
simulatori e console -> Apicurio Registry (ccompat) -> PostgreSQL
```

| Componente | Ruolo |
|---|---|
| NATS con JetStream | unico trasporto e store persistente degli eventi |
| `order-simulator` | genera ordini deterministici |
| `payment-simulator` | consuma gli ordini e produce gli esiti di pagamento |
| `fulfillment-simulator` | consuma i pagamenti autorizzati e completa il flusso |
| Apicurio Registry | registra gli schemi Avro tramite API `ccompat` |
| PostgreSQL | persiste i dati di Apicurio |
| `console` | osserva JetStream con consumer effimeri, decodifica Avro e pubblica il flusso SSE |
| `dashboard` | presenta flusso live, topologia JetStream e mappatura Kafka/Confluent |

La console è un osservatore passivo: non usa i durable consumer dei simulatori e non sottrae loro messaggi.

## Flusso applicativo e funzionalità dimostrate

I tre simulatori Java 21/Spring Boot producono una catena di eventi Avro:

```text
OrderCreated -> PaymentAuthorized -> FulfillmentCompleted
```

Gli eventi condividono `aggregateId` e `correlationId`; ogni `causationId` punta all'evento precedente. La suite funzionale verifica questa proprietà contro lo stack reale.

La demo esercita:

- provisioning idempotente di stream e consumer;
- ack esplicito e messaggi pending;
- redelivery oltre `MaxDeliver`, backoff e instradamento in DLQ;
- deduplica del publish tramite `Nats-Msg-Id`;
- replay per sequence e per tempo;
- persistenza dello stream al riavvio del server;
- serializzazione Avro e compatibilità positiva e negativa tramite Apicurio.

`PaymentRejected` e `FulfillmentStarted` non esistono ancora: il dominio simulato rappresenta solo il percorso minimo necessario alla demo.

## Endpoint locali

| Indirizzo | Contenuto |
|---|---|
| [http://localhost:8090](http://localhost:8090) | dashboard da aprire nel browser |
| [http://localhost:8222](http://localhost:8222) | monitoring HTTP di NATS |
| [http://localhost:8222/jsz?streams=1&consumers=1&config=1](http://localhost:8222/jsz?streams=1&consumers=1&config=1) | stato dettagliato di JetStream |
| [http://localhost:8081/apis/ccompat/v7/subjects](http://localhost:8081/apis/ccompat/v7/subjects) | subject registrati in Apicurio |

La porta `4222` è il protocollo NATS usato dalle applicazioni, non una pagina web.

## Persistenza dei dati

JetStream e PostgreSQL scrivono sul filesystem dell'host:

| Servizio | Percorso predefinito |
|---|---|
| NATS JetStream | `./data/nats` |
| PostgreSQL/Apicurio | `./data/registry-db` |

La variabile `KAFKANUTS_DATA_DIR` permette di cambiare la radice senza modificare `compose.yaml`. I dati sopravvivono a `docker compose down` e anche a `docker compose down -v`, perché sono bind mount e non volumi Docker. Per cancellarli occorre rimuovere esplicitamente la directory configurata; consulta prima il [runbook](docs/RUNBOOK.md).

## Test

Esegui build, test unitari e controlli statici nel container:

```bash
docker compose --profile test run --rm --no-deps --build test
```

Con lo stack già avviato, esegui la suite funzionale contro NATS JetStream e Apicurio reali:

```bash
docker compose --profile suite run --rm feature-suite
```

Usa sempre `--build` per il profilo `test`, così il risultato corrisponde ai sorgenti presenti nel working tree.

## Evoluzione possibile: non presente nello stack attuale

Le voci seguenti descrivono direzioni future o lavoro archiviato. **Non sono funzionalità disponibili nella release corrente.** Qualunque reintroduzione che cambi il perimetro richiederà una decisione esplicita e nuovi gate.

- **Kafka e Confluent Platform**: riattivare un secondo data plane con Kafka KRaft, Confluent Schema Registry, Kafka Streams o ksqlDB per un confronto eseguito, non soltanto documentale.
- **Migrazione Kafka -> NATS**: dual publish, shadow consumer, verifica di parità, cutover e rollback per servizio.
- **Flink**: rivalutare stream processing e connettori solo se esiste un caso d'uso concreto; il gate precedente ha escluso Flink/NATS dal perimetro corrente.
- **Osservabilità completa**: Prometheus, Grafana, Loki e OpenTelemetry oltre alla dashboard leggera attuale.
- **Produzione e sicurezza**: TLS, autenticazione, autorizzazione, secret manager, hardening e deployment su Kubernetes o servizi cloud gestiti.
- **Resilienza e capacità**: cluster NATS in alta disponibilità, failure test di rete, benchmark, burst, soak, sizing, RTO e RPO.
- **Dominio applicativo**: payload che evolvono, `PaymentRejected` distinto dal fallimento tecnico e `FulfillmentStarted`.
- **Supply chain**: analisi statica, scansione immagini, SBOM, provenance e firma degli artefatti.

La baseline storica con Kafka e Flink è recuperabile dal tag `archive/kafka-flink-baseline-v0`; non fa parte del prodotto corrente. La decisione che ha ristretto lo stack è documentata in [ADR 0006](docs/adr/0006-solo-nats-jetstream.md).

## Documentazione

- [Runbook della demo](docs/RUNBOOK.md)
- [Architettura e reti](docs/ARCHITECTURE.md)
- [Brief di deployment](docs/DEPLOYMENT.md)
- [Perimetro di verifica](docs/QA-SCOPE.md)
- [Note di rilascio v0.2.1](docs/RELEASE-NOTES-v0.2.1.md)
- [Backlog e stato dei task](TASKS.md)
- [Decisioni architetturali](docs/adr/)

## Limiti dichiarati

`kafkanuts` è un laboratorio tecnico, non un sizing di produzione. Questa sezione è normativa: elenca ciò che la release **non** dimostra. I gate in [docs/gates/](docs/gates/) ne sono la fonte machine-readable.

### Migrazione e confronto

| Voce | Stato | Motivo |
|---|---|---|
| migrazione da Kafka a NATS | `NOT_EXERCISED` | nessuna migrazione viene eseguita; Kafka è assente dallo stack per [ADR 0006](docs/adr/0006-solo-nats-jetstream.md) |
| parità, cutover e rollback fra due trasporti | `NOT_EXERCISED` | richiedevano due data plane in piedi |
| migrazione registry e rimappatura degli schema ID | `NOT_EXERCISED` | richiedeva Confluent Schema Registry accanto ad Apicurio |
| equivalenza misurata fra costrutti Confluent e JetStream | `NOT_EXERCISED` | la corrispondenza mostrata dalla dashboard è dichiarata e citata, mai misurata |

### Feature senza controparte dimostrata

| Voce | Stato | Motivo |
|---|---|---|
| compacted topic e compattazione per chiave | `NOT_EXERCISED` | fuori dal perimetro corrente |
| transazioni ed exactly-once processing | `NOT_EXERCISED` | fuori dal perimetro corrente |
| stream processing, Kafka Streams, ksqlDB, Flink | `NOT_EXERCISED` | esclusi dal gate T10 e dall'ADR 0006 |

### Prestazioni, capacità e disponibilità

| Voce | Stato | Motivo |
|---|---|---|
| percentili di latenza, throughput, burst, soak | `NOT_TESTED` | fuori perimetro per [ADR 0005](docs/adr/0005-perimetro-qa-ridotto.md) |
| dataset di capacità, RTO e RPO misurati | `NOT_TESTED` | fuori perimetro per ADR 0005 |
| profilo NATS in alta disponibilità, cluster, partizioni di rete, failover | `NOT_EXERCISED` | verificato solo il riavvio di un singolo nodo |
| costi, risparmio, sizing economico | `NOT_EXERCISED` | nessun dato di costo è prodotto o derivabile dalla release |

I contatori esposti dai simulatori e dalla dashboard sono conteggi funzionali, non benchmark.

### Sicurezza e operatività

| Voce | Stato | Motivo |
|---|---|---|
| TLS e autenticazione su NATS | `NOT_EXERCISED` | assetto di laboratorio |
| autenticazione su Apicurio e sulla dashboard | `NOT_EXERCISED` | assetto di laboratorio |
| autenticazione e controllo d'accesso su `console` e `/api/events` | `NOT_EXERCISED` | l'endpoint è leggibile da chiunque raggiunga la porta `8090` |
| analisi statica, scanner immagini, SBOM, provenance, firma | `NOT_EXERCISED` | rimandati a un'evoluzione futura |
| osservabilità con Prometheus, Grafana, Loki e OpenTelemetry | `NOT_EXERCISED` | la dashboard legge direttamente monitoring NATS e console |
| render della dashboard su una matrice di browser | `NOT_EXERCISED` | non verificato su browser e piattaforme differenti |
| backup e ripristino di `./data` come procedura provata | `NOT_EXERCISED` | è verificata la persistenza, non un ciclo completo di backup e restore |
| bind mount su filesystem diversi da Docker Desktop per Windows | `NOT_EXERCISED` | non verificato su altre piattaforme |
| deployment su cloud, Kubernetes, servizi gestiti, multi-region e disaster recovery | `NOT_EXERCISED` | [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) descrive i requisiti, ma non li implementa |

Le credenziali PostgreSQL in Compose sono valori di laboratorio con default espliciti e non sono riusabili fuori dalla demo.

## Licenza

Apache License 2.0. Vedi [LICENSE](LICENSE).
