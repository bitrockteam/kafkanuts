# kafkanuts v0.2.0

Tre cose rispetto a `v0.1.0`: i dati persistono **su un percorso del filesystem dell'host**, la dashboard mostra il **flusso degli eventi dal vivo**, e la concatenazione causale degli eventi è **verificata da un test**, non affermata in prosa.

Il percorso resta locale: `docker compose up -d --wait`, e <http://localhost:8090>.

## Perché `v0.2.0` e non `v0.1.1`

Il cambio di percorso dei dati è un cambiamento operativo. Chi ha già i volumi Docker con nome `kafkanuts-nats-data` e `kafkanuts-registry-db` **non se li vede migrare da solo**: alla prima partenza con questa versione lo stack riparte da uno store vuoto sotto `./data`, e i vecchi volumi restano dov'erano finché non li rimuove a mano.

## Persistenza fuori dal container

I due servizi con stato scrivono su bind mount:

| Servizio | Percorso host | Contenuto |
|---|---|---|
| `nats` | `${KAFKANUTS_DATA_DIR:-./data}/nats` | store JetStream, file storage |
| `registry-db` | `${KAFKANUTS_DATA_DIR:-./data}/registry-db` | database di Apicurio |

I dati restano visibili, salvabili e spostabili dal filesystem dell'host, e `KAFKANUTS_DATA_DIR` permette di puntarli a un disco dedicato senza toccare il Compose. PostgreSQL usa `PGDATA` in una sottodirectory del mount, così crea e possiede la propria directory invece di pretendere owner e permessi sulla radice montata.

La cache Maven resta un volume gestito da Docker di proposito: è una cache di build, non un dato, e tenerla fuori da `./data` evita che un `rm -rf data/` porti via anche quella.

**Conseguenza da conoscere:** `docker compose down -v` non cancella più i dati. È una proprietà voluta — ed è esattamente quello che la verifica di questa release dimostra — ma cambia la procedura di pulizia. Per ripartire davvero da zero occorre rimuovere `./data` a mano. Il runbook lo dice.

La prova, dal gate:

```
prima   KAFKANUTS_EVENTS  first_seq 1  last_seq 8105     subject kafkanuts.events-value
docker compose down -v
        data/nats 3,5 MiB e data/registry-db 48 MiB ancora sull'host
dopo    KAFKANUTS_EVENTS  first_seq 1  last_seq 8123     compatibilityLevel BACKWARD
```

La sequence prosegue invece di ripartire. Con i volumi con nome questa verifica falliva per costruzione.

## Il flusso live

Un container nuovo, `console`: Spring Boot che riusa `transport-core`, si lega a **consumer effimeri** con ack policy `none` su `kafkanuts.events.>` e `kafkanuts.dlq.>`, decodifica Avro via `ccompat` e ripubblica il flusso in Server-Sent Events. La dashboard nginx fa da proxy su `/api/`, così flusso applicativo e output di JetStream restano **nella stessa pagina**, a un solo indirizzo.

È un osservatore passivo: non si lega mai ai durable `payment-worker` e `fulfillment-worker`, quindi non sottrae messaggi ai simulatori e non lascia consumer sul server quando il container si ferma.

In pagina, sopra a quello che c'era già, **una riga per ordine, che cambia stato sul posto**:

```
Ora        Ordine     Percorso  Stato          Seq         Catena causale   Ultimo evento
14:22:06   a1605225   ● ● ● ⬢   dead letter    3771–3775   ✓ b2a9cce1       in dead letter dopo PaymentAuthorized
14:22:04   64464e1b   ● ● ●     completato     3772–3774   ✓ 79d4d327       FulfillmentCompleted da fulfillment-simulator
14:22:02   c05d681d   ● ○ ○     in pagamento   3773        —                OrderCreated da order-simulator
```

Il numero è il **sequence dello stream**, non un contatore della pagina: è ciò che rende visibile che sotto c'è un log e non una coda.

Se la connessione cade la barra in alto diventa rossa; se regge ma non arrivano eventi per venti secondi, la pagina dichiara da quanto è ferma. Una console stantia che sembra viva è il difetto più grave di questo genere di strumenti.

## Gli eventi sono concatenati, e c'è un test che lo dice

`JetStreamFeatureTest.lifecycleEventsAreCausallyChained` drena lo stream, ricostruisce un ciclo di vita completo e asserisce tipi, `aggregateId` comune, `correlationId` comune, `causationId` che punta all'evento precedente, `eventId` distinti e i tre producer attesi. La suite passa da sei a sette test.

```
catena verificata, aggregato order-76f74b17-5d7a-414c-9e50-c98359adff24
  correlationId comune: 76f74b17-5d7a-414c-9e50-c98359adff24
  OrderCreated          eventId=76f74b17…  causationId=76f74b17…  producer=order-simulator
  PaymentAuthorized     eventId=693f7747…  causationId=76f74b17…  producer=payment-simulator
  FulfillmentCompleted  eventId=a50b275f…  causationId=693f7747…  producer=fulfillment-simulator
```

La stessa proprietà è verificata dal vivo in pagina, nella colonna *Catena causale*.

## Una correzione di onestà

Le pillole della tabella di corrispondenza erano verde, giallo e grigio. **Il verde su «equivalente» si leggeva come un `PASS` misurato contro Kafka**, che in questo laboratorio non esiste. Ora sono acromatiche, distinte per riempimento. Il colore resta solo nella sezione del flusso, dove i dati sono misurati sul sistema vivo.

È una modifica di tre righe di CSS e riguarda esattamente la disciplina che questo repository esiste per proteggere.

## Il brief di deployment

`docs/DEPLOYMENT.md` è un documento autosufficiente da consegnare a chi distribuirà lo stack fuori dal portatile: inventario degli otto servizi, requisito di persistenza non negoziabile, mappa per AWS, GCP e Azure, otto precondizioni di sicurezza scritte come precondizioni, backup e ripristino, limiti di scaling, e cosa non portare in un ambiente condiviso.

**Non implementa niente di ciò che prescrive.** TLS, autenticazione NATS, secret manager, autenticazione su Apicurio e sulla dashboard, rete privata: tutto da fare, tutto elencato come precondizione.

## Limitations

Questa sezione è normativa. Restano valide **tutte** le voci di `v0.1.0`, riportate per intero nella sezione *Limiti dichiarati* del README. In aggiunta, specifiche di questa release:

- **bind mount su piattaforme diverse da Docker Desktop per Windows** — `NOT_EXERCISED`.
- **backup e ripristino come procedura provata** — `NOT_EXERCISED`. È verificata la sopravvivenza dei dati alla rimozione dei volumi, non un ciclo completo di restore da archivio.
- **tutto il contenuto di `docs/DEPLOYMENT.md`** — `NOT_EXERCISED`. È documentazione, non lavoro eseguito.
- **render della dashboard su browser reali** — `NOT_EXERCISED`. Verificata la sintassi dello script e il flusso SSE, non il rendering.
- **console sotto carico e con molti client simultanei** — `NOT_EXERCISED`. Il ritmo osservato è quello dei simulatori, circa 1,5 eventi al secondo.
- **autenticazione sull'endpoint `/api/events`** — `NOT_EXERCISED`. È leggibile da chiunque raggiunga la porta `8090`.
- **sequenza dei singoli tentativi di consegna** — non ricostruibile: **JetStream non emette advisory per il `nak`**. Emette `MAX_DELIVERIES` e, con `SampleFrequency` attivo, le metriche di ack. La pagina mostra lo stato osservato dell'ordine, non la cronaca dei tentativi.
- **ricchezza del dominio** — `NOT_EXERCISED`. Il payload è `{"orderId":"<uuid>"}` identico ai tre stadi: nessuno stadio aggiunge informazione, e gli schemi `Order.avsc`, `Payment.avsc` e `Fulfillment.avsc` non sono usati da alcun codice. **La catena è provata a livello di envelope, non di contenuto di dominio.**
- **analisi statica, SBOM, provenance, firma, osservabilità** — `NOT_EXERCISED`, slittate a `v0.3.0`.

## Verifica

```
docker compose config --quiet                                    OK
docker compose --profile test run --rm --no-deps --build test    BUILD SUCCESS, 8 moduli
docker compose up -d --wait                                      8 servizi healthy
docker compose down -v poi up -d --wait                          first_seq 1, last_seq 8105 → 8123
docker compose --profile suite run --rm feature-suite            7 + 4 + 7, 0 failure, 0 skipped
gitleaks v8.21.2, 61 commit                                      no leaks found
```

Gate machine-readable in [docs/gates/t19-gate.json](gates/t19-gate.json) e [docs/gates/t20-gate.json](gates/t20-gate.json).

## Come si parte

```bash
git clone https://github.com/bitrockteam/kafkanuts.git
cd kafkanuts
docker compose up -d --wait
```

Poi <http://localhost:8090>. Il percorso completo è in [docs/RUNBOOK.md](RUNBOOK.md).
