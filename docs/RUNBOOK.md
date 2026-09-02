# Runbook della demo

Percorso dimostrativo unico di `kafkanuts` v0.1.0: avviare lo stack NATS JetStream, vederlo lavorare, esercitarne le feature e spegnerlo. Circa quindici minuti la prima volta, tre le successive.

Non esiste un secondo percorso. Kafka non è presente nello stack e nessuna migrazione viene eseguita: vedi [ADR 0006](adr/0006-solo-nats-jetstream.md).

## 1. Prerequisiti

| Requisito | Valore |
|---|---|
| Docker | Engine 24 o successivo, con `docker compose` v2 |
| CPU logiche | 8 |
| RAM disponibile a Docker | 6 GiB |
| Disco libero | 40 GiB, in gran parte per la cache Maven e le immagini |
| Porte libere sull'host | `4222`, `8222`, `8081`, `8090` |

Java e Maven **non** servono sull'host: build e test girano in container.

Verifica rapida:

```bash
docker version
docker compose version
```

## 2. Avvio

```bash
git clone https://github.com/bitrockteam/kafkanuts.git
cd kafkanuts
docker compose up -d --wait
```

La prima esecuzione scarica le immagini e compila i simulatori: mettere in conto qualche minuto. `--wait` ritorna solo quando ogni healthcheck è verde.

```bash
docker compose ps
```

Attesi otto servizi `healthy`: `nats`, `registry-db`, `registry`, i tre simulatori, `console` e `dashboard`.

## 3. Il percorso dimostrativo

### 3.1 La dashboard

Apri <http://localhost:8090>.

Ha quattro parti.

- **Flusso live degli eventi**: una riga per ordine, che cambia stato sul posto man mano che l'ordine
  attraversa pagamento ed evasione. Mostra il numero di sequence dello stream, il percorso
  `● ● ●`, lo stato, e la verifica della catena causale — stesso `correlationId`, `causationId` che
  punta all'evento precedente. Gli eventi arrivano in SSE dal servizio `console`, che li legge da
  JetStream con un **consumer effimero** e li decodifica Avro. La console è un osservatore passivo:
  non si lega mai ai durable `payment-worker` e `fulfillment-worker`, quindi non sottrae messaggi
  ai simulatori. Se il flusso si interrompe la barra in alto diventa rossa: una console ferma deve
  sembrare rotta, non tranquilla.

Le altre tre parti si aggiornano ogni cinque secondi leggendo `/varz` e `/jsz` dal monitoring NATS:

- **Stack**: server, versione, connessioni, stream, consumer, messaggi salvati, storage su disco;
- **Stream e Consumer**: la topologia reale, con ack policy, `MaxDeliver`, riconsegne e pending;
- **Corrispondenza con Confluent Platform e Kafka**: per ogni feature JetStream, se è in uso qui e quale costrutto Kafka le corrisponderebbe.

> La colonna di sinistra è misurata sul sistema vivo. Quella di destra **no**: è documentale, con la fonte in fondo a ogni riga. Con Kafka assente dallo stack non esiste alcun confronto sperimentale, e la pagina lo dichiara in modo permanente. È il punto su cui non fare affermazioni oltre l'evidenza.

### 3.2 Il ciclo di vita

I tre simulatori girano in continuo. `order-simulator` crea ordini, `payment-simulator` li autorizza o li rifiuta, `fulfillment-simulator` li evade.

```bash
for s in order payment fulfillment; do
  docker compose exec -T $s-simulator /bin/busybox wget -q -O- http://127.0.0.1:8080/health
  echo
done
```

Ogni servizio riporta i propri contatori:

| Campo | Significato |
|---|---|
| `published` | eventi pubblicati su JetStream |
| `deliveries` | consegne ricevute, riconsegne incluse |
| `uniqueEvents` | eventi distinti effettivamente elaborati |
| `duplicateDeliveries` | riconsegne scartate dallo store di idempotenza |
| `duplicateAcks` | pubblicazioni riconosciute come duplicate dal server |
| `deadLettered` | messaggi finiti in DLQ dopo aver esaurito `MaxDeliver` |

`deliveries` maggiore di `uniqueEvents` non è un difetto: è la riconsegna at-least-once che funziona, con l'idempotenza applicativa che la assorbe.

### 3.3 La topologia JetStream

```bash
curl -s "http://localhost:8222/jsz?streams=1&consumers=1&config=1" | less
```

Due stream. `KAFKANUTS_EVENTS` raccoglie `kafkanuts.events.>` con i consumer durabili `payment-worker` e `fulfillment-worker`; `KAFKANUTS_DLQ` raccoglie `kafkanuts.dlq.>` e cresce solo quando un messaggio esaurisce le consegne.

Entrambi su storage `file`, con finestra di deduplica di due minuti e `max_age` di ventiquattro ore.

### 3.4 Il registry

```bash
curl -s http://localhost:8081/apis/ccompat/v7/subjects
curl -s http://localhost:8081/apis/ccompat/v7/config/kafkanuts.events-value
```

Il primo comando risponde `["kafkanuts.events-value"]`, il secondo `{"compatibilityLevel":"BACKWARD"}`.

Quel `BACKWARD` è imposto dall'applicazione, non ereditato. **Interrogato via API `ccompat`, Apicurio parte da `NONE` e accetterebbe anche uno schema incompatibile**, mentre Confluent Schema Registry parte da `BACKWARD`. È una divergenza da conoscere prima di sostituire un registry con l'altro.

### 3.5 La suite funzionale

```bash
docker compose --profile suite run --rm feature-suite
```

Sei test contro lo stack vivo: provisioning idempotente, round trip Avro via `ccompat`, compatibilità positiva e negativa, deduplica lato server, esaurimento delle consegne fino alla DLQ, replay per sequence e per tempo.

La suite è protetta da `T18_LIVE`. Senza data plane viene **saltata**, non dichiarata verde: il repository preferisce un test assente a un `PASS` che non prova nulla.

### 3.6 La persistenza

```bash
curl -s http://localhost:8222/jsz | grep '"messages"'
docker compose restart nats
sleep 15
curl -s http://localhost:8222/jsz | grep '"messages"'
```

Il conteggio dopo il riavvio non scende. Può salire, perché i simulatori si riconnettono e riprendono a pubblicare.

## 4. Ricostruire e rieseguire i test

```bash
docker compose --profile test run --rm --no-deps --build test
```

Build, test unitari, Spotless, Checkstyle e SpotBugs con soglia High.

> Usa sempre `--build`. Senza, un layer Docker in cache può compilare sorgenti vecchi e restituire un `BUILD SUCCESS` che non corrisponde al codice che hai davanti. È successo davvero due volte in questo progetto.

Se Spotless segnala violazioni di formato, il profilo `test` non può correggerle: copia i sorgenti nell'immagine invece di montarli, quindi `spotless:apply` scriverebbe dentro il container. Per riformattare sull'host serve un bind mount:

```bash
docker run --rm -v "$PWD:/workspace" -w /workspace -v kafkanuts-maven-cache:/root/.m2 maven:3.9.11-eclipse-temurin-21 mvn -B -ntp spotless:apply
```

## 5. Troubleshooting

| Sintomo | Causa probabile | Rimedio |
|---|---|---|
| `docker compose up --wait` scade su `registry` | Apicurio impiega più del previsto a migrare lo schema SQL | rilancia il comando; se persiste, `docker compose logs registry` |
| un simulatore resta `starting` | non ha ancora raggiunto NATS o il registry | `docker compose logs <servizio>`; l'health espone la causa in `error` |
| la dashboard mostra "Dashboard non disponibile" | `nats` non è healthy oppure il proxy non lo raggiunge | `docker compose ps nats` e `curl -s localhost:8222/healthz` |
| la dashboard ha valori vuoti nella colonna viva | JetStream non ha ancora stream | attendi che i simulatori pubblichino, oppure lancia la suite |
| `feature-suite` riporta test saltati | manca `T18_LIVE` oppure gli endpoint | usa il profilo `suite`, che li imposta |
| il test della DLQ non vede crescere il contatore | su un consumer pull il server valuta `MaxDeliver` solo all'arrivo di una nuova richiesta | è già gestito dal test, che ripete fetch e `nak` |
| porta già occupata | un'altra istanza è viva | `docker compose down` oppure cambia il mapping in `compose.yaml` |
| `BUILD SUCCESS` sospetto dopo una modifica | layer Docker in cache | rilancia con `--build` |

Log utili:

```bash
docker compose logs -f nats
docker compose logs -f payment-simulator
curl -s http://localhost:8222/varz
curl -s http://localhost:8222/connz
```

## 6. Cleanup

Ferma i container ma conserva i dati:

```bash
docker compose down
```

Rimuovi anche i dati persistenti, cioè stream JetStream, database del registry e cache Maven:

```bash
docker compose down -v
```

Il secondo comando è **distruttivo e irreversibile** per lo stato della demo: cancella i volumi `kafkanuts-nats-data`, `kafkanuts-registry-db` e `kafkanuts-maven-cache`. Non tocca nulla al di fuori di questo progetto. La successiva ripartenza sarà lenta quanto la prima.

Per liberare anche le immagini costruite localmente:

```bash
docker compose down -v --rmi local
```

## 7. Cosa questa demo non fa

L'elenco completo delle voci `NOT_TESTED` e `NOT_EXERCISED` è nella sezione *Limiti dichiarati* del [README](../README.md) e nei gate in [docs/gates/](gates/). In sintesi: nessuna migrazione, nessuna misura di prestazione o capacità, nessuna garanzia di sicurezza del data plane, nessuna corrispondenza misurata con Confluent.
