# Brief di deployment

Documento autosufficiente da consegnare a chi distribuirà `kafkanuts` fuori dal portatile. Descrive **cosa gira, cosa deve persistere e cosa manca**; non costruisce nulla. Non contiene Terraform, manifest Kubernetes, Helm o pipeline.

Il percorso primario e l'unico verificato resta quello locale: `docker compose up -d --wait`. Tutto ciò che segue è `NOT_EXERCISED`.

> **Avvertenza preliminare.** Questo stack è un laboratorio dimostrativo. Non ha TLS, non ha autenticazione, e i tre simulatori sono **generatori di carico sintetico che pubblicano in continuo**. Non va portato in un ambiente condiviso senza le precondizioni della sezione 4, e i simulatori non vanno lasciati accesi in un ambiente che non sia dedicato alla demo.

## 1. Inventario dei servizi

| Servizio | Immagine | Porte | Stato | CPU | Memoria |
|---|---|---|---|---:|---:|
| `nats` | `nats:2.11.8-alpine` | `4222` client, `8222` monitoring | **sì**, store JetStream | 0,5 | 512 MiB |
| `registry-db` | `postgres:16.4-alpine` | `5432` interna | **sì**, database | 0,5 | 512 MiB |
| `registry` | `apicurio/apicurio-registry:3.0.6` | `8080` → `8081` host | no | 1,0 | 1 GiB |
| `order-simulator` | build locale, `Dockerfile.simulator` | `8080` interna | no | 0,5 | 512 MiB |
| `payment-simulator` | build locale, `Dockerfile.simulator` | `8080` interna | no | 0,5 | 512 MiB |
| `fulfillment-simulator` | build locale, `Dockerfile.simulator` | `8080` interna | no | 0,5 | 512 MiB |
| `console` | build locale, `Dockerfile.simulator` | `8080` interna | no | 0,5 | 512 MiB |
| `dashboard` | `nginx:1.27-alpine` | `8080` → `8090` host | no | 0,25 | 128 MiB |

Totale dichiarato: circa **3,75 CPU** e **3,5 GiB**. Tutte le immagini sono pinnate a versione esplicita; ogni servizio ha un healthcheck.

### Variabili d'ambiente

| Servizio | Variabile | Valore nel laboratorio |
|---|---|---|
| simulatori, `console` | `NATS_URL` | `nats://nats:4222` |
| simulatori, `console` | `REGISTRY_URL` | `http://registry:8080/apis/ccompat/v7` |
| `registry` | `APICURIO_STORAGE_KIND` | `sql` |
| `registry`, `registry-db` | credenziali PostgreSQL | valori di laboratorio, **da sostituire** |
| tutti | `KAFKANUTS_DATA_DIR` | radice dei dati sull'host, default `./data` |

### Dipendenze di avvio

`nats` e `registry-db` per primi; `registry` dopo `registry-db`; simulatori e `console` dopo `nats` e `registry`; `dashboard` per ultima, perché nginx risolve gli upstream all'avvio.

Il provisioning di stream e consumer è **idempotente e ritentato da ogni ruolo**: l'ordine di avvio non è una dipendenza rigida, ma gli healthcheck vanno rispettati.

## 2. Persistenza — requisito non negoziabile

Due sole cose hanno stato:

| Cosa | Percorso nel container | Contenuto |
|---|---|---|
| store JetStream | `/data` | stream `KAFKANUTS_EVENTS` e `KAFKANUTS_DLQ`, file storage |
| database Apicurio | `/var/lib/postgresql/data` | schemi Avro e configurazione di compatibilità |

Entrambi **devono** stare su storage persistente esterno al container.

- Mai lo strato scrivibile del container, mai `emptyDir`, mai il disco effimero dell'istanza.
- JetStream su file storage assume un **filesystem POSIX con `fsync` affidabile**: serve un volume a blocchi (EBS, Persistent Disk, Managed Disk), **non** un object store montato tipo S3/GCS via FUSE, e non NFS senza garanzie di durabilità.
- PostgreSQL usa il pattern `PGDATA=/var/lib/postgresql/data/pgdata`, cioè una sottodirectory del punto di mount: serve a far creare e possedere la directory a Postgres invece di pretendere owner e permessi sulla radice montata.

In locale i due percorsi sono bind mount su `${KAFKANUTS_DATA_DIR:-./data}/nats` e `${KAFKANUTS_DATA_DIR:-./data}/registry-db`. Fuori dal locale la scelta del backing store è di chi distribuisce; il requisito è la durabilità, non il meccanismo.

## 3. Mappa per cloud

Indicativa, `NOT_EXERCISED` in ogni riga.

| | AWS | GCP | Azure |
|---|---|---|---|
| dove girano i container | ECS su EC2, o EKS | GKE | AKS, o Container Apps con volume |
| storage a blocchi | EBS `gp3` | Persistent Disk balanced | Managed Disk Premium SSD |
| PostgreSQL gestito, alternativa al container | RDS for PostgreSQL 16 | Cloud SQL for PostgreSQL 16 | Azure Database for PostgreSQL 16 |
| ingress per la dashboard | ALB | HTTPS Load Balancer | Application Gateway |
| segreti | Secrets Manager | Secret Manager | Key Vault |

> **Il servizio serverless più ovvio di ogni cloud non è adatto a NATS con JetStream.** Cloud Run, Container Apps con scale-to-zero e Fargate senza volume persistente sono stateless per progetto: JetStream è stateful e perderebbe lo store. Vanno bene, semmai, per `registry`, `console` e `dashboard`, che stato non ne hanno.
>
> `console` mantiene in memoria un buffer di 400 eventi che si ricostruisce da solo dal log: può essere riavviata liberamente, ma **non va replicata dietro un load balancer senza sticky session**, perché ogni replica osserverebbe un flusso diverso.

## 4. Precondizioni di sicurezza

**Scritte come precondizioni, non come raccomandazioni.** Nessuna di queste è implementata oggi. Distribuire senza averle soddisfatte espone un data plane in chiaro e senza autenticazione.

| # | Precondizione | Stato oggi |
|---|---|---|
| 1 | TLS sulle connessioni client di NATS (`4222`) | assente |
| 2 | Autenticazione NATS: account, utenti, o NKey/JWT, con permessi per subject | assente, accesso anonimo |
| 3 | Credenziali PostgreSQL da un secret manager, generate, ruotabili | default di laboratorio dichiarati in `.env.example` |
| 4 | Autenticazione e autorizzazione su Apicurio Registry | assente, scrittura aperta |
| 5 | Controllo d'accesso sulla dashboard e sull'endpoint SSE `/api/events` | assente |
| 6 | Porta di monitoring `8222` **mai** raggiungibile dall'esterno | oggi pubblicata sull'host |
| 7 | Rete privata fra i servizi, nessuna porta interna esposta | in locale sono esposte per comodità |
| 8 | TLS terminato sull'ingress per `8090` e `8081` | assente |

La porta `8222` espone `/varz`, `/jsz` e `/connz`: topologia, consumer e connessioni. È di sola lettura, ma è ricognizione servita su un piatto.

## 5. Backup e ripristino

Il laboratorio dimostra che i dati **sopravvivono** alla rimozione dei container e dei volumi Docker, non che esista una procedura di backup provata.

Ciclo minimo, `NOT_EXERCISED` come procedura formale:

```bash
docker compose stop nats registry-db
tar czf kafkanuts-data-$(date +%F).tgz data/
docker compose start nats registry-db
```

Per il ripristino: fermare lo stack, sostituire `./data`, riavviare. La coerenza richiede che JetStream e il registry siano dello **stesso istante**: uno store che contiene eventi serializzati con uno schema che il registry non conosce non è decodificabile.

Su cloud, l'equivalente sono gli snapshot del volume a blocchi e il backup gestito del database, presi in modo coordinato.

## 6. Scaling e limiti noti

| Voce | Stato |
|---|---|
| singolo nodo NATS | è l'unica configurazione provata |
| cluster NATS, replica degli stream, failover | `NOT_EXERCISED` |
| alta disponibilità, partizioni di rete, perdita di disco | `NOT_EXERCISED` |
| percentili di latenza, throughput, burst, soak | `NOT_TESTED` per [ADR 0005](adr/0005-perimetro-qa-ridotto.md) |
| dimensionamento economico | `NOT_EXERCISED`, nessun dato di costo è prodotto o derivabile |

Gli stream hanno `max_age` di ventiquattro ore e una finestra di deduplica di due minuti: il disco non cresce senza limite, ma il dimensionamento va rifatto per qualunque volume reale.

Il ritmo attuale è di **un ordine ogni due secondi**, circa 1,5 eventi al secondo. È una cadenza scelta per essere leggibile in una demo, non un carico.

## 7. Cosa non portare in un ambiente condiviso

- I tre simulatori. Pubblicano in continuo e non si fermano da soli.
- Le credenziali PostgreSQL di `.env.example`.
- La pubblicazione di `8222` e `8081` su un indirizzo raggiungibile.

Una distribuzione ragionevole per una demo remota tiene i simulatori accesi solo per la durata della sessione, dietro autenticazione, su una rete privata, con TLS terminato all'ingress.
