# Contribuire

## Prerequisiti host

- Git recente;
- Docker Engine o Docker Desktop con Docker Compose v2;
- almeno 24 GiB di RAM assegnabili a Docker per il profilo completo; i profili ridotti richiederanno meno risorse;
- porte indicate in seguito nel file `.env.example` libere.

Java, Maven, Kafka, NATS e Flink non devono essere installati sull'host.

## Branch e commit

- `feat/TNN-descrizione`
- `fix/TNN-descrizione`
- `docs/TNN-descrizione`
- `chore/TNN-descrizione`

Usare Conventional Commits, per esempio `feat(simulator): add dual-publish adapter`.

## Pull request

Una PR deve:

- riferirsi a un task di `TASKS.md` e, dopo il bootstrap, a una issue GitHub;
- essere piccola, riproducibile e priva di modifiche non correlate;
- includere comandi di test eseguiti e risultati;
- aggiornare documentazione/ADR quando necessario;
- non contenere segreti o artefatti generati non previsti.

Il merge previsto è squash. La protezione iniziale richiede PR e risoluzione delle conversazioni; i required checks vengono aggiunti non appena esistono i relativi workflow, per evitare una regola impossibile da soddisfare durante il bootstrap.
