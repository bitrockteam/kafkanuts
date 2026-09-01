# T04 — simulatori e transport adapter

T04 fornisce tre applicazioni Spring Boot indipendenti: `order-simulator`, `payment-simulator` e `fulfillment-simulator`. Ogni immagine è costruita separatamente con `Dockerfile.simulator` multi-stage: Maven resta nel build stage, mentre il runtime contiene solo JRE 21, il fat jar Spring Boot e BusyBox per il healthcheck HTTP. Il profilo Compose `t04` espone inoltre un runner smoke senza Kafka/NATS reali e un verifier HTTP delle tre app.

## Routing

`TransportMode` supporta `kafka`, `nats` e `dual`. Gli adapter T04 sono porte applicative deterministiche: la selezione del transport non richiede rebuild. L'invio duale è esplicito verso entrambe le destinazioni.

Ogni evento usa `EventEnvelope` immutabile con `eventId`, tipo, aggregato, timestamp e `TelemetryContext` (`traceId`/`spanId`). `CanonicalEventMapper` lo converte esplicitamente nell'`EventEnvelope` Avro canonico di T03 al boundary. `IdempotencyStore` è thread-safe e applica un effetto logico per `eventId` nel processo.

Le app sono long-running e espongono `/health`, che restituisce `UP`, servizio e modalità effettivamente cablata nel singleton `TransportRouter`; `TRANSPORT_MODE` usa il relaxed binding Spring (`transport.mode`).

Questa PR non introduce connettori Flink/NATS e non dichiara processing Flink/NATS: l'outcome C di T10 resta vincolante. L'adapter NATS applicativo è una porta minima e sostituibile, non un data-plane connector.

## Gate

```sh
docker compose --profile t04 config
docker compose --profile t04 build order-simulator payment-simulator fulfillment-simulator t04-smoke
docker compose --profile t04 run --rm t04-smoke
```

Lo smoke esegue unit test dominio/adapter per tutti i moduli senza avviare alcun data plane Kafka o NATS.
