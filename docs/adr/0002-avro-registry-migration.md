# ADR 0002: Avro e migrazione dei registry

> **Ridotta dall'[ADR 0006](0006-solo-nats-jetstream.md) il 2026-09-02.** Con Confluent Schema Registry rimosso, la migrazione fra registry non viene eseguita né simulata. Resta valido l'uso di Apicurio come registry degli schemi Avro tramite API `ccompat`, e resta valido il principio che gli ID numerici di un registry non sono identità portabile.


- Stato: accettato con spike di validazione
- Data: 2026-08-31

## Contesto

Kafka usa naturalmente il wire format Confluent e Schema Registry. NATS trasporta byte e header senza imporre un registry. Durante la migrazione occorre separare il cambio di trasporto dal cambio di registry ed evitare di assumere che gli schema ID siano uguali tra Confluent e Apicurio.

## Decisione

- `.avsc` e fingerprint sono la fonte portabile del contratto;
- Kafka usa serializer/deserializer Confluent;
- nella prima fase NATS usa Confluent Registry e framing Confluent, così il cambio di trasporto può essere verificato da solo;
- nella fase successiva NATS usa Apicurio, preferendo API ccompat se i test ne confermano il comportamento;
- supportare una variante con schema/global ID in header NATS per rendere esplicita la metadata strategy;
- mantenere mapping per subject/version/fingerprint e ID dei due registry, oppure fare decode/re-encode nel bridge;
- testare messaggi storici, incompatibilità, cache, outage e rollback.

## Conseguenze

- una variabile di migrazione alla volta;
- codec condiviso e test cross-registry obbligatori;
- costo di mapping/re-encoding esplicito;
- nessun claim di wire compatibility finché la suite non lo dimostra.
