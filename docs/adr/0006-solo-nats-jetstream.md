# ADR 0006: Solo NATS JetStream, nessuna migrazione eseguita

- Stato: accettato
- Data: 2026-09-02
- Estende: `docs/adr/0005-perimetro-qa-ridotto.md`
- Sostituisce: `docs/adr/0001-two-flink-clusters.md`, `docs/adr/0004-early-flink-nats-gate.md`
- Ridefinisce: `docs/PLAN.md` sezioni 1, 2, 4, 7 e 8; task T05-T14

## Contesto

Il laboratorio era impostato come **dimostrazione di migrazione**: due data plane in piedi, Kafka con Confluent Schema Registry da una parte, NATS JetStream con Apicurio dall'altra, più due cluster Flink e un percorso di cutover con verifica di parità.

Tre fatti hanno reso quell'impostazione sproporzionata:

1. Il gate T10 ha dato esito **C**: il processing Flink/NATS è fuori dal perimetro commerciale iniziale. Mantenere Flink significava mantenere infrastruttura per una capacità già esclusa.
2. Tenere in piedi entrambi i data plane costa la maggior parte delle risorse Docker e la maggior parte del tempo di sviluppo, per dimostrare un travaso che l'interlocutore non ha chiesto di vedere eseguito.
3. La domanda reale a cui il laboratorio deve rispondere non è *"la migrazione funziona?"* ma **"cosa sa fare JetStream, e a cosa corrisponde ciò che uso oggi su Confluent?"**

## Decisione

Il laboratorio dimostra **le capacità del solo stack NATS JetStream**. Nessuna migrazione viene eseguita.

1. **Stack unico in piedi**: NATS con JetStream persistente, Apicurio Registry con PostgreSQL, i tre simulatori sul ciclo di vita ordine, pagamento e fulfillment. Nessun secondo data plane.
2. **Kafka rimosso fisicamente**: broker KRaft, Confluent Schema Registry, ksqlDB, init dei topic, modulo `kafka-baseline` e relativi gate escono dal repository.
3. **Flink rimosso fisicamente**: entrambi i cluster, il modulo `flink-nats-spike` e i relativi gate escono dal repository. Il cluster Flink Kafka di T06 resta sul branch `feat/T06-flink-kafka` e non viene unito.
4. **La migrazione è evocata, non eseguita**: una dashboard mostra quali feature JetStream lo stack sta usando e a quale costrutto Confluent Platform o Kafka corrisponderebbero.
5. **La mappatura è dichiarativa, non misurata.** Con Kafka assente non esiste confronto sperimentale. Ogni riga della dashboard cita la propria fonte documentale ed è etichettata come corrispondenza dichiarata. Presentarla come misura sarebbe esattamente il claim non supportato che questo repository vieta.
6. **CI minima obbligatoria**: build, unit e functional test in container, validazione Compose e ricerca segreti su ogni pull request verso `main`. Con un solo esecutore che apre, revisiona e unisce le proprie PR, la CI è l'unico controllo indipendente.

Lo stato precedente è recuperabile dal tag `archive/kafka-flink-baseline-v0`.

## Conseguenze

- Il budget risorse scende da circa dieci CPU a circa tre: NATS, PostgreSQL, Apicurio e tre simulatori.
- Il repository non può più sostenere alcuna affermazione su parità, cutover, rollback o equivalenza misurata fra Kafka e NATS. Quelle affermazioni erano il perimetro di T11 e non vengono prodotte.
- Il valore commerciale si sposta dalla prova di migrazione alla **leggibilità della corrispondenza**: mostrare a un interlocutore Confluent cosa userebbe su JetStream e cosa non troverebbe.
- La colonna della dashboard che dichiara "nessun equivalente diretto" è la parte più utile e la più facile da contestare: va tenuta conservativa e citata.
- Il lavoro di T05, T06 e T10 viene rimosso dal prodotto pur essendo stato completato. È una perdita accettata: mantenerlo costava più del valore che aggiungeva alla domanda attuale.
