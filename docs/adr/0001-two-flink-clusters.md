# ADR 0001: due cluster Flink indipendenti

- Stato: accettato
- Data: 2026-08-31

## Contesto

La demo confronta processing equivalente su Kafka e NATS. Un singolo cluster ridurrebbe memoria, ma condividerebbe classpath, lifecycle, failure domain e risorse, rendendo meno netto il confronto. L'host espone a Docker circa 30,2 GiB e può sostenere due cluster piccoli entro un budget completo di 18-20 GiB.

## Decisione

Usare due cluster indipendenti:

- `flink-kafka`, con JobManager, TaskManager, checkpoint e rete Kafka;
- `flink-nats`, con JobManager, TaskManager, checkpoint e rete NATS.

Condividono il codice della funzione logica e i contratti Avro, non il runtime. Il parity verifier confronta output esterni.

## Conseguenze

- confronto e isolamento migliori;
- possibilità di versionare/configurare connector separatamente;
- consumo aggiuntivo stimato 2-3 GiB rispetto al cluster condiviso;
- Compose e osservabilità devono distinguere chiaramente label, metriche e checkpoint;
- i profili ridotti possono avviare un solo cluster, ma il profilo migration li usa entrambi.
