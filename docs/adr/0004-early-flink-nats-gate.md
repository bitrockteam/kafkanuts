# ADR 0004: anticipare il gate Flink/NATS

- Stato: accettato
- Data: 2026-09-01

## Contesto

La disponibilità di source e sink NATS per la DataStream API non dimostra che esista una Table Factory utilizzabile da Flink SQL. Inoltre ack, checkpoint, recovery, split e ordering possono cambiare la garanzia del percorso. Il piano commerciale richiede che questa incertezza venga risolta nella prima settimana, mentre il backlog tecnico originario collocava la spike dopo simulatori, baseline Kafka, JetStream e registry.

Costruire prima il resto della demo renderebbe tardiva la decisione con il maggiore impatto sullo scope e permetterebbe claim Flink non ancora provati.

## Decisione

T10 mantiene il proprio identificativo GitHub, ma viene eseguito subito dopo T01-T03 e prima di T04. Deve produrre uno dei tre esiti:

- **A:** adapter Table/SQL minimo, realmente eseguibile e sostenibile;
- **B:** supporto limitato alla DataStream API;
- **C:** processing Flink/NATS escluso dal displacement commerciale iniziale.

Il gate usa un cluster `flink-nats` isolato e prova almeno Avro con registry, event time/watermark, trasformazioni rappresentative, checkpoint/recovery, redelivery, duplicati, parallelismo, backpressure e packaging. L'eventuale adapter è confinato e non diventa un connettore general-purpose senza una nuova decisione.

### Outcome T10 (2026-09-01)

La spike ha avviato un cluster isolato `flink-nats` (Flink 1.20.2, NATS 2.11.8) e ha verificato packaging Maven containerizzato, licenze Apache-2.0, healthcheck e connettività jnats 2.20.5. Non ha sottomesso un job al JobManager e non ha provato registry, event time/window-join runtime, checkpoint/recovery, redelivery/dedup o backpressure. La garanzia effettiva osservata è quindi soltanto connettività; non si dichiara exactly-once o processing Flink/NATS.

L'esito del gate è **C — processing Flink/NATS escluso dal displacement commerciale iniziale**. La regola è meccanica: B è eleggibile solo se ogni criterio runtime mandatory ha status `PASS` nel report machine-readable; qualsiasi `NOT_TESTED` o `FAIL` forza C.

Budget della spike: limiti Compose JobManager 2 GiB, TaskManager 2 GiB, NATS 256 MiB e runner 1 GiB (massimo configurato 5,25 GiB); osservazione idle 315,5 MiB. Il packaging non introduce un connettore general-purpose. La manutenzione resta confinata alla verifica versionata della spike; eventuali versioni future richiedono riesecuzione del gate.

T01-T03 sono prerequisiti della piattaforma dimostrativa. Quando il laboratorio viene riusato in un pilot commerciale, il time-box della prima settimana parte dal gate T10 e non include fondazioni già disponibili.

## Conseguenze

- il rischio tecnico principale viene ritirato prima dei simulatori e della narrativa demo;
- la numerazione dei task non coincide con l'ordine di esecuzione;
- il percorso B o C riduce i claim commerciali senza bloccare transport, schema e migrazione;
- con l'outcome C, T04 non può dipendere da un connettore Flink/NATS né dichiarare processing portabile verso NATS;
- Flink/NATS resta documentato come area di ricerca separata, non come capacità del displacement iniziale;
- un ritorno a B richiede una nuova spike/decisione con job realmente sottomesso e PASS per ogni criterio runtime mandatory;
- T10 può richiedere più lavoro iniziale, ma evita investimento tardivo su un'ipotesi non provata.
