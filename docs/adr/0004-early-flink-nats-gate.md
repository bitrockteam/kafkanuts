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

T01-T03 sono prerequisiti della piattaforma dimostrativa. Quando il laboratorio viene riusato in un pilot commerciale, il time-box della prima settimana parte dal gate T10 e non include fondazioni già disponibili.

## Conseguenze

- il rischio tecnico principale viene ritirato prima dei simulatori e della narrativa demo;
- la numerazione dei task non coincide con l'ordine di esecuzione;
- il percorso B o C riduce i claim commerciali senza bloccare transport, schema e migrazione;
- un esito C richiede un ADR di scope prima di rimuovere Flink/NATS dalla Definition of Done della release;
- T10 può richiedere più lavoro iniziale, ma evita investimento tardivo su un'ipotesi non provata.
