# AGENTS.md

Queste istruzioni si applicano all'intero repository.

## Fonte della verità

Git e GitHub sono il contratto operativo. Non assumere che lo stato di una finestra, di una chat o di un terminale sia disponibile al turno successivo. Prima di agire, leggere nell'ordine:

1. `AGENTS.md`;
2. `TASKS.md`;
3. `docs/PLAN.md`;
4. `docs/EXECUTION-HANDOFF.md`;
5. issue e pull request assegnate.

Ogni modifica materiale deve essere committata. Nessun lavoro utile deve esistere soltanto nel working tree a fine turno.

## Mandato approvato e autonomia

Lo sponsor umano ha definito e approvato esplicitamente architettura, obiettivi, vincoli e piano di alto livello nel bootstrap T00. L'esecuzione entro quel perimetro è agentica end-to-end: Luna implementa e prepara l'handoff; Codex/Sol apre e gestisce la PR, svolge code review e unisce su `main`; il watcher mantiene continuità e avanza il backlog dopo il merge.

Non chiedere approvazioni umane di routine per decisioni già coperte da piano, ADR, task e criteri di accettazione. Fermarsi e chiedere nuova autorità soltanto per cambi di architettura/scope/semantica/budget, accettazione di rischio High/Critical, nuovi permessi o credenziali, azioni distruttive non previste o alternative con conseguenze materialmente diverse. Le approvazioni tecniche imposte dalla piattaforma, dal sistema operativo o dagli strumenti restano vincolanti.

## Vincoli non negoziabili

- Tutto lo stack runtime e tutti i test d'integrazione devono girare in Docker Compose.
- Il flusso deve funzionare da Windows, Linux e macOS usando comandi documentati e portabili.
- Usare Java 21 e Spring Boot per i tre simulatori.
- Conservare due cluster Flink separati, `flink-kafka` e `flink-nats`.
- Usare Kafka OSS/KRaft e NATS JetStream, non NATS Streaming/STAN.
- Avro è il formato canonico degli eventi per entrambi i trasporti.
- Non inserire segreti, token, password reali o dati sensibili nel repository.
- Non ridurre test, controlli di sicurezza o limiti risorse per far passare una pull request senza documentare e approvare la decisione.

## Flusso dell'esecutore Luna

L'esecutore previsto è `gpt-5.6-luna`, reasoning `medium`.

Per ogni turno:

1. sincronizzare `main` e selezionare il primo task `READY` non bloccato in `TASKS.md`;
2. creare un branch `type/TNN-descrizione-breve` e, quando disponibile, un worktree dedicato;
3. implementare soltanto lo scope del task e i prerequisiti strettamente necessari;
4. eseguire i test e i gate elencati nel task;
5. aggiornare documentazione e stato del task nello stesso branch;
6. creare commit piccoli con messaggi Conventional Commits;
7. aggiornare il task a `HANDOFF_READY`, pushare il branch e pubblicare nell'issue un handoff con commit HEAD, risultati, limiti, rischi, rollback e comando esatto per riprodurre i test;
8. fermarsi senza aprire la pull request. Luna non gestisce PR, non unisce e non modifica direttamente `main`;
9. dopo richieste di review, correggere soltanto lo stesso branch e aggiornare l'handoff senza iniziare un altro task.

Se un requisito è ambiguo e cambia architettura, sicurezza, compatibilità o costo risorse, non improvvisare: aggiungere una nota `BLOCKED` al task/issue e chiedere decisione nella PR.

## Ruolo di Codex

Il reviewer previsto è `gpt-5.6` (alias di `gpt-5.6-sol`), reasoning `medium`. Codex opera come architetto/reviewer/merge steward e babysitter operativo di Luna, non come esecutore seriale di boilerplate. Deve:

- leggere prima diff, check e commenti GitHub;
- verificare l'handoff `HANDOFF_READY`, aprire la PR dal branch Luna e compilarne il template;
- confrontare prima l'elenco dei file modificati con lo scope del task e bloccare la PR, prima della review semantica, se contiene file estranei senza motivazione approvata;
- iniziare la review semantica solo dopo che i gate richiesti hanno risultati visibili; failure o controlli mancanti tornano a Luna senza spendere token in una review completa;
- concentrare i token su correttezza architetturale, migrazione, sicurezza, concorrenza, failure mode e qualità dei test;
- richiedere correzioni tramite review tracciata;
- fare squash merge soltanto quando criteri di accettazione e check sono soddisfatti;
- aggiornare/assegnare il task successivo dopo il merge;
- non duplicare l'implementazione di Luna o rileggere cronologie complete quando branch, PR e report contengono già lo stato necessario;
- evitare di ricostruire lo stato osservando terminali Herdr.

## Regole di modifica

- Non fare push diretto su `main` dopo il bootstrap.
- Una PR deve riguardare un solo task principale.
- Non modificare file estranei allo scope senza spiegazione nel corpo della PR.
- Se cambia una decisione architetturale, aggiornare o aggiungere un ADR.
- Pin delle immagini container a versione esplicita; prima della release usare anche digest dove pratico.
- Pin delle GitHub Actions a commit SHA con commento della versione.
- Lockfile/BOM/version catalog devono essere aggiornati insieme alle dipendenze.
- Gli script host devono avere equivalenti PowerShell e POSIX oppure essere sostituiti da target Compose portabili.

## Definition of Done generale

Un task è completato solo quando:

- codice, test e documentazione sono nel branch;
- i test richiesti passano in container;
- non ci sono segreti o vulnerabilità High/Critical non accettate esplicitamente;
- limiti CPU/memoria e healthcheck sono presenti per i nuovi servizi;
- la PR descrive verifica, rischi e rollback;
- la PR è stata revisionata e unita in `main`;
- `TASKS.md` riflette lo stato risultante.
