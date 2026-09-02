# AGENTS.md

Queste istruzioni si applicano all'intero repository.

## Fonte della verità

Git e GitHub sono il contratto operativo. Non assumere che lo stato di una finestra, di una chat o di un terminale sia disponibile al turno successivo. Prima di agire, leggere nell'ordine:

1. `AGENTS.md`;
2. `TASKS.md`;
3. `docs/PLAN.md`;
4. `docs/QA-SCOPE.md`;
5. `docs/EXECUTION-HANDOFF.md`;
6. issue e pull request assegnate.

Ogni modifica materiale deve essere committata. Nessun lavoro utile deve esistere soltanto nel working tree a fine turno.

## Mandato approvato e autonomia

Lo sponsor umano ha definito e approvato esplicitamente architettura, obiettivi, vincoli e piano di alto livello nel bootstrap T00, e il perimetro di verifica ridotto in `docs/adr/0005-perimetro-qa-ridotto.md`. L'esecuzione entro quel perimetro è agentica end-to-end: un esecutore singolo implementa, apre la pull request, svolge la review, unisce su `main` e avanza il backlog.

Non chiedere approvazioni umane di routine per decisioni già coperte da piano, ADR, task e criteri di accettazione. Fermarsi e chiedere nuova autorità soltanto per cambi di architettura/scope/semantica/budget, accettazione di rischio High/Critical, nuovi permessi o credenziali, azioni distruttive non previste o alternative con conseguenze materialmente diverse. Le approvazioni tecniche imposte dalla piattaforma, dal sistema operativo o dagli strumenti restano vincolanti.

## Vincoli non negoziabili

- Tutto lo stack runtime e tutti i test d'integrazione devono girare in Docker Compose.
- Il flusso deve funzionare da Windows, Linux e macOS usando comandi documentati e portabili.
- Usare Java 21 e Spring Boot per i tre simulatori.
- Conservare due cluster Flink separati, `flink-kafka` e `flink-nats`.
- Usare Kafka OSS/KRaft e NATS JetStream, non NATS Streaming/STAN.
- Avro è il formato canonico degli eventi per entrambi i trasporti.
- Non inserire segreti, token, password reali o dati sensibili nel repository.
- Non ridurre test, controlli di sicurezza o limiti risorse per far passare una pull request senza documentare e approvare la decisione. La riduzione approvata per la release v0.1.0 è quella, e soltanto quella, descritta in `docs/adr/0005-perimetro-qa-ridotto.md` e `docs/QA-SCOPE.md`.
- Un requisito di verifica fuori perimetro assume lo stato `NOT_TESTED` con motivazione nel report; non può diventare un `PASS` semantico né sostenere un claim.

## Modello di esecuzione

Dal 2026-09-02 il ciclo a tre attori (esecutore Luna, watcher PowerShell, reviewer Codex/Sol) è sostituito da un **esecutore singolo** che implementa, apre la pull request, svolge la review e unisce su `main`. Il contratto Git resta invariato: è la parte che produce tracciabilità, non l'orchestrazione. Il modello precedente e la sua motivazione restano leggibili in `docs/adr/0003-git-contract.md`; questa sezione lo aggiorna.

Per ogni turno:

1. sincronizzare `main` e selezionare il primo task `READY` non bloccato in `TASKS.md`;
2. creare un branch `type/TNN-descrizione-breve`;
3. implementare soltanto lo scope del task e i prerequisiti strettamente necessari;
4. eseguire i test e i gate elencati nel task, entro il perimetro di `docs/QA-SCOPE.md`;
5. registrare l'esito in `reports/tNN-gate.json`, con `NOT_TESTED` motivato per ogni requisito fuori perimetro;
6. aggiornare documentazione e stato del task nello stesso branch;
7. creare commit piccoli con messaggi Conventional Commits;
8. pushare il branch e aprire la pull request con il template compilato;
9. svolgere la review sul diff, non sul ricordo dell'implementazione, e registrarne l'esito nel corpo della PR: scope, rischi, requisiti non esercitati, rollback;
10. portare il task a `DONE` sullo stesso branch, poi squash merge, sincronizzare `main` e verificare il commit risultante;
11. selezionare il task successivo.

Poiché autore e reviewer coincidono, la review deve essere esplicita e scritta, e vale la regola di astensione: se la review individua un problema di architettura, sicurezza, semantica o budget, il task va marcato `BLOCKED` nell'issue e portato allo sponsor umano, non risolto unilateralmente.

Se un requisito è ambiguo e cambia architettura, sicurezza, compatibilità o costo risorse, non improvvisare: aggiungere una nota `BLOCKED` al task/issue e chiedere decisione prima di procedere.

## Confini della review a esecutore singolo

La review deve concentrare l'attenzione su ciò che l'autoreview tende a mancare:

- confronto fra file modificati e scope dichiarato del task;
- correttezza architetturale, migrazione, sicurezza, concorrenza e failure mode;
- coerenza fra ciò che il report dichiara `PASS` e ciò che il gate ha davvero eseguito;
- assenza di segreti, pin delle immagini e limiti risorse sui nuovi servizi;
- allineamento fra `TASKS.md`, issue e stato reale.

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
