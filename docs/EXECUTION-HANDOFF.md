# Handoff esecutivo

## Mandato

Implementare `kafkanuts` in incrementi revisionabili seguendo `TASKS.md`, entro il perimetro di verifica di `docs/QA-SCOPE.md`.

Dal 2026-09-02 il modello è a **esecutore singolo**: lo stesso agente implementa, apre la pull request, svolge la review scritta, unisce su `main` e seleziona il task successivo. Il ciclo precedente a tre attori (esecutore Luna su Pi, watcher PowerShell, reviewer Codex/Sol su Herdr) è ritirato; la sua documentazione operativa resta nel workspace `nats-kafka` come storia, non come contratto.

Il contratto Git di `docs/adr/0003-git-contract.md` resta valido in tutto ciò che riguarda branch, commit, issue, pull request e `TASKS.md`. Cambia soltanto chi esegue quali passi.

## Ciclo di un task

1. `git fetch --all --prune` e verifica working tree pulito.
2. Leggi `main:TASKS.md`, l'issue del task e `docs/QA-SCOPE.md`.
3. Crea `feat/TNN-slug` (o tipo appropriato) da `origin/main`.
4. Marca il task `IN_PROGRESS` nel branch; nessun commit diretto su `main`.
5. Implementa lo scope minimo completo.
6. Esegui i gate containerizzati previsti dal task e scrivi `reports/tNN-gate.json`.
7. Registra `NOT_TESTED` con motivazione per ogni requisito fuori perimetro. Un requisito non esercitato non diventa mai `PASS`.
8. Aggiorna documentazione e `TASKS.md`, commit Conventional Commits, push del branch.
9. Apri la pull request con il template compilato e commenta l'issue con commit HEAD ed esito dei gate.
10. Svolgi la review leggendo il diff, non il ricordo dell'implementazione. Scrivi l'esito nel corpo della PR.
11. Porta il task a `DONE` sullo stesso branch, poi squash merge, sincronizza `main`, verifica il commit risultante.
12. Seleziona il task `READY` successivo.

## Review a esecutore singolo

Autore e reviewer coincidono, quindi la review non può essere implicita. Deve essere scritta nel corpo della PR e coprire almeno:

- file modificati confrontati con lo scope dichiarato del task;
- correttezza architetturale, migrazione, sicurezza, concorrenza e failure mode;
- coerenza fra i `PASS` del report e ciò che il gate ha davvero eseguito;
- segreti, pin delle immagini, utente non-root e limiti risorse dei nuovi servizi;
- allineamento fra `TASKS.md`, issue e stato reale.

**Regola di astensione**: se la review individua un problema di architettura, sicurezza, semantica o budget, il task va marcato `BLOCKED` nell'issue e portato allo sponsor umano. L'esecutore singolo non si auto-assolve su una decisione che non gli compete.

## Quando fermarsi

Marcare `BLOCKED` e rendere visibile il blocco se:

- manca un permesso, un account o un segreto necessario;
- due alternative cambiano una decisione architetturale o una garanzia semantica;
- un'azione può perdere dati o modificare risorse esterne non previste;
- lo stesso blocco persiste dopo tre tentativi ragionati;
- il budget risorse non può essere rispettato senza cambiare scope;
- un gate non può passare se non indebolendo una garanzia dichiarata al lettore.

Non fermarsi per un normale errore di compilazione o test prima di averne investigato causa e alternative in-scope.

## Ripresa dopo interruzione

Git e GitHub sono la fonte durevole. La sequenza è sempre:

1. `git status` e branch corrente;
2. `git log --oneline --decorate -10`;
3. issue e pull request aperte del repository;
4. check e review della PR corrente;
5. `TASKS.md` su `origin/main`.

Se terminale e Git divergono, prevale Git. Modifiche non committate vanno ispezionate e salvate in un commit del branch corretto prima di proseguire; non cancellarle automaticamente.
