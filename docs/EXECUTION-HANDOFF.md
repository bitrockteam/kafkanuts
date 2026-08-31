# Handoff esecutivo per Luna

## Mandato

Implementare `kafkanuts` in pull request incrementali seguendo `TASKS.md`. Modello previsto: `gpt-5.6-luna`, reasoning `medium`. L'esecutore produce codice, test, documentazione, commit e PR; non fa merge e non decide unilateralmente cambi architetturali.

## Prompt di avvio consigliato

> Lavora come esecutore del repository kafkanuts. Git e GitHub sono l'unica fonte di verità. Leggi integralmente AGENTS.md, TASKS.md, docs/PLAN.md e gli ADR applicabili. Sincronizza main, seleziona il primo task READY con prerequisiti soddisfatti e lavora soltanto su quello in un branch dedicato. Tutta la build e i test devono essere eseguibili tramite Docker Compose su Windows, Linux e macOS. Crea commit Conventional Commits, pusha il branch e apri una pull request completa di evidenze; non fare merge. Se incontri una decisione architetturale, una richiesta di permesso o lo stesso errore per tre tentativi fondati, marca il task BLOCKED e descrivi esattamente il punto nella issue/PR prima di fermarti.

Questo prompt non sostituisce i documenti del repository: li indica come contratto stabile.

## Ciclo di un task

1. `git fetch --all --prune` e verifica working tree pulito.
2. Leggi `main:TASKS.md` e l'issue del task.
3. Marca il task `IN_PROGRESS` nel branch o usa l'assegnazione/label GitHub; non fare commit diretto su main.
4. Crea `feat/TNN-slug` (o tipo appropriato) da `origin/main`.
5. Scrivi prima criteri/test quando chiariscono la semantica.
6. Implementa lo scope minimo completo.
7. Esegui gate locali containerizzati e conserva output sintetico.
8. Riesamina diff, segreti, dipendenze, limiti risorse e documentazione.
9. Aggiorna `TASKS.md` a `PR_OPEN` nel branch.
10. Commit, push e apertura PR con template compilato.
11. Inserisci un commento finale nella PR con commit HEAD e stato gate.
12. Fermati. Il watcher non deve spingere Luna al task successivo finché la PR non è stata unita o chiusa.

## Pacchetto minimo di handoff in PR

- task e issue;
- risultato ottenuto;
- file/componenti modificati;
- comandi esatti eseguiti;
- esito test e report;
- delta CPU/RAM se cambia lo stack;
- rischi, limiti e debito introdotto;
- istruzioni di rollback;
- commit HEAD;
- eventuali decisioni richieste al reviewer.

## Confini per l'efficienza dei token

- non rileggere l'intera cronologia delle chat: leggere repository, issue e PR;
- non chiedere a Codex di generare boilerplate già assegnato a Luna;
- non accumulare più task in una PR;
- preferire output sintetici e allegati/report rispetto a log completi nei commenti;
- salvare decisioni durevoli in ADR, non ripeterle a ogni turno;
- dopo feedback di review, modificare solo i punti richiesti e riportare test mirati più regressione necessaria.

## Quando fermarsi

Fermarsi e rendere visibile `BLOCKED` se:

- manca un permesso/account/segreto necessario;
- due alternative cambiano una decisione architetturale o una garanzia semantica;
- un'azione può perdere dati o modificare risorse esterne non previste;
- lo stesso blocco persiste dopo tre tentativi ragionati;
- il budget risorse non può essere rispettato senza cambiare scope;
- un controllo segnala una vulnerabilità High/Critical senza fix sicuro nello scope.

Non fermarsi per un normale errore di compilazione o test prima di averne investigato causa e alternative in-scope.

## Review e merge

Codex legge la PR e i check, non il terminale dell'esecutore. Una volta risolti commenti e gate, Codex effettua squash merge, verifica `main`, porta il task a `DONE` se non già incluso e rende `READY` il task successivo. Se l'account GitHub è lo stesso per autore e reviewer, l'approvazione formale può non essere conteggiata: il ruleset iniziale richiede PR e conversazioni risolte senza imporre un'approvazione impossibile. Quando esiste un reviewer distinto/team, elevare il requisito ad almeno una approval.

## Ripresa dopo interruzione

La sequenza è sempre:

1. `git status` e branch corrente;
2. `git log --oneline --decorate -10`;
3. issue/PR aperte del repository;
4. check e review della PR assegnata;
5. `TASKS.md` su `origin/main`.

Se terminale e Git divergono, prevale Git. Modifiche non committate vanno ispezionate e salvate in un commit del branch corretto prima di proseguire; non cancellarle automaticamente.
