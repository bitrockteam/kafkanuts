# ADR 0003: Git e GitHub come contratto degli agenti

- Stato: accettato
- Data: 2026-08-31

## Contesto

L'esecuzione coinvolge Luna, watcher Herdr/PowerShell, un dispatcher e Codex in finestre/sessioni diverse. Lo stato del terminale è transitorio, costoso da ricostruire e non adatto a review o audit.

## Decisione

Commit, branch, issue, handoff, pull request, check, review, ADR e `TASKS.md` costituiscono l'unica fonte di verità. Luna implementa, testa, pusha il branch e pubblica `HANDOFF_READY`; il watcher mantiene liveness senza modificare il repository; Codex/Sol apre e gestisce la PR, esegue code review e unisce su `main` attraverso GitHub.

Lo sponsor umano approva architettura, piano e confini di autorità. Entro il piano approvato, il ciclo è agentico e non richiede una conferma umana per ogni task o merge. Nuova autorità è richiesta soltanto quando il lavoro cambia perimetro o decisione, introduce un rischio non coperto, necessita nuovi permessi/credenziali oppure comporta un'azione distruttiva non prevista.

Main richiede pull request, cronologia lineare, conversazioni risolte e blocco di force push/delete. Inizialmente non imponiamo una approval formale perché autore e reviewer operativo possono condividere lo stesso account GitHub; il requisito sarà elevato quando è disponibile una identità distinta.

## Conseguenze

- le sessioni possono interrompersi e riprendere senza affidarsi alla chat;
- ogni turno utile termina con commit/PR o blocco visibile;
- più disciplina documentale, compensata da meno token spesi per ricostruire contesto;
- il watcher resta semplice e non acquisisce autorità di implementazione.

## Aggiornamento del 2026-09-02

Il contratto Git di questa ADR resta in vigore. Cambia soltanto la distribuzione dei ruoli: il ciclo a tre attori (Luna implementa, watcher mantiene liveness, Codex/Sol apre PR e unisce) è sostituito da un esecutore singolo che implementa, apre la pull request, svolge una review scritta e unisce su `main`. Vedi `AGENTS.md`, `docs/EXECUTION-HANDOFF.md` e `docs/adr/0005-perimetro-qa-ridotto.md`.

Poiché autore e reviewer coincidono per costruzione, vale la regola di astensione: un problema di architettura, sicurezza, semantica o budget individuato in review non si risolve unilateralmente, ma torna allo sponsor umano come `BLOCKED`.
