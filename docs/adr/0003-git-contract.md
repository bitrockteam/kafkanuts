# ADR 0003: Git e GitHub come contratto degli agenti

- Stato: accettato
- Data: 2026-08-31

## Contesto

L'esecuzione coinvolge Luna, watcher Herdr/PowerShell, un dispatcher e Codex in finestre/sessioni diverse. Lo stato del terminale è transitorio, costoso da ricostruire e non adatto a review o audit.

## Decisione

Commit, branch, issue, pull request, check, review, ADR e `TASKS.md` costituiscono l'unica fonte di verità. Luna implementa e apre PR; il watcher mantiene liveness senza modificare il repository; Codex revisiona e unisce attraverso GitHub.

Main richiede pull request, cronologia lineare, conversazioni risolte e blocco di force push/delete. Inizialmente non imponiamo una approval formale perché autore e reviewer operativo possono condividere lo stesso account GitHub; il requisito sarà elevato quando è disponibile una identità distinta.

## Conseguenze

- le sessioni possono interrompersi e riprendere senza affidarsi alla chat;
- ogni turno utile termina con commit/PR o blocco visibile;
- più disciplina documentale, compensata da meno token spesi per ricostruire contesto;
- il watcher resta semplice e non acquisisce autorità di implementazione.
