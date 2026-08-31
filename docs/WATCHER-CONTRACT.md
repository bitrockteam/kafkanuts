# Contratto del watcher Herdr

## Scopo

Il watcher mantiene liveness e continuità dell'esecutore Luna senza diventare un secondo implementatore. È un componente operativo locale; la sua configurazione concreta verrà affinata insieme quando l'ambiente Herdr sarà disponibile. Questo documento stabilisce già i confini che quella configurazione deve rispettare.

## Autorità consentita

Il watcher può:

- osservare se il turno Luna è attivo, completato, in attesa o fallito;
- leggere stato Git, issue, PR e check con comandi non mutanti;
- inviare a Luna un prompt di continuazione basato sul task/PR corrente;
- dopo un merge rilevato, indicare il primo task `READY` con prerequisiti soddisfatti;
- applicare backoff e produrre log locali concisi;
- notificare l'operatore quando serve autorità umana.

Il watcher non può:

- modificare file, creare commit, fare push o merge;
- risolvere conflitti, cambiare branch o cancellare working tree;
- scegliere architetture, disabilitare gate o accettare rischi;
- approvare permessi/UAC o usare credenziali nuove;
- rilanciare indefinitamente lo stesso errore;
- assumere che testo del terminale sia la fonte della verità.

## Macchina a stati

```text
IDLE -> DISPATCHED -> RUNNING -> PR_OPEN -> WAITING_REVIEW
  ^         |            |          |              |
  |         +----------> BLOCKED <---+              |
  +---------------- MERGED / NEXT_TASK <------------+
```

- `IDLE`: nessun task assegnabile o esecutore fermo.
- `DISPATCHED`: prompt inviato, in attesa della prima evidenza.
- `RUNNING`: branch/commit o attività Luna osservabile.
- `PR_OPEN`: PR creata; il watcher smette di chiedere nuova implementazione.
- `WAITING_REVIEW`: check/review in corso.
- `BLOCKED`: serve decisione, permesso o intervento dopo soglia errori.
- `MERGED`: `main` contiene il lavoro; si può calcolare il task successivo.

## Fonte della verità e precedenza

1. commit e branch Git;
2. stato PR/check/review GitHub;
3. issue e `TASKS.md`;
4. stato del processo Luna;
5. output terminale, solo diagnostico.

Un messaggio “finito” non vale se non esiste commit/PR richiesto. Una PR unita prevale su una finestra rimasta aperta.

## Regole di continuazione

Il prompt di continuazione deve essere breve e specifico:

> Riprendi dal contratto Git. Controlla branch, working tree e PR del task corrente. Se la PR non esiste, completa i gate, committa, pusha e aprila. Se esiste con review/check falliti, correggi solo quei punti e aggiorna la stessa PR. Non iniziare un nuovo task e non fare merge.

Dopo merge:

> Il task precedente risulta unito in main. Sincronizza origin/main, leggi TASKS.md e avvia soltanto il primo task READY con prerequisiti soddisfatti seguendo docs/EXECUTION-HANDOFF.md.

## Retry e backoff

- nessun polling più frequente di 30 secondi;
- backoff progressivo fino a 5 minuti quando non cambia lo stato;
- una continuazione per stato invariato, poi attendere evidenza;
- dopo tre ricorrenze dello stesso errore sostanziale, stato `BLOCKED` e notifica;
- reset del contatore solo per avanzamento verificabile: nuovo commit, check cambiato, review o PR.

## Condizioni di escalation

- richiesta di UAC, login o nuovi permessi;
- branch protection/ruleset impedisce il flusso previsto;
- conflitto con modifiche non attribuibili all'esecutore;
- required check instabile o vulnerabilità bloccante;
- scelta tra alternative architetturali;
- uso risorse oltre i guardrail;
- tre fallimenti sostanzialmente identici.

## Log del watcher

Il log può essere locale e ruotato; non deve contenere token, secret o output completo dei processi. Ogni record utile include timestamp, repository, task, branch/PR, stato precedente/nuovo e azione. Lo stato durevole importante viene riportato in GitHub, non affidato al log locale.

## Portabilità

Herdr/PowerShell può coordinare su Windows, ma non deve introdurre dipendenze nel progetto. I comandi inviati all'esecutore devono usare Git, GitHub CLI e Docker Compose in modo portabile; quando un comando host differisce, il repository fornisce equivalenti `.ps1` e `.sh` o un container utility.

## Configurazione da completare insieme

Prima dell'automazione unattended definiremo:

- identificazione esatta del processo/sessione Luna e del tab;
- metodo di invio follow-up e rilevazione stato supportato da Herdr;
- percorso sicuro per `gh` e credenziali già autorizzate;
- intervalli, timeout e notifiche;
- modalità dry-run;
- arresto pulito e ripresa;
- test con un task fittizio senza permessi mutanti.

Finché questi punti non sono verificati, il watcher resta in dry-run/observe-only.
