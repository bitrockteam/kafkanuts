## Task

- Task: TNN
- Issue: closes #

## Risultato

Descrivere il risultato osservabile e lo scope escluso.

## Verifica

Comandi eseguiti esclusivamente tramite Docker Compose, con esito:

```text
docker compose ...
```

- [ ] unit/contract test richiesti
- [ ] integration/functional test richiesti
- [ ] `docker compose config`
- [ ] Gitleaks/Trivy/controlli di sicurezza applicabili
- [ ] documentazione e ADR aggiornati

## Portabilità

- [ ] nessuna dipendenza runtime host oltre Git/Docker/Compose
- [ ] path e comandi verificati o progettati per Windows, Linux e macOS
- [ ] immagini multi-arch o limitazioni documentate

## Risorse

Delta CPU/RAM, misure `docker stats`, porte e volumi aggiunti. Scrivere `nessuno` se non applicabile.

## Sicurezza e supply chain

Nuove dipendenze/immagini, vulnerabilità note, privilegi, secret e mitigazioni.

## Rischi e rollback

Rischi residui e comando/procedura di rollback.

## Handoff

- Commit HEAD:
- Report/artefatti:
- Decisioni richieste al reviewer:
