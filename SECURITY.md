# Security Policy

## Segnalazione

Non aprire issue pubbliche per vulnerabilità non corrette. Usare GitHub Private Vulnerability Reporting quando sarà abilitato oppure contattare i maintainer dell'organizzazione Bitrock tramite un canale aziendale riservato.

## Baseline richiesta

- Gitleaks per segreti;
- Trivy per filesystem, configurazione e immagini;
- CodeQL per codice Java e script supportati;
- GitHub Dependency Review e Dependabot;
- SpotBugs con plugin di sicurezza, Checkstyle e formatter;
- CycloneDX SBOM per ogni release;
- immagini non-root ove supportato e privilegi/capability minimi;
- Actions pin a SHA e permessi workflow minimali;
- credenziali demo generate/locali, mai committate.

Vulnerabilità Critical o High bloccano merge e release salvo eccezione temporanea documentata in PR con owner, motivazione, compensazione e scadenza.
