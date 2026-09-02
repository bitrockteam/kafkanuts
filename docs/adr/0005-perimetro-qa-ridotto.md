# ADR 0005: Perimetro QA ridotto per la release v0.1.0

- Stato: accettato
- Data: 2026-09-02
- Sostituisce parzialmente: `docs/PLAN.md` sezioni 12 e 16, criteri dei task T11-T15

## Contesto

`AGENTS.md` vieta di ridurre test, controlli di sicurezza o limiti risorse senza una decisione documentata e approvata. Questa ADR è quella decisione.

Il piano approvato in T00 descrive una strategia di verifica dimensionata su un prodotto, non su un laboratorio dimostrativo: matrice failure completa, chaos test, burst, soak, dataset di capacità, percentili di latenza, profilo NATS HA, layer Testcontainers, CI cross-platform su tre runner e una supply chain completa (CodeQL, Trivy, SBOM, provenance, firma).

Alla data di questa ADR sono `DONE` T00-T05 e T10, con T06 in attesa di pull request. Il costo residuo della sola verifica supera il costo di implementazione dei task rimanenti, e la maggior parte di quel costo non produce l'evidenza che il progetto deve dimostrare: che una migrazione Kafka/Confluent verso NATS/JetStream/Apicurio preserva contratto evento, identità degli schema e outcome logici.

Lo sponsor umano ha esplicitamente richiesto di ridurre il perimetro di QA e testing mantenendo l'essenziale, e di portare a termine il piano entro il perimetro ridotto.

## Decisione

Il perimetro di verifica della release v0.1.0 è ridotto secondo la tabella in `docs/QA-SCOPE.md`, che è normativa e prevale sulla sezione 12 di `docs/PLAN.md`.

Criteri della riduzione:

1. **Resta** ciò che verifica una garanzia dichiarata al lettore del laboratorio: contratto Avro, compatibilità positiva e negativa, identità degli schema attraverso i registry, parità funzionale, rollback, e i modi di fallimento che il progetto afferma di gestire.
2. **Cade** ciò che produce numeri di dimensionamento o robustezza operativa: percentili, throughput, burst, soak, dataset di capacità, alta disponibilità, matrice failure esaustiva.
3. **Cade** l'infrastruttura di verifica ridondante rispetto a Compose: layer Testcontainers, runner CI Windows e macOS, report JUnit per le demo.
4. **Si riduce** la supply chain a build, test e ricerca di segreti, rimandando analisi statica, scanner immagini, SBOM, provenance e firma.

Ogni requisito non più esercitato assume lo stato `NOT_TESTED` con motivazione, nel report del gate corrispondente e nella sezione *Limitations* di `README.md` e delle release notes. Non è ammesso convertire un requisito rimosso in un `PASS` semantico, né derivarne claim di prestazione, capacità o risparmio.

Il Gate G3 mantiene i criteri di correttezza (outcome terminali, assenza di perdita silenziosa, parità al 100% su stato finale e checksum normalizzato, visibilità dei duplicati, leggibilità dei messaggi prima e dopo il cambio registry, rollback) e perde i criteri prestazionali (latenza p50/p95/p99, throughput, RTO confrontato con soglie).

I gate G0, G1 e G2 restano invariati: sono già stati superati e riguardano fondazioni e contratto evento.

## Conseguenze

- La release v0.1.0 dimostra correttezza della migrazione, non idoneità alla produzione, e lo dichiara esplicitamente.
- Nessun dato di capacità o costo può essere derivato dal repository per la release v0.1.0; il modello economico esterno resta privo di input tecnici da questo laboratorio finché non viene approvata una nuova campagna di misura.
- I requisiti rimossi restano tracciati come `NOT_TESTED`, quindi reintroducibili in v0.2.0 senza riscrivere il piano.
- Il rischio accettato è che difetti visibili solo sotto carico, in HA o dopo lunga esecuzione non vengano rilevati prima della release.
