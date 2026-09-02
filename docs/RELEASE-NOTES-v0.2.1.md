# kafkanuts v0.2.1

Release documentale che riallinea il README al prodotto realmente eseguito.

## Cosa cambia

- NATS JetStream, i tre simulatori, Apicurio, PostgreSQL, console e dashboard sono presentati come prodotto corrente fin dall'apertura.
- Kafka, Confluent Platform, la migrazione e Flink sono spostati nella sezione delle evoluzioni possibili e dichiarati esplicitamente assenti dallo stack.
- La procedura di avvio chiarisce prerequisiti, attesa degli healthcheck e verifica degli otto servizi.
- L'URL della dashboard è evidenziato come indirizzo da aprire manualmente nel browser: <http://localhost:8090>.
- È spiegata la differenza tra la porta web `8090`, il monitoring NATS `8222` e la porta applicativa NATS `4222`.
- Persistenza, test e limitazioni restano descritti senza introdurre nuovi claim funzionali.

## Compatibilità

Nessun file applicativo o di configurazione è cambiato. I comandi, le porte, i dati persistenti e il comportamento dello stack sono identici a `v0.2.0`.

## Verifica

La release modifica soltanto documentazione. Sono stati verificati staticamente i riferimenti del README contro `compose.yaml`, il runbook e i file presenti nel repository. Lo stack non è stato avviato.

## Limitations

Restano valide tutte le voci `NOT_TESTED` e `NOT_EXERCISED` riportate nella sezione [Limiti dichiarati](../README.md#limiti-dichiarati) del README. Questa release non aggiunge evidenze funzionali, prestazionali, di sicurezza o di compatibilità.
