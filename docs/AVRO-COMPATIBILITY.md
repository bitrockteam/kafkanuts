# Avro compatibility policy

T03 usa localmente la regola **BACKWARD**: un reader nuovo deve poter leggere dati scritti con lo schema precedente. La policy è verificata con `SchemaCompatibility.checkReaderWriterCompatibility(reader, writer)` nei contract test.

Questa è una policy locale del modulo e non sostituisce i futuri gate contro Confluent Schema Registry o Apicurio Registry, che saranno introdotti nei task successivi. In particolare, non implica ancora una garanzia cross-registry o wire compatibility.
