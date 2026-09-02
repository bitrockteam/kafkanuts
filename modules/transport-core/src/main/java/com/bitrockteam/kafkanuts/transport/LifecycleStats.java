package com.bitrockteam.kafkanuts.transport;

/**
 * Counters exposed by a running simulator.
 *
 * @param published events accepted by JetStream
 * @param duplicateAcks publishes the server answered as duplicates
 * @param deliveries deliveries received, redeliveries included
 * @param duplicateDeliveries deliveries carrying an already processed event
 * @param uniqueEvents distinct logical events processed
 * @param deadLettered messages routed to the dead letter stream
 */
public record LifecycleStats(
    long published,
    long duplicateAcks,
    long deliveries,
    long duplicateDeliveries,
    int uniqueEvents,
    long deadLettered) {}
