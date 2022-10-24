/*
 *  Copyright (c) 2021 - 2022 Daimler TSS GmbH
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       Daimler TSS GmbH - Initial API and Implementation
 *       Fraunhofer Institute for Software and Systems Engineering - add contract negotiation functionality
 *       Bayerische Motoren Werke Aktiengesellschaft (BMW AG) - improvements
 *       Microsoft Corporation - improvements
 *
 */

package org.eclipse.edc.connector.contract;

import org.eclipse.dataspaceconnector.runtime.metamodel.annotation.CoreExtension;
import org.eclipse.dataspaceconnector.runtime.metamodel.annotation.Inject;
import org.eclipse.dataspaceconnector.runtime.metamodel.annotation.Provides;
import org.eclipse.dataspaceconnector.runtime.metamodel.annotation.Setting;
import org.eclipse.edc.connector.contract.listener.ContractNegotiationEventListener;
import org.eclipse.edc.connector.contract.negotiation.ConsumerContractNegotiationManagerImpl;
import org.eclipse.edc.connector.contract.negotiation.ProviderContractNegotiationManagerImpl;
import org.eclipse.edc.connector.contract.observe.ContractNegotiationObservableImpl;
import org.eclipse.edc.connector.contract.offer.ContractDefinitionServiceImpl;
import org.eclipse.edc.connector.contract.offer.ContractOfferResolverImpl;
import org.eclipse.edc.connector.contract.policy.PolicyArchiveImpl;
import org.eclipse.edc.connector.contract.policy.PolicyEquality;
import org.eclipse.edc.connector.contract.spi.negotiation.ConsumerContractNegotiationManager;
import org.eclipse.edc.connector.contract.spi.negotiation.NegotiationWaitStrategy;
import org.eclipse.edc.connector.contract.spi.negotiation.ProviderContractNegotiationManager;
import org.eclipse.edc.connector.contract.spi.negotiation.observe.ContractNegotiationObservable;
import org.eclipse.edc.connector.contract.spi.negotiation.store.ContractNegotiationStore;
import org.eclipse.edc.connector.contract.spi.offer.ContractDefinitionService;
import org.eclipse.edc.connector.contract.spi.offer.ContractOfferResolver;
import org.eclipse.edc.connector.contract.spi.offer.store.ContractDefinitionStore;
import org.eclipse.edc.connector.contract.spi.types.negotiation.ContractNegotiation;
import org.eclipse.edc.connector.contract.spi.types.negotiation.command.ContractNegotiationCommand;
import org.eclipse.edc.connector.contract.spi.validation.ContractValidationService;
import org.eclipse.edc.connector.contract.validation.ContractValidationServiceImpl;
import org.eclipse.edc.connector.policy.spi.store.PolicyArchive;
import org.eclipse.edc.connector.policy.spi.store.PolicyDefinitionStore;
import org.eclipse.edc.policy.engine.spi.PolicyEngine;
import org.eclipse.edc.spi.agent.ParticipantAgentService;
import org.eclipse.edc.spi.asset.AssetIndex;
import org.eclipse.edc.spi.command.BoundedCommandQueue;
import org.eclipse.edc.spi.command.CommandHandlerRegistry;
import org.eclipse.edc.spi.command.CommandQueue;
import org.eclipse.edc.spi.command.CommandRunner;
import org.eclipse.edc.spi.event.EventRouter;
import org.eclipse.edc.spi.message.RemoteMessageDispatcherRegistry;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.retry.ExponentialWaitStrategy;
import org.eclipse.edc.spi.system.ExecutorInstrumentation;
import org.eclipse.edc.spi.system.ServiceExtension;
import org.eclipse.edc.spi.system.ServiceExtensionContext;
import org.eclipse.edc.spi.telemetry.Telemetry;
import org.eclipse.edc.statemachine.retry.EntitySendRetryManager;
import org.jetbrains.annotations.NotNull;

import java.time.Clock;

@Provides({
        ContractOfferResolver.class, ContractValidationService.class, ConsumerContractNegotiationManager.class,
        PolicyArchive.class, ProviderContractNegotiationManager.class, ContractNegotiationObservable.class,
        ContractDefinitionService.class
})
@CoreExtension
public class ContractServiceExtension implements ServiceExtension {

    private static final long DEFAULT_ITERATION_WAIT = 5000; // millis
    @Setting
    private static final String NEGOTIATION_CONSUMER_STATE_MACHINE_BATCH_SIZE = "edc.negotiation.consumer.state-machine.batch-size";
    @Setting
    private static final String NEGOTIATION_PROVIDER_STATE_MACHINE_BATCH_SIZE = "edc.negotiation.provider.state-machine.batch-size";
    @Setting
    private static final String NEGOTIATION_CONSUMER_SEND_RETRY_LIMIT = "edc.negotiation.consumer.send.retry.limit";
    @Setting
    private static final String NEGOTIATION_PROVIDER_SEND_RETRY_LIMIT = "edc.negotiation.provider.send.retry.limit";
    @Setting
    private static final String NEGOTIATION_CONSUMER_SEND_RETRY_BASE_DELAY_MS = "edc.negotiation.consumer.send.retry.base-delay.ms";
    @Setting
    private static final String NEGOTIATION_PROVIDER_SEND_RETRY_BASE_DELAY_MS = "edc.negotiation.provider.send.retry.base-delay.ms";

    private ConsumerContractNegotiationManagerImpl consumerNegotiationManager;
    private ProviderContractNegotiationManagerImpl providerNegotiationManager;
    @Inject
    private AssetIndex assetIndex;

    @Inject
    private ContractDefinitionStore contractDefinitionStore;

    @Inject
    private RemoteMessageDispatcherRegistry dispatcherRegistry;

    @Inject
    private CommandHandlerRegistry commandHandlerRegistry;

    @Inject
    private ContractNegotiationStore store;

    @Inject
    private ParticipantAgentService agentService;

    @Inject
    private PolicyEngine policyEngine;

    @Inject
    private PolicyDefinitionStore policyStore;

    @Inject
    private Monitor monitor;

    @Inject
    private Telemetry telemetry;

    @Inject
    private Clock clock;

    @Inject
    private EventRouter eventRouter;

    @Override
    public String name() {
        return "Core Contract Service";
    }

    @Override
    public void initialize(ServiceExtensionContext context) {
        registerTypes(context);
        registerServices(context);
    }

    @Override
    public void start() {
        consumerNegotiationManager.start();
        providerNegotiationManager.start();
    }

    @Override
    public void shutdown() {
        if (consumerNegotiationManager != null) {
            consumerNegotiationManager.stop();
        }

        if (providerNegotiationManager != null) {
            providerNegotiationManager.stop();
        }
    }

    private void registerServices(ServiceExtensionContext context) {
        var definitionService = new ContractDefinitionServiceImpl(monitor, contractDefinitionStore, policyEngine, policyStore);
        context.registerService(ContractDefinitionService.class, definitionService);

        var contractOfferService = new ContractOfferResolverImpl(agentService, definitionService, assetIndex, policyStore);
        context.registerService(ContractOfferResolver.class, contractOfferService);

        var policyEquality = new PolicyEquality(context.getTypeManager());
        var validationService = new ContractValidationServiceImpl(agentService, definitionService, assetIndex, policyStore, clock, policyEngine, policyEquality);
        context.registerService(ContractValidationService.class, validationService);

        var waitStrategy = context.hasService(NegotiationWaitStrategy.class) ? context.getService(NegotiationWaitStrategy.class) : new ExponentialWaitStrategy(DEFAULT_ITERATION_WAIT);

        CommandQueue<ContractNegotiationCommand> commandQueue = new BoundedCommandQueue<>(10);
        CommandRunner<ContractNegotiationCommand> commandRunner = new CommandRunner<>(commandHandlerRegistry, monitor);

        var observable = new ContractNegotiationObservableImpl();
        observable.registerListener(new ContractNegotiationEventListener(eventRouter, clock));

        context.registerService(ContractNegotiationObservable.class, observable);
        context.registerService(PolicyArchive.class, new PolicyArchiveImpl(store));

        consumerNegotiationManager = ConsumerContractNegotiationManagerImpl.Builder.newInstance()
                .waitStrategy(waitStrategy)
                .dispatcherRegistry(dispatcherRegistry)
                .monitor(monitor)
                .validationService(validationService)
                .commandQueue(commandQueue)
                .commandRunner(commandRunner)
                .observable(observable)
                .clock(clock)
                .telemetry(telemetry)
                .executorInstrumentation(context.getService(ExecutorInstrumentation.class))
                .store(store)
                .policyStore(policyStore)
                .batchSize(context.getSetting(NEGOTIATION_CONSUMER_STATE_MACHINE_BATCH_SIZE, 5))
                .sendRetryManager(consumerSendRetryManager(context))
                .build();

        providerNegotiationManager = ProviderContractNegotiationManagerImpl.Builder.newInstance()
                .waitStrategy(waitStrategy)
                .dispatcherRegistry(dispatcherRegistry)
                .monitor(monitor)
                .validationService(validationService)
                .commandQueue(commandQueue)
                .commandRunner(commandRunner)
                .observable(observable)
                .clock(clock)
                .telemetry(telemetry)
                .executorInstrumentation(context.getService(ExecutorInstrumentation.class))
                .store(store)
                .policyStore(policyStore)
                .batchSize(context.getSetting(NEGOTIATION_PROVIDER_STATE_MACHINE_BATCH_SIZE, 5))
                .sendRetryManager(providerSendRetryManager(context))
                .build();

        context.registerService(ConsumerContractNegotiationManager.class, consumerNegotiationManager);
        context.registerService(ProviderContractNegotiationManager.class, providerNegotiationManager);
    }

    private EntitySendRetryManager providerSendRetryManager(ServiceExtensionContext context) {
        var retryLimit = context.getSetting(NEGOTIATION_PROVIDER_SEND_RETRY_LIMIT, 7);
        var retryBaseDelay = context.getSetting(NEGOTIATION_PROVIDER_SEND_RETRY_BASE_DELAY_MS, 100L);
        return new EntitySendRetryManager(monitor, () -> new ExponentialWaitStrategy(retryBaseDelay), clock, retryLimit);
    }

    @NotNull
    private EntitySendRetryManager consumerSendRetryManager(ServiceExtensionContext context) {
        var retryLimit = context.getSetting(NEGOTIATION_CONSUMER_SEND_RETRY_LIMIT, 7);
        var retryBaseDelay = context.getSetting(NEGOTIATION_CONSUMER_SEND_RETRY_BASE_DELAY_MS, 100L);
        return new EntitySendRetryManager(monitor, () -> new ExponentialWaitStrategy(retryBaseDelay), clock, retryLimit);
    }

    private void registerTypes(ServiceExtensionContext context) {
        var typeManager = context.getTypeManager();
        typeManager.registerTypes(ContractNegotiation.class);
    }

}
