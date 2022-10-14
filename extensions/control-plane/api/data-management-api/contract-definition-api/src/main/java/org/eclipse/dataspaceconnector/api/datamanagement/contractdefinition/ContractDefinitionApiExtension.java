/*
 *  Copyright (c) 2020 - 2022 Microsoft Corporation
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       Microsoft Corporation - initial API and implementation
 *       Bayerische Motoren Werke Aktiengesellschaft (BMW AG) - improvements
 *
 */

package org.eclipse.dataspaceconnector.api.datamanagement.contractdefinition;

import org.eclipse.dataspaceconnector.api.datamanagement.configuration.DataManagementApiConfiguration;
import org.eclipse.dataspaceconnector.api.datamanagement.contractdefinition.service.ContractDefinitionEventListener;
import org.eclipse.dataspaceconnector.api.datamanagement.contractdefinition.service.ContractDefinitionService;
import org.eclipse.dataspaceconnector.api.datamanagement.contractdefinition.service.ContractDefinitionServiceImpl;
import org.eclipse.dataspaceconnector.api.datamanagement.contractdefinition.transform.ContractDefinitionRequestDtoToContractDefinitionTransformer;
import org.eclipse.dataspaceconnector.api.datamanagement.contractdefinition.transform.ContractDefinitionToContractDefinitionResponseDtoTransformer;
import org.eclipse.dataspaceconnector.api.transformer.DtoTransformerRegistry;
import org.eclipse.dataspaceconnector.runtime.metamodel.annotation.Extension;
import org.eclipse.dataspaceconnector.runtime.metamodel.annotation.Inject;
import org.eclipse.dataspaceconnector.runtime.metamodel.annotation.Provides;
import org.eclipse.dataspaceconnector.spi.WebService;
import org.eclipse.dataspaceconnector.spi.contract.definition.observe.ContractDefinitionObservableImpl;
import org.eclipse.dataspaceconnector.spi.contract.offer.store.ContractDefinitionStore;
import org.eclipse.dataspaceconnector.spi.event.EventRouter;
import org.eclipse.dataspaceconnector.spi.system.ServiceExtension;
import org.eclipse.dataspaceconnector.spi.system.ServiceExtensionContext;
import org.eclipse.dataspaceconnector.spi.transaction.TransactionContext;

import java.time.Clock;

@Provides(ContractDefinitionService.class)
@Extension(value = ContractDefinitionApiExtension.NAME)
public class ContractDefinitionApiExtension implements ServiceExtension {
    public static final String NAME = "Data Management API: Contract Definition";
    @Inject
    WebService webService;

    @Inject
    DataManagementApiConfiguration config;

    @Inject
    DtoTransformerRegistry transformerRegistry;

    @Inject
    ContractDefinitionStore contractDefinitionStore;

    @Inject
    TransactionContext transactionContext;

    @Inject
    Clock clock;

    @Inject
    EventRouter eventRouter;

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void initialize(ServiceExtensionContext context) {

        transformerRegistry.register(new ContractDefinitionToContractDefinitionResponseDtoTransformer());
        transformerRegistry.register(new ContractDefinitionRequestDtoToContractDefinitionTransformer());

        var monitor = context.getMonitor();

        var contractDefinitionObservable = new ContractDefinitionObservableImpl();
        contractDefinitionObservable.registerListener(new ContractDefinitionEventListener(clock, eventRouter));

        var service = new ContractDefinitionServiceImpl(contractDefinitionStore, transactionContext, contractDefinitionObservable);
        context.registerService(ContractDefinitionService.class, service);

        webService.registerResource(config.getContextAlias(), new ContractDefinitionApiController(monitor, service, transformerRegistry));
    }
}
