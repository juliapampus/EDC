/*
 *  Copyright (c) 2022 Fraunhofer Institute for Software and Systems Engineering
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       Fraunhofer Institute for Software and Systems Engineering - initial API and implementation
 *
 */

package org.eclipse.dataspaceconnector.ids.api.multipart.dispatcher.sender;

import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.OkHttpClient;
import org.eclipse.dataspaceconnector.ids.spi.transform.IdsTransformerRegistry;
import org.eclipse.dataspaceconnector.spi.iam.IdentityService;
import org.eclipse.dataspaceconnector.spi.message.MessageContext;
import org.eclipse.dataspaceconnector.spi.monitor.Monitor;
import org.jetbrains.annotations.NotNull;

public class DelegateMessageContext implements MessageContext {
    private final String connectorId;
    private final OkHttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final Monitor monitor;
    private final IdentityService identityService;
    private final IdsTransformerRegistry transformerRegistry;

    public DelegateMessageContext(@NotNull String connectorId,
                                  @NotNull OkHttpClient httpClient,
                                  @NotNull ObjectMapper objectMapper,
                                  @NotNull Monitor monitor,
                                  @NotNull IdentityService identityService,
                                  @NotNull IdsTransformerRegistry transformerRegistry) {
        this.connectorId = connectorId;
        this.httpClient = httpClient;
        this.objectMapper = objectMapper;
        this.monitor = monitor;
        this.identityService = identityService;
        this.transformerRegistry = transformerRegistry;
    }

    @Override
    public String getProcessId() {
        return null;
    }

    public String getConnectorId() {
        return connectorId;
    }

    public OkHttpClient getHttpClient() {
        return httpClient;
    }

    public ObjectMapper getObjectMapper() {
        return objectMapper;
    }

    public Monitor getMonitor() {
        return monitor;
    }

    public IdentityService getIdentityService() {
        return identityService;
    }

    public IdsTransformerRegistry getTransformerRegistry() {
        return transformerRegistry;
    }
}
