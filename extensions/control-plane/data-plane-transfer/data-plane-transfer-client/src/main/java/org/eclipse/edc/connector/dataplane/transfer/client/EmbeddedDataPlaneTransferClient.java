/*
 *  Copyright (c) 2022 Amadeus
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       Amadeus - initial API and implementation
 *
 */

package org.eclipse.edc.connector.dataplane.transfer.client;

import io.opentelemetry.extension.annotations.WithSpan;
import org.eclipse.edc.connector.dataplane.spi.manager.DataPlaneManager;
import org.eclipse.edc.connector.dataplane.transfer.spi.client.DataPlaneTransferClient;
import org.eclipse.edc.spi.response.ResponseStatus;
import org.eclipse.edc.spi.response.StatusResult;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowRequest;

/**
 * Implementation of a {@link DataPlaneTransferClient} that uses a local {@link DataPlaneManager},
 * i.e. one that runs in the same JVM as the control plane.
 */
public class EmbeddedDataPlaneTransferClient implements DataPlaneTransferClient {

    private final DataPlaneManager dataPlaneManager;

    public EmbeddedDataPlaneTransferClient(DataPlaneManager dataPlaneManager) {
        this.dataPlaneManager = dataPlaneManager;
    }

    @WithSpan
    @Override
    public StatusResult<Void> transfer(DataFlowRequest request) {
        var result = dataPlaneManager.validate(request);
        if (result.failed()) {
            return StatusResult.failure(ResponseStatus.FATAL_ERROR, String.join(", ", result.getFailureMessages()));
        }
        dataPlaneManager.initiateTransfer(request);
        return StatusResult.success();
    }
}
