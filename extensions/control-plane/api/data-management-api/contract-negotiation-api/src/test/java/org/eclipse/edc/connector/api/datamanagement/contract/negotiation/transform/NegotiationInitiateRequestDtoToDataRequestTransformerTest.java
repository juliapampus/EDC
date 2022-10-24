/*
 *  Copyright (c) 2022 Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       Bayerische Motoren Werke Aktiengesellschaft (BMW AG) - initial API and implementation
 *
 */

package org.eclipse.edc.connector.api.datamanagement.contract.negotiation.transform;

import org.eclipse.edc.connector.api.datamanagement.contract.negotiation.model.NegotiationInitiateRequestDto;
import org.eclipse.edc.transform.spi.TransformerContext;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.edc.connector.api.datamanagement.contract.negotiation.TestFunctions.createOffer;
import static org.eclipse.edc.connector.contract.spi.types.negotiation.ContractOfferRequest.Type.INITIAL;
import static org.mockito.Mockito.mock;

class NegotiationInitiateRequestDtoToDataRequestTransformerTest {

    private final TransformerContext context = mock(TransformerContext.class);
    private final NegotiationInitiateRequestDtoToDataRequestTransformer transformer = new NegotiationInitiateRequestDtoToDataRequestTransformer();

    @Test
    void inputOutputType() {
        assertThat(transformer.getInputType()).isNotNull();
        assertThat(transformer.getOutputType()).isNotNull();
    }

    @Test
    void verify_transform() {
        var dto = NegotiationInitiateRequestDto.Builder.newInstance()
                .connectorId("connectorId")
                .connectorAddress("address")
                .protocol("protocol")
                .offer(createOffer("offerId", "assetId"))
                .build();

        var request = transformer.transform(dto, context);

        assertThat(request.getConnectorId()).isEqualTo("connectorId");
        assertThat(request.getConnectorAddress()).isEqualTo("address");
        assertThat(request.getProtocol()).isEqualTo("protocol");
        assertThat(request.getType()).isEqualTo(INITIAL);
        assertThat(request.getContractOffer().getId()).isEqualTo("offerId");
        assertThat(request.getContractOffer().getPolicy()).isNotNull();
    }
}