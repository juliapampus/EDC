/*
 *  Copyright (c) 2020 - 2022 Fraunhofer Institute for Software and Systems Engineering
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       Fraunhofer Institute for Software and Systems Engineering - initial API and implementation
 *       Fraunhofer Institute for Software and Systems Engineering - refactoring
 *
 */

package org.eclipse.dataspaceconnector.ids.api.multipart.dispatcher.sender;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.fraunhofer.iais.eis.ArtifactRequestMessageBuilder;
import de.fraunhofer.iais.eis.DynamicAttributeToken;
import de.fraunhofer.iais.eis.Message;
import de.fraunhofer.iais.eis.RequestInProcessMessageImpl;
import de.fraunhofer.iais.eis.ResponseMessageImpl;
import org.eclipse.dataspaceconnector.ids.api.multipart.dispatcher.sender.response.IdsMultipartParts;
import org.eclipse.dataspaceconnector.ids.api.multipart.dispatcher.sender.response.MultipartResponse;
import org.eclipse.dataspaceconnector.ids.core.util.CalendarUtil;
import org.eclipse.dataspaceconnector.ids.spi.IdsType;
import org.eclipse.dataspaceconnector.ids.spi.spec.extension.ArtifactRequestMessagePayload;
import org.eclipse.dataspaceconnector.ids.spi.transform.IdsTransformerRegistry;
import org.eclipse.dataspaceconnector.ids.transform.IdsProtocol;
import org.eclipse.dataspaceconnector.ids.transform.util.TransformationUtil;
import org.eclipse.dataspaceconnector.spi.EdcException;
import org.eclipse.dataspaceconnector.spi.security.Vault;
import org.eclipse.dataspaceconnector.spi.types.domain.transfer.DataRequest;
import org.jetbrains.annotations.NotNull;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import static org.eclipse.dataspaceconnector.ids.api.multipart.dispatcher.sender.response.ResponseUtil.parseMultipartStringResponse;
import static org.eclipse.dataspaceconnector.ids.spi.IdsConstants.IDS_WEBHOOK_ADDRESS_PROPERTY;

/**
 * IdsMultipartSender implementation for data requests. Sends IDS ArtifactRequestMessages and
 * expects an IDS RequestInProcessMessage as the response.
 */
public class MultipartArtifactRequestSender implements MultipartSenderDelegate<DataRequest, MultipartResponse<String>> {
    private final Vault vault;
    private final String idsWebhookAddress;
    private final ObjectMapper objectMapper;
    private final IdsTransformerRegistry transformerRegistry;
    private final URI connectorId;

    public MultipartArtifactRequestSender(@NotNull DelegateMessageContext messageContext,
                                          @NotNull String idsWebhookAddress,
                                          @NotNull Vault vault) {
        this.vault = Objects.requireNonNull(vault);
        this.idsWebhookAddress = idsWebhookAddress;

        this.objectMapper = Objects.requireNonNull(messageContext.getObjectMapper());
        this.transformerRegistry = Objects.requireNonNull(messageContext.getTransformerRegistry());
        this.connectorId = URI.create(messageContext.getConnectorId());
    }

    /**
     * Builds an {@link de.fraunhofer.iais.eis.ArtifactRequestMessage} for the given {@link DataRequest}.
     *
     * @param request the request.
     * @param token   the dynamic attribute token.
     * @return an ArtifactRequestMessage
     */
    @Override
    public Message buildMessageHeader(DataRequest request, DynamicAttributeToken token) {
        var artifactIdsId = TransformationUtil.buildIdsId(request.getAssetId(), IdsType.ARTIFACT);
        var contractIdsId = TransformationUtil.buildIdsId(request.getContractId(), IdsType.CONTRACT);

        var artifactTransformationResult = transformerRegistry.transform(artifactIdsId, URI.class);
        if (artifactTransformationResult.failed()) {
            throw new EdcException("Failed to create artifact ID from asset.");
        }

        var contractTransformationResult = transformerRegistry.transform(contractIdsId, URI.class);
        if (contractTransformationResult.failed()) {
            throw new EdcException("Failed to create contract ID from asset.");
        }

        var artifactId = artifactTransformationResult.getContent();
        var contractId = contractTransformationResult.getContent();

        var messageId = request.getId() != null ? request.getId() : UUID.randomUUID().toString();
        var message = new ArtifactRequestMessageBuilder(URI.create(messageId))
                ._modelVersion_(IdsProtocol.INFORMATION_MODEL_VERSION)
                ._issued_(CalendarUtil.gregorianNow())
                ._securityToken_(token)
                ._issuerConnector_(connectorId)
                ._senderAgent_(connectorId)
                ._recipientConnector_(Collections.singletonList(URI.create(request.getConnectorId())))
                ._requestedArtifact_(artifactId)
                ._transferContract_(contractId)
                .build();

        message.setProperty(IDS_WEBHOOK_ADDRESS_PROPERTY, idsWebhookAddress);

        request.getProperties().forEach(message::setProperty);
        return message;
    }

    /**
     * Builds the payload for the artifact request. The payload contains the data destination and a secret key.
     *
     * @param request the request.
     * @return the message payload.
     * @throws Exception if parsing the payload fails.
     */
    @Override
    public String buildMessagePayload(DataRequest request) throws Exception {
        var builder = ArtifactRequestMessagePayload.Builder.newInstance()
                .dataDestination(request.getDataDestination());

        if (request.getDataDestination().getKeyName() != null) {
            String secret = vault.resolveSecret(request.getDataDestination().getKeyName());
            builder = builder.secret(secret);
        }

        return objectMapper.writeValueAsString(builder.build());
    }

    /**
     * Parses the response content.
     *
     * @param parts container object for response header and payload input streams.
     * @return a MultipartResponse containing the message header and the response payload as string.
     * @throws Exception if parsing header or payload fails.
     */
    @Override
    public MultipartResponse<String> getResponseContent(IdsMultipartParts parts) throws Exception {
        return parseMultipartStringResponse(parts, objectMapper);
    }

    @Override
    public List<Class<? extends Message>> getAllowedResponseTypes() {
        return List.of(ResponseMessageImpl.class, RequestInProcessMessageImpl.class); // TODO remove ResponseMessage.class
    }

    @Override
    public Class<DataRequest> getMessageType() {
        return DataRequest.class;
    }

    @Override
    public DelegateMessageContext getMessageContext() {
        return me;
    }
}
