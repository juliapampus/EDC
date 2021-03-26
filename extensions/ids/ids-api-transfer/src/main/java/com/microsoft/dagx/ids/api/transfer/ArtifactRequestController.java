package com.microsoft.dagx.ids.api.transfer;

import com.microsoft.dagx.ids.spi.daps.DapsService;
import com.microsoft.dagx.spi.transfer.flow.DataFlowInitiateResponse;
import com.microsoft.dagx.spi.transfer.flow.DataFlowManager;
import com.microsoft.dagx.spi.iam.VerificationResult;
import com.microsoft.dagx.spi.metadata.MetadataStore;
import com.microsoft.dagx.spi.monitor.Monitor;
import com.microsoft.dagx.spi.types.domain.metadata.DataEntry;
import com.microsoft.dagx.spi.types.domain.transfer.DataRequest;
import de.fraunhofer.iais.eis.ArtifactRequestMessage;
import de.fraunhofer.iais.eis.ArtifactResponseMessageBuilder;
import de.fraunhofer.iais.eis.RejectionMessageBuilder;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import static de.fraunhofer.iais.eis.RejectionReason.BAD_PARAMETERS;
import static de.fraunhofer.iais.eis.RejectionReason.NOT_AUTHENTICATED;
import static de.fraunhofer.iais.eis.RejectionReason.NOT_FOUND;
import static de.fraunhofer.iais.eis.RejectionReason.TEMPORARILY_NOT_AVAILABLE;
import static java.util.UUID.randomUUID;

/**
 * Receives incoming data transfer requests and processes them.
 */
@Consumes({MediaType.APPLICATION_JSON})
@Produces({MediaType.APPLICATION_JSON})
@Path("/ids")
public class ArtifactRequestController {
    private DapsService dapsService;
    private MetadataStore metadataStore;
    private DataFlowManager dataFlowManager;
    private Monitor monitor;

    public ArtifactRequestController(DapsService dapsService, MetadataStore metadataStore, DataFlowManager dataFlowManager, Monitor monitor) {
        this.dapsService = dapsService;
        this.metadataStore = metadataStore;
        this.dataFlowManager = dataFlowManager;
        this.monitor = monitor;
    }

    @POST
    @Path("request")
    public Response request(ArtifactRequestMessage message) {

        VerificationResult verificationResult = dapsService.verifyAndConvertToken(message.getSecurityToken().getTokenValue());
        if (!verificationResult.valid()) {
            return Response.status(Response.Status.FORBIDDEN).entity(new RejectionMessageBuilder()._rejectionReason_(NOT_AUTHENTICATED).build()).build();
        }

        // TODO enforce policy

        var dataUrn = message.getRequestedArtifact().toString();
        monitor.debug(() -> "Received artifact request for: " + dataUrn);

        DataEntry<?> entry = metadataStore.findForId(dataUrn);

        if (entry == null) {
            return Response.status(Response.Status.BAD_REQUEST).entity(new RejectionMessageBuilder()._rejectionReason_(NOT_FOUND).build()).build();
        }

        DataRequest dataRequest = DataRequest.Builder.newInstance().id(randomUUID().toString()).dataEntry(entry).build();

        DataFlowInitiateResponse response = dataFlowManager.initiate(dataRequest);
        switch (response.getStatus()) {
            case OK:
                monitor.info("Data transfer request initiated");
                ArtifactResponseMessageBuilder messageBuilder = new ArtifactResponseMessageBuilder();
                return Response.ok().entity(messageBuilder.build()).build();
            case FATAL_ERROR:
                return Response.status(Response.Status.BAD_REQUEST).entity(new RejectionMessageBuilder()._rejectionReason_(BAD_PARAMETERS).build()).build();
            default:
                return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(new RejectionMessageBuilder()._rejectionReason_(TEMPORARILY_NOT_AVAILABLE).build()).build();
        }
    }

}