/*
 *  Copyright (c) 2021 Microsoft Corporation
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       Microsoft Corporation - initial API and implementation
 *       Fraunhofer Institute for Software and Systems Engineering - improvements, add policy engine
 *       Bayerische Motoren Werke Aktiengesellschaft (BMW AG) - improvements
 *
 */

package org.eclipse.edc.connector.contract.validation;

import org.eclipse.edc.connector.contract.policy.PolicyEquality;
import org.eclipse.edc.connector.contract.spi.ContractId;
import org.eclipse.edc.connector.contract.spi.offer.ContractDefinitionService;
import org.eclipse.edc.connector.contract.spi.types.agreement.ContractAgreement;
import org.eclipse.edc.connector.contract.spi.types.offer.ContractOffer;
import org.eclipse.edc.connector.contract.spi.validation.ContractValidationService;
import org.eclipse.edc.connector.policy.spi.store.PolicyDefinitionStore;
import org.eclipse.edc.policy.engine.spi.PolicyEngine;
import org.eclipse.edc.spi.agent.ParticipantAgentService;
import org.eclipse.edc.spi.asset.AssetIndex;
import org.eclipse.edc.spi.iam.ClaimToken;
import org.eclipse.edc.spi.result.Result;
import org.jetbrains.annotations.NotNull;

import java.time.Clock;

import static java.lang.String.format;

/**
 * Implementation of the {@link ContractValidationService}.
 */
public class ContractValidationServiceImpl implements ContractValidationService {

    private final ParticipantAgentService agentService;
    private final ContractDefinitionService contractDefinitionService;
    private final AssetIndex assetIndex;
    private final PolicyDefinitionStore policyStore;
    private final Clock clock;
    private final PolicyEngine policyEngine;
    private final PolicyEquality policyEquality;

    public ContractValidationServiceImpl(ParticipantAgentService agentService, ContractDefinitionService contractDefinitionService,
                                         AssetIndex assetIndex, PolicyDefinitionStore policyStore, Clock clock,
                                         PolicyEngine policyEngine, PolicyEquality policyEquality) {
        this.agentService = agentService;
        this.contractDefinitionService = contractDefinitionService;
        this.assetIndex = assetIndex;
        this.policyStore = policyStore;
        this.clock = clock;
        this.policyEngine = policyEngine;
        this.policyEquality = policyEquality;
    }

    @Override
    @NotNull
    public Result<ContractOffer> validateInitialOffer(ClaimToken token, ContractOffer offer) {
        if (isMandatoryAttributeMissing(offer)) {
            return Result.failure("Mandatory attributes are missing.");
        }

        var contractId = ContractId.parse(offer.getId());
        if (!contractId.isValid()) {
            return Result.failure("Invalid id: " + offer.getId());
        }

        var agent = agentService.createFor(token);
        var contractDefinition = contractDefinitionService.definitionFor(agent, contractId.definitionPart());
        if (contractDefinition == null) {
            return Result.failure("The ContractDefinition with id %s either does not exist or the access to it is not granted.");
        }

        var targetAsset = assetIndex.findById(offer.getAsset().getId());
        if (targetAsset == null) {
            return Result.failure("Invalid target: " + offer.getAsset().getId());
        }

        var contractPolicyDef = policyStore.findById(contractDefinition.getContractPolicyId());
        if (contractPolicyDef == null) {
            return Result.failure(format("Policy %s not found", contractDefinition.getContractPolicyId()));
        }

        if (!policyEquality.test(contractPolicyDef.getPolicy().withTarget(targetAsset.getId()), offer.getPolicy())) {
            return Result.failure("Policy in the contract offer is not equal to the one in the contract definition");
        }

        var contractPolicyResult = policyEngine.evaluate(NEGOTIATION_SCOPE, contractPolicyDef.getPolicy(), agent);
        if (contractPolicyResult.failed()) {
            return Result.failure(format("Policy %s not fulfilled", contractPolicyDef.getUid()));
        }

        var validatedOffer = ContractOffer.Builder.newInstance()
                .id(offer.getId())
                .asset(targetAsset)
                .policy(contractPolicyDef.getPolicy())
                .build();

        return Result.success(validatedOffer);
    }

    @Override
    public @NotNull Result<ContractOffer> validate(ClaimToken token, ContractOffer offer, ContractOffer latestOffer) {
        if (isMandatoryAttributeMissing(offer)) {
            return Result.failure("Mandatory attributes are missing.");
        }

        var contractId = ContractId.parse(offer.getId());
        if (!contractId.isValid()) {
            return Result.failure("Invalid id: " + offer.getId());
        }

        // TODO implement validation against latest offer within the negotiation

        var agent = agentService.createFor(token);
        var policyResult = policyEngine.evaluate(NEGOTIATION_SCOPE, offer.getPolicy(), agent);
        if (policyResult.failed()) {
            return Result.failure(format("Policy not fulfilled for ContractOffer %s", offer.getId()));
        }

        return Result.success(null);
    }

    @Override
    public boolean validateAgreement(ClaimToken token, ContractAgreement agreement) {
        var contractId = ContractId.parse(agreement.getId());
        if (!contractId.isValid()) {
            return false;
        }

        if (!isStarted(agreement) || isExpired(agreement)) {
            return false;
        }

        var agent = agentService.createFor(token);
        var contractDefinition = contractDefinitionService.definitionFor(agent, contractId.definitionPart());
        if (contractDefinition == null) {
            return false;
        }

        return policyEngine.evaluate(NEGOTIATION_SCOPE, agreement.getPolicy(), agent).succeeded();
        // TODO validate counter-party
    }

    @Override
    public Result<Void> validateConfirmed(ContractAgreement agreement, ContractOffer latestOffer) {
        var contractId = ContractId.parse(agreement.getId());
        if (!contractId.isValid()) {
            return Result.failure(format("ContractId %s does not follow the expected schema.", agreement.getId()));
        }

        if (!policyEquality.test(agreement.getPolicy(), latestOffer.getPolicy())) {
            return Result.failure("Policy in the contract agreement is not equal to the one in the contract offer");
        }

        return Result.success();
    }

    private boolean isExpired(ContractAgreement contractAgreement) {
        return contractAgreement.getContractEndDate() * 1000L < clock.millis();
    }

    private boolean isStarted(ContractAgreement contractAgreement) {
        return contractAgreement.getContractStartDate() * 1000L <= clock.millis();
    }

    private boolean isMandatoryAttributeMissing(ContractOffer offer) {
        // TODO add contractStart and contractEnd check as soon as the Infomodel serializer is used
        return offer.getProvider() == null || offer.getConsumer() == null;
    }
}
