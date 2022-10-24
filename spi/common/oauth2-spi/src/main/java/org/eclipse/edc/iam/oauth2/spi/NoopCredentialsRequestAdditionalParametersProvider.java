/*
 *  Copyright (c) 2022 Microsoft Corporation
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       Microsoft Corporation - initial API and implementation
 *
 */

package org.eclipse.edc.iam.oauth2.spi;

import org.eclipse.edc.spi.iam.TokenParameters;
import org.jetbrains.annotations.NotNull;

import java.util.Map;

import static java.util.Collections.emptyMap;

/**
 * No-op implementation for CredentialsRequestAdditionalParametersProvider
 */
public class NoopCredentialsRequestAdditionalParametersProvider implements CredentialsRequestAdditionalParametersProvider {

    @Override
    public @NotNull Map<String, String> provide(TokenParameters parameters) {
        return emptyMap();
    }
}
