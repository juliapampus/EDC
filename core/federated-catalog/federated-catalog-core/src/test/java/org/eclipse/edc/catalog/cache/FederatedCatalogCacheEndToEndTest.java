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
 *       Microsoft Corporation - Initial implementation
 *
 */

package org.eclipse.edc.catalog.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.Call;
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.eclipse.edc.catalog.spi.FederatedCacheStore;
import org.eclipse.edc.connector.contract.spi.types.offer.ContractOffer;
import org.eclipse.edc.junit.extensions.EdcExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.edc.catalog.cache.TestUtil.createOffer;
import static org.eclipse.edc.junit.testfixtures.TestUtils.getFreePort;
import static org.eclipse.edc.junit.testfixtures.TestUtils.testOkHttpClient;


@ExtendWith(EdcExtension.class)
class FederatedCatalogCacheEndToEndTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String PORT = String.valueOf(getFreePort());

    @BeforeEach
    void setup(EdcExtension extension) {
        extension.setConfiguration(Map.of("web.http.port", PORT));
    }

    @Test
    void verifySuccess(FederatedCacheStore store) throws IOException {
        int nbAssets = 3;

        // generate assets and populate the store
        List<ContractOffer> assets = new ArrayList<>();
        for (int i = 0; i < nbAssets; i++) {
            assets.add(createOffer("some-offer-" + i));
        }
        assets.forEach(store::save);

        // here the content of the catalog cache store can be queried through http://localhost:8181/api/catalog
        RequestBody body = RequestBody.create("{}", MediaType.parse("application/json"));
        Request request = new Request.Builder()
                .url("http://localhost:" + PORT + "/api/federatedcatalog")
                .post(body)
                .build();

        Call call = testOkHttpClient().newCall(request);
        Response response = call.execute();

        // test
        assertThat(response.code()).isEqualTo(200);
        List<ContractOffer> actualAssets = Arrays.asList(MAPPER.readValue(Objects.requireNonNull(response.body()).string(), ContractOffer[].class));
        compareAssetsById(actualAssets, assets);
    }

    private void compareAssetsById(List<ContractOffer> actual, List<ContractOffer> expected) {
        List<String> actualAssetIds = actual.stream().map(co -> co.getAsset().getId()).sorted().collect(Collectors.toList());
        List<String> expectedAssetIds = expected.stream().map(co -> co.getAsset().getId()).sorted().collect(Collectors.toList());
        assertThat(actualAssetIds).isEqualTo(expectedAssetIds);
    }
}