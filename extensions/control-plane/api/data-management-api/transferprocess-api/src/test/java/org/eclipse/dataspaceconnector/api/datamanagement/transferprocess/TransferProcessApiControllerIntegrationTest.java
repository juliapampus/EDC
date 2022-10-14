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
 *
 */

package org.eclipse.dataspaceconnector.api.datamanagement.transferprocess;

import io.restassured.specification.RequestSpecification;
import org.eclipse.dataspaceconnector.api.datamanagement.transferprocess.model.TransferRequestDto;
import org.eclipse.dataspaceconnector.api.model.CriterionDto;
import org.eclipse.dataspaceconnector.api.query.QuerySpecDto;
import org.eclipse.dataspaceconnector.junit.extensions.EdcExtension;
import org.eclipse.dataspaceconnector.spi.transfer.store.TransferProcessStore;
import org.eclipse.dataspaceconnector.spi.types.domain.DataAddress;
import org.eclipse.dataspaceconnector.spi.types.domain.transfer.DataRequest;
import org.eclipse.dataspaceconnector.spi.types.domain.transfer.TransferProcess;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static io.restassured.RestAssured.given;
import static io.restassured.http.ContentType.JSON;
import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.dataspaceconnector.junit.testfixtures.TestUtils.getFreePort;
import static org.eclipse.dataspaceconnector.spi.types.domain.transfer.TransferProcessStates.COMPLETED;
import static org.eclipse.dataspaceconnector.spi.types.domain.transfer.TransferProcessStates.INITIAL;
import static org.eclipse.dataspaceconnector.spi.types.domain.transfer.TransferProcessStates.IN_PROGRESS;
import static org.eclipse.dataspaceconnector.spi.types.domain.transfer.TransferProcessStates.PROVISIONING;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;


@ExtendWith(EdcExtension.class)
class TransferProcessApiControllerIntegrationTest {

    public static final String PROCESS_ID = "processId";
    private final int port = getFreePort();
    private final String authKey = "123456";

    @BeforeEach
    void setUp(EdcExtension extension) {
        extension.setConfiguration(Map.of(
                "web.http.port", String.valueOf(getFreePort()),
                "web.http.path", "/api",
                "web.http.data.port", String.valueOf(port),
                "web.http.data.path", "/api/v1/data",
                "edc.api.auth.key", authKey
        ));
    }

    @Test
    void getAllTransferProcesses(TransferProcessStore store) {
        store.create(createTransferProcess(PROCESS_ID));

        baseRequest()
                .get("/transferprocess")
                .then()
                .statusCode(200)
                .contentType(JSON)
                .body("size()", is(1));
    }

    @Test
    void getAll_invalidQuery() {
        baseRequest()
                .get("/transferprocess?limit=1&offset=-1&filter=&sortField=")
                .then()
                .statusCode(400);
    }


    @Test
    void queryAllTransferProcesses(TransferProcessStore store) {
        store.create(createTransferProcess(PROCESS_ID));

        baseRequest()
                .contentType(JSON)
                .post("/transferprocess/request")
                .then()
                .statusCode(200)
                .contentType(JSON)
                .body("size()", is(1));
    }

    @Test
    void queryAllTransferProcesses_withPaging(TransferProcessStore store) {
        IntStream.range(0, 10)
                .forEach(i -> store.create(createTransferProcess(PROCESS_ID + i)));

        baseRequest()
                .contentType(JSON)
                .body(QuerySpecDto.Builder.newInstance().limit(5).offset(3).build())
                .post("/transferprocess/request")
                .then()
                .statusCode(200)
                .contentType(JSON)
                .body("size()", is(5));
    }

    @Test
    void queryAllTransferProcesses_withFilter(TransferProcessStore store) {
        IntStream.range(0, 10)
                .forEach(i -> store.create(createTransferProcess(PROCESS_ID + i)));

        baseRequest()
                .contentType(JSON)
                .body(QuerySpecDto.Builder.newInstance().filterExpression(List.of(CriterionDto.from("id", "in", List.of(PROCESS_ID + 1, PROCESS_ID + 2)))).build())
                .post("/transferprocess/request")
                .then()
                .statusCode(200)
                .contentType(JSON)
                .body("size()", is(2));
    }


    @Test
    void getSingleTransferProcess(TransferProcessStore store) {
        store.create(createTransferProcess(PROCESS_ID));
        baseRequest()
                .get("/transferprocess/" + PROCESS_ID)
                .then()
                .statusCode(200)
                .contentType(JSON)
                .body("id", is(PROCESS_ID))
                .body("dataRequest.id", notNullValue());

    }

    @Test
    void getSingletransferProcess_notFound() {
        baseRequest()
                .get("/transferprocess/nonExistingId")
                .then()
                .statusCode(404);
    }

    @Test
    void getSingleTransferProcessState(TransferProcessStore store) {
        store.create(createTransferProcess(PROCESS_ID, PROVISIONING.code()));

        var state = baseRequest()
                .get("/transferprocess/" + PROCESS_ID + "/state")
                .then()
                .statusCode(200)
                .contentType(JSON)
                .extract().asString();

        assertThat(state).isEqualTo("{\"state\":\"PROVISIONING\"}");
    }

    @Test
    void getSingletransferProcessState_notFound() {
        baseRequest()
                .get("/transferprocess/nonExistingId/state")
                .then()
                .statusCode(404);
    }

    @Test
    void cancel(TransferProcessStore store) {
        store.create(createTransferProcess(PROCESS_ID, IN_PROGRESS.code()));

        baseRequest()
                .contentType(JSON)
                .post("/transferprocess/" + PROCESS_ID + "/cancel")
                .then()
                .statusCode(204);
    }

    @Test
    void cancel_notFound() {
        baseRequest()
                .contentType(JSON)
                .post("/transferprocess/nonExistingId/cancel")
                .then()
                .statusCode(404);
    }

    @Test
    void cancel_conflict(TransferProcessStore store) {
        store.create(createTransferProcess(PROCESS_ID, COMPLETED.code()));
        baseRequest()
                .contentType(JSON)
                .post("/transferprocess/" + PROCESS_ID + "/cancel")
                .then()
                .statusCode(409);
    }

    @Test
    void deprovision(TransferProcessStore store) {
        store.create(createTransferProcess(PROCESS_ID, COMPLETED.code()));

        baseRequest()
                .contentType(JSON)
                .post("/transferprocess/" + PROCESS_ID + "/deprovision")
                .then()
                .statusCode(204);
    }

    @Test
    void deprovision_notFound() {
        baseRequest()
                .contentType(JSON)
                .post("/transferprocess/nonExistingId/deprovision")
                .then()
                .statusCode(404);
    }

    @Test
    void deprovision_conflict(TransferProcessStore store) {
        store.create(createTransferProcess(PROCESS_ID, INITIAL.code()));

        baseRequest()
                .contentType(JSON)
                .post("/transferprocess/" + PROCESS_ID + "/deprovision")
                .then()
                .statusCode(409);
    }

    @Test
    void initiateRequest() {
        var request = TransferRequestDto.Builder.newInstance()
                .id("id")
                .connectorAddress("http://some-contract")
                .contractId("some-contract")
                .protocol("test-asset")
                .assetId("assetId")
                .dataDestination(DataAddress.Builder.newInstance().type("test-type").build())
                .connectorId("connectorId")
                .properties(Map.of("prop", "value"))
                .build();

        baseRequest()
                .contentType(JSON)
                .body(request)
                .post("/transferprocess")
                .then()
                .statusCode(200)
                .contentType(JSON)
                .body("id", not(emptyString()))
                .body("createdAt", not("0"));
    }

    @Test
    void initiateRequest_invalidBody() {
        var request = TransferRequestDto.Builder.newInstance()
                .connectorAddress("http://some-contract")
                .contractId(null) //violation
                .protocol("test-asset")
                .assetId("assetId")
                .dataDestination(DataAddress.Builder.newInstance().type("test-type").build())
                .connectorId("connectorId")
                .properties(Map.of("prop", "value"))
                .build();

        var result = baseRequest()
                .contentType(JSON)
                .body(request)
                .post("/transferprocess")
                .then()
                .statusCode(400)
                .extract().body().asString();

        assertThat(result).isNotBlank();
    }

    @Test
    void initiateRequest_badRequest() {
        baseRequest()
                .contentType(JSON)
                .body("bad-request")
                .post("/transferprocess")
                .then()
                .statusCode(400)
                .extract().body().asString();
    }

    private RequestSpecification baseRequest() {
        return given()
                .baseUri("http://localhost:" + port)
                .basePath("/api/v1/data")
                .header("x-api-key", authKey)
                .when();
    }

    private TransferProcess createTransferProcess(String processId) {
        return createTransferProcessBuilder()
                .id(processId)
                .dataRequest(DataRequest.Builder.newInstance().id("id").destinationType("file").build())
                .build();
    }

    private TransferProcess createTransferProcess(String processId, int state) {
        return createTransferProcessBuilder()
                .id(processId)
                .state(state)
                .dataRequest(DataRequest.Builder.newInstance().destinationType("file").build())
                .build();
    }

    private TransferProcess.Builder createTransferProcessBuilder() {
        return TransferProcess.Builder.newInstance();
    }

}
