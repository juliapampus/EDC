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

package org.eclipse.edc.connector.store.sql.contract.definition;


import org.eclipse.edc.connector.contract.spi.offer.store.ContractDefinitionStore;
import org.eclipse.edc.connector.contract.spi.testfixtures.offer.store.ContractDefinitionStoreTestBase;
import org.eclipse.edc.connector.contract.spi.testfixtures.offer.store.TestFunctions;
import org.eclipse.edc.connector.store.sql.contract.definition.schema.BaseSqlDialectStatements;
import org.eclipse.edc.connector.store.sql.contract.definition.schema.postgres.PostgresDialectStatements;
import org.eclipse.edc.policy.model.PolicyRegistrationTypes;
import org.eclipse.edc.spi.query.Criterion;
import org.eclipse.edc.spi.query.QuerySpec;
import org.eclipse.edc.spi.query.SortOrder;
import org.eclipse.edc.spi.types.TypeManager;
import org.eclipse.edc.sql.testfixtures.PostgresqlLocalInstance;
import org.eclipse.edc.transaction.datasource.spi.DataSourceRegistry;
import org.eclipse.edc.transaction.spi.NoopTransactionContext;
import org.eclipse.edc.transaction.spi.TransactionContext;
import org.eclipse.edc.util.testfixtures.annotations.PostgresqlDbIntegrationTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.IntStream;
import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.eclipse.edc.sql.SqlQueryExecutor.executeQuery;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@PostgresqlDbIntegrationTest
class PostgresContractDefinitionStoreTest extends ContractDefinitionStoreTestBase {

    private static final String DATASOURCE_NAME = "contractdefinition";

    private final TransactionContext transactionContext = new NoopTransactionContext();
    private final DataSourceRegistry dataSourceRegistry = mock(DataSourceRegistry.class);
    private final BaseSqlDialectStatements statements = new PostgresDialectStatements();
    private final DataSource dataSource = mock(DataSource.class);
    private final Connection connection = spy(PostgresqlLocalInstance.getTestConnection());

    private SqlContractDefinitionStore sqlContractDefinitionStore;

    @BeforeAll
    static void prepare() {
        PostgresqlLocalInstance.createTestDatabase();
    }

    @BeforeEach
    void setUp() throws IOException, SQLException {
        when(dataSourceRegistry.resolve(DATASOURCE_NAME)).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        doNothing().when(connection).close();

        var typeManager = new TypeManager();
        typeManager.registerTypes(PolicyRegistrationTypes.TYPES.toArray(Class<?>[]::new));

        sqlContractDefinitionStore = new SqlContractDefinitionStore(dataSourceRegistry, DATASOURCE_NAME, transactionContext, statements, typeManager);
        var schema = Files.readString(Paths.get("./docs/schema.sql"));
        transactionContext.execute(() -> executeQuery(connection, schema));
    }

    @AfterEach
    void tearDown() throws SQLException {
        transactionContext.execute(() -> executeQuery(connection, "DROP TABLE " + statements.getContractDefinitionTable() + " CASCADE"));
        doCallRealMethod().when(connection).close();
        connection.close();
    }

    @Test
    @DisplayName("Context Loads, tables exist")
    void contextLoads() {
        var query = String.format("SELECT 1 FROM %s", statements.getContractDefinitionTable());

        var result = executeQuery(connection, query);

        assertThat(result).isNotNull();
    }

    @Test
    @DisplayName("Verify empty result when query contains invalid keys")
    void findAll_queryByInvalidKey() {

        var definitionsExpected = TestFunctions.createContractDefinitions(20);
        getContractDefinitionStore().save(definitionsExpected);

        var spec = QuerySpec.Builder.newInstance()
                .filter(List.of(new Criterion("notexist", "=", "somevalue")))
                .build();

        assertThatThrownBy(() -> getContractDefinitionStore().findAll(spec))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("Translation failed for Model");

    }

    // Override in PG since it does not have the field mapping
    @Test
    void findAll_verifySorting_invalidProperty() {
        IntStream.range(0, 10).mapToObj(i -> TestFunctions.createContractDefinition("id" + i)).forEach(getContractDefinitionStore()::save);
        var query = QuerySpec.Builder.newInstance().sortField("notexist").sortOrder(SortOrder.DESC).build();

        assertThatThrownBy(() -> getContractDefinitionStore().findAll(query)).isInstanceOf(IllegalArgumentException.class);
    }

    @Override
    protected ContractDefinitionStore getContractDefinitionStore() {
        return sqlContractDefinitionStore;
    }

    @Override
    protected boolean supportsCollectionQuery() {
        return true;
    }

    @Override
    protected boolean supportsCollectionIndexQuery() {
        return false;
    }

    @Override
    protected boolean supportsSortOrder() {
        return true;
    }

}
