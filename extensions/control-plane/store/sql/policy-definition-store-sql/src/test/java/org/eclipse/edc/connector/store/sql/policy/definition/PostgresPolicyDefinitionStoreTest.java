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

package org.eclipse.edc.connector.store.sql.policy.definition;

import org.eclipse.edc.connector.policy.spi.PolicyDefinition;
import org.eclipse.edc.connector.policy.spi.testfixtures.store.PolicyDefinitionStoreTestBase;
import org.eclipse.edc.connector.store.sql.policy.definition.store.SqlPolicyDefinitionStore;
import org.eclipse.edc.connector.store.sql.policy.definition.store.schema.postgres.PostgresDialectStatements;
import org.eclipse.edc.policy.model.PolicyRegistrationTypes;
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
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.stream.IntStream;
import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.eclipse.edc.connector.policy.spi.testfixtures.TestFunctions.createPolicy;
import static org.eclipse.edc.connector.policy.spi.testfixtures.TestFunctions.createPolicyBuilder;
import static org.eclipse.edc.connector.policy.spi.testfixtures.TestFunctions.createQuery;
import static org.eclipse.edc.sql.SqlQueryExecutor.executeQuery;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * This test aims to verify those parts of the policy definition store, that are specific to Postgres, e.g. JSON query
 * operators.
 */
@PostgresqlDbIntegrationTest
class PostgresPolicyDefinitionStoreTest extends PolicyDefinitionStoreTestBase {
    private static final String DATASOURCE_NAME = "policydefinition";

    private final DataSourceRegistry dataSourceRegistry = mock(DataSourceRegistry.class);
    private final TransactionContext txManager = new NoopTransactionContext();
    private final PostgresDialectStatements statements = new PostgresDialectStatements();
    private final DataSource dataSource = mock(DataSource.class);
    private final Connection connection = spy(PostgresqlLocalInstance.getTestConnection());
    private SqlPolicyDefinitionStore sqlPolicyStore;

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

        sqlPolicyStore = new SqlPolicyDefinitionStore(dataSourceRegistry, DATASOURCE_NAME, txManager, typeManager, statements);

        var schema = Files.readString(Paths.get("./docs/schema.sql"));
        txManager.execute(() -> executeQuery(connection, schema));
    }

    @AfterEach
    void tearDown() throws SQLException {
        txManager.execute(() -> executeQuery(connection, "DROP TABLE " + statements.getPolicyTable() + " CASCADE"));
        doCallRealMethod().when(connection).close();
        connection.close();
    }

    @Test
    void find_queryByProperty_notExist() {
        var policy = createPolicyBuilder("test-policy")
                .assigner("test-assigner")
                .assignee("test-assignee")
                .build();

        var policyDef1 = PolicyDefinition.Builder.newInstance().id("test-policy").policy(policy).build();
        getPolicyDefinitionStore().save(policyDef1);

        // query by prohibition assignee
        assertThatThrownBy(() -> getPolicyDefinitionStore().findAll(createQuery("notexist=foobar")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("Translation failed for Model");
    }

    @Test
    void findAll_sorting_nonExistentProperty() {

        IntStream.range(0, 10).mapToObj(i -> createPolicy("test-policy")).forEach((d) -> getPolicyDefinitionStore().save(d));


        var query = QuerySpec.Builder.newInstance().sortField("notexist").sortOrder(SortOrder.DESC).build();


        assertThatThrownBy(() -> getPolicyDefinitionStore().findAll(query))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("Translation failed for Model");

    }

    @Override
    protected SqlPolicyDefinitionStore getPolicyDefinitionStore() {
        return sqlPolicyStore;
    }

    @Override
    protected boolean supportCollectionQuery() {
        return true;
    }

    @Override
    protected boolean supportCollectionIndexQuery() {
        return false;
    }

    @Override
    protected Boolean supportSortOrder() {
        return true;
    }

}
