package org.eclipse.dataspaceconnector.ids.transform.util;

import org.eclipse.dataspaceconnector.ids.spi.IdsId;
import org.eclipse.dataspaceconnector.ids.spi.IdsIdParser;
import org.eclipse.dataspaceconnector.ids.spi.IdsType;

import java.net.URI;
import java.util.Objects;

public final class TransformationUtil {

    public static URI getConnectorIdAsUri(String connectorId) {
        return URI.create(String.join(
                IdsIdParser.DELIMITER,
                IdsIdParser.SCHEME,
                IdsType.CONNECTOR.getValue(),
                connectorId));
    }

    /**
     * Builds an ids id with a value and ids type as input.
     *
     * @param value The value.
     * @param type Ids type.
     * @return the built ids id.
     */
    public static IdsId buildIdsId(String value, IdsType type) {
        Objects.requireNonNull(value);
        Objects.requireNonNull(type);

        return IdsId.Builder.newInstance()
                .value(value)
                .type(type)
                .build();
    }
}
