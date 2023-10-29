package io.dataspray.singletable;

import lombok.NonNull;
import lombok.Value;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.Map;
import java.util.Optional;

@Value
public class ShardAndExclusiveStartKey {
    @NonNull
    int shard;
    @NonNull
    Optional<Map<String, AttributeValue>> exclusiveStartKey;
}
