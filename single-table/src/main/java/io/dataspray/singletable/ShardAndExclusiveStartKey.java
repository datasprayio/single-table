package io.dataspray.singletable;

import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import lombok.NonNull;
import lombok.Value;

import java.util.Optional;

@Value
public class ShardAndExclusiveStartKey {
    @NonNull
    int shard;
    @NonNull
    Optional<PrimaryKey> exclusiveStartKey;
}
