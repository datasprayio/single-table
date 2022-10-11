package io.dataspray.singletable;

import com.google.common.collect.ImmutableList;
import lombok.NonNull;
import lombok.Value;

import java.util.Optional;

@Value
public class ShardPageResult<T> {

    @NonNull
    ImmutableList<T> items;

    @NonNull
    Optional<String> cursorOpt;
}
