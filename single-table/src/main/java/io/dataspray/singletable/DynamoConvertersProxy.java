// SPDX-FileCopyrightText: 2019-2022 Matus Faro <matus@smotana.com>
// SPDX-License-Identifier: Apache-2.0
package io.dataspray.singletable;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import lombok.Value;
import org.apache.commons.lang.ArrayUtils;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.utils.DateUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.*;
import java.util.stream.Stream;


class DynamoConvertersProxy {

    private static Converters convertersCache = null;

    @Value
    public static class Converters {
        /**
         * Marshaller for AttributeValue Scalar
         */
        public final ImmutableSet<Map.Entry<Class<?>, MarshallerAttrVal>> mp;
        /**
         * UnMarshaller for AttributeValue Scalar
         */
        public final ImmutableSet<Map.Entry<Class<?>, UnMarshallerAttrVal>> up;
        /**
         * Marshaller for AttributeValue Collection
         */
        public final ImmutableSet<Map.Entry<Class<?>, CollectionMarshallerAttrVal>> mc;
        /**
         * UnMarshaller for AttributeValue Collection
         */
        public final ImmutableSet<Map.Entry<Class<?>, CollectionUnMarshallerAttrVal>> uc;
        /**
         * Default instance
         */
        public final ImmutableSet<Map.Entry<Class<?>, DefaultInstanceGetter>> di;
    }

    public static Converters proxy() {
        if (convertersCache != null) {
            return convertersCache;
        }

        ImmutableMap.Builder<Class<?>, MarshallerAttrVal> mp = new ImmutableMap.Builder<>();
        ImmutableMap.Builder<Class<?>, UnMarshallerAttrVal> up = new ImmutableMap.Builder<>();
        ImmutableMap.Builder<Class<?>, CollectionMarshallerAttrVal> mc = new ImmutableMap.Builder<>();
        ImmutableMap.Builder<Class<?>, CollectionUnMarshallerAttrVal> uc = new ImmutableMap.Builder<>();
        ImmutableMap.Builder<Class<?>, DefaultInstanceGetter> di = new ImmutableMap.Builder<>();

        mp.put(Date.class, o -> AttributeValue.fromS(DateUtils.formatIso8601Date(((Date) o).toInstant())));
        di.put(Date.class, () -> new Date(0L));
        mp.put(Calendar.class, o -> AttributeValue.fromS(DateUtils.formatIso8601Date(((Calendar) o).getTime().toInstant())));
        di.put(Calendar.class, Calendar::getInstance);
        mp.put(Boolean.class, o -> AttributeValue.fromBool((Boolean) o));
        di.put(Boolean.class, () -> Boolean.FALSE);
        mp.put(boolean.class, o -> AttributeValue.fromBool((boolean) o));
        di.put(boolean.class, () -> false);
        mp.put(Number.class, o -> AttributeValue.fromN((o).toString()));
        di.put(Number.class, () -> 0L);
        mp.put(byte.class, o -> AttributeValue.fromN(Byte.toString((byte) o)));
        di.put(byte.class, () -> 0);
        mp.put(short.class, o -> AttributeValue.fromN((o).toString()));
        di.put(short.class, () -> 0);
        mp.put(int.class, o -> AttributeValue.fromN((o).toString()));
        di.put(int.class, () -> 0);
        mp.put(Integer.class, o -> AttributeValue.fromN((o).toString()));
        di.put(Integer.class, () -> 0);
        mp.put(long.class, o -> AttributeValue.fromN((o).toString()));
        di.put(long.class, () -> 0L);
        mp.put(float.class, o -> AttributeValue.fromN((o).toString()));
        di.put(float.class, () -> 0f);
        mp.put(double.class, o -> AttributeValue.fromN((o).toString()));
        di.put(double.class, () -> 0d);
        mp.put(BigDecimal.class, o -> AttributeValue.fromN(((BigDecimal) o).toPlainString()));
        di.put(BigDecimal.class, () -> BigDecimal.ZERO);
        mp.put(BigInteger.class, o -> AttributeValue.fromN(o.toString()));
        di.put(BigInteger.class, () -> BigInteger.ZERO);
        mp.put(String.class, o -> ((String) o).isEmpty() ? null : AttributeValue.fromS((String) o));
        di.put(String.class, () -> "");
        mp.put(UUID.class, o -> AttributeValue.fromS(o.toString()));
        di.put(UUID.class, () -> new UUID(0, 0));
        mp.put(ByteBuffer.class, o -> AttributeValue.fromB(SdkBytes.fromByteBuffer((ByteBuffer) o)));
        di.put(ByteBuffer.class, () -> ByteBuffer.allocate(0));
        mp.put(byte[].class, o -> AttributeValue.fromB(SdkBytes.fromByteArray((byte[]) o)));
        di.put(byte[].class, () -> new byte[]{});
        mp.put(Byte[].class, o -> AttributeValue.fromB(SdkBytes.fromByteArrayUnsafe(ArrayUtils.toPrimitive((Byte[]) o))));
        di.put(Byte[].class, () -> new Byte[]{});
        mp.put(Instant.class, o -> AttributeValue.fromS(((Instant) o).toString()));
        di.put(Instant.class, () -> Instant.EPOCH);

        up.put(double.class, a -> Double.valueOf(a.n()));
        up.put(Double.class, a -> Double.valueOf(a.n()));
        up.put(BigDecimal.class, a -> new BigDecimal(a.n()));
        up.put(BigInteger.class, a -> new BigInteger(a.n()));
        up.put(int.class, a -> Integer.valueOf(a.n()));
        up.put(Integer.class, a -> Integer.valueOf(a.n()));
        up.put(float.class, a -> Float.valueOf(a.n()));
        up.put(Float.class, a -> Float.valueOf(a.n()));
        up.put(byte.class, a -> Byte.valueOf(a.n()));
        up.put(Byte.class, a -> Byte.valueOf(a.n()));
        up.put(long.class, a -> Long.valueOf(a.n()));
        up.put(Long.class, a -> Long.valueOf(a.n()));
        up.put(short.class, a -> Short.valueOf(a.n()));
        up.put(Short.class, a -> Short.valueOf(a.n()));
        up.put(boolean.class, a -> a.bool() != null ? a.bool() : "1".equals(a.n()));
        up.put(Boolean.class, a -> a.bool() != null ? a.bool() : "1".equals(a.n()));
        up.put(Date.class, a -> Date.from(DateUtils.parseIso8601Date(a.s())));
        up.put(Calendar.class, a -> {
            Calendar cal = GregorianCalendar.getInstance();
            cal.setTime(Date.from(DateUtils.parseIso8601Date(a.s())));
            return cal;
        });
        up.put(ByteBuffer.class, a -> a.b().asByteBuffer());
        up.put(byte[].class, a -> a.b().asByteArray());
        up.put(Byte[].class, a -> ArrayUtils.toObject(a.b().asByteArray()));
        up.put(UUID.class, a -> UUID.fromString(a.s()));
        up.put(String.class, AttributeValue::s);
        up.put(Instant.class, a -> Instant.parse(a.s()));

        Stream.of(List.class, ImmutableList.class).forEach(listClazz -> {
            mc.put(listClazz, (o, m) -> o == null ? null : AttributeValue.fromL(((List<?>) o).stream()
                    .map(m::marshall)
                    .collect(ImmutableList.toImmutableList())));
            uc.put(listClazz, (a, u) -> a == null || Boolean.TRUE.equals(a.nul()) ? null : a.l().stream()
                    .map(u::unmarshall)
                    .collect(ImmutableList.toImmutableList()));
            di.put(listClazz, ImmutableList::of);
        });
        Stream.of(Map.class, ImmutableMap.class).forEach(mapClazz -> {
            mc.put(mapClazz, (o, m) -> o == null ? null : AttributeValue.fromM(((Map<?, ?>) o).entrySet().stream()
                    .collect(ImmutableMap.toImmutableMap(
                            e -> (String) e.getKey(),
                            e -> m.marshall(e.getValue())
                    ))));
            uc.put(mapClazz, (a, u) -> a == null || Boolean.TRUE.equals(a.nul()) || !a.hasM() ? null : a.m().entrySet().stream()
                    .collect(ImmutableMap.toImmutableMap(
                            Map.Entry::getKey,
                            e -> u.unmarshall(e.getValue())
                    )));
            di.put(mapClazz, ImmutableMap::of);
        });

        Stream.of(Set.class, ImmutableSet.class).forEach(setClazz -> {
            mc.put(setClazz, (o, m) -> {
                // Empty set not allowed by DynamoDB so:
                // empty set == null in DB
                if (o == null || ((Set<?>) o).isEmpty()) {
                    return null;
                }
                int[] setType = {0};
                ImmutableSet<?> set = ((Set<?>) o).stream()
                        .map(m::marshall)
                        .map(v -> {
                            if (!Strings.isNullOrEmpty(v.s())) {
                                setType[0] = 0;
                                return v.s();
                            } else if (!Strings.isNullOrEmpty(v.n())) {
                                setType[0] = 1;
                                return v.n();
                            } else if (v.b() != null) {
                                setType[0] = 2;
                                return v.b();
                            } else if (v.nul() != null) {
                                throw new IllegalStateException("Set cannot have null item");
                            } else {
                                throw new IllegalStateException("Set of unsupported type: " + v.toString());
                            }
                        })
                        .collect(ImmutableSet.toImmutableSet());
                if (setType[0] == 0) {
                    //noinspection unchecked
                    return AttributeValue.builder().ss((ImmutableSet<String>) set).build();
                } else if (setType[0] == 1) {
                    //noinspection unchecked
                    return AttributeValue.builder().ns((ImmutableSet<String>) set).build();
                } else {
                    //noinspection unchecked
                    return AttributeValue.builder().bs((ImmutableSet<SdkBytes>) set).build();
                }
            });
            uc.put(setClazz, (a, u) -> {
                // Empty set not allowed by DynamoDB, also null value prevents from adding to set, so:
                // Missing in DB == empty set
                if (a == null || Boolean.TRUE.equals(a.nul())) {
                    return ImmutableSet.of();
                } else if (a.hasSs()) {
                    return a.ss().stream().map(i -> u.unmarshall(AttributeValue.fromS(i))).collect(ImmutableSet.toImmutableSet());
                } else if (a.hasNs()) {
                    return a.ns().stream().map(i -> u.unmarshall(AttributeValue.fromN(i))).collect(ImmutableSet.toImmutableSet());
                } else if (a.hasBs()) {
                    return a.bs().stream().map(i -> u.unmarshall(AttributeValue.fromB(i))).collect(ImmutableSet.toImmutableSet());
                } else {
                    return ImmutableSet.of();
                }
            });
            di.put(setClazz, ImmutableSet::of);
        });

        convertersCache = new Converters(
                mp.build().entrySet(),
                up.build().entrySet(),
                mc.build().entrySet(),
                uc.build().entrySet(),
                di.build().entrySet());
        return convertersCache;
    }

    @FunctionalInterface
    public interface MarshallerAttrVal {
        AttributeValue marshall(Object object);
    }

    @FunctionalInterface
    public interface UnMarshallerAttrVal {
        Object unmarshall(AttributeValue attributeValue);
    }

    @FunctionalInterface
    public interface CollectionMarshallerAttrVal {
        AttributeValue marshall(Object object, MarshallerAttrVal marshaller);
    }

    @FunctionalInterface
    public interface CollectionUnMarshallerAttrVal {
        Object unmarshall(AttributeValue attributeValue, UnMarshallerAttrVal unMarshaller);
    }

    @FunctionalInterface
    public interface DefaultInstanceGetter {
        Object getDefaultInstance();
    }

}
