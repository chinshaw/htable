package com.ch.htable;

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

public class HBaseUtil {

    private HBaseUtil(){}


    /**
     * This is used to construct a generic key or potentially a partial key for a find.
     *
     * This will concatenate each value with a colon.
     *
     * @param objects List of objects that support to string
     * @return String key concatenated with colons :
     */
    public static String key(Object ... objects) {
        return Arrays.stream(objects).map(Objects::toString)
                .collect(Collectors.joining(":"));
    }
}
