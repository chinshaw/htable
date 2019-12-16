package com.ch.htable;

import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.stream.Stream;

public class HBaseImportService {

    private static final Logger logger = LoggerFactory.getLogger(HBaseImportService.class);

    private final HEntityManager repository;

    public HBaseImportService(HEntityManager repository) {
        this.repository = repository;
    }

    /*
     * This skips the first entity non parallel and then attempts to use parallel stream to save entities.
     */
    public <V> void streamIn(Stream<V> input, Class<V> clazz) {
        final Stopwatch timer = Stopwatch.createStarted();
        repository.saveAll(() -> input.parallel()
                .onClose(() -> logger.info("HBase Import Time:  " + timer.stop()))
                .iterator(),
                clazz);

    }
}
