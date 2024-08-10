package io.github.vcvitaly.producercommon;

public record TimestampedDto(String guid, long timestampMillis, String data) {
}
