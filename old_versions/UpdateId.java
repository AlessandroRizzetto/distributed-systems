package it.ds1;

import java.util.Objects;

public class UpdateId {
    private final int epoch;
    private final int sequenceNumber;

    public UpdateId(int epoch, int sequenceNumber) {
        this.epoch = epoch;
        this.sequenceNumber = sequenceNumber;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (other == null || getClass() != other.getClass()) return false;
        UpdateId updateId = (UpdateId) other;
        return epoch == updateId.epoch && sequenceNumber == updateId.sequenceNumber;
    }

    @Override
    public int hashCode() {
        return Objects.hash(epoch, sequenceNumber);
    }
}
