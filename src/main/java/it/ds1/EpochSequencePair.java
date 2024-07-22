package it.ds1;

import java.util.Objects;

public class EpochSequencePair {
    private final int epoch;
    private final int sequenceNumber;

    public EpochSequencePair(int epoch, int sequenceNumber) {
        this.epoch = epoch;
        this.sequenceNumber = sequenceNumber;
    }

    public int getEpoch() {
        return epoch;
    }

    public int getSequenceNumber() {
        return sequenceNumber;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EpochSequencePair that = (EpochSequencePair) o;
        return epoch == that.epoch && sequenceNumber == that.sequenceNumber;
    }

    @Override
    public int hashCode() {
        return Objects.hash(epoch, sequenceNumber);
    }

    @Override
    public String toString() {
        return "<" + epoch + ", " + sequenceNumber + ">";
    }
}
