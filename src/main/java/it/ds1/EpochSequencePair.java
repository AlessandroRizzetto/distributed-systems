package it.ds1;
import java.io.Serializable;
import java.util.Objects;

// Class to represent an Epoch-Sequence pair
public class EpochSequencePair implements Serializable {
    private final int epoch;
    private final int sequenceNumber;

    // Constructor to initialize the epoch and sequence number
    public EpochSequencePair(int epoch, int sequenceNumber) {
        this.epoch = epoch;
        this.sequenceNumber = sequenceNumber;
    }

    // Getters for the epoch and sequence number
    public int getEpoch() {
        return epoch;
    }

    public int getSequenceNumber() {
        return sequenceNumber;
    }

    // Overriding equals method for comparing two EpochSequencePair objects
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EpochSequencePair that = (EpochSequencePair) o;
        return epoch == that.epoch && sequenceNumber == that.sequenceNumber;
    }

    // Overriding hashCode method for hashing EpochSequencePair objects
    @Override
    public int hashCode() {
        return Objects.hash(epoch, sequenceNumber);
    }

    // Overriding toString method for easy printing of EpochSequencePair objects
    @Override
    public String toString() {
        return "EpochSequencePair{" +
                "epoch=" + epoch +
                ", sequenceNumber=" + sequenceNumber +
                '}';
    }
}
