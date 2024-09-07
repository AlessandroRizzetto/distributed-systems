package it.ds1;

public class EpochSequencePair {
    public final int epoch;
    public final int seqNum;

    public EpochSequencePair(int epoch, int seqNum) {
        this.epoch = epoch;
        this.seqNum = seqNum;
    }

    @Override
    public String toString() {
        return epoch + ":" + seqNum;
    }

    @Override
    public int hashCode() {
        return epoch * 31 + seqNum;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null || getClass() != obj.getClass())
            return false;
        EpochSequencePair that = (EpochSequencePair) obj;
        return epoch == that.epoch && seqNum == that.seqNum;
    }
}
